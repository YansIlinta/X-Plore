package ops

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// Sweep 的 HTTP 面。遵循现有 /api 风格：GET 只读，POST 是 ACTION。

func (a *API) registerSweepRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/sweeps", a.handleSweeps)
	mux.HandleFunc("/api/sweeps/", a.handleSweepItem)
	mux.HandleFunc("/api/regimes", a.handleRegimes)
	mux.HandleFunc("/api/regime-analysis", a.handleRegimeAnalysis)
}

func (a *API) handleSweeps(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		limit := 50
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 200 {
				limit = n
			}
		}
		all, err := a.sweeps().List()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if len(all) > limit {
			all = all[:limit]
		}
		writeJSON(w, http.StatusOK, map[string]any{"sweeps": all, "active_id": nullOrID(a.sweeps().ActiveID())})

	case http.MethodPost:
		var req SweepRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
			return
		}
		sw, err := a.sweeps().Create(req)
		if err != nil {
			code := http.StatusBadRequest
			if strings.Contains(err.Error(), "too large") || strings.Contains(err.Error(), "unknown regime") || strings.Contains(err.Error(), "unknown sweep parameter") {
				code = http.StatusUnprocessableEntity
			}
			writeJSON(w, code, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"sweep": sw})

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (a *API) handleSweepItem(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/sweeps/")
	parts := strings.SplitN(rest, "/", 2)
	id := parts[0]
	if err := ValidateSweepID(id); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}
	switch action {
	case "":
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		sw, err := a.sweeps().Get(id)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"sweep": sw, "running": a.sweeps().Running(id)})

	case "start":
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
			return
		}
		if err := a.sweeps().Start(id); err != nil {
			code := http.StatusConflict
			if strings.Contains(err.Error(), "not found") {
				code = http.StatusNotFound
			}
			if strings.Contains(err.Error(), "already completed") {
				code = http.StatusConflict
			}
			writeJSON(w, code, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"started": true, "id": id})

	case "stop":
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
			return
		}
		if err := a.sweeps().Stop(id); err != nil {
			code := http.StatusConflict
			if strings.Contains(err.Error(), "not running") {
				code = http.StatusConflict
			}
			writeJSON(w, code, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"stopped": true, "id": id})

	case "report":
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET only"})
			return
		}
		out, err := a.sweeps().Report(id)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, out)

	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown action " + action})
	}
}

// handleRegimes GET /api/regimes：返回 workload regime 模板。
func (a *API) handleRegimes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET only"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"regimes":   a.em.RegimeInfos(regimeDefaultTarget(r)),
		"objective": SensibleRankObjective(),
	})
}

// handleRegimeAnalysis GET /api/regime-analysis?left=<...>&right=<...>：
// 基于已完成实验（跨 regime × config）的确定性 cross-regime 视图。
// 也支持 ?experiments=e1,e2,... 显式指定参与实验。
func (a *API) handleRegimeAnalysis(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET only"})
		return
	}
	// 收集参与分析的实验（带上它们的 regime/config 元数据）。
	all, err := a.em.List()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	var rows []SweepConfigResult
	for _, e := range all {
		if e.Status != ExpStatusCompleted && e.Status != ExpStatusPartial {
			continue
		}
		if e.Regime == "" || e.SpecHash == "" {
			continue // 老实验无 regime/spec → 不参与
		}
		rows = append(rows, sweepResultFromExperiment(e))
	}
	objective := parseObjective(r)
	tunable := false
	report := BuildCrossRegimeReport(rows, objective, uniqueRegimes(rows), tunable)
	writeJSON(w, http.StatusOK, map[string]any{
		"objective": objective,
		"report":    report,
	})
}

// sweepResultFromExperiment 从已完成实验生成 SweepConfigResult（跨 regime 分析用）。
func sweepResultFromExperiment(e *Experiment) SweepConfigResult {
	r := SweepConfigResult{
		Regime: e.Regime,
		Config: configLabelOf(e),
		ExpID:  e.ID,
		Status: e.Status,
	}
	if e.Aggregate != nil {
		r.Repetitions = e.Aggregate.TotalRepetitions
		r.SuccessReps = e.Aggregate.SuccessfulRepetitions
		r.Throughput = e.Aggregate.Metrics["receive_rate"]
		r.P99 = e.Aggregate.Metrics["p99_latency_us"]
		r.P90 = e.Aggregate.Metrics["p90_latency_us"]
		r.DeliveryRate = e.Aggregate.Metrics["delivery_rate"]
		r.CPU = e.Aggregate.Metrics["cpu_pct"]
	}
	return r
}

// configLabelOf 返回实验的配置标签（优先 ConfigLabel，否则系统配置 label）。
func configLabelOf(e *Experiment) string {
	if e.ConfigLabel != "" {
		return e.ConfigLabel
	}
	if !e.SystemConfig.Empty() {
		return e.SystemConfig.Label()
	}
	return "default"
}

func uniqueRegimes(rows []SweepConfigResult) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range rows {
		if !seen[r.Regime] {
			seen[r.Regime] = true
			out = append(out, r.Regime)
		}
	}
	return out
}

func parseObjective(r *http.Request) RankingObjective {
	o := SensibleRankObjective()
	q := r.URL.Query()
	switch q.Get("primary") {
	case "p99", "delivery_rate", "throughput":
		o.Primary = q.Get("primary")
	}
	if q.Get("maximize") == "true" {
		o.Maximize = true
	}
	if q.Get("maximize") == "false" {
		o.Maximize = false
	}
	if v := q.Get("p99_max_us"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			o.P99MaxUS = f
		}
	}
	if v := q.Get("delivery_min"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			o.DeliveryMin = f
		}
	}
	if v := q.Get("cpu_max"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			o.CPUMax = f
		}
	}
	return o
}

func regimeDefaultTarget(r *http.Request) string {
	// 从 query target 或请求头推断（环境相关）；无则默认。
	if t := r.URL.Query().Get("target"); t != "" {
		return t
	}
	return "ws://localhost:8081"
}

// sweeps 返回 sweep 管理器（可能 nil）。
func (a *API) sweeps() *SweepManager {
	if a == nil || a.sweepMgr == nil {
		return nil
	}
	return a.sweepMgr
}
