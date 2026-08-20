package ops

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// Realtime Systems Lab 的 HTTP 面。全部遵循现有 /api 风格：
// GET 只读，POST 是 ACTION（带 409/422/503 明确错误）；handler 内不产生
// goroutine，全部同步、快速、受 r.Context() 约束；不触碰消息主链。

func (a *API) registerExperimentRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/experiments", a.handleExperiments)     // GET list / POST create
	mux.HandleFunc("/api/experiments/", a.handleExperimentItem) // {id} 详情 / start / stop / report
	mux.HandleFunc("/api/compare", a.handleCompare)
	mux.HandleFunc("/api/evidence", a.handleEvidence)
	mux.HandleFunc("/api/presets", a.handlePresets)
}

func (a *API) handleExperiments(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		limit := 50
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 200 {
				limit = n
			}
		}
		all, err := a.em.List()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if len(all) > limit {
			all = all[:limit]
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"experiments": all,
			"active_id":   nullOrID(a.em.ActiveID()),
		})

	case http.MethodPost:
		var req CreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
			return
		}
		exp, err := a.em.Create(req)
		if err != nil {
			code := http.StatusBadRequest
			if strings.Contains(err.Error(), "unknown preset") {
				code = http.StatusUnprocessableEntity
			}
			writeJSON(w, code, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"experiment": exp})

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

// handleExperimentItem 解析 /api/experiments/{id}[/{action}]
func (a *API) handleExperimentItem(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/experiments/")
	parts := strings.SplitN(rest, "/", 2)
	id := parts[0]
	if err := ValidateExperimentID(id); err != nil {
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
		exp, err := a.em.Get(id)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		resp := map[string]any{"experiment": exp}
		if a.em.Running(id) {
			resp["live"] = a.em.Live()
		}
		writeJSON(w, http.StatusOK, resp)

	case "start":
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
			return
		}
		if err := a.em.Start(id); err != nil {
			code := http.StatusConflict
			if strings.Contains(err.Error(), "already running") {
				code = http.StatusConflict
			}
			if strings.Contains(err.Error(), "not found") {
				code = http.StatusNotFound
			}
			if strings.Contains(err.Error(), "not running") {
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
		if err := a.em.Stop(id); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"stopped": true, "id": id})

	case "report":
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET only"})
			return
		}
		rep, err := a.em.Report(id)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, rep)

	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown action " + action})
	}
}

// handleCompare GET /api/compare?left=<id>&right=<id>
func (a *API) handleCompare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET only"})
		return
	}
	left := r.URL.Query().Get("left")
	right := r.URL.Query().Get("right")
	if left == "" || right == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "both left and right experiment ids are required"})
		return
	}
	out, err := a.em.Compare(left, right)
	if err != nil {
		code := http.StatusNotFound
		if strings.Contains(err.Error(), "requires completed") {
			code = http.StatusUnprocessableEntity
		}
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// handleEvidence GET /api/evidence
func (a *API) handleEvidence(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET only"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"claims":   a.em.Evidence(),
		"statuses": ClaimStatuses,
	})
}

// handlePresets GET /api/presets
func (a *API) handlePresets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET only"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"presets": a.em.Presets()})
}

// nullOrID 把空 id 序列化为 null（而不是空串）。
func nullOrID(id string) any {
	if id == "" {
		return nil
	}
	return id
}
