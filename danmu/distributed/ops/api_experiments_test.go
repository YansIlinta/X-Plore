package ops

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testAPI 构造带 fake runner 的完整 API 面。
func testAPI(t *testing.T, runSec float64) (*API, *ExperimentManager, func()) {
	t.Helper()
	dir := t.TempDir()
	store, err := NewExperimentStore(filepath.Join(dir, "data"), 200)
	if err != nil {
		t.Fatal(err)
	}
	bin := writeFakeLoadtest(t, dir, "loadtest", standardReportJSON(), 0, runSec, nil)
	runner := NewLoadtestManager(bin, "tok", context.Background())
	m := NewExperimentManager(store, runner, dir, nil)
	return NewAPI(nil, m), m, func() {}
}

func doJSON(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rd)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestAPICreateAndList(t *testing.T) {
	api, _, done := testAPI(t, 0.01)
	defer done()
	h := api.Handler()

	code, out := doJSON(t, h, http.MethodPost, "/api/experiments", map[string]any{
		"name": "run-1", "preset": "low-fanout", "architecture": "monolith",
	})
	if code != http.StatusOK || out["experiment"] == nil {
		t.Fatalf("create: %d %v", code, out)
	}
	var created struct {
		ID           string `json:"id"`
		Status       string `json:"status"`
		Architecture string `json:"architecture"`
	}
	b, _ := json.Marshal(out["experiment"])
	_ = json.Unmarshal(b, &created)
	if created.Status != ExpStatusCreated || created.Architecture != ArchMonolith {
		t.Fatalf("created: %+v", created)
	}
	id := created.ID

	code, out = doJSON(t, h, http.MethodGet, "/api/experiments", nil)
	if code != http.StatusOK {
		t.Fatalf("list: %d", code)
	}
	arr, _ := out["experiments"].([]any)
	if len(arr) != 1 {
		t.Fatalf("list len=%d", len(arr))
	}
	// 详情
	code, out = doJSON(t, h, http.MethodGet, "/api/experiments/"+id, nil)
	if code != http.StatusOK || out["experiment"] == nil {
		t.Fatalf("detail: %d %v", code, out)
	}
	// 404
	code, _ = doJSON(t, h, http.MethodGet, "/api/experiments/exp-nope", nil)
	if code != http.StatusNotFound {
		t.Fatalf("detail unknown: %d", code)
	}
}

func TestAPICreateValidation(t *testing.T) {
	api, _, done := testAPI(t, 0.01)
	defer done()
	h := api.Handler()

	code, out := doJSON(t, h, http.MethodPost, "/api/experiments", map[string]any{
		"architecture": "k8s", "preset": "custom",
		"workload": map[string]any{"connections": 10, "rooms": 2, "message_rate": 1, "duration": "10s", "target": "http://bad"},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("bad arch expected 400, got %d %v", code, out)
	}
	code, _ = doJSON(t, h, http.MethodPost, "/api/experiments", map[string]any{"preset": "wat"})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("unknown preset expected 422, got %d", code)
	}
}

func TestAPIStartLiveReport(t *testing.T) {
	api, m, done := testAPI(t, 0.2)
	defer done()
	h := api.Handler()

	code, out := doJSON(t, h, http.MethodPost, "/api/experiments", map[string]any{
		"preset":   "custom",
		"workload": map[string]any{"connections": 100, "rooms": 10, "message_rate": 1, "duration": "5s", "target": "ws://h"},
	})
	b, _ := json.Marshal(out["experiment"])
	var e struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(b, &e)
	id := e.ID

	code, out = doJSON(t, h, http.MethodPost, "/api/experiments/"+id+"/start", nil)
	if code != http.StatusOK || out["started"] != true {
		t.Fatalf("start: %d %v", code, out)
	}
	// 正在运行：detail 带 live
	code, out = doJSON(t, h, http.MethodGet, "/api/experiments/"+id, nil)
	if code != http.StatusOK || out["live"] == nil {
		t.Fatalf("running detail should include live: %d %v", code, out)
	}
	// 双重 start → 409
	code, out = doJSON(t, h, http.MethodPost, "/api/experiments/"+id+"/start", nil)
	if code != http.StatusConflict {
		t.Fatalf("double start expected 409, got %d %v", code, out)
	}

	// 等完成，report 应含结果与 claims 关联
	waitStatus(t, m, id, ExpStatusCompleted, 5*time.Second)
	code, out = doJSON(t, h, http.MethodGet, "/api/experiments/"+id+"/report", nil)
	if code != http.StatusOK {
		t.Fatalf("report: %d", code)
	}
	if out["claims"] == nil {
		t.Fatalf("report should include linked claims")
	}
}

func TestAPICompareAndEvidence(t *testing.T) {
	api, m, done := testAPI(t, 0.02)
	defer done()
	h := api.Handler()

	mk := func(name string, conns int64) string {
		code, out := doJSON(t, h, http.MethodPost, "/api/experiments", map[string]any{
			"name": name, "preset": "custom",
			"workload": map[string]any{"connections": conns, "rooms": 10, "message_rate": 0, "duration": "5s", "target": "ws://h"},
		})
		if code != http.StatusOK {
			t.Fatalf("create %s: %d", name, code)
		}
		b, _ := json.Marshal(out["experiment"])
		var e struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(b, &e)
		if err := m.Start(e.ID); err != nil {
			t.Fatal(err)
		}
		waitStatus(t, m, e.ID, ExpStatusCompleted, 5*time.Second)
		return e.ID
	}
	a := mk("A", 100)
	b := mk("B", 100)

	// compare 缺参 → 400
	code, _ := doJSON(t, h, http.MethodGet, "/api/compare", nil)
	if code != http.StatusBadRequest {
		t.Fatalf("compare missing params: %d", code)
	}
	// compare 正常
	code, out := doJSON(t, h, http.MethodGet, "/api/compare?left="+a+"&right="+b, nil)
	if code != http.StatusOK || out["rows"] == nil || out["summary"] == nil {
		t.Fatalf("compare: %d %v", code, out)
	}
	rows, _ := json.Marshal(out["rows"])
	// 行的形状：每行至少含 metric + left（可为 null 表达 N/A）
	if !strings.Contains(string(rows), `"metric"`) || !strings.Contains(string(rows), `"left"`) {
		t.Fatalf("compare rows shape unexpected: %s", rows)
	}
	// compare 未知 id → 404
	code, _ = doJSON(t, h, http.MethodGet, "/api/compare?left="+a+"&right=exp-nope", nil)
	if code != http.StatusNotFound {
		t.Fatalf("compare unknown: %d", code)
	}

	// evidence
	code, out = doJSON(t, h, http.MethodGet, "/api/evidence", nil)
	if code != http.StatusOK || out["claims"] == nil {
		t.Fatalf("evidence: %d", code)
	}

	// presets
	code, out = doJSON(t, h, http.MethodGet, "/api/presets", nil)
	if code != http.StatusOK || out["presets"] == nil {
		t.Fatalf("presets: %d", code)
	}
}

func TestAPILegacyLoadtestCompat(t *testing.T) {
	api, m, done := testAPI(t, 0.05)
	defer done()
	h := api.Handler()

	code, out := doJSON(t, h, http.MethodPost, "/api/loadtest/start", map[string]any{
		"server": "ws://h", "conns": 100, "rooms": 10, "rate": 1, "duration": "5s",
	})
	if code != http.StatusOK || out["started"] != true {
		t.Fatalf("legacy start: %d %v", code, out)
	}
	code, out = doJSON(t, h, http.MethodGet, "/api/loadtest/status", nil)
	if code != http.StatusOK {
		t.Fatalf("legacy status: %d", code)
	}
	if out["active_experiment"] == nil {
		t.Fatalf("legacy status should expose active experiment: %v", out)
	}
	active := out["active_experiment"].(map[string]any)
	activeID := active["id"].(string)
	// 等完成
	for {
		code, out = doJSON(t, h, http.MethodGet, "/api/loadtest/status", nil)
		_ = code
		if out["running"] == false {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	waitStatus(t, m, activeID, ExpStatusCompleted, 5*time.Second)
	// 停止一个未运行 → 也幂等成功（兼容旧语义）
	code, out = doJSON(t, h, http.MethodPost, "/api/loadtest/stop", nil)
	if code != http.StatusOK || out["stopped"] != true {
		t.Fatalf("legacy stop: %d %v", code, out)
	}
	_ = m
}

func TestAPIStartConflictMessage(t *testing.T) {
	api, m, done := testAPI(t, 0.3)
	defer done()
	h := api.Handler()
	code, out := doJSON(t, h, http.MethodPost, "/api/experiments", map[string]any{
		"preset":   "custom",
		"workload": map[string]any{"connections": 10, "rooms": 2, "message_rate": 1, "duration": "5s", "target": "ws://h"},
	})
	b, _ := json.Marshal(out["experiment"])
	var e struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(b, &e)
	doJSON(t, h, http.MethodPost, "/api/experiments/"+e.ID+"/start", nil)
	code, out = doJSON(t, h, http.MethodPost, "/api/experiments/"+e.ID+"/start", nil)
	if code != http.StatusConflict || !strings.Contains(out["error"].(string), "already running") {
		t.Fatalf("conflict message: %d %v", code, out)
	}
	// 等后台 run 收尾，避免测试退出后临时目录被清理
	waitStatus(t, m, e.ID, ExpStatusCompleted, 5*time.Second)
}
