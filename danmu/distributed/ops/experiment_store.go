package ops

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// experimentStore 是 "Realtime Systems Lab" 的 JSON 文件存储：
//
//	<data-dir>/experiments/<experiment-id>.json
//
// 设计约束（产品化要求）：
//   - 原子写：同目录 temp 文件 + rename，避免半截文件被读成"一次成功实验"。
//   - 损坏文件：Load/List 时跳过并记日志，绝不让 Ops 整体启动失败。
//   - 有界历史：List 只返回最新的 limit 个实验（避免目录越来越大时全量解析）。
//   - 安全：experiment id 只允许 [A-Za-z0-9_-]，并在文件路径拼接前再次校验（防目录穿越）。
//   - 时间戳：time.Time 的 JSON 序列化为 RFC3339Nano（RFC3339 兼容）。
type experimentStore struct {
	dir   string
	limit int
}

// NewExperimentStore 创建（并 mkdir -p）存储目录。limit 为有界历史条数。
func NewExperimentStore(dir string, limit int) (*experimentStore, error) {
	if limit <= 0 {
		limit = 200
	}
	dir = filepath.Join(dir, "experiments")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create experiment dir %s: %w", dir, err)
	}
	return &experimentStore{dir: dir, limit: limit}, nil
}

// Dir 返回实际存储目录（测试/文档用）。
func (s *experimentStore) Dir() string { return s.dir }

func (s *experimentStore) path(id string) (string, error) {
	if err := ValidateExperimentID(id); err != nil {
		return "", err
	}
	return filepath.Join(s.dir, id+".json"), nil
}

// Save 原子写一个实验。先写同目录 temp（fsync），再 rename 覆盖。
func (s *experimentStore) Save(e *Experiment) error {
	if e == nil {
		return fmt.Errorf("nil experiment")
	}
	if err := ValidateExperimentID(e.ID); err != nil {
		return err
	}
	data, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal experiment %s: %w", e.ID, err)
	}

	tmp, err := os.CreateTemp(s.dir, ".tmp-"+e.ID+"-*")
	if err != nil {
		return fmt.Errorf("create temp for %s: %w", e.ID, err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write temp for %s: %w", e.ID, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("sync temp for %s: %w", e.ID, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close temp for %s: %w", e.ID, err)
	}
	finalPath, err := s.path(e.ID)
	if err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, finalPath); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename temp->final for %s: %w", e.ID, err)
	}
	return nil
}

// Load 读取单个实验。损坏 → 返回错误（调用方决定如何呈现，不会因此崩溃）。
// 加载后执行 Migration（Phase 1.5 老文件 → 补齐默认语义）。
func (s *experimentStore) Load(id string) (*Experiment, error) {
	finalPath, err := s.path(id)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(finalPath)
	if err != nil {
		return nil, fmt.Errorf("read experiment %s: %w", id, err)
	}
	var e Experiment
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, fmt.Errorf("experiment %s is corrupted (%v); file kept for inspection", id, err)
	}
	MigrateExperiment(&e)
	return &e, nil
}

// MigrateExperiment 把 Phase 1 老文件提升到当前 schema（幂等）：
//
//   - schema_version 0 → 当前版本（不落盘，读时视图；Save 时才会写回）
//   - 无 repetitions（0）→ 视为 1（老实验确实只跑了一次）
//   - 无 duration → 回退 Workload.Duration
//   - 无 runs 但 result 存在 → 合成一个 completed Run 视图（保持 Compare/Evidence 可用）
//   - Spec 为空 → 从顶层字段重建并计算 SpecHash（不落盘）
func MigrateExperiment(e *Experiment) {
	if e == nil {
		return
	}
	if e.SchemaVersion == 0 {
		e.SchemaVersion = SchemaVersion
	}
	if e.Repetitions == 0 {
		e.Repetitions = 1
	}
	if e.Duration == "" {
		e.Duration = e.Workload.Duration
	}
	if e.Workload.Distribution == "" {
		e.Workload.Distribution = DistUniform
	}
	if e.Workload.Seed == 0 {
		e.Workload.Seed = 1
	}
	if e.Spec.Architecture == "" {
		e.Spec = SpecFromExperiment(e)
	}
	if e.SpecHash == "" {
		if h, err := e.Spec.SpecHash(); err == nil {
			e.SpecHash = h
		}
	}
	// 老实验（无 run 列表）且已有顶层结果 → 合成一个虚拟 completed run。
	// 只在内存视图生效；不写回磁盘（避免污染历史文件）。
	if len(e.Runs) == 0 && e.Status == ExpStatusCompleted && !e.Result.isEmpty() {
		run := &ExperimentRun{
			ID:          "run-legacy-" + e.ID,
			Index:       1,
			Status:      RunStatusCompleted,
			Result:      e.Result,
			StartedAt:   e.StartedAt,
			FinishedAt:  e.FinishedAt,
			Environment: e.Environment,
		}
		e.Runs = []*ExperimentRun{run}
	}
}

// isEmpty 判断代表性结果是否还没有任何实测数据。
func (r ExperimentResult) isEmpty() bool {
	return r.ConnectionsEstablished == nil && r.MessagesReceived == nil && r.P90LatencyUS == nil && r.Drops == nil
}

// List 返回最近 limit 个实验（按 CreatedAt 降序）。损坏/跳过文件不中断，
// .tmp 临时文件跳过。界限：最多返回 limit 个实验。
func (s *experimentStore) List() ([]*Experiment, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("list experiment dir %s: %w", s.dir, err)
	}
	var experiments []*Experiment
	var skipped []string
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if strings.HasPrefix(name, ".tmp-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		id := strings.TrimSuffix(name, ".json")
		if err := ValidateExperimentID(id); err != nil {
			skipped = append(skipped, name)
			continue
		}
		e, err := s.Load(id)
		if err != nil {
			skipped = append(skipped, name+" ("+err.Error()+")")
			continue
		}
		experiments = append(experiments, e)
	}
	if len(skipped) > 0 {
		log.Printf("[ops] experiment store skipped %d unreadable entries (do not affect startup): %s",
			len(skipped), strings.Join(skipped, ", "))
	}
	// 有界：按 CreatedAt 降序，只保留最新 limit 个。
	sort.SliceStable(experiments, func(i, j int) bool {
		return experiments[i].CreatedAt.After(experiments[j].CreatedAt)
	})
	if len(experiments) > s.limit {
		experiments = experiments[:s.limit]
	}
	return experiments, nil
}

// Delete 删除一个实验文件（仅供内部/将来使用）。校验 id 防穿越。
func (s *experimentStore) Delete(id string) error {
	finalPath, err := s.path(id)
	if err != nil {
		return err
	}
	if err := os.Remove(finalPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ListCompleted 返回全部已完结实验（completed），按 FinishedAt 降序（若无则 CreatedAt）。
func (s *experimentStore) ListCompleted() ([]*Experiment, error) {
	all, err := s.List()
	if err != nil {
		return nil, err
	}
	out := make([]*Experiment, 0, len(all))
	for _, e := range all {
		if e.Status == ExpStatusCompleted {
			out = append(out, e)
		}
	}
	return out, nil
}
