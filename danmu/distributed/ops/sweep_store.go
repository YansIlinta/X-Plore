package ops

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// sweepStore 是 sweep 的 JSON 文件存储：
//
//	<data-dir>/sweeps/<sweep-id>.json
//
// 与 experimentStore 相同设计约束：原子写、损坏容忍、有界、id 校验防穿越。
type sweepStore struct {
	dir   string
	limit int
}

// NewSweepStore 创建（并 mkdir -p）sweep 存储目录。limit 为有界历史条数。
func NewSweepStore(dir string, limit int) (*sweepStore, error) {
	if limit <= 0 {
		limit = 100
	}
	dir = filepath.Join(dir, "sweeps")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create sweep dir %s: %w", dir, err)
	}
	return &sweepStore{dir: dir, limit: limit}, nil
}

// Dir 返回实际存储目录。
func (s *sweepStore) Dir() string { return s.dir }

func (s *sweepStore) path(id string) (string, error) {
	if err := ValidateSweepID(id); err != nil {
		return "", err
	}
	return filepath.Join(s.dir, id+".json"), nil
}

// Save 原子写一个 sweep。
func (s *sweepStore) Save(sweep *Sweep) error {
	if sweep == nil {
		return fmt.Errorf("nil sweep")
	}
	if err := ValidateSweepID(sweep.ID); err != nil {
		return err
	}
	data, err := json.MarshalIndent(sweep, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal sweep %s: %w", sweep.ID, err)
	}
	tmp, err := os.CreateTemp(s.dir, ".tmp-"+sweep.ID+"-*")
	if err != nil {
		return fmt.Errorf("create temp for %s: %w", sweep.ID, err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	finalPath, err := s.path(sweep.ID)
	if err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, finalPath); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename temp->final for %s: %w", sweep.ID, err)
	}
	return nil
}

// Load 读取单个 sweep。损坏 → 返回错误。
func (s *sweepStore) Load(id string) (*Sweep, error) {
	finalPath, err := s.path(id)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(finalPath)
	if err != nil {
		return nil, fmt.Errorf("read sweep %s: %w", id, err)
	}
	var sw Sweep
	if err := json.Unmarshal(data, &sw); err != nil {
		return nil, fmt.Errorf("sweep %s is corrupted (%v); file kept for inspection", id, err)
	}
	return &sw, nil
}

// List 返回最近 limit 个 sweep（按 CreatedAt 降序），损坏跳过。
func (s *sweepStore) List() ([]*Sweep, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("list sweep dir %s: %w", s.dir, err)
	}
	var out []*Sweep
	var skipped []string
	for _, ent := range entries {
		if ent.IsDir() || strings.HasPrefix(ent.Name(), ".tmp-") || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(ent.Name(), ".json")
		sw, err := s.Load(id)
		if err != nil {
			skipped = append(skipped, id+" ("+err.Error()+")")
			continue
		}
		out = append(out, sw)
	}
	if len(skipped) > 0 {
		log.Printf("[ops] sweep store skipped %d unreadable entries: %s", len(skipped), strings.Join(skipped, ", "))
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if len(out) > s.limit {
		out = out[:s.limit]
	}
	return out, nil
}

// Delete 删除一个 sweep 文件。
func (s *sweepStore) Delete(id string) error {
	finalPath, err := s.path(id)
	if err != nil {
		return err
	}
	if err := os.Remove(finalPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ValidateSweepID 拒绝任何目录穿越 / 非法文件名。
func ValidateSweepID(id string) error {
	if id == "" {
		return fmt.Errorf("sweep id is required")
	}
	if len(id) > 64 {
		return fmt.Errorf("sweep id too long")
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '-' && r != '_' {
			return fmt.Errorf("sweep id %q contains invalid characters", id)
		}
	}
	if id == "." || id == ".." {
		return fmt.Errorf("sweep id %q not allowed", id)
	}
	return nil
}

// sweepFinishedAt 记录 sweep 结束时间戳（宽松 helper）。
func (s *Sweep) touchFinished() {
	now := time.Now().UTC()
	s.FinishedAt = &now
}

// pendingUnits 返回尚未完成的执行单元。
func (s *Sweep) pendingUnits() []*SweepUnit {
	var out []*SweepUnit
	for i := range s.Plan {
		if !s.Plan[i].Done {
			out = append(out, &s.Plan[i])
		}
	}
	return out
}

// completedUnits 返回已完结单元数。
func (s *Sweep) completedUnits() int {
	n := 0
	for i := range s.Plan {
		if s.Plan[i].Done {
			n++
		}
	}
	return n
}
