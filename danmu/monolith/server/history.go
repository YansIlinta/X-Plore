package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// HistoryDB 是 server 侧对 ClickHouse 的只读封装，实现 HistoryQuerier。
// 落库由独立的 consumer 进程负责；server 仅在配置了 -clickhouse-addr 时
// 建立只读连接，让 /api/v1/history 能查到 consumer 已写入的历史弹幕。
// 未配置时 api.historyDB 保持 nil，history 接口按既有逻辑返回空。
type HistoryDB struct {
	db *sql.DB
}

func NewHistoryDB(addr, database, username, password string) (*HistoryDB, error) {
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
		Settings: clickhouse.Settings{
			"max_execution_time": 30,
		},
	})
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &HistoryDB{db: db}, nil
}

// Query 按房间倒序分页查询历史弹幕，与 consumer/db.go 的表结构一致
func (h *HistoryDB) Query(roomID string, page, limit int) ([]HistoryItem, int, error) {
	var total int
	if err := h.db.QueryRow("SELECT count() FROM danmu_history WHERE room_id = ?", roomID).Scan(&total); err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * limit
	rows, err := h.db.Query(
		"SELECT uid, content, server_ts FROM danmu_history WHERE room_id = ? ORDER BY server_ts DESC LIMIT ? OFFSET ?",
		roomID, limit, offset,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := []HistoryItem{}
	for rows.Next() {
		var it HistoryItem
		if err := rows.Scan(&it.UID, &it.Content, &it.TimeMS); err != nil {
			return nil, 0, err
		}
		items = append(items, it)
	}
	return items, total, rows.Err()
}

// QueryPeaks 查询房间弹幕密度序列并挑高能点。
// bucket: "sec" | "min"；from/to 为 unix 毫秒（0=不限）。
// 优先读物化视图 danmu_count_per_sec；若视图不存在则回退到直接 GROUP BY。
func (h *HistoryDB) QueryPeaks(roomID string, fromMS, toMS int64, bucket string, topK int) ([]PeakPoint, []PeakPoint, error) {
	// 密度序列
	series, err := h.queryDensity(roomID, fromMS, toMS, bucket)
	if err != nil {
		return nil, nil, err
	}
	peaks := FindPeaks(series, topK, 0.9)
	return series, peaks, nil
}

func (h *HistoryDB) queryDensity(roomID string, fromMS, toMS int64, bucket string) ([]PeakPoint, error) {
	// 桶表达式：秒或分钟
	bucketExpr := "toStartOfSecond(fromUnixTimestamp64Milli(server_ts))"
	if bucket == "min" {
		bucketExpr = "toStartOfMinute(fromUnixTimestamp64Milli(server_ts))"
	}
	// 时间过滤
	where := "room_id = ?"
	args := []interface{}{roomID}
	if fromMS > 0 {
		where += " AND server_ts >= ?"
		args = append(args, fromMS)
	}
	if toMS > 0 {
		where += " AND server_ts < ?"
		args = append(args, toMS)
	}

	// 优先尝试物化视图（秒桶）；分钟桶或视图不存在时回退直接聚合
	query := fmt.Sprintf(
		"SELECT toUnixTimestamp(%s) AS ts, count() AS cnt FROM danmu_history WHERE %s GROUP BY ts ORDER BY ts",
		bucketExpr, where,
	)
	if bucket == "sec" {
		// 尝试 MV：danmu_count_per_sec(room_id, ts_bucket DateTime, cnt)
		mvQuery := "SELECT toUnixTimestamp(ts_bucket) AS ts, sum(cnt) AS cnt FROM danmu_count_per_sec WHERE room_id = ?"
		mvArgs := []interface{}{roomID}
		if fromMS > 0 {
			mvQuery += " AND ts_bucket >= fromUnixTimestamp64Milli(?)"
			mvArgs = append(mvArgs, fromMS)
		}
		if toMS > 0 {
			mvQuery += " AND ts_bucket < fromUnixTimestamp64Milli(?)"
			mvArgs = append(mvArgs, toMS)
		}
		mvQuery += " GROUP BY ts ORDER BY ts"
		if series, err := h.scanPeaks(mvQuery, mvArgs...); err == nil {
			return series, nil
		}
		// 视图不存在或查询失败 → 回退
	}

	return h.scanPeaks(query, args...)
}

func (h *HistoryDB) scanPeaks(query string, args ...interface{}) ([]PeakPoint, error) {
	rows, err := h.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var series []PeakPoint
	for rows.Next() {
		var p PeakPoint
		if err := rows.Scan(&p.TS, &p.Count); err != nil {
			return nil, err
		}
		series = append(series, p)
	}
	return series, rows.Err()
}

func (h *HistoryDB) Close() error {
	return h.db.Close()
}
