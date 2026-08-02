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

func (h *HistoryDB) Close() error {
	return h.db.Close()
}
