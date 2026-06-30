package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// DB 封装 SQLite 操作，用于 Kafka 落库消费者
type DB struct {
	db *sql.DB
}

func NewDB(path string) (*DB, error) {
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// 创建表
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS danmu_history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			room_id TEXT NOT NULL,
			uid TEXT NOT NULL,
			content TEXT NOT NULL,
			client_ts INTEGER NOT NULL DEFAULT 0,
			server_ts INTEGER NOT NULL DEFAULT 0,
			source_server TEXT NOT NULL DEFAULT '',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_room_ts ON danmu_history(room_id, server_ts DESC);
	`)
	if err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	// 优化写入性能
	db.SetMaxOpenConns(1) // SQLite 单写者
	db.SetMaxIdleConns(1)

	return &DB{db: db}, nil
}

// BatchInsert 批量插入弹幕记录
func (d *DB) BatchInsert(msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	// 构造批量 INSERT
	valueStrings := make([]string, 0, len(msgs))
	valueArgs := make([]interface{}, 0, len(msgs)*5)
	for _, msg := range msgs {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs, msg.RoomID, msg.UID, msg.Content,
			msg.ClientTS, msg.ServerTS, msg.SourceServer)
	}

	query := fmt.Sprintf(
		"INSERT INTO danmu_history (room_id, uid, content, client_ts, server_ts, source_server) VALUES %s",
		strings.Join(valueStrings, ","),
	)

	_, err := d.db.Exec(query, valueArgs...)
	return err
}

// Query 查询历史弹幕，按时间倒序，支持分页
func (d *DB) Query(roomID string, page, limit int) ([]HistoryItem, int, error) {
	// 查询总数
	var total int
	err := d.db.QueryRow("SELECT COUNT(*) FROM danmu_history WHERE room_id = ?", roomID).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * limit
	rows, err := d.db.Query(
		"SELECT uid, content, server_ts FROM danmu_history WHERE room_id = ? ORDER BY server_ts DESC LIMIT ? OFFSET ?",
		roomID, limit, offset,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var items []HistoryItem
	for rows.Next() {
		var item HistoryItem
		if err := rows.Scan(&item.UID, &item.Content, &item.TimeMS); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	if items == nil {
		items = []HistoryItem{}
	}
	return items, total, nil
}

// HistoryItem 与 server 的 HistoryItem 保持一致
type HistoryItem struct {
	UID     string `json:"uid"`
	Content string `json:"content"`
	TimeMS  int64  `json:"time_ms"`
}

// Close 关闭数据库
func (d *DB) Close() error {
	return d.db.Close()
}
