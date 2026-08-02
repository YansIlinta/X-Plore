package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// DB 封装 ClickHouse 操作，用于 Kafka 落库消费者
//
// 用 ClickHouse 替代 SQLite：SQLite 单写者模型在高 QPS 下成为瓶颈且不支持分布式部署，
// 而弹幕历史是典型的写多读少时序数据——ClickHouse 的 MergeTree 列式存储对批量 INSERT
// 的吞吐远超 SQLite，天然支持按天分区和水平扩展。
type DB struct {
	db *sql.DB
}

func NewDB(addr, database, username, password string) (*DB, error) {
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
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	// MergeTree：按天分区（event_date 由 server_ts 派生），(room_id, server_ts) 排序键，
	// 匹配"按房间查最近弹幕"这一主查询模式，也让同房间数据在磁盘上物理相邻
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS danmu_history (
			room_id String,
			uid String,
			content String,
			client_ts Int64,
			server_ts Int64,
			source_server String,
			event_date Date DEFAULT toDate(intDiv(server_ts, 1000))
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMMDD(event_date)
		ORDER BY (room_id, server_ts)
	`)
	if err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	return &DB{db: db}, nil
}

// BatchInsert 批量插入弹幕记录
// clickhouse-go 在一个事务内的多次 Exec 会在客户端攒批，Commit 时一次性发送，
// 这正是 ClickHouse 官方推荐的批量写入方式（避免小批量 INSERT 拖垮 MergeTree 合并）
func (d *DB) BatchInsert(msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("begin batch: %w", err)
	}

	stmt, err := tx.Prepare("INSERT INTO danmu_history (room_id, uid, content, client_ts, server_ts, source_server) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare batch: %w", err)
	}
	defer stmt.Close()

	for _, msg := range msgs {
		if _, err := stmt.Exec(msg.RoomID, msg.UID, msg.Content, msg.ClientTS, msg.ServerTS, msg.SourceServer); err != nil {
			tx.Rollback()
			return fmt.Errorf("batch exec: %w", err)
		}
	}

	return tx.Commit()
}

// Query 查询历史弹幕，按时间倒序，支持分页
func (d *DB) Query(roomID string, page, limit int) ([]HistoryItem, int, error) {
	var total int
	err := d.db.QueryRow("SELECT count() FROM danmu_history WHERE room_id = ?", roomID).Scan(&total)
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

// Close 关闭数据库连接
func (d *DB) Close() error {
	return d.db.Close()
}
