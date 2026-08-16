//go:build smoke

package main

import (
	"testing"
	"time"
)

// 手动跑：go test -tags smoke ./consumer/ -run TestClickHouseSmoke -v
// 需要本地跑着一个 ClickHouse（见开发时用的 -clickhouse-addr=localhost:19000）
func TestClickHouseSmoke(t *testing.T) {
	db, err := NewDB("localhost:19000", "default", "default", "")
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}
	defer db.Close()

	room := "smoke-room-" + time.Now().Format("150405.000000")
	now := time.Now().UnixMilli()

	msgs := []Message{
		{RoomID: room, UID: "u1", Content: "hello", ClientTS: now, ServerTS: now, SourceServer: "srv1"},
		{RoomID: room, UID: "u2", Content: "world", ClientTS: now + 1, ServerTS: now + 1, SourceServer: "srv1"},
		{RoomID: room, UID: "u3", Content: "third", ClientTS: now + 2, ServerTS: now + 2, SourceServer: "srv1"},
	}
	if err := db.BatchInsert(msgs); err != nil {
		t.Fatalf("BatchInsert failed: %v", err)
	}

	// ClickHouse MergeTree 是最终一致，insert 后立即查询通常可见，但留一点余量
	time.Sleep(500 * time.Millisecond)

	items, total, err := db.Query(room, 1, 20)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if total != 3 {
		t.Errorf("expected total=3, got %d", total)
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d: %+v", len(items), items)
	}
	// ORDER BY server_ts DESC
	if items[0].UID != "u3" || items[2].UID != "u1" {
		t.Errorf("unexpected order: %+v", items)
	}

	// 分页
	page1, _, err := db.Query(room, 1, 2)
	if err != nil {
		t.Fatalf("paginated query failed: %v", err)
	}
	if len(page1) != 2 {
		t.Errorf("expected page size 2, got %d", len(page1))
	}
}
