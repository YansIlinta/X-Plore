package lb

import (
	"fmt"
	"testing"
)

var nodes = []string{"10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80"}

// 粘性：同一个 key 永远返回同一个节点。
func TestStickiness(t *testing.T) {
	r := NewRing(100)
	r.Reset(nodes)
	first := r.Get("room-42")
	for i := 0; i < 100; i++ {
		if got := r.Get("room-42"); got != first {
			t.Fatalf("key mapping not stable: %s vs %s", got, first)
		}
	}
}

// 最小重映射：删掉一个节点，只有原本属于它的 key 改归属，其余纹丝不动。
func TestMinimalRemapping(t *testing.T) {
	const n = 10000
	r := NewRing(100)
	r.Reset(nodes)

	before := make(map[string]string, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("room-%d", i)
		before[key] = r.Get(key)
	}

	removed := nodes[1]
	r.Reset([]string{nodes[0], nodes[2]}) // 摘掉 10.0.0.2

	moved := 0
	for key, oldAddr := range before {
		newAddr := r.Get(key)
		if oldAddr == removed {
			continue // 死节点的 key 必须迁走，不算"额外移动"
		}
		if newAddr != oldAddr {
			moved++ // 原本不在死节点上的 key 也动了 —— 这是一致性哈希不该有的
		}
	}
	if moved != 0 {
		t.Fatalf("%d keys moved that shouldn't have (minimal remapping violated)", moved)
	}

	// 对照组心智模型: 如果是 hash%N，从 3 节点变 2 节点，
	// 约 2/3 的 key 都会换归属 —— 一致性哈希把这个数字压到 0（只迁死节点的份额）
}

// 虚拟节点让负载大致均匀：3 节点 10000 key，每家份额应在 20%~47% 之间。
func TestVirtualNodeBalance(t *testing.T) {
	r := NewRing(100)
	r.Reset(nodes)
	count := make(map[string]int)
	const n = 10000
	for i := 0; i < n; i++ {
		count[r.Get(fmt.Sprintf("user-%d", i))]++
	}
	for _, addr := range nodes {
		share := float64(count[addr]) / n
		if share < 0.20 || share > 0.47 {
			t.Fatalf("node %s got %.1f%% of keys, too skewed: %v", addr, share*100, count)
		}
	}
}

// 空环不崩溃。
func TestEmptyRing(t *testing.T) {
	r := NewRing(100)
	if got := r.Get("any"); got != "" {
		t.Fatalf("empty ring should return empty string, got %q", got)
	}
}
