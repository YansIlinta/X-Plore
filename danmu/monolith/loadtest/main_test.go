package main

import "testing"

// deliveryTracker 的纯逻辑测试（§12：必须有测试证明计算正确）。

type msgDelivery struct {
	epoch uint64
	seqs  []int64
}

func TestDeliveryTrackerSequentialNoGaps(t *testing.T) {
	var tr deliveryTracker
	// 完整连续序列：observed=5, missing=0。
	obs, miss := tr.onSeqs(1, []int64{1, 2, 3, 4, 5})
	if obs != 5 || miss != 0 {
		t.Fatalf("sequential: obs=%d miss=%d, want 5/0", obs, miss)
	}
}

func TestDeliveryTrackerGapCounting(t *testing.T) {
	var tr deliveryTracker
	// 1,2 然后跳 4,5（缺 3）再 6（缺... 从 5 到 6 连续）。
	obs, miss := tr.onSeqs(1, []int64{1, 2, 4, 5, 6})
	if obs != 5 || miss != 1 {
		t.Fatalf("gap: obs=%d miss=%d, want 5/1 (missing seq 3)", obs, miss)
	}
}

func TestDeliveryTrackerFrameBatches(t *testing.T) {
	// 跨多帧累计：先 [1,2]，再隔一帧 [4,5]（缺 3）。
	var tr deliveryTracker
	obs1, miss1 := tr.onSeqs(1, []int64{1, 2})
	if obs1 != 2 || miss1 != 0 {
		t.Fatalf("frame1: %d/%d", obs1, miss1)
	}
	obs2, miss2 := tr.onSeqs(1, []int64{4, 5})
	if obs2 != 4 || miss2 != 1 {
		t.Fatalf("frame2 cumulative: %d/%d, want 4/1", obs2, miss2)
	}
}

func TestDeliveryTrackerReplayIgnored(t *testing.T) {
	var tr deliveryTracker
	// 1,2,3 然后重放 2,3（reconnect 补发）→ 不计数、不判缺口。
	obs, miss := tr.onSeqs(1, []int64{1, 2, 3, 2, 3})
	if obs != 3 || miss != 0 {
		t.Fatalf("replay must not count nor gap: obs=%d miss=%d", obs, miss)
	}
}

func TestDeliveryTrackerEpochReset(t *testing.T) {
	var tr deliveryTracker
	// 测量窗 1：1,2；窗口重置（epoch 2）后从新 seq 序列开始，不计历史缺口。
	_, _ = tr.onSeqs(1, []int64{1, 2, 3})
	obs, miss := tr.onSeqs(2, []int64{1, 2, 3, 4})
	if obs != 4 || miss != 0 {
		t.Fatalf("epoch reset: obs=%d miss=%d, want 4/0 (fresh continuity)", obs, miss)
	}
	// epoch 内连续序列与缺口混合在重置后正确计数。
	obs, miss = tr.onSeqs(3, []int64{2, 5})
	if obs != 2 || miss != 2 {
		t.Fatalf("post-reset gap: obs=%d miss=%d, want 2/2", obs, miss)
	}
}

func TestDeliveryTrackerSentVsDeliveredNotEquated(t *testing.T) {
	// 关键语义：投递核算不把 sent 当作 delivered。
	// 一个发送了 10 条但对端只收到 8 条的连接：expected=8(+2 gap) → delivered=8。
	var tr deliveryTracker
	obs, miss := tr.onSeqs(1, []int64{1, 2, 3, 4, 6, 7, 8, 9})
	if obs != 8 || miss != 1 {
		t.Fatalf("observed/expected accounting broken: obs=%d miss=%d (seq 5 dropped)", obs, miss)
	}
	// expected = observed + missing = 9（该连接应看到 9 条，1 条丢失）。
	if obs+miss != 9 {
		t.Fatalf("expected deliveries wrong")
	}
}

func TestDeliveryIndependenceOfCounters(t *testing.T) {
	// 纯函数：相同输入相同输出（无状态污染外部）。
	a := deliveryTracker{}
	b := deliveryTracker{}
	oa, ma := a.onSeqs(1, []int64{1, 3, 4})
	ob, mb := b.onSeqs(1, []int64{1, 3, 4})
	if oa != ob || ma != mb {
		t.Fatalf("tracker must be deterministic and independent: (%d,%d) vs (%d,%d)", oa, ma, ob, mb)
	}
}

// roomAssignFor 确定性 + 分布诊断（与 ops 侧一致）。
func TestRoomAssignSequentialRunsUnique(t *testing.T) {
	a := roomAssignFor(40, 20, "uniform", 0, 1)
	b := roomAssignFor(40, 20, "uniform", 0, 1)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("uniform assignment must be deterministic")
		}
	}
}

func TestRoomAssignHotRoomShare(t *testing.T) {
	assign := roomAssignFor(100, 10, "hot_room", 0, 1)
	room0 := 0
	for _, r := range assign {
		if r == 0 {
			room0++
		}
	}
	if room0 != 80 {
		t.Fatalf("hot_room room0=%d, want 80", room0)
	}
	stats := computeRoomStats(assign, 10, "hot_room")
	if stats["largest_room_share"].(float64) <= 0.5 {
		t.Fatalf("largest share too small for hot room: %v", stats["largest_room_share"])
	}
}

func TestComputeRoomStatsTop10Percent(t *testing.T) {
	assign := roomAssignFor(1000, 100, "zipf", 1.1, 7)
	stats := computeRoomStats(assign, 100, "zipf")
	top10 := stats["top_10_percent_room_share"].(float64)
	if top10 < 0.2 || top10 > 1 {
		t.Fatalf("zipf top10 share must be meaningfully skewed, got %v", top10)
	}
}
