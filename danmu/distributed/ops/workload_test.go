package ops

import (
	"testing"
)

// --- Workload skew（§13）与 diagnostics（§13.1）---

func TestRoomAssignUniform(t *testing.T) {
	assign := roomAssign(10, 5, DistUniform, 0, 1)
	want := []int{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}
	for i := range want {
		if assign[i] != want[i] {
			t.Fatalf("uniform assign[%d]=%d, want %d", i, assign[i], want[i])
		}
	}
}

func TestRoomAssignHotRoom(t *testing.T) {
	assign := roomAssign(100, 10, DistHotRoom, 0, 1)
	room0 := 0
	others := map[int]int{}
	for _, r := range assign {
		if r == 0 {
			room0++
		} else {
			others[r]++
		}
	}
	if room0 != 80 {
		t.Fatalf("hot_room must put 80 percent in room 0, got %d", room0)
	}
	for r := 1; r <= 9; r++ {
		if others[r] == 0 {
			t.Fatalf("room %d must have some connections", r)
		}
	}
}

func TestZipfDeterministicSeed(t *testing.T) {
	a := roomAssign(500, 100, DistZipf, 1.1, 42)
	b := roomAssign(500, 100, DistZipf, 1.1, 42)
	if len(a) != len(b) {
		t.Fatal("length mismatch")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("zipf must be deterministic under same seed at %d", i)
		}
	}
	c := roomAssign(500, 100, DistZipf, 1.1, 43)
	same := 0
	for i := range a {
		if a[i] == c[i] {
			same++
		}
	}
	if same == len(a) {
		t.Fatalf("different seed must produce a different assignment")
	}
}

func TestRoomDiagnostics(t *testing.T) {
	assign := roomAssign(100, 10, DistHotRoom, 0, 1)
	d := diagnosticsFromAssign(assign, 10, DistHotRoom)
	if d == nil {
		t.Fatal("nil diagnostics")
	}
	if d.LargestRoomShare != 0.8 && d.LargestRoomShare < 0.5 {
		t.Fatalf("largest_room_share=%v, hot room should dominate", d.LargestRoomShare)
	}
	if d.MaxRoomSize != 80 || d.MinRoomSize < 1 {
		t.Fatalf("max/min room size wrong: %+v", d)
	}
	mean := float64(len(assign)) / 10
	if d.MeanRoomSize != mean {
		t.Fatalf("mean_room_size=%v, want %v", d.MeanRoomSize, mean)
	}
	if d.Top10PercentRoomShare < d.LargestRoomShare {
		t.Fatalf("top10 share must be >= largest share")
	}
	if len(d.RoomSizes) > 200 {
		t.Fatalf("room sizes must be bounded")
	}
	// 诊断可证明 "hot room" 不只是名字。
	uniformDiagnostics := diagnosticsFromAssign(roomAssign(100, 10, DistUniform, 0, 1), 10, DistUniform)
	if d.LargestRoomShare <= uniformDiagnostics.LargestRoomShare {
		t.Fatalf("hot_room largest share %v must exceed uniform %v", d.LargestRoomShare, uniformDiagnostics.LargestRoomShare)
	}
}

func TestZipfGeneratorValid(t *testing.T) {
	g := newZipfGenerator(50, 1.1)
	if len(g.cdf) != 50 {
		t.Fatalf("cdf size =%d", len(g.cdf))
	}
	if g.cdf[49] < 0.999 || g.cdf[49] > 1.001 {
		t.Fatalf("cdf must reach ~1.0, got %v", g.cdf[49])
	}
}
