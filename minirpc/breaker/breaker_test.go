package breaker

import (
	"errors"
	"testing"
	"time"
)

func TestTripsAfterThreshold(t *testing.T) {
	b := New(3, time.Minute)
	for i := 0; i < 3; i++ {
		if err := b.Allow(); err != nil {
			t.Fatalf("failure %d: should allow, got %v", i, err)
		}
		b.Failure()
	}
	if b.State() != Open {
		t.Fatalf("want Open after 3 failures, got %v", b.State())
	}
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("want ErrOpen, got %v", err)
	}
}

// 成功会清零连续失败计数：2 败 + 1 成 + 2 败（阈值 3）不应熔断。
func TestSuccessResetsCounter(t *testing.T) {
	b := New(3, time.Minute)
	b.Failure()
	b.Failure()
	b.Success()
	b.Failure()
	b.Failure()
	if b.State() != Closed {
		t.Fatalf("want Closed (counter was reset), got %v", b.State())
	}
}

// 冷却期满 → 半开：第一个请求是探针，同期其他请求仍被拒。
func TestHalfOpenSingleProbe(t *testing.T) {
	b := New(1, 50*time.Millisecond)
	b.Failure() // 阈值 1，直接熔断
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("cooling: want ErrOpen, got %v", err)
	}

	time.Sleep(80 * time.Millisecond) // 冷却期满

	if err := b.Allow(); err != nil {
		t.Fatalf("probe should be allowed, got %v", err)
	}
	if b.State() != HalfOpen {
		t.Fatalf("want HalfOpen, got %v", b.State())
	}
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("second request during probe: want ErrOpen, got %v", err)
	}
}

func TestProbeSuccessCloses(t *testing.T) {
	b := New(1, 50*time.Millisecond)
	b.Failure()
	time.Sleep(80 * time.Millisecond)
	_ = b.Allow() // 放出探针
	b.Success()
	if b.State() != Closed {
		t.Fatalf("want Closed after probe success, got %v", b.State())
	}
	if err := b.Allow(); err != nil {
		t.Fatalf("recovered: should allow, got %v", err)
	}
}

func TestProbeFailureReopens(t *testing.T) {
	b := New(1, 50*time.Millisecond)
	b.Failure()
	time.Sleep(80 * time.Millisecond)
	_ = b.Allow() // 放出探针
	b.Failure()   // 探针失败
	if b.State() != Open {
		t.Fatalf("want Open after probe failure, got %v", b.State())
	}
	// 冷却计时必须已重置：立刻 Allow 仍应被拒
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("cooldown restarted: want ErrOpen, got %v", err)
	}
}
