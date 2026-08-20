package core

import "testing"

func TestSubscriptionIndexSubscribeIsIdempotent(t *testing.T) {
	h := mkHub(t)
	c := addConn(t, h, "u1", "web", "r1")
	channel := DanmuChannelID("r1")

	// Re-subscribing the same connection to the same channel must not create
	// duplicate forward or reverse membership state.
	h.subs.Subscribe(channel, c)
	h.subs.Subscribe(channel, c)

	if got := len(h.subs.GetSubscribers(channel)); got != 1 {
		t.Fatalf("subscribers=%d want 1 after duplicate Subscribe", got)
	}
	if got := h.subs.Count(); got != 1 {
		t.Fatalf("channel count=%d want 1 after duplicate Subscribe", got)
	}

	// One Unsubscribe fully removes the membership. The old counter-based reverse
	// index required multiple Unsubscribe calls after duplicate Subscribe.
	h.subs.Unsubscribe(channel, c)
	if got := len(h.subs.GetSubscribers(channel)); got != 0 {
		t.Fatalf("subscribers=%d want 0 after one Unsubscribe", got)
	}
	if got := h.subs.Count(); got != 0 {
		t.Fatalf("channel count=%d want 0 after one Unsubscribe", got)
	}

	// Repeated Unsubscribe is also idempotent and must not underflow counters.
	h.subs.Unsubscribe(channel, c)
	if got := h.subs.Count(); got != 0 {
		t.Fatalf("channel count=%d want 0 after repeated Unsubscribe", got)
	}
}
