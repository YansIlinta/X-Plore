package core

import (
	"context"
	"testing"
)

func TestHubConnectionLifecycleHooksAreIdempotent(t *testing.T) {
	h := NewHub("gw-test", context.Background())
	added, removed := 0, 0
	var addedID, removedID string

	h.OnConnectionAdded = func(c *Client) {
		added++
		addedID = c.ConnectionID
	}
	h.OnConnectionRemoved = func(c *Client) {
		removed++
		removedID = c.ConnectionID
	}

	c := NewClient(h, nil, "u1", "room-a", h.Context())
	c.DeviceID = "web"
	h.AddClient(c)

	if added != 1 {
		t.Fatalf("added hook calls=%d want 1", added)
	}
	if addedID == "" || c.ConnectionID != addedID {
		t.Fatalf("added hook saw connection_id=%q client=%q", addedID, c.ConnectionID)
	}
	if got := h.GetConnCount(); got != 1 {
		t.Fatalf("conn count=%d want 1", got)
	}

	h.RemoveClient(c)
	h.RemoveClient(c) // idempotent duplicate cleanup must not emit a second route removal.
	if removed != 1 {
		t.Fatalf("removed hook calls=%d want 1", removed)
	}
	if removedID != addedID {
		t.Fatalf("removed connection_id=%q want %q", removedID, addedID)
	}
	if got := h.GetConnCount(); got != 0 {
		t.Fatalf("conn count=%d want 0", got)
	}
}

func TestHubLifecycleHooksAreOptional(t *testing.T) {
	h := NewHub("gw-test", context.Background())
	c := NewClient(h, nil, "u1", "room-a", h.Context())
	h.AddClient(c)
	h.RemoveClient(c)
}
