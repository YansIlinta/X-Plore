package routing

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestMemoryStoreUserRouteRefcount(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	ttl := time.Minute

	for _, route := range []ConnectionRoute{
		{ConnectionID: "c1", UserID: "u1", DeviceID: "web", GatewayID: "gw-a", ChannelIDs: []string{"room:1"}},
		{ConnectionID: "c2", UserID: "u1", DeviceID: "mobile", GatewayID: "gw-a", ChannelIDs: []string{"room:1"}},
	} {
		if err := store.UpsertConnection(ctx, route, ttl); err != nil {
			t.Fatal(err)
		}
	}

	if got, _ := store.LookupUser(ctx, "u1"); !reflect.DeepEqual(got, []string{"gw-a"}) {
		t.Fatalf("LookupUser=%v want [gw-a]", got)
	}

	// Removing one of two local connections must not erase u1 -> gw-a.
	if err := store.RemoveConnection(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.LookupUser(ctx, "u1"); !reflect.DeepEqual(got, []string{"gw-a"}) {
		t.Fatalf("after removing c1 LookupUser=%v want [gw-a]", got)
	}

	if err := store.RemoveConnection(ctx, "c2"); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.LookupUser(ctx, "u1"); len(got) != 0 {
		t.Fatalf("after removing last connection LookupUser=%v want empty", got)
	}
}

func TestMemoryStoreDeviceScopeAndChannelDedup(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	ttl := time.Minute

	routes := []ConnectionRoute{
		{ConnectionID: "c1", UserID: "u1", DeviceID: "web", GatewayID: "gw-b", ChannelIDs: []string{"live:1", "live:1"}},
		{ConnectionID: "c2", UserID: "u2", DeviceID: "web", GatewayID: "gw-a", ChannelIDs: []string{"live:1"}},
		{ConnectionID: "c3", UserID: "u1", DeviceID: "web", GatewayID: "gw-a", ChannelIDs: []string{"live:2"}},
	}
	for _, route := range routes {
		if err := store.UpsertConnection(ctx, route, ttl); err != nil {
			t.Fatal(err)
		}
	}

	if got, _ := store.LookupDevice(ctx, "u1", "web"); !reflect.DeepEqual(got, []string{"gw-a", "gw-b"}) {
		t.Fatalf("u1/web=%v want [gw-a gw-b]", got)
	}
	if got, _ := store.LookupDevice(ctx, "u2", "web"); !reflect.DeepEqual(got, []string{"gw-a"}) {
		t.Fatalf("u2/web=%v want [gw-a]", got)
	}
	if got, _ := store.LookupChannel(ctx, "live:1"); !reflect.DeepEqual(got, []string{"gw-a", "gw-b"}) {
		t.Fatalf("live:1=%v want [gw-a gw-b]", got)
	}
}

func TestMemoryStoreUpsertReplacesDerivedIndexes(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	ttl := time.Minute

	if err := store.UpsertConnection(ctx, ConnectionRoute{
		ConnectionID: "c1", UserID: "u1", DeviceID: "web", GatewayID: "gw-a", ChannelIDs: []string{"old"},
	}, ttl); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertConnection(ctx, ConnectionRoute{
		ConnectionID: "c1", UserID: "u1", DeviceID: "web", GatewayID: "gw-b", ChannelIDs: []string{"new"},
	}, ttl); err != nil {
		t.Fatal(err)
	}

	if got, _ := store.LookupUser(ctx, "u1"); !reflect.DeepEqual(got, []string{"gw-b"}) {
		t.Fatalf("user route=%v want [gw-b]", got)
	}
	if got, _ := store.LookupChannel(ctx, "old"); len(got) != 0 {
		t.Fatalf("old channel route=%v want empty", got)
	}
	if got, _ := store.LookupChannel(ctx, "new"); !reflect.DeepEqual(got, []string{"gw-b"}) {
		t.Fatalf("new channel route=%v want [gw-b]", got)
	}
}

func TestMemoryStoreTTLAndRefresh(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(100, 0)
	store := newMemoryStore(func() time.Time { return now })

	if err := store.UpsertConnection(ctx, ConnectionRoute{
		ConnectionID: "c1", UserID: "u1", DeviceID: "web", GatewayID: "gw-a", ChannelIDs: []string{"live:1"},
	}, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	now = now.Add(9 * time.Second)
	if err := store.RefreshConnection(ctx, "c1", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	now = now.Add(9 * time.Second)
	if got, _ := store.LookupUser(ctx, "u1"); !reflect.DeepEqual(got, []string{"gw-a"}) {
		t.Fatalf("refreshed route=%v want [gw-a]", got)
	}

	now = now.Add(2 * time.Second)
	if got, _ := store.LookupUser(ctx, "u1"); len(got) != 0 {
		t.Fatalf("expired route=%v want empty", got)
	}
	if err := store.RefreshConnection(ctx, "c1", time.Second); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("refresh expired error=%v want ErrRouteNotFound", err)
	}
}

func TestMemoryStoreValidationAndIdempotentRemove(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	cases := []ConnectionRoute{
		{UserID: "u1", DeviceID: "web", GatewayID: "gw-a"},
		{ConnectionID: "c1", DeviceID: "web", GatewayID: "gw-a"},
		{ConnectionID: "c1", UserID: "u1", GatewayID: "gw-a"},
		{ConnectionID: "c1", UserID: "u1", DeviceID: "web"},
		{ConnectionID: "c1", UserID: "u1", DeviceID: "web", GatewayID: "gw-a", ChannelIDs: []string{""}},
	}
	for i, route := range cases {
		if err := store.UpsertConnection(ctx, route, time.Minute); err == nil {
			t.Fatalf("case %d should fail validation", i)
		}
	}
	if err := store.UpsertConnection(ctx, ConnectionRoute{
		ConnectionID: "c1", UserID: "u1", DeviceID: "web", GatewayID: "gw-a",
	}, 0); err == nil {
		t.Fatal("zero ttl should fail")
	}

	if err := store.RemoveConnection(ctx, "missing"); err != nil {
		t.Fatalf("idempotent remove returned %v", err)
	}
}

func TestMemoryStoreHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := NewMemoryStore()

	if err := store.UpsertConnection(ctx, ConnectionRoute{
		ConnectionID: "c1", UserID: "u1", DeviceID: "web", GatewayID: "gw-a",
	}, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("UpsertConnection error=%v want context.Canceled", err)
	}
	if _, err := store.LookupUser(ctx, "u1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("LookupUser error=%v want context.Canceled", err)
	}
}
