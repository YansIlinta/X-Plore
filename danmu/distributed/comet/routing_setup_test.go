package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/YansIlinta/danmu-distributed/core"
	"github.com/YansIlinta/danmu-distributed/routing"
)

// TestRouteLeaseIntegration verifies the actual control-plane chain:
// Hub.AddClient -> nonblocking lifecycle hook -> LeaseManager -> RedisStore,
// followed by clean disconnect removal. It is opt-in outside CI.
func TestRouteLeaseIntegration(t *testing.T) {
	addr := os.Getenv("DANMU_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("DANMU_TEST_REDIS_ADDR not set")
	}

	prefix := fmt.Sprintf("xplore:test:comet-route:%d", time.Now().UnixNano())
	t.Setenv("DANMU_ROUTE_REDIS_ADDR", addr)
	t.Setenv("DANMU_ROUTE_REDIS_PREFIX", prefix)
	t.Setenv("DANMU_ROUTE_TTL", "5s")
	t.Setenv("DANMU_ROUTE_REDIS_DB", "0")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := core.NewHub("logical-comet-a", ctx)
	closeRoute, enabled, err := setupRouteLeases(ctx, hub, "comet-a:7500")
	if err != nil {
		t.Fatal(err)
	}
	defer closeRoute()
	if !enabled {
		t.Fatal("route leases should be enabled")
	}

	inspectClient := redis.NewClient(&redis.Options{Addr: addr})
	defer inspectClient.Close()
	store, err := routing.NewRedisStore(inspectClient, prefix)
	if err != nil {
		t.Fatal(err)
	}

	client := core.NewClient(hub, nil, "u1", "room-1", hub.Context())
	client.DeviceID = "web"
	hub.AddClient(client)

	channelID := core.DanmuChannelID("room-1")
	waitGateways(t, func() ([]string, error) {
		return store.LookupChannel(context.Background(), channelID)
	}, []string{"comet-a:7500"})
	waitGateways(t, func() ([]string, error) {
		return store.LookupUser(context.Background(), "u1")
	}, []string{"comet-a:7500"})
	waitGateways(t, func() ([]string, error) {
		return store.LookupDevice(context.Background(), "u1", "web")
	}, []string{"comet-a:7500"})

	hub.RemoveClient(client)
	waitGateways(t, func() ([]string, error) {
		return store.LookupChannel(context.Background(), channelID)
	}, nil)
	waitGateways(t, func() ([]string, error) {
		return store.LookupUser(context.Background(), "u1")
	}, nil)
}

func waitGateways(t *testing.T, lookup func() ([]string, error), want []string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := lookup()
		if err == nil && sameGateways(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lookup=%v err=%v want=%v", got, err, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// sameGateways compares semantic route results. Redis ZSET lookup naturally
// returns an empty-but-non-nil slice after removal, while callers may express
// the expected empty route as nil; both mean zero target Gateways.
func sameGateways(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
