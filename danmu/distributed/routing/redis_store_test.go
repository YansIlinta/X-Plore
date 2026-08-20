package routing

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisRouteMemberRoundTrip(t *testing.T) {
	cases := [][2]string{
		{"gw-a:7500", "gw-a-conn-1"},
		{"网关-A", "连接/1"},
		{"a.b:c", "x:y.z"},
	}
	for _, tc := range cases {
		member := encodeRouteMember(tc[0], tc[1])
		gatewayID, connectionID, err := decodeRouteMember(member)
		if err != nil {
			t.Fatalf("decode %q: %v", member, err)
		}
		if gatewayID != tc[0] || connectionID != tc[1] {
			t.Fatalf("round trip got (%q,%q) want (%q,%q)", gatewayID, connectionID, tc[0], tc[1])
		}
	}
}

func TestDecodeRouteMemberRejectsInvalid(t *testing.T) {
	for _, member := range []string{"", "abc", ".", "***.abc", "abc.***"} {
		if _, _, err := decodeRouteMember(member); err == nil {
			t.Fatalf("decodeRouteMember(%q) should fail", member)
		}
	}
}

func TestNewRedisStorePrefixNormalization(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()

	store, err := NewRedisStore(client, "custom")
	if err != nil {
		t.Fatal(err)
	}
	if store.prefix != "custom:" {
		t.Fatalf("prefix=%q want custom:", store.prefix)
	}
	store, err = NewRedisStore(client, "")
	if err != nil {
		t.Fatal(err)
	}
	if store.prefix != defaultRedisPrefix {
		t.Fatalf("default prefix=%q want %q", store.prefix, defaultRedisPrefix)
	}
	if _, err := NewRedisStore(nil, "x"); err == nil {
		t.Fatal("nil Redis client should fail")
	}
}

// TestRedisStoreIntegration is opt-in because unit-test environments do not
// necessarily provide Redis. Example:
//
//   DANMU_TEST_REDIS_ADDR=127.0.0.1:6379 go test ./routing -run RedisStoreIntegration -v
func TestRedisStoreIntegration(t *testing.T) {
	addr := os.Getenv("DANMU_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("DANMU_TEST_REDIS_ADDR not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping %s: %v", addr, err)
	}

	prefix := fmt.Sprintf("xplore:test:route:%d", time.Now().UnixNano())
	store, err := NewRedisStore(client, prefix)
	if err != nil {
		t.Fatal(err)
	}
	ttl := 30 * time.Second

	// Same user, two connections, same Gateway: lookup must deduplicate Gateway
	// and removing one connection must keep the other route alive.
	for _, route := range []ConnectionRoute{
		{ConnectionID: "c1", UserID: "u1", DeviceID: "web", GatewayID: "gw-a", ChannelIDs: []string{"live:1"}},
		{ConnectionID: "c2", UserID: "u1", DeviceID: "mobile", GatewayID: "gw-a", ChannelIDs: []string{"live:1"}},
	} {
		if err := store.UpsertConnection(ctx, route, ttl); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := store.LookupUser(ctx, "u1"); err != nil || !reflect.DeepEqual(got, []string{"gw-a"}) {
		t.Fatalf("LookupUser=%v err=%v", got, err)
	}
	if err := store.RemoveConnection(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if got, err := store.LookupUser(ctx, "u1"); err != nil || !reflect.DeepEqual(got, []string{"gw-a"}) {
		t.Fatalf("LookupUser after c1 removal=%v err=%v", got, err)
	}

	// Upserting c2 to another Gateway/channel removes the old live membership.
	if err := store.UpsertConnection(ctx, ConnectionRoute{
		ConnectionID: "c2", UserID: "u1", DeviceID: "mobile", GatewayID: "gw-b", ChannelIDs: []string{"live:2"},
	}, ttl); err != nil {
		t.Fatal(err)
	}
	if got, err := store.LookupUser(ctx, "u1"); err != nil || !reflect.DeepEqual(got, []string{"gw-b"}) {
		t.Fatalf("LookupUser after upsert=%v err=%v", got, err)
	}
	if got, err := store.LookupChannel(ctx, "live:1"); err != nil || len(got) != 0 {
		t.Fatalf("old channel=%v err=%v want empty", got, err)
	}
	if got, err := store.LookupChannel(ctx, "live:2"); err != nil || !reflect.DeepEqual(got, []string{"gw-b"}) {
		t.Fatalf("new channel=%v err=%v", got, err)
	}

	if err := store.RefreshConnection(ctx, "c2", ttl); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveConnection(ctx, "c2"); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveConnection(ctx, "c2"); err != nil {
		t.Fatalf("second RemoveConnection should be idempotent: %v", err)
	}
	if got, err := store.LookupUser(ctx, "u1"); err != nil || len(got) != 0 {
		t.Fatalf("final LookupUser=%v err=%v want empty", got, err)
	}
}
