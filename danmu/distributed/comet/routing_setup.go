package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/YansIlinta/danmu-distributed/core"
	"github.com/YansIlinta/danmu-distributed/routing"
)

const defaultRouteTTL = 30 * time.Second

// setupRouteLeases wires the Gateway-local connection lifecycle into the
// distributed RouteStore. Routing is opt-in: when DANMU_ROUTE_REDIS_ADDR is
// empty, no hooks are installed and the legacy Danmu path behaves exactly as
// before.
//
// GatewayID in the Phase-3 Store currently carries the dialable advertised gRPC
// endpoint (for example comet-1:7500), because Job must be able to use resolver
// output directly as an RPC destination. Client.GatewayID remains the logical
// comet instance ID used for local observability.
func setupRouteLeases(ctx context.Context, hub *core.Hub, gatewayEndpoint string) (func() error, bool, error) {
	addr := strings.TrimSpace(os.Getenv("DANMU_ROUTE_REDIS_ADDR"))
	if addr == "" {
		return func() error { return nil }, false, nil
	}
	if strings.TrimSpace(gatewayEndpoint) == "" {
		return nil, false, fmt.Errorf("gateway endpoint is required when route store is enabled")
	}

	ttl := defaultRouteTTL
	if raw := strings.TrimSpace(os.Getenv("DANMU_ROUTE_TTL")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return nil, false, fmt.Errorf("invalid DANMU_ROUTE_TTL %q", raw)
		}
		ttl = parsed
	}

	db := 0
	if raw := strings.TrimSpace(os.Getenv("DANMU_ROUTE_REDIS_DB")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return nil, false, fmt.Errorf("invalid DANMU_ROUTE_REDIS_DB %q", raw)
		}
		db = parsed
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("DANMU_ROUTE_REDIS_PASSWORD"),
		DB:       db,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err := client.Ping(pingCtx).Err()
	cancel()
	if err != nil {
		_ = client.Close()
		return nil, false, fmt.Errorf("route redis ping %s: %w", addr, err)
	}

	store, err := routing.NewRedisStore(client, os.Getenv("DANMU_ROUTE_REDIS_PREFIX"))
	if err != nil {
		_ = client.Close()
		return nil, false, err
	}
	manager, err := routing.NewLeaseManager(store, ttl)
	if err != nil {
		_ = client.Close()
		return nil, false, err
	}

	hub.OnConnectionAdded = func(c *core.Client) {
		manager.Track(routing.ConnectionRoute{
			ConnectionID: c.ConnectionID,
			UserID:       c.UID,
			DeviceID:     c.DeviceID,
			GatewayID:    gatewayEndpoint,
			ChannelIDs:   []string{core.DanmuChannelID(c.RoomID)},
		})
	}
	hub.OnConnectionRemoved = func(c *core.Client) {
		manager.Untrack(c.ConnectionID)
	}
	go manager.Run(ctx)

	return client.Close, true, nil
}
