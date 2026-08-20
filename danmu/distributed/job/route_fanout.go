package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/metadata"

	"github.com/YansIlinta/danmu-distributed/core"
	"github.com/YansIlinta/danmu-distributed/pb"
	"github.com/YansIlinta/danmu-distributed/routing"
)

const (
	fanoutBroadcastAll = "broadcast_all"
	fanoutRouteAware   = "route_aware"
)

var (
	statRouteResolveErr atomic.Int64
	statRouteCandidates atomic.Int64
	statRouteRPC        atomic.Int64
	statRouteMissing    atomic.Int64
)

type realtimeTarget struct {
	addr   string
	client pb.RealtimeDeliveryServiceClient
}

// realtimeTargets reuses the etcd-maintained gRPC connections from cometPool.
// RouteStore returns dialable advertised endpoints; stale route endpoints that
// are no longer present in etcd are intentionally skipped rather than redialed.
func (p *cometPool) realtimeTargets(addrs []string) []realtimeTarget {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]realtimeTarget, 0, len(addrs))
	for _, addr := range addrs {
		conn := p.conns[addr]
		if conn == nil {
			continue
		}
		out = append(out, realtimeTarget{addr: addr, client: pb.NewRealtimeDeliveryServiceClient(conn)})
	}
	return out
}

type routeFanout struct {
	pool     *cometPool
	resolver routing.TargetResolver
	redis    *redis.Client
}

// setupRouteFanout preserves broadcast_all as the default baseline. route_aware
// is explicit and requires the same Redis RouteStore used by Comet leases; a
// missing/unreachable store is a startup error instead of a silent fallback.
func setupRouteFanout(ctx context.Context, mode string, pool *cometPool) (*routeFanout, func() error, error) {
	switch mode {
	case fanoutBroadcastAll:
		return nil, func() error { return nil }, nil
	case fanoutRouteAware:
		// continue below
	default:
		return nil, nil, fmt.Errorf("invalid fanout mode %q (want %s or %s)", mode, fanoutBroadcastAll, fanoutRouteAware)
	}

	addr := strings.TrimSpace(os.Getenv("DANMU_ROUTE_REDIS_ADDR"))
	if addr == "" {
		return nil, nil, fmt.Errorf("DANMU_ROUTE_REDIS_ADDR is required for route_aware fanout")
	}
	db := 0
	if raw := strings.TrimSpace(os.Getenv("DANMU_ROUTE_REDIS_DB")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return nil, nil, fmt.Errorf("invalid DANMU_ROUTE_REDIS_DB %q", raw)
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
		return nil, nil, fmt.Errorf("route redis ping %s: %w", addr, err)
	}
	store, err := routing.NewRedisStore(client, os.Getenv("DANMU_ROUTE_REDIS_PREFIX"))
	if err != nil {
		_ = client.Close()
		return nil, nil, err
	}
	return &routeFanout{
		pool:     pool,
		resolver: routing.TargetResolver{Store: store},
		redis:    client,
	}, client.Close, nil
}

func (r *routeFanout) pushRoom(roomID string, payload []byte, traceIDs []string) {
	env := core.MessageEnvelope{
		TargetType:    core.TargetChannel,
		TargetID:      core.DanmuChannelID(roomID),
		DeliveryClass: core.DeliveryEphemeral,
		MessageType:   core.MessageDanmu,
	}
	resolveCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	addrs, err := r.resolver.Resolve(resolveCtx, env)
	cancel()
	if err != nil {
		statRouteResolveErr.Add(1)
		log.Printf("[job] route resolve room=%s error: %v", roomID, err)
		return
	}
	statRouteCandidates.Add(int64(len(addrs)))
	targets := r.pool.realtimeTargets(addrs)
	if missing := len(addrs) - len(targets); missing > 0 {
		statRouteMissing.Add(int64(missing))
	}

	var delivered atomic.Int64
	var wg sync.WaitGroup
	for _, target := range targets {
		target := target
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if len(traceIDs) > 0 {
				ctx = metadata.AppendToOutgoingContext(ctx, core.TraceMetadataKey, strings.Join(traceIDs, ","))
			}
			resp, err := target.client.PushEnvelope(ctx, &pb.PushEnvelopeReq{
				Envelope: &pb.DeliveryEnvelope{
					TargetType:    pb.TargetType_TARGET_CHANNEL,
					TargetId:      core.DanmuChannelID(roomID),
					DeliveryClass: pb.DeliveryClass_DELIVERY_EPHEMERAL,
					MessageType:   string(core.MessageDanmu),
				},
				ClientPayload: payload,
			})
			statRouteRPC.Add(1)
			if err != nil {
				statPushErr.Add(1)
				log.Printf("[job] PushEnvelope room=%s target=%s error: %v", roomID, target.addr, err)
				return
			}
			statPushOK.Add(1)
			statDelivered.Add(int64(resp.Delivered))
			delivered.Add(int64(resp.Delivered))
		}()
	}
	wg.Wait()

	if len(traceIDs) > 0 && tracer != nil {
		now := time.Now().UnixNano()
		detail := "mode=route_aware candidates=" + strconv.Itoa(len(addrs)) +
			" rpc=" + strconv.Itoa(len(targets)) +
			" delivered=" + strconv.FormatInt(delivered.Load(), 10)
		for _, id := range traceIDs {
			tracer.Record(id, core.HopJobPush, roomID, detail, now)
		}
	}
}
