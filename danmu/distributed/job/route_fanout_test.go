package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/YansIlinta/danmu-distributed/core"
	"github.com/YansIlinta/danmu-distributed/pb"
	"github.com/YansIlinta/danmu-distributed/routing"
)

type recordingRealtimeServer struct {
	pb.UnimplementedRealtimeDeliveryServiceServer
	mu        sync.Mutex
	calls     int
	lastTarget string
}

func (s *recordingRealtimeServer) PushEnvelope(_ context.Context, req *pb.PushEnvelopeReq) (*pb.PushEnvelopeResp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if req != nil && req.Envelope != nil {
		s.lastTarget = req.Envelope.TargetId
	}
	return &pb.PushEnvelopeResp{Delivered: 1}, nil
}

func (s *recordingRealtimeServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func newRealtimeBufConn(t *testing.T, srv pb.RealtimeDeliveryServiceServer) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterRealtimeDeliveryServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
	})

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestRouteAwareFanoutOnlyCallsResolvedGateway(t *testing.T) {
	statRouteResolveErr.Store(0)
	statRouteCandidates.Store(0)
	statRouteRPC.Store(0)
	statRouteMissing.Store(0)
	statPushOK.Store(0)
	statPushErr.Store(0)
	statDelivered.Store(0)

	a := &recordingRealtimeServer{}
	b := &recordingRealtimeServer{}
	pool := newCometPool()
	pool.conns["gw-a:7500"] = newRealtimeBufConn(t, a)
	pool.conns["gw-b:7500"] = newRealtimeBufConn(t, b)

	store := routing.NewMemoryStore()
	if err := store.UpsertConnection(context.Background(), routing.ConnectionRoute{
		ConnectionID: "c-a",
		UserID:       "u1",
		DeviceID:     "web",
		GatewayID:    "gw-a:7500",
		ChannelIDs:   []string{core.DanmuChannelID("room-1")},
	}, time.Minute); err != nil {
		t.Fatal(err)
	}

	r := &routeFanout{
		pool:     pool,
		resolver: routing.TargetResolver{Store: store},
	}
	r.pushRoom("room-1", []byte(`[{"type":"danmu"}]`), nil)

	if got := a.count(); got != 1 {
		t.Fatalf("gateway A calls=%d want 1", got)
	}
	if got := b.count(); got != 0 {
		t.Fatalf("gateway B calls=%d want 0", got)
	}
	if a.lastTarget != core.DanmuChannelID("room-1") {
		t.Fatalf("target=%q want %q", a.lastTarget, core.DanmuChannelID("room-1"))
	}
	if got := statRouteCandidates.Load(); got != 1 {
		t.Fatalf("route candidates=%d want 1", got)
	}
	if got := statRouteRPC.Load(); got != 1 {
		t.Fatalf("route rpc=%d want 1", got)
	}
	if got := statDelivered.Load(); got != 1 {
		t.Fatalf("delivered=%d want 1", got)
	}
}

func TestRouteAwareFanoutSkipsStaleEndpointMissingFromEtcdPool(t *testing.T) {
	statRouteMissing.Store(0)
	statRouteRPC.Store(0)

	store := routing.NewMemoryStore()
	if err := store.UpsertConnection(context.Background(), routing.ConnectionRoute{
		ConnectionID: "c-stale",
		UserID:       "u1",
		DeviceID:     "web",
		GatewayID:    "gone:7500",
		ChannelIDs:   []string{core.DanmuChannelID("room-1")},
	}, time.Minute); err != nil {
		t.Fatal(err)
	}

	r := &routeFanout{
		pool:     newCometPool(),
		resolver: routing.TargetResolver{Store: store},
	}
	r.pushRoom("room-1", []byte(`[]`), nil)
	if got := statRouteRPC.Load(); got != 0 {
		t.Fatalf("route rpc=%d want 0", got)
	}
	if got := statRouteMissing.Load(); got != 1 {
		t.Fatalf("route missing=%d want 1", got)
	}
}

func TestSetupRouteFanoutKeepsLegacyDefault(t *testing.T) {
	r, closeFn, err := setupRouteFanout(context.Background(), fanoutBroadcastAll, newCometPool())
	if err != nil {
		t.Fatal(err)
	}
	if r != nil {
		t.Fatal("broadcast_all must not create route-aware engine")
	}
	if err := closeFn(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := setupRouteFanout(context.Background(), "unknown", newCometPool()); err == nil {
		t.Fatal("unknown fanout mode must fail")
	}
}
