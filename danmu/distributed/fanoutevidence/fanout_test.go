package fanoutevidence

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDiffBroadcastAll(t *testing.T) {
	before := JobStats{
		FanoutMode: ModeBroadcastAll, UptimeMS: 1000,
		ConsumedTotal: 100, PushOKTotal: 300, PushErrTotal: 10, DeliveredTotal: 900,
	}
	after := JobStats{
		FanoutMode: ModeBroadcastAll, UptimeMS: 5000,
		ConsumedTotal: 120, PushOKTotal: 360, PushErrTotal: 10, DeliveredTotal: 1080,
	}
	r, err := Diff(before, after)
	if err != nil {
		t.Fatal(err)
	}
	if r.Delta.ConsumedMessages != 20 || r.Delta.InternalRPCs != 60 || r.Delta.Delivered != 180 {
		t.Fatalf("unexpected delta: %+v", r.Delta)
	}
	if r.RPCPerConsumedMessage == nil || *r.RPCPerConsumedMessage != 3 {
		t.Fatalf("rpc/message=%v want 3", r.RPCPerConsumedMessage)
	}
	if r.CandidatesPerConsumed != nil {
		t.Fatalf("broadcast_all candidates/message must be N/A, got %v", *r.CandidatesPerConsumed)
	}
}

func TestDiffRouteAware(t *testing.T) {
	before := JobStats{
		FanoutMode: ModeRouteAware, UptimeMS: 1000,
		ConsumedTotal: 100, PushOKTotal: 50, PushErrTotal: 2,
		RouteCandidatesTotal: 60, RouteRPCTotal: 52, RouteMissingCometTotal: 8,
	}
	after := JobStats{
		FanoutMode: ModeRouteAware, UptimeMS: 9000,
		ConsumedTotal: 140, PushOKTotal: 70, PushErrTotal: 4,
		RouteCandidatesTotal: 88, RouteRPCTotal: 74, RouteMissingCometTotal: 14,
	}
	r, err := Diff(before, after)
	if err != nil {
		t.Fatal(err)
	}
	if r.Delta.ConsumedMessages != 40 || r.Delta.InternalRPCs != 22 || r.Delta.RouteCandidates != 28 || r.Delta.RouteMissingComets != 6 {
		t.Fatalf("unexpected delta: %+v", r.Delta)
	}
	if r.RPCPerConsumedMessage == nil || *r.RPCPerConsumedMessage != 0.55 {
		t.Fatalf("rpc/message=%v want 0.55", r.RPCPerConsumedMessage)
	}
	if r.CandidatesPerConsumed == nil || *r.CandidatesPerConsumed != 0.7 {
		t.Fatalf("candidates/message=%v want 0.7", r.CandidatesPerConsumed)
	}
	if r.RouteMissingPerCandidate == nil || *r.RouteMissingPerCandidate != 6.0/28.0 {
		t.Fatalf("missing/candidate=%v", r.RouteMissingPerCandidate)
	}
}

func TestDiffRejectsRestartAndModeChange(t *testing.T) {
	if _, err := Diff(
		JobStats{FanoutMode: ModeBroadcastAll, UptimeMS: 2000},
		JobStats{FanoutMode: ModeBroadcastAll, UptimeMS: 1000},
	); err == nil {
		t.Fatal("restart must invalidate the run")
	}
	if _, err := Diff(
		JobStats{FanoutMode: ModeBroadcastAll, UptimeMS: 1000},
		JobStats{FanoutMode: ModeRouteAware, UptimeMS: 2000},
	); err == nil {
		t.Fatal("fanout mode change must invalidate the run")
	}
	if _, err := Diff(
		JobStats{FanoutMode: ModeRouteAware, UptimeMS: 1000, ConsumedTotal: 10},
		JobStats{FanoutMode: ModeRouteAware, UptimeMS: 2000, ConsumedTotal: 9},
	); err == nil {
		t.Fatal("counter decrease must invalidate the run")
	}
}

func TestDiffNoConsumedKeepsRatiosNA(t *testing.T) {
	r, err := Diff(
		JobStats{FanoutMode: ModeRouteAware, UptimeMS: 1000},
		JobStats{FanoutMode: ModeRouteAware, UptimeMS: 2000},
	)
	if err != nil {
		t.Fatal(err)
	}
	if r.RPCPerConsumedMessage != nil || r.CandidatesPerConsumed != nil {
		t.Fatal("per-message ratios must remain N/A when consumed delta is zero")
	}
	if len(r.Notes) == 0 {
		t.Fatal("N/A reason should be recorded")
	}
}

func TestFetchJobStats(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"server_id":"job","uptime_ms":1234,"fanout_mode":"route_aware","consumed_total":9,"push_ok_total":4}`))
	}))
	defer srv.Close()

	got, err := FetchJobStats(context.Background(), srv.Client(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if got.FanoutMode != ModeRouteAware || got.ConsumedTotal != 9 || got.PushOKTotal != 4 {
		t.Fatalf("unexpected stats: %+v", got)
	}
}
