package fanoutevidence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	ModeBroadcastAll = "broadcast_all"
	ModeRouteAware   = "route_aware"
)

// JobStats is the subset of Job /api/v1/stats used by the fan-out experiment.
// All counters are process-cumulative; experiment evidence MUST be derived from
// a before/after delta rather than treating one snapshot as a run result.
type JobStats struct {
	ServerID               string `json:"server_id"`
	UptimeMS               int64  `json:"uptime_ms"`
	FanoutMode             string `json:"fanout_mode"`
	ConsumedTotal          int64  `json:"consumed_total"`
	PushOKTotal            int64  `json:"push_ok_total"`
	PushErrTotal           int64  `json:"push_err_total"`
	DeliveredTotal         int64  `json:"delivered_total"`
	RouteResolveErrTotal   int64  `json:"route_resolve_err_total"`
	RouteCandidatesTotal   int64  `json:"route_candidates_total"`
	RouteRPCTotal          int64  `json:"route_rpc_total"`
	RouteMissingCometTotal int64  `json:"route_missing_comet_total"`
}

// CounterDelta is the work attributable to one measured workload window.
type CounterDelta struct {
	ConsumedMessages   int64 `json:"consumed_messages"`
	InternalRPCs       int64 `json:"internal_rpcs"`
	SuccessfulRPCs     int64 `json:"successful_rpcs"`
	FailedRPCs         int64 `json:"failed_rpcs"`
	Delivered          int64 `json:"delivered"`
	RouteResolveErrors int64 `json:"route_resolve_errors"`
	RouteCandidates    int64 `json:"route_candidates"`
	RouteRPCs          int64 `json:"route_rpcs"`
	RouteMissingComets int64 `json:"route_missing_comets"`
}

// Report is an evidence record for one fan-out mode. Ratios are pointers so an
// unmeasured denominator (for example no consumed messages) remains null/N/A.
type Report struct {
	FanoutMode               string       `json:"fanout_mode"`
	Before                   JobStats     `json:"before"`
	After                    JobStats     `json:"after"`
	Delta                    CounterDelta `json:"delta"`
	RPCPerConsumedMessage    *float64     `json:"rpc_per_consumed_message"`
	CandidatesPerConsumed    *float64     `json:"route_candidates_per_consumed_message"`
	RPCSuccessRate           *float64     `json:"rpc_success_rate"`
	RouteMissingPerCandidate *float64     `json:"route_missing_per_candidate"`
	Notes                    []string     `json:"notes,omitempty"`
}

// Artifact combines the Job delta with the exact controlled loadtest report and
// invocation metadata. LoadtestReport is intentionally RawMessage: the loadtest
// remains the source of truth for its schema and this package does not duplicate
// or reinterpret latency/delivery fields.
type Artifact struct {
	SchemaVersion  int             `json:"schema_version"`
	GeneratedAt    time.Time       `json:"generated_at"`
	JobStatsURL    string          `json:"job_stats_url"`
	Settle         string          `json:"settle"`
	Workload       Workload        `json:"workload"`
	Fanout         Report          `json:"fanout"`
	LoadtestReport json.RawMessage `json:"loadtest_report"`
}

type Workload struct {
	Server        string  `json:"server"`
	Connections   int     `json:"connections"`
	Rooms         int     `json:"rooms"`
	Rate          float64 `json:"rate"`
	Duration      string  `json:"duration"`
	Warmup        string  `json:"warmup,omitempty"`
	Distribution  string  `json:"distribution"`
	ZipfS         float64 `json:"zipf_s,omitempty"`
	Seed          int64   `json:"seed"`
	DeliveryCheck bool    `json:"delivery_check"`
}

func FetchJobStats(ctx context.Context, client *http.Client, url string) (JobStats, error) {
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Second}
	}
	if strings.TrimSpace(url) == "" {
		return JobStats{}, errors.New("job stats URL is required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return JobStats{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return JobStats{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return JobStats{}, fmt.Errorf("job stats returned HTTP %d", resp.StatusCode)
	}
	var stats JobStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return JobStats{}, fmt.Errorf("decode job stats: %w", err)
	}
	if stats.FanoutMode != ModeBroadcastAll && stats.FanoutMode != ModeRouteAware {
		return JobStats{}, fmt.Errorf("unknown job fanout_mode %q", stats.FanoutMode)
	}
	return stats, nil
}

func Diff(before, after JobStats) (Report, error) {
	if before.FanoutMode == "" || after.FanoutMode == "" {
		return Report{}, errors.New("fanout_mode is required in both snapshots")
	}
	if before.FanoutMode != after.FanoutMode {
		return Report{}, fmt.Errorf("fanout mode changed during run: %s -> %s", before.FanoutMode, after.FanoutMode)
	}
	if after.UptimeMS < before.UptimeMS {
		return Report{}, fmt.Errorf("job restarted during run: uptime_ms %d -> %d", before.UptimeMS, after.UptimeMS)
	}

	pairs := []struct {
		name          string
		before, after int64
	}{
		{"consumed_total", before.ConsumedTotal, after.ConsumedTotal},
		{"push_ok_total", before.PushOKTotal, after.PushOKTotal},
		{"push_err_total", before.PushErrTotal, after.PushErrTotal},
		{"delivered_total", before.DeliveredTotal, after.DeliveredTotal},
		{"route_resolve_err_total", before.RouteResolveErrTotal, after.RouteResolveErrTotal},
		{"route_candidates_total", before.RouteCandidatesTotal, after.RouteCandidatesTotal},
		{"route_rpc_total", before.RouteRPCTotal, after.RouteRPCTotal},
		{"route_missing_comet_total", before.RouteMissingCometTotal, after.RouteMissingCometTotal},
	}
	for _, p := range pairs {
		if p.after < p.before {
			return Report{}, fmt.Errorf("job counter %s decreased (%d -> %d); process reset/restart makes this run invalid", p.name, p.before, p.after)
		}
	}

	d := CounterDelta{
		ConsumedMessages:   after.ConsumedTotal - before.ConsumedTotal,
		SuccessfulRPCs:     after.PushOKTotal - before.PushOKTotal,
		FailedRPCs:         after.PushErrTotal - before.PushErrTotal,
		Delivered:          after.DeliveredTotal - before.DeliveredTotal,
		RouteResolveErrors: after.RouteResolveErrTotal - before.RouteResolveErrTotal,
		RouteCandidates:    after.RouteCandidatesTotal - before.RouteCandidatesTotal,
		RouteRPCs:          after.RouteRPCTotal - before.RouteRPCTotal,
		RouteMissingComets: after.RouteMissingCometTotal - before.RouteMissingCometTotal,
	}
	d.InternalRPCs = d.SuccessfulRPCs + d.FailedRPCs

	r := Report{FanoutMode: before.FanoutMode, Before: before, After: after, Delta: d}
	if d.ConsumedMessages > 0 {
		v := float64(d.InternalRPCs) / float64(d.ConsumedMessages)
		r.RPCPerConsumedMessage = &v
		if before.FanoutMode == ModeRouteAware {
			c := float64(d.RouteCandidates) / float64(d.ConsumedMessages)
			r.CandidatesPerConsumed = &c
		}
	} else {
		r.Notes = append(r.Notes, "no Kafka messages were consumed during the measurement window; per-message ratios are N/A")
	}
	if d.InternalRPCs > 0 {
		v := float64(d.SuccessfulRPCs) / float64(d.InternalRPCs)
		r.RPCSuccessRate = &v
	}
	if before.FanoutMode == ModeRouteAware && d.RouteCandidates > 0 {
		v := float64(d.RouteMissingComets) / float64(d.RouteCandidates)
		r.RouteMissingPerCandidate = &v
	}
	if before.FanoutMode == ModeRouteAware && d.RouteRPCs != d.InternalRPCs {
		r.Notes = append(r.Notes, fmt.Sprintf("route_rpc delta (%d) differs from total PushEnvelope attempts inferred from push counters (%d)", d.RouteRPCs, d.InternalRPCs))
	}
	if before.FanoutMode == ModeBroadcastAll {
		r.Notes = append(r.Notes, "route-specific fields are counters from the same Job process but are not interpreted for broadcast_all mode")
	}
	return r, nil
}
