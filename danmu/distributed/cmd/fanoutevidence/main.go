package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/YansIlinta/danmu-distributed/fanoutevidence"
)

func main() {
	jobStats := flag.String("job-stats", "http://localhost:7420/api/v1/stats", "Job /api/v1/stats URL")
	loadtestBin := flag.String("loadtest-bin", "../monolith/bin/loadtest", "path to the existing X-Plore loadtest binary")
	server := flag.String("server", "ws://localhost:8080", "WebSocket target(s), comma separated")
	conns := flag.Int("conns", 2000, "connections")
	rooms := flag.Int("rooms", 1000, "rooms")
	rate := flag.Float64("rate", 1, "messages per connection per second")
	duration := flag.String("duration", "60s", "measurement duration")
	warmup := flag.String("warmup", "", "optional loadtest warmup duration")
	dist := flag.String("dist", "uniform", "room distribution: uniform, hot_room, zipf")
	zipfS := flag.Float64("zipf-s", 0, "zipf s parameter when dist=zipf")
	seed := flag.Int64("seed", 1, "deterministic loadtest seed")
	deliveryCheck := flag.Bool("delivery-check", true, "enable loadtest sequence-gap delivery accounting")
	token := flag.String("token", envOr("DANMU_AUTH_TOKEN", "danmu-secret-token"), "WebSocket auth token")
	settle := flag.Duration("settle", 2*time.Second, "post-loadtest drain/settle window before the after snapshot")
	output := flag.String("output", "", "artifact JSON path; default fanout-evidence-<mode>-<unix>.json")
	flag.Parse()

	if err := validate(*loadtestBin, *server, *conns, *rooms, *rate, *duration, *warmup, *dist, *zipfS, *settle); err != nil {
		fatal(err)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	before, err := fanoutevidence.FetchJobStats(ctx, client, *jobStats)
	cancel()
	if err != nil {
		fatal(fmt.Errorf("before snapshot: %w", err))
	}

	reportPath := filepath.Join(os.TempDir(), fmt.Sprintf("xplore-fanout-loadtest-%d.json", time.Now().UnixNano()))
	defer os.Remove(reportPath)
	args := []string{
		"-server", *server,
		"-conns", strconv.Itoa(*conns),
		"-rooms", strconv.Itoa(*rooms),
		"-rate", strconv.FormatFloat(*rate, 'f', -1, 64),
		"-duration", *duration,
		"-token", *token,
		"-output-json", reportPath,
		"-dist", *dist,
		"-seed", strconv.FormatInt(*seed, 10),
		"-ramp", "2s",
	}
	if *warmup != "" {
		args = append(args, "-warmup", *warmup)
	}
	if *zipfS > 0 {
		args = append(args, "-zipf-s", strconv.FormatFloat(*zipfS, 'f', -1, 64))
	}
	if *deliveryCheck {
		args = append(args, "-delivery-check")
	}

	cmd := exec.Command(*loadtestBin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fatal(fmt.Errorf("loadtest failed: %w", err))
	}

	loadtestJSON, err := os.ReadFile(reportPath)
	if err != nil {
		fatal(fmt.Errorf("read loadtest report: %w", err))
	}
	if !json.Valid(loadtestJSON) {
		fatal(fmt.Errorf("loadtest report is not valid JSON"))
	}

	if *settle > 0 {
		time.Sleep(*settle)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	after, err := fanoutevidence.FetchJobStats(ctx, client, *jobStats)
	cancel()
	if err != nil {
		fatal(fmt.Errorf("after snapshot: %w", err))
	}

	report, err := fanoutevidence.Diff(before, after)
	if err != nil {
		fatal(fmt.Errorf("fanout evidence invalid: %w", err))
	}

	artifact := fanoutevidence.Artifact{
		SchemaVersion: 1,
		GeneratedAt:   time.Now().UTC(),
		JobStatsURL:   *jobStats,
		Settle:        settle.String(),
		Workload: fanoutevidence.Workload{
			Server: *server, Connections: *conns, Rooms: *rooms, Rate: *rate,
			Duration: *duration, Warmup: *warmup, Distribution: *dist, ZipfS: *zipfS,
			Seed: *seed, DeliveryCheck: *deliveryCheck,
		},
		Fanout:         report,
		LoadtestReport: json.RawMessage(loadtestJSON),
	}
	if *output == "" {
		*output = fmt.Sprintf("fanout-evidence-%s-%d.json", report.FanoutMode, time.Now().Unix())
	}
	data, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(*output, data, 0o644); err != nil {
		fatal(err)
	}

	fmt.Printf("\nfanout evidence: mode=%s consumed=%d internal_rpcs=%d delivered=%d\n",
		report.FanoutMode, report.Delta.ConsumedMessages, report.Delta.InternalRPCs, report.Delta.Delivered)
	if report.RPCPerConsumedMessage != nil {
		fmt.Printf("rpc_per_consumed_message=%.6f\n", *report.RPCPerConsumedMessage)
	} else {
		fmt.Println("rpc_per_consumed_message=N/A")
	}
	if report.CandidatesPerConsumed != nil {
		fmt.Printf("route_candidates_per_consumed_message=%.6f\n", *report.CandidatesPerConsumed)
	}
	fmt.Printf("artifact=%s\n", *output)
}

func validate(bin, server string, conns, rooms int, rate float64, duration, warmup, dist string, zipfS float64, settle time.Duration) error {
	if strings.TrimSpace(bin) == "" {
		return fmt.Errorf("loadtest-bin is required")
	}
	if fi, err := os.Stat(bin); err != nil || fi.IsDir() {
		return fmt.Errorf("loadtest binary not found: %s", bin)
	}
	if conns < 1 || conns > 100000 {
		return fmt.Errorf("conns must be in [1,100000]")
	}
	if rooms < 1 || rooms > 10000 {
		return fmt.Errorf("rooms must be in [1,10000]")
	}
	if rate <= 0 || rate > 10000 {
		return fmt.Errorf("rate must be in (0,10000]")
	}
	for _, target := range strings.Split(server, ",") {
		target = strings.TrimSpace(target)
		if !strings.HasPrefix(target, "ws://") && !strings.HasPrefix(target, "wss://") {
			return fmt.Errorf("server target %q must start with ws:// or wss://", target)
		}
	}
	if _, err := time.ParseDuration(duration); err != nil {
		return fmt.Errorf("invalid duration %q: %w", duration, err)
	}
	if warmup != "" {
		if _, err := time.ParseDuration(warmup); err != nil {
			return fmt.Errorf("invalid warmup %q: %w", warmup, err)
		}
	}
	switch dist {
	case "uniform", "hot_room", "zipf":
	default:
		return fmt.Errorf("dist must be uniform, hot_room or zipf")
	}
	if dist == "hot_room" && rooms < 2 {
		return fmt.Errorf("hot_room requires at least 2 rooms")
	}
	if dist == "zipf" && (zipfS <= 0 || zipfS > 5) {
		return fmt.Errorf("zipf-s must be in (0,5] for zipf")
	}
	if settle < 0 || settle > time.Minute {
		return fmt.Errorf("settle must be in [0,1m]")
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "fanoutevidence:", err)
	os.Exit(1)
}
