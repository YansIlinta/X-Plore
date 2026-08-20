# Phase 3.5 — Fan-out Evidence Run

This document defines the first reproducible evidence protocol for comparing X-Plore's two distributed delivery modes:

```text
broadcast_all  — legacy Job -> every live Comet -> PushRoom
route_aware    — Job -> Redis RouteStore -> live target Comets -> PushEnvelope
```

The goal is to measure the trade-off, not to assume the new architecture is faster.

## Evidence rule: delta, never cumulative snapshots

Job `/api/v1/stats` counters are process-cumulative. A value such as:

```json
{"push_ok_total": 120000}
```

is **not** a result for the current experiment.

`cmd/fanoutevidence` therefore takes a fresh Job snapshot immediately before the controlled workload and another snapshot after the workload plus a configurable settle window. Only `after - before` is written as run evidence.

The run is rejected if:

- Job `fanout_mode` changes during the workload;
- Job uptime decreases (restart);
- any cumulative counter decreases (counter reset/restart);
- the Job stats endpoint or loadtest report is unavailable/invalid.

Unmeasured ratios remain `null` / N/A. They are never replaced with zero.

## What is measured now

The artifact contains both the exact existing loadtest JSON and Job counter deltas.

Fan-out metrics:

```text
consumed_messages
internal_rpcs              = successful_rpcs + failed_rpcs
rpc_per_consumed_message   = internal_rpcs / consumed_messages
successful_rpcs
failed_rpcs
delivered
route_resolve_errors
route_candidates
route_rpcs
route_missing_comets
route_candidates_per_consumed_message   (route_aware only)
route_missing_per_candidate             (route_aware only)
rpc_success_rate
```

Loadtest JSON remains the source of truth for:

```text
connections established/failed
messages sent/received
E2E P50/P90/P99/max
room distribution diagnostics
delivery-check observed/missing/expected/rate
```

### Important denominator boundary

`rpc_per_consumed_message` is an exact ratio for the counters currently exposed by Job, but it is **not** the same as `RPC / logical room batch` because Job may batch multiple Kafka messages for one room within the 10 ms flush window.

Do not label it `RPC amplification per batch` in README/Evidence yet. A future isolated Job instrumentation change should expose `logical_batches_total`; only then can the stronger metric be computed directly. The current runner intentionally records the exact available denominator rather than inferring batch count from traffic shape.

## Build

From the repository root:

```bash
cd danmu/monolith
go build -o bin/loadtest ./loadtest/

cd ../distributed
go build -o bin/fanoutevidence ./cmd/fanoutevidence/
go build -o bin/fanoutcompare ./cmd/fanoutcompare/
```

## Fair A/B setup

For a clean architecture comparison, keep everything except Job fan-out mode constant:

- same Git commit;
- same Comet/Logic/Kafka/etcd/Redis versions and topology;
- same number of Comets;
- same loadtest target list;
- same connections / rooms / rate / duration / distribution / seed;
- same `-settle` window;
- same trace configuration;
- same machine/resource limits.

**Enable Comet RouteStore leases in both A and B.** This avoids changing the Comet connection lifecycle between runs and isolates the experiment primarily to Job's delivery selection path.

Example Comet environment for both modes:

```bash
export DANMU_ROUTE_REDIS_ADDR=127.0.0.1:6379
export DANMU_ROUTE_REDIS_PREFIX=realtime:route:
export DANMU_ROUTE_TTL=30s
```

Comet `-advertise` must be a gRPC endpoint the Job can dial.

## Run A — broadcast_all

Start the distributed stack with the Job in the legacy mode:

```text
job ... -fanout-mode=broadcast_all
```

Then run one controlled workload:

```bash
./bin/fanoutevidence \
  -job-stats http://localhost:7420/api/v1/stats \
  -loadtest-bin ../monolith/bin/loadtest \
  -server ws://localhost:8080 \
  -conns 2000 \
  -rooms 1000 \
  -rate 1 \
  -duration 60s \
  -dist uniform \
  -seed 42 \
  -delivery-check \
  -settle 2s \
  -output /tmp/fanout-broadcast-all.json
```

The runner does not start, stop, restart, or reconfigure Job. It measures the mode already reported by Job.

## Run B — route_aware

Restart only the components required by your deployment to switch Job startup mode, keeping the workload and Comet routing configuration unchanged:

```text
DANMU_ROUTE_REDIS_ADDR=127.0.0.1:6379 \
job ... -fanout-mode=route_aware
```

Wait until Comets are live in etcd and their WebSocket connection leases can populate Redis, then execute the **same** workload:

```bash
./bin/fanoutevidence \
  -job-stats http://localhost:7420/api/v1/stats \
  -loadtest-bin ../monolith/bin/loadtest \
  -server ws://localhost:8080 \
  -conns 2000 \
  -rooms 1000 \
  -rate 1 \
  -duration 60s \
  -dist uniform \
  -seed 42 \
  -delivery-check \
  -settle 2s \
  -output /tmp/fanout-route-aware.json
```

## Compare A and B

```bash
./bin/fanoutcompare \
  -left /tmp/fanout-broadcast-all.json \
  -right /tmp/fanout-route-aware.json \
  -output /tmp/fanout-compare.json
```

`fanoutcompare` requires identical workload and settle semantics. If they differ, it emits `comparable=false` and exits with code 2 rather than producing a misleading A/B result.

For comparable artifacts it reports descriptive deltas for:

```text
rpc_per_consumed_message
internal_rpcs
consumed_messages
route_resolve_errors
route_missing_comets
p50 / p90 / p99 E2E latency
messages_received
connections_established
delivery_rate
missing_deliveries
```

`delta = right - left`; `delta_pct` is relative to the left artifact when the left value is non-zero.

One A/B pair is descriptive evidence, **not statistical proof**. Repeat the pair under identical workload/environment before upgrading a performance claim; the existing Systems Lab repetition/aggregate layer remains the intended place for confidence intervals and stability analysis.

## Recommended regimes

Run at least these three controlled regimes before drawing architectural conclusions:

```text
1. Low fan-out
   many rooms, relatively few subscribers per room
   expected to expose broadcast-all RPC amplification most clearly

2. Hot room
   few rooms, many subscribers per room
   route-aware may approach most/all Comets, reducing its routing advantage

3. Gateway scale
   same room/subscriber workload while increasing live Comet count
   tests whether legacy internal RPC work grows with Gateway count
```

A useful fourth run is Zipf room skew, because real realtime workloads are rarely uniform.

## Evidence status

The runner/compare code being present and passing unit/CI checks proves only the **measurement mechanism** is implemented.

It does **not** by itself verify:

- a deployed Kafka -> route-aware Job -> multiple real Comets -> WebSocket E2E run;
- lower latency;
- lower CPU/network usage;
- better scalability;
- reliable delivery semantics.

Those claims remain unverified until artifacts from real controlled runs exist and are attached to the Systems Lab evidence record.
