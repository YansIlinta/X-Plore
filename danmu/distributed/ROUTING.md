# Phase 3 — Route-aware Delivery

Phase 3 evolves X-Plore's distributed Danmu path from **broadcast every room batch to every Comet** into an optional **route-aware** path that sends delivery RPCs only to live Comets that currently hold subscribers for the target channel.

The legacy path remains the default and is intentionally preserved as the experimental baseline.

## Modes

Job supports two fan-out modes:

```text
-fanout-mode=broadcast_all   # default; legacy baseline
-fanout-mode=route_aware     # Redis RouteStore + PushEnvelope
```

`broadcast_all` keeps the existing behavior:

```text
Kafka -> Job -> every live Comet -> PushRoom -> local room filter/broadcast
```

`route_aware` uses:

```text
WebSocket connect/disconnect
        -> Comet Hub lifecycle hooks
        -> LeaseManager
        -> Redis RouteStore

Kafka -> Job
        -> TargetResolver(channel)
        -> Redis: channel -> advertised Comet endpoint(s)
        -> intersect with live etcd Comet pool
        -> PushEnvelope only to resolved live Comets
        -> Hub.DeliverEnvelope(channel)
```

## Why Redis and etcd are separate

They carry different kinds of state:

- **etcd** remains service discovery: which Comet gRPC endpoints are live and dialable.
- **Redis RouteStore** carries high-churn connection routing state: which user/device/channel currently maps to which advertised Comet endpoint.

Job never trusts Redis alone to resurrect a stale endpoint. RouteStore candidates are intersected with the current etcd-backed connection pool; a route pointing at a Comet that has disappeared from etcd is skipped.

## Lease model

The lease unit is a connection, not a coarse `user -> gateway` pair:

```text
ConnectionRoute
  connection_id
  user_id
  device_id
  gateway endpoint
  channel_ids[]
```

This matters for multi-device and multi-tab semantics. If two connections for the same user live on one Comet, removing one connection must not remove the route while the other remains.

Comet does not perform Redis I/O synchronously inside `Hub.AddClient` / `Hub.RemoveClient`. The Hub emits optional lifecycle hooks; `LeaseManager` records Track/Untrack in memory and one worker performs Store I/O. Active leases are periodically refreshed. Redis TTL is the crash-recovery bound.

Redis write failures are retained for periodic retry and deliberately do **not** immediately re-wake the worker, avoiding a hot retry loop during a Redis outage.

## Configuration

Routing is opt-in. Without `DANMU_ROUTE_REDIS_ADDR`, Comet installs no RouteStore hooks and the legacy path is unchanged.

Environment variables:

```text
DANMU_ROUTE_REDIS_ADDR=127.0.0.1:6379
DANMU_ROUTE_REDIS_PASSWORD=
DANMU_ROUTE_REDIS_DB=0
DANMU_ROUTE_REDIS_PREFIX=realtime:route:
DANMU_ROUTE_TTL=30s
```

When routing is explicitly enabled, Comet and route-aware Job ping Redis during setup. An unreachable configured RouteStore is a startup error rather than a silent fallback.

For multi-host deployment, Comet's `-advertise` value must be a gRPC endpoint Job can actually dial. Phase 3 stores that advertised endpoint in RouteStore. `Client.GatewayID` remains the logical Comet instance ID used for local observability.

## Observability

Job exposes the selected `fanout_mode` and route-aware counters including:

```text
route_resolve_err_total
route_candidates_total
route_rpc_total
route_missing_comet_total
```

The key comparison metric for the next experiment is RPC amplification:

```text
internal delivery RPCs / logical room batches
```

For the legacy path this tends toward the number of live Comets per room batch. For route-aware delivery it should track only Comets that actually hold subscribers. This is a hypothesis to measure, not a performance claim.

## Verification status

Verified in GitHub Actions:

- module metadata normalization
- `go test ./...`
- `go vet ./...`
- protobuf regeneration and regenerated-tree compile
- RouteStore unit tests and deterministic MemoryStore tests
- route-aware Job test with two live in-memory gRPC Gateways where only the resolved Gateway receives `PushEnvelope`
- real Redis service integration for `RedisStore`
- real Redis control-plane integration: `Hub.AddClient -> lifecycle hook -> LeaseManager -> Redis route lookup`, followed by `Hub.RemoveClient -> route disappears`

Not yet verified as a deployed end-to-end claim:

- a full `Kafka -> Job(route_aware) -> Redis route resolution -> multiple real Comets -> WebSocket clients` run
- performance improvement over `broadcast_all`
- failure behavior under Redis partitions at sustained load
- route convergence latency at connection churn scale

Those belong in the experiment/evidence layer before any performance or scalability claim is upgraded.

## Reliability boundary

Phase 3 remains **EPHEMERAL** delivery. `DeliveryReliable` exists in the generic schema but is not a durability guarantee. Reliable messaging still requires persistence, idempotent client message IDs, sequence allocation, durable server acknowledgement, device delivery ACKs, and offline replay/sync.
