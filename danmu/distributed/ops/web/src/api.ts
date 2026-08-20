// API 类型与请求封装。所有字段与 ops 后端 JSON 一一对应。
// 数据真实性：后端拿不到的值是 null，前端一律渲染 N/A，不做任何填充。

import React from "react";

export type Health = "healthy" | "degraded" | "critical";

export interface Overview {
  mock: boolean;
  ts: string;
  health: Health;
  health_detail: string[];
  active_connections: number | null;
  active_rooms: number | null;
  msg_in_rate: number | null;
  msg_out_rate: number | null;
  comet_instances: { total: number; healthy: number };
  kafka: KafkaInfo;
}

export interface Instance {
  http_addr: string;
  rpc_addr?: string;
  healthy: boolean;
  err?: string;
  msg_in_rate: number | null;
  msg_out_rate: number | null;
  rates?: Record<string, number>; // *_total 计数器差分速率（key 去掉 _total）
  stats: Record<string, unknown> | null;
}

export interface KafkaInfo {
  available: boolean;
  topic?: string;
  partitions?: number;
  lag: Record<string, number | null>;
  produced_rate: number | null;
  consumed_rate: number | null;
  err?: string;
}

export interface RoomView {
  room_id: string;
  online_count: number;
  comets: string[];
  is_active: boolean;
}

export interface RoomsResp {
  mock: boolean;
  ts: string;
  partial: boolean;
  total: number;
  rooms: RoomView[];
}

export interface RoomDetailResp {
  mock: boolean;
  ts: string;
  partial: boolean;
  room: RoomView | null;
}

export interface TraceSpan {
  msg_id: string;
  hop: string;
  node: string;
  ts_nano: number;
  room_id?: string;
  detail?: string;
}

export interface Trace {
  msg_id: string;
  room_id?: string;
  spans: TraceSpan[];
  duration_ms: number;
  complete: boolean;
  missing_hops?: string[];
}

export interface TracesResp {
  mock: boolean;
  ts: string;
  sources: Record<string, unknown>;
  traces: Trace[];
}

export interface LoadtestStatus {
  available: boolean;
  running: boolean;
  params: Record<string, unknown> | null;
  started_at: string;
  elapsed_s: number | null;
  latest: Record<string, number> | null;
  report: {
    summary?: Record<string, unknown>;
    snapshots?: Record<string, unknown>[];
  } | null;
  err: string;
}

export interface ServicesResp {
  mock: boolean;
  ts: string;
  services: { name: string; instances: Instance[] }[];
}

export interface TopologyResp {
  mock: boolean;
  ts: string;
  nodes: { id: string; kind: string; label: string; healthy: boolean | null }[];
  edges: { from: string; to: string; kind: string }[];
}

export interface OpsEvent {
  ts: string;
  level: "INFO" | "WARNING" | "ERROR";
  kind: string;
  message: string;
}

export interface EventsResp {
  mock: boolean;
  ts: string;
  events: OpsEvent[];
}

async function getJSON<T>(path: string): Promise<T> {
  const resp = await fetch(path);
  if (!resp.ok) throw new Error(`${path}: ${resp.status}`);
  return (await resp.json()) as T;
}

// ---- Realtime Systems Lab：实验 / 对比 / 证据 / Sweep / Regime ----

export type ExpStatus = "created" | "running" | "completed" | "partial" | "failed" | "stopped";
export type RunStatus = "running" | "completed" | "failed" | "stopped";
export type Architecture = "monolith" | "distributed";
export type WorkloadDist = "uniform" | "hot_room" | "zipf";

export interface Workload {
  connections: number;
  rooms: number;
  message_rate: number;
  duration: string;
  target: string;
  distribution?: WorkloadDist;
  zipf_s?: number;
  seed?: number;
}

export interface EnvironmentSnapshot {
  git_commit: string | null;
  git_dirty: boolean | null;
  go_version: string;
  os: string;
  arch: string;
  cpu_cores: number;
  memory_bytes: number | null;
  hostname: string | null;
}

export interface DistributedSnapshot {
  comet_total: number;
  comet_healthy: number;
  logic_total: number;
  job_total: number;
  etcd_up: boolean;
  health: string;
  free_text?: string;
}

export interface ExperimentResult {
  connections_requested: number | null;
  connections_established: number | null;
  connections_failed: number | null;
  messages_sent: number | null;
  messages_received: number | null;
  write_errors: number | null;
  read_errors: number | null;
  drops: number | null;
  missing_deliveries?: number | null;
  expected_deliveries?: number | null;
  delivery_rate?: number | null;
  p50_latency_us: number | null;
  p90_latency_us: number | null;
  p99_latency_us: number | null;
  max_latency_us: number | null;
  send_rate: number | null;
  receive_rate: number | null;
  kafka_available: boolean | null;
  kafka_lag: number | null;
  etcd_up: boolean | null;
  trace_samples: number | null;
  trace_completion_rate: number | null;
  service_snapshot: DistributedSnapshot | null;
  representative_traces?: unknown[];
  notes?: string[];
}

export interface SystemConfig {
  batch_size?: number;
  batch_timeout?: string;
  workers?: number;
  requires_restart?: boolean;
}

export interface MetricAggregate {
  key: string;
  label: string;
  unit: string;
  group: string;
  total_rep: number;
  samples: number;
  measured: boolean;
  mean?: number | null;
  median?: number | null;
  min?: number | null;
  max?: number | null;
  stddev?: number | null;
  cv?: number | null;
  ci95_low?: number | null;
  ci95_high?: number | null;
  ci_status?: string;
}

export interface ExperimentAggregate {
  generated_at: string;
  successful_repetitions: number;
  failed_repetitions: number;
  stopped_repetitions: number;
  total_repetitions: number;
  status: string;
  metrics: Record<string, MetricAggregate>;
  stability: string;
  stability_note: string;
}

export interface WorkloadDiagnostics {
  distribution: string;
  rooms: number;
  connections: number;
  largest_room_share: number;
  top_10_percent_room_share: number;
  mean_room_size: number;
  max_room_size: number;
  min_room_size: number;
  room_sizes?: number[];
}

export interface ResourceSample {
  t: number;
  goroutines?: number;
  heap_mb?: number;
  rss_mb?: number;
  cpu_pct?: number;
  gc_per_min?: number;
}

export interface ResourceSummary {
  sampled: boolean;
  first_sample?: string | null;
  last_sample?: string | null;
  sample_count: number;
  unavailable_reason?: string;
  cpu_pct_mean?: number | null;
  cpu_pct_peak?: number | null;
  rss_mb_mean?: number | null;
  rss_mb_peak?: number | null;
  heap_mb_mean?: number | null;
  goroutines_mean?: number | null;
  gc_total?: number | null;
  gc_pause_ms?: number | null;
  samples?: ResourceSample[];
}

export interface ExperimentRun {
  id: string;
  index: number;
  status: RunStatus;
  started_at: string | null;
  finished_at: string | null;
  measurement_start: string | null;
  measurement_end: string | null;
  warmup_duration?: string;
  measurement_duration?: string;
  environment?: EnvironmentSnapshot | null;
  workload_diagnostics?: WorkloadDiagnostics | null;
  resource?: ResourceSummary | null;
  result: ExperimentResult;
  error?: string;
}

export interface ExperimentSpec {
  architecture: Architecture;
  regime?: string;
  config_label?: string;
  workload: Workload;
  system?: SystemConfig;
  warmup: string;
  duration: string;
  repetitions: number;
}

export interface Experiment {
  id: string;
  name: string;
  architecture: Architecture;
  preset: string;
  status: ExpStatus;
  workload: Workload;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  environment: EnvironmentSnapshot | null;
  result: ExperimentResult;
  error?: string;
  schema_version?: number;
  regime?: string;
  config_label?: string;
  warmup?: string;
  duration?: string;
  repetitions?: number;
  spec?: ExperimentSpec;
  spec_hash?: string;
  system_config?: SystemConfig;
  sweep_id?: string;
  runs?: ExperimentRun[];
  aggregate?: ExperimentAggregate | null;
}

export interface ExperimentLive {
  running: boolean;
  params?: Record<string, unknown>;
  started_at?: string;
  elapsed_s?: number | null;
  latest?: Record<string, number> | null;
  repetition?: number;
  repetitions?: number;
}

export interface ExperimentDetailResp {
  experiment: Experiment;
  live?: ExperimentLive;
}
export interface ExperimentListResp {
  experiments: Experiment[];
  active_id: string | null;
}
export interface ExperimentReportResp {
  experiment: Experiment;
  claims: Claim[];
}

export interface Preset {
  name: string;
  label: string;
  description: string;
  question: string;
  architecture: Architecture;
  workload: Workload;
}
export interface PresetsResp {
  presets: Preset[];
}

export type ClaimStatus = "VERIFIED" | "PARTIALLY VERIFIED" | "CODE VERIFIED" | "TARGET" | "UNKNOWN";
export interface Claim {
  id: string;
  claim: string;
  status: ClaimStatus;
  evidence: string[];
  experiment_id: string | null;
  environment?: EnvironmentSnapshot | null;
  commit: string | null;
  date: string | null;
  notes: string;
}
export interface EvidenceResp {
  claims: Claim[];
  statuses: ClaimStatus[];
}

export interface CompareRow {
  metric: string;
  label: string;
  unit: string;
  group: string;
  direction: string;
  left: number | null;
  right: number | null;
  delta: number | null;
  delta_pct: number | null;
  verdict: string;
}
export interface CompareRef {
  id: string;
  name: string;
  architecture: string;
  preset: string;
  status: string;
  workload: Workload;
  started_at: string | null;
  finished_at: string | null;
}
export interface CompareResp {
  left: CompareRef;
  right: CompareRef;
  rows: CompareRow[];
  summary: string[];
  net: string;
}

// ---- Sweep / Regime（Phase 1.5）----

export type SweepStatus = "created" | "running" | "completed" | "failed" | "stopped" | "partial";

export interface SweepParam {
  name: string;
  values: string[];
}

export interface SweepUnit {
  config_idx: number;
  label: string;
  regime: string;
  experiment_id?: string;
  done: boolean;
  status?: string;
}

export interface SweepConfigResult {
  regime: string;
  config: string;
  config_index: number;
  experiment_id: string;
  status: string;
  repetitions?: number;
  success_reps?: number;
  throughput?: MetricAggregate | null;
  p99?: MetricAggregate | null;
  p90?: MetricAggregate | null;
  delivery_rate?: MetricAggregate | null;
  cpu?: MetricAggregate | null;
  best?: boolean;
  error?: string;
}

export interface BestConfig {
  config: string;
  config_index: number;
  experiment_id: string;
  score: number;
  feasible: boolean;
  throughput: number;
  p99: number;
  delivery_rate: number;
}

export interface DominationResult {
  one_config_dominates: boolean;
  dominant_config?: string;
  static_optimum_shifts: boolean;
  conclusion: string;
}

export interface AdaptiveGateResult {
  go: boolean;
  verdict: "GO" | "NOT YET JUSTIFIED";
  condition_a_shifts: boolean;
  condition_b_improves: boolean;
  condition_c_low_variance: boolean;
  condition_d_tunable_param: boolean;
  evidence: string[];
}

export interface SweepReport {
  generated_at: string;
  regimes: string[];
  configs: string[];
  best_per_regime: Record<string, BestConfig>;
  domination: DominationResult;
  adaptive_gate: AdaptiveGateResult;
}

export interface Sweep {
  id: string;
  name: string;
  status: SweepStatus;
  architecture: Architecture;
  regimes: string[];
  params: SweepParam[];
  repetitions: number;
  warmup: string;
  duration: string;
  target: string;
  config_count: number;
  total_runs: number;
  max_configs: number;
  max_total_runs: number;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  error?: string;
  plan: SweepUnit[];
  results?: SweepConfigResult[];
  report?: SweepReport | null;
}

export interface RegimeInfo {
  name: string;
  label: string;
  description: string;
  workload: Workload;
  target: string;
}

export interface RankingObjective {
  primary: "throughput" | "p99" | "delivery_rate";
  maximize: boolean;
  p99_max_us: number;
  delivery_min: number;
  cpu_max: number;
}

export const api = {
  overview: () => getJSON<Overview>("/api/overview"),
  services: () => getJSON<ServicesResp>("/api/services"),
  topology: () => getJSON<TopologyResp>("/api/topology"),
  events: (limit = 100) => getJSON<EventsResp>(`/api/events?limit=${limit}`),
  rooms: () => getJSON<RoomsResp>("/api/rooms"),
  roomDetail: (id: string) => getJSON<RoomDetailResp>(`/api/rooms/${encodeURIComponent(id)}`),
  traces: (limit = 50) => getJSON<TracesResp>(`/api/traces?limit=${limit}`),
  presets: () => getJSON<PresetsResp>("/api/presets"),
  experiments: (limit = 100) => getJSON<ExperimentListResp>(`/api/experiments?limit=${limit}`),
  createExperiment: (body: {
    name?: string;
    architecture: Architecture;
    preset: string;
    regime?: string;
    warmup?: string;
    duration?: string;
    repetitions?: number;
    workload: Workload;
  }) =>
    fetch("/api/experiments", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).then(async (r) => {
      const j = (await r.json()) as { experiment?: Experiment; error?: string };
      if (!r.ok) throw new Error(j.error ?? `HTTP ${r.status}`);
      return j.experiment!;
    }),
  experimentDetail: (id: string) => getJSON<ExperimentDetailResp>(`/api/experiments/${encodeURIComponent(id)}`),
  experimentReport: (id: string) => getJSON<ExperimentReportResp>(`/api/experiments/${encodeURIComponent(id)}/report`),
  experimentStart: (id: string) =>
    fetch(`/api/experiments/${encodeURIComponent(id)}/start`, { method: "POST" }).then(async (r) => {
      if (!r.ok) throw new Error(((await r.json()) as { error?: string }).error ?? `HTTP ${r.status}`);
    }),
  experimentStop: (id: string) =>
    fetch(`/api/experiments/${encodeURIComponent(id)}/stop`, { method: "POST" }).then(async (r) => {
      if (!r.ok) throw new Error(((await r.json()) as { error?: string }).error ?? `HTTP ${r.status}`);
    }),
  compare: (left: string, right: string) =>
    getJSON<CompareResp>(`/api/compare?left=${encodeURIComponent(left)}&right=${encodeURIComponent(right)}`),
  evidence: () => getJSON<EvidenceResp>("/api/evidence"),
  regimes: () => getJSON<{ regimes: RegimeInfo[]; objective: RankingObjective }>("/api/regimes"),
  sweeps: (limit = 50) => getJSON<{ sweeps: Sweep[]; active_id: string | null }>(`/api/sweeps?limit=${limit}`),
  createSweep: (body: {
    name?: string;
    architecture: Architecture;
    regimes?: string[];
    params: SweepParam[];
    repetitions?: number;
    warmup?: string;
    duration?: string;
    target?: string;
  }) =>
    fetch("/api/sweeps", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).then(async (r) => {
      const j = (await r.json()) as { sweep?: Sweep; error?: string };
      if (!r.ok) throw new Error(j.error ?? `HTTP ${r.status}`);
      return j.sweep!;
    }),
  sweepDetail: (id: string) => getJSON<{ sweep: Sweep; running: boolean }>(`/api/sweeps/${encodeURIComponent(id)}`),
  sweepReport: (id: string) =>
    getJSON<{ sweep: Sweep; report: SweepReport | null; active: boolean }>(`/api/sweeps/${encodeURIComponent(id)}/report`),
  sweepStart: (id: string) =>
    fetch(`/api/sweeps/${encodeURIComponent(id)}/start`, { method: "POST" }).then(async (r) => {
      if (!r.ok) throw new Error(((await r.json()) as { error?: string }).error ?? `HTTP ${r.status}`);
    }),
  sweepStop: (id: string) =>
    fetch(`/api/sweeps/${encodeURIComponent(id)}/stop`, { method: "POST" }).then(async (r) => {
      if (!r.ok) throw new Error(((await r.json()) as { error?: string }).error ?? `HTTP ${r.status}`);
    }),
  regimeAnalysis: () => getJSON<{ objective: RankingObjective; report: SweepReport | null }>("/api/regime-analysis"),
  loadtestStatus: () => getJSON<LoadtestStatus>("/api/loadtest/status"),
  loadtestStart: (params: Record<string, unknown>) =>
    fetch("/api/loadtest/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    }).then(async (r) => {
      if (!r.ok) throw new Error(((await r.json()) as { error?: string }).error ?? `HTTP ${r.status}`);
    }),
  loadtestStop: () => fetch("/api/loadtest/stop", { method: "POST" }).then((r) => r.ok),
};

// usePoll：按间隔轮询一个 API；组件卸载即停。
export function usePoll<T>(fn: () => Promise<T>, intervalMs: number): { data: T | null; error: string | null } {
  const [data, setData] = React.useState<T | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const fnRef = React.useRef(fn);
  fnRef.current = fn;
  React.useEffect(() => {
    let cancelled = false;
    let timer: number | undefined;
    const tick = async () => {
      try {
        const d = await fnRef.current();
        if (!cancelled) {
          setData(d);
          setError(null);
        }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
      if (!cancelled) timer = window.setTimeout(tick, intervalMs);
    };
    tick();
    return () => {
      cancelled = true;
      if (timer) window.clearTimeout(timer);
    };
  }, [intervalMs]);
  return { data, error };
}
