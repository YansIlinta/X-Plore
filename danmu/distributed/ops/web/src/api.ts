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

// ---- Realtime Systems Lab：实验 / 对比 / 证据 ----

export type ExpStatus = "created" | "running" | "completed" | "failed" | "stopped";
export type Architecture = "monolith" | "distributed";

export interface Workload {
  connections: number;
  rooms: number;
  message_rate: number;
  duration: string;
  target: string;
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
}

export interface ExperimentLive {
  running: boolean;
  params?: Record<string, unknown>;
  started_at?: string;
  elapsed_s?: number | null;
  latest?: Record<string, number> | null;
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
