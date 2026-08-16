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

export const api = {
  overview: () => getJSON<Overview>("/api/overview"),
  services: () => getJSON<ServicesResp>("/api/services"),
  topology: () => getJSON<TopologyResp>("/api/topology"),
  events: (limit = 100) => getJSON<EventsResp>(`/api/events?limit=${limit}`),
  rooms: () => getJSON<RoomsResp>("/api/rooms"),
  roomDetail: (id: string) => getJSON<RoomDetailResp>(`/api/rooms/${encodeURIComponent(id)}`),
  traces: (limit = 50) => getJSON<TracesResp>(`/api/traces?limit=${limit}`),
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
