import React from "react";
import {
  api,
  usePoll,
  Experiment,
  ExperimentReportResp,
  Architecture,
  Overview,
  ExperimentResult,
} from "../api";
import { fmtNum, fmtRate, fmtTime } from "../format";
import RateChart, { SeriesPoint } from "../components/RateChart";

const MAX_POINTS = 240;

// 空 Result 的缺省值：尚未跑完时所有指标都是 null（N/A）。
const emptyResult: ExperimentResult = {
  connections_requested: null,
  connections_established: null,
  connections_failed: null,
  messages_sent: null,
  messages_received: null,
  write_errors: null,
  read_errors: null,
  drops: null,
  p50_latency_us: null,
  p90_latency_us: null,
  p99_latency_us: null,
  max_latency_us: null,
  send_rate: null,
  receive_rate: null,
  kafka_available: null,
  kafka_lag: null,
  etcd_up: null,
  trace_samples: null,
  trace_completion_rate: null,
  service_snapshot: null,
};

const statusColor: Record<string, string> = {
  completed: "var(--green)",
  running: "#4db2d8",
  failed: "var(--red)",
  stopped: "var(--amber)",
  created: "var(--text-dim)",
};

function StatusBadge({ status }: { status: string }) {
  return (
    <span
      style={{
        color: statusColor[status] ?? "var(--text-dim)",
        border: `1px solid ${statusColor[status] ?? "var(--border)"}`,
        borderRadius: 4,
        padding: "1px 7px",
        fontFamily: "var(--mono)",
        fontSize: 11,
      }}
    >
      {status.toUpperCase()}
    </span>
  );
}

const presetLabel: Record<string, string> = {
  "low-fanout": "Low Fan-out",
  "hot-room": "Hot Room",
  custom: "Custom",
};

// ResultTable 渲染一次实验的结果指标（nil → N/A，绝不填 0）。
function ResultTable({ result }: { result: ExperimentResult }) {
  const rows: [string, string | null][] = [
    ["Connections requested", result.connections_requested !== null ? `${fmtNum(result.connections_requested)}` : null],
    ["Connections established", result.connections_established !== null ? `${fmtNum(result.connections_established)}` : null],
    ["Connections failed", result.connections_failed !== null ? `${fmtNum(result.connections_failed)}` : null],
    ["Messages sent", result.messages_sent !== null ? `${fmtNum(result.messages_sent)}` : null],
    ["Messages received", result.messages_received !== null ? `${fmtNum(result.messages_received)}` : null],
    ["P50 latency", result.p50_latency_us !== null ? `${fmtNum(result.p50_latency_us)} µs` : null],
    ["P90 latency", result.p90_latency_us !== null ? `${fmtNum(result.p90_latency_us)} µs` : null],
    ["P99 latency", result.p99_latency_us !== null ? `${fmtNum(result.p99_latency_us)} µs` : null],
    ["Max latency", result.max_latency_us !== null ? `${fmtNum(result.max_latency_us)} µs` : null],
    ["Write errors", result.write_errors !== null ? `${fmtNum(result.write_errors)}` : null],
    ["Read errors", result.read_errors !== null ? `${fmtNum(result.read_errors)}` : null],
    ["Drops", result.drops !== null ? `${fmtNum(result.drops)}` : null],
    ["Send rate", result.send_rate !== null ? fmtRate(result.send_rate) : null],
    ["Receive rate", result.receive_rate !== null ? fmtRate(result.receive_rate) : null],
  ];
  return (
    <table className="comp-table">
      <tbody>
        {rows.map(([label, value]) => (
          <tr key={label}>
            <td>{label}</td>
            <td style={{ fontFamily: "var(--mono)" }} className={value === null ? "na-cell" : ""}>
              {value ?? "N/A"}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function EnvironmentInfo({ exp }: { exp: Experiment }) {
  const e = exp.environment;
  if (!e) return <div className="hint">Environment not recorded (experiment not started yet).</div>;
  return (
    <div className="env-grid">
      <div>go {e.go_version}</div>
      <div>{e.os}/{e.arch}</div>
      <div>cpu={e.cpu_cores}</div>
      <div>mem={e.memory_bytes !== null ? `${Math.round(e.memory_bytes / 1024 / 1024)}MB` : "N/A"}</div>
      <div>host={e.hostname ?? "N/A"}</div>
      <div>
        git: <span className={e.git_dirty ? "git-dirty" : ""}>{e.git_commit ? e.git_commit.slice(0, 12) : "N/A"}</span>
        {e.git_dirty ? " (dirty)" : ""}
      </div>
    </div>
  );
}

export default function Experiments() {
  const { data: listData, error: listErr } = usePoll(() => api.experiments(100), 2000);
  const { data: presetResp } = usePoll(() => api.presets(), 60000);
  const presets = presetResp?.presets ?? [];

  const [form, setForm] = React.useState({
    name: "",
    preset: "low-fanout",
    architecture: "monolith" as Architecture,
    connections: "2000",
    rooms: "1000",
    rate: "1",
    duration: "60s",
    target: "ws://localhost:8081",
  });
  const [busy, setBusy] = React.useState(false);
  const [formErr, setFormErr] = React.useState<string | null>(null);
  const [selected, setSelected] = React.useState<Experiment | null>(null);
  const [selectedReport, setSelectedReport] = React.useState<ExperimentReportResp | null>(null);

  const experiments: Experiment[] = listData?.experiments ?? [];
  const running = experiments.find((e) => e.status === "running") ?? null;

  // 运行中：1s 轮询详情拿 live 快照
  const { data: detail } = usePoll(
    () => (running ? api.experimentDetail(running.id) : Promise.resolve(null as never)),
    running ? 1000 : 100000,
  );
  const live = detail?.live;

  // 运行中且 distributed：复用 overview 展示旁路观测面
  const { data: overview } = usePoll(
    () => (running && running.architecture === "distributed" ? api.overview() : Promise.resolve(null as never)),
    running && running.architecture === "distributed" ? 2000 : 100000,
  );

  const [points, setPoints] = React.useState<SeriesPoint[]>([]);
  React.useEffect(() => {
    if (!live?.latest) return;
    const l = live.latest;
    setPoints((prev) => {
      const next = [...prev, { t: Date.now() / 1000, a: l.send_qps, b: l.recv_qps }];
      return next.length > MAX_POINTS ? next.slice(next.length - MAX_POINTS) : next;
    });
  }, [live?.latest]);
  React.useEffect(() => {
    if (running) setPoints([]);
  }, [running?.id]);

  const applyPreset = (name: string) => {
    const p = presets.find((pp) => pp.name === name);
    if (!p) return;
    setForm((f) => ({
      ...f,
      preset: p.name,
      architecture: p.architecture,
      connections: String(p.workload.connections),
      rooms: String(p.workload.rooms),
      rate: String(p.workload.message_rate),
      duration: p.workload.duration,
      target: p.workload.target,
    }));
  };

  const runExperiment = async () => {
    setBusy(true);
    setFormErr(null);
    try {
      const wl = {
        connections: Number(form.connections),
        rooms: Number(form.rooms),
        message_rate: Number(form.rate),
        duration: form.duration,
        target: form.target,
      };
      const exp = await api.createExperiment({
        name: form.name || undefined,
        architecture: form.architecture,
        preset: form.preset,
        workload: wl,
      });
      await api.experimentStart(exp.id);
      setSelected(exp);
      setSelectedReport(null);
    } catch (e) {
      setFormErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const stopExperiment = async () => {
    if (!running) return;
    setBusy(true);
    try {
      await api.experimentStop(running.id);
    } catch (e) {
      setFormErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const viewReport = async (id: string) => {
    try {
      const rep = await api.experimentReport(id);
      setSelectedReport(rep);
      setSelected(rep.experiment);
    } catch (e) {
      setFormErr(e instanceof Error ? e.message : String(e));
    }
  };

  const field = (label: string, key: string, width = "100%", mono = true) => (
    <div style={{ display: "flex", flexDirection: "column", gap: 4, width }}>
      <label className="field-label">{label}</label>
      <input
        value={form[key as keyof typeof form] as string}
        onChange={(e) => setForm({ ...form, [key]: e.target.value })}
        disabled={!!running}
        className="field-input"
        style={mono ? { fontFamily: "var(--mono)" } : undefined}
      />
    </div>
  );

  return (
    <>
      {/* ---- 新建实验 ---- */}
      <div className="panel">
        <div className="panel-head">New Experiment — Realtime Systems Lab</div>
        <div className="panel-body" style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }} className="preset-row">
            {presets.map((p) => (
              <button
                key={p.name}
                className={`preset-btn ${form.preset === p.name ? "active" : ""}`}
                onClick={() => applyPreset(p.name)}
                disabled={!!running}
                title={p.description}
              >
                {p.label}
              </button>
            ))}
            <div style={{ marginLeft: "auto", display: "flex", flexDirection: "column", gap: 4 }}>
              <label className="field-label">Architecture</label>
              <select
                value={form.architecture}
                onChange={(e) => setForm({ ...form, architecture: e.target.value as Architecture })}
                disabled={!!running}
                className="field-input"
              >
                <option value="monolith">monolith</option>
                <option value="distributed">distributed</option>
              </select>
            </div>
          </div>
          {(() => {
            const p = presets.find((pp) => pp.name === form.preset);
            return p ? (
              <div className="hint" style={{ marginTop: -6 }}>
                <b>{p.question}</b> — {p.description}
              </div>
            ) : null;
          })()}
          <div style={{ display: "flex", gap: 12, alignItems: "flex-end", flexWrap: "wrap" }}>
            {field("Experiment name", "name", "220px", false)}
            {field("Target", "target", "300px")}
            {field("Conns", "connections", "100px")}
            {field("Rooms", "rooms", "100px")}
            {field("Rate (msg/s/conn)", "rate", "110px")}
            {field("Duration", "duration", "100px")}
            {running ? (
              <button className="btn danger" onClick={stopExperiment} disabled={busy}>
                Stop
              </button>
            ) : (
              <button className="btn" onClick={runExperiment} disabled={busy}>
                {busy ? "…" : "Run Experiment"}
              </button>
            )}
          </div>
          {formErr && <div className="error-banner">{formErr}</div>}
          {listErr && <div className="error-banner">list: {listErr}</div>}
        </div>
      </div>

      {/* ---- 运行中状态 ---- */}
      {running && (
        <>
          <div className="kpi-grid">
            <div className="kpi">
              <div className="kpi-label">Status</div>
              <div className="kpi-value" style={{ color: "#4db2d8", fontSize: 16 }}>
                RUNNING
              </div>
              <div className="kpi-sub">
                started {live?.started_at ? fmtTime(String(live.started_at)) : "…"}
              </div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Connections</div>
              <div className={`kpi-value ${live?.latest ? "" : "na"}`}>
                {live?.latest ? `${fmtNum(live.latest.active_conns)} / ${fmtNum(live.latest.target_conns)}` : "N/A"}
              </div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Send / Recv QPS</div>
              <div className={`kpi-value ${live?.latest ? "" : "na"}`}>
                {live?.latest ? `${fmtNum(live.latest.send_qps)} / ${fmtNum(live.latest.recv_qps)}` : "N/A"}
              </div>
            </div>
            <div className="kpi">
              <div className="kpi-label">E2E p50/p90/p99</div>
              <div className={`kpi-value ${live?.latest ? "" : "na"}`} style={{ fontSize: 15 }}>
                {live?.latest
                  ? `${fmtNum(live.latest.e2e_p50_us)} / ${fmtNum(live.latest.e2e_p90_us)} / ${fmtNum(live.latest.e2e_p99_us)} µs`
                  : "N/A"}
              </div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Errors (w / r)</div>
              <div className={`kpi-value ${live?.latest ? "" : "na"}`}>
                {live?.latest ? `${fmtNum(live.latest.write_errors)} / ${fmtNum(live.latest.read_errors)}` : "N/A"}
              </div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Elapsed</div>
              <div className="kpi-value">
                {live?.elapsed_s !== null && live?.elapsed_s !== undefined ? `${Math.round(live.elapsed_s)}s` : "N/A"}
              </div>
            </div>
          </div>
          <RateChart title={`Loadtest Throughput — ${running.id}`} labelA="send qps" labelB="recv qps" points={points} />
          {running.architecture === "distributed" && <DistributedLive overview={overview} exp={running} />}
        </>
      )}

      {/* ---- 历史 ---- */}
      <div className="panel">
        <div className="panel-head">Recent Experiments</div>
        <div className="panel-body">
          {experiments.length === 0 ? (
            <div className="hint">No experiments yet. Choose a preset above and run one.</div>
          ) : (
            <table className="comp-table">
              <thead>
                <tr>
                  <th>Name / ID</th>
                  <th>Arch</th>
                  <th>Preset</th>
                  <th>Status</th>
                  <th>Workload</th>
                  <th>Established</th>
                  <th>P90</th>
                  <th>Started</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {experiments.map((e) => (
                  <tr key={e.id} className={selected?.id === e.id ? "row-selected" : ""}>
                    <td>
                      <div>{e.name}</div>
                      <div className="mono-dim">{e.id}</div>
                    </td>
                    <td>{e.architecture}</td>
                    <td>{presetLabel[e.preset] ?? e.preset}</td>
                    <td>
                      <StatusBadge status={e.status} />
                    </td>
                    <td className="mono-dim">
                      {e.workload.connections}c/{e.workload.rooms}r @{e.workload.message_rate}/s {e.workload.duration}
                    </td>
                    <td style={{ fontFamily: "var(--mono)" }}>
                      {e.result?.connections_established !== null && e.result?.connections_established !== undefined
                        ? fmtNum(e.result.connections_established)
                        : "N/A"}
                    </td>
                    <td style={{ fontFamily: "var(--mono)" }}>
                      {e.result?.p90_latency_us != null ? `${fmtNum(e.result.p90_latency_us)}µs` : "N/A"}
                    </td>
                    <td className="mono-dim">{e.started_at ? fmtTime(e.started_at) : "—"}</td>
                    <td>
                      <button className="btn small" onClick={() => viewReport(e.id)} disabled={!!running && e.id === running.id}>
                        Report
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* ---- 报告详情 ---- */}
      {selectedReport && (
        <div className="panel">
          <div className="panel-head">Experiment Report — {selectedReport.experiment.id}</div>
          <div className="panel-body" style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
            <div style={{ flex: "1 1 320px", minWidth: 280 }}>
              <div className="panel-sub">Result</div>
              <ResultTable result={selectedReport.experiment.result ?? emptyResult} />
              {selectedReport.experiment.status === "failed" && (
                <div className="error-banner">{selectedReport.experiment.error ?? "failed"}</div>
              )}
              {(selectedReport.experiment.result?.notes?.length ?? 0) > 0 && (
                <div className="hint" style={{ marginTop: 8 }}>
                  {selectedReport.experiment.result!.notes!.map((n) => (
                    <div key={n}>• {n}</div>
                  ))}
                </div>
              )}
            </div>
            <div style={{ flex: "1 1 360px", minWidth: 300 }}>
              <div className="panel-sub">Reproducibility</div>
              <EnvironmentInfo exp={selectedReport.experiment} />
              <div className="mono-dim" style={{ marginTop: 8 }}>
                target: {selectedReport.experiment.workload.target}
                <br />
                created: {selectedReport.experiment.created_at}
                {selectedReport.experiment.finished_at ? (
                  <>
                    <br />
                    finished: {selectedReport.experiment.finished_at}
                  </>
                ) : null}
              </div>
            </div>
            {selectedReport.experiment.result?.service_snapshot && (
              <div style={{ flex: "1 1 320px", minWidth: 280 }}>
                <div className="panel-sub">Distributed snapshot (at finish)</div>
                <div className="mono-dim">
                  comet {selectedReport.experiment.result.service_snapshot.comet_healthy}/
                  {selectedReport.experiment.result.service_snapshot.comet_total} healthy · logic{" "}
                  {selectedReport.experiment.result.service_snapshot.logic_total} · job{" "}
                  {selectedReport.experiment.result.service_snapshot.job_total} · etcd{" "}
                  {selectedReport.experiment.result.service_snapshot.etcd_up ? "up" : "down"}
                  {selectedReport.experiment.result.service_snapshot.free_text ? (
                    <>
                      <br />
                      {selectedReport.experiment.result.service_snapshot.free_text}
                    </>
                  ) : null}
                </div>
              </div>
            )}
          </div>
          {selectedReport.claims.length > 0 && (
            <div className="panel-body" style={{ borderTop: "1px solid var(--border)" }}>
              <div className="panel-sub">This experiment is evidence for</div>
              <table className="comp-table">
                <tbody>
                  {selectedReport.claims.map((c) => (
                    <tr key={c.id}>
                      <td>{c.claim}</td>
                      <td>
                        <ClaimBadge status={c.status} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </>
  );
}

// DistributedLive：运行分布式实验时复用 Overview 聚合展示旁路观测面。
function DistributedLive({ overview, exp }: { overview: Overview | null; exp: Experiment }) {
  return (
    <div className="panel">
      <div className="panel-head">Distributed side-channel — {exp.architecture} run</div>
      <div className="panel-body">
        {!overview ? (
          <div className="hint">ops observer unavailable (no /api/overview data).</div>
        ) : (
          <div style={{ display: "flex", gap: 28, flexWrap: "wrap", fontFamily: "var(--mono)", fontSize: 12.5 }}>
            <div>
              <div className="field-label">Comet</div>
              {overview.comet_instances.healthy}/{overview.comet_instances.total} healthy
            </div>
            <div>
              <div className="field-label">Kafka</div>
              {overview.kafka.available ? "available" : <span className="na-cell">unavailable</span>}
            </div>
            <div>
              <div className="field-label">msg in / out</div>
              {fmtRate(overview.msg_in_rate)} / {fmtRate(overview.msg_out_rate)}
            </div>
            <div>
              <div className="field-label">active connections</div>
              {fmtNum(overview.active_connections)}
            </div>
            <div>
              <div className="field-label">system health</div>
              {overview.health}
            </div>
          </div>
        )}
        <div className="hint" style={{ marginTop: 8 }}>
          Kafka lag / trace completion for this run are captured into the persisted result at finish (only when a
          distributed fleet is reachable).
        </div>
      </div>
    </div>
  );
}

export function ClaimBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    VERIFIED: "var(--green)",
    "PARTIALLY VERIFIED": "var(--amber)",
    "CODE VERIFIED": "#4db2d8",
    TARGET: "var(--text-dim)",
    UNKNOWN: "var(--red)",
  };
  const c = colors[status] ?? "var(--text-dim)";
  return (
    <span
      style={{
        color: c,
        border: `1px solid ${c}`,
        borderRadius: 4,
        padding: "1px 7px",
        fontFamily: "var(--mono)",
        fontSize: 10.5,
        whiteSpace: "nowrap",
      }}
    >
      {status}
    </span>
  );
}
