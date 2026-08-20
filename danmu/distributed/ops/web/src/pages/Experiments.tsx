import React from "react";
import {
  api,
  usePoll,
  Experiment,
  ExperimentRun,
  ExperimentAggregate,
  MetricAggregate,
  Architecture,
  WorkloadDist,
  ExperimentSpec,
  ExperimentResult,
} from "../api";
import { fmtNum, fmtRate, fmtTime } from "../format";
import RateChart, { SeriesPoint } from "../components/RateChart";

const MAX_POINTS = 240;

const statusColor: Record<string, string> = {
  completed: "var(--green)",
  partial: "var(--amber)",
  running: "#4db2d8",
  failed: "var(--red)",
  stopped: "var(--amber)",
  created: "var(--text-dim)",
};

export function StatusBadge({ status }: { status: string }) {
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

const regimeLabel: Record<string, string> = {
  "low-fanout": "Low Fanout",
  "hot-room": "Hot Room",
  "skewed-hot-room": "Skewed Hot Room",
  "high-rate": "High Rate",
};

// ---- Aggregate 卡片 ----

function MetricAggCard({ agg, title }: { agg?: MetricAggregate | null; title: string }) {
  if (!agg || !agg.measured) {
    return (
      <div className="kpi">
        <div className="kpi-label">{title}</div>
        <div className="kpi-value na">N/A</div>
        <div className="kpi-sub">not measured</div>
      </div>
    );
  }
  const fmt = (v?: number | null, d = 1) => (v === null || v === undefined ? "N/A" : v.toFixed(d));
  const ci = agg.ci_status === "ok" ? `[${fmt(agg.ci95_low)}, ${fmt(agg.ci95_high)}]` : agg.ci_status;
  return (
    <div className="kpi">
      <div className="kpi-label">
        {title} <span className="mono-dim">({agg.samples}/{agg.total_rep})</span>
      </div>
      <div className="kpi-value" style={{ fontSize: 15 }}>
        {fmt(agg.median)}
      </div>
      <div className="kpi-sub">
        mean {fmt(agg.mean)} · CV {fmt(agg.cv, 3)}
        <br />
        CI95 {ci}
      </div>
    </div>
  );
}

// ---- Run 明细（可展开）----

function RunResultTable({ result }: { result: ExperimentResult }) {
  const rows: [string, string | null][] = [
    ["Connections established", result.connections_established !== null ? `${fmtNum(result.connections_established)}` : null],
    ["Messages sent", result.messages_sent !== null ? `${fmtNum(result.messages_sent)}` : null],
    ["Messages received", result.messages_received !== null ? `${fmtNum(result.messages_received)}` : null],
    ["P50 latency", result.p50_latency_us !== null ? `${fmtNum(result.p50_latency_us)} µs` : null],
    ["P90 latency", result.p90_latency_us !== null ? `${fmtNum(result.p90_latency_us)} µs` : null],
    ["P99 latency", result.p99_latency_us !== null ? `${fmtNum(result.p99_latency_us)} µs` : null],
    ["Send rate", result.send_rate !== null ? fmtRate(result.send_rate) : null],
    ["Receive rate", result.receive_rate !== null ? fmtRate(result.receive_rate) : null],
    ["Delivery rate", result.delivery_rate !== null && result.delivery_rate !== undefined ? result.delivery_rate.toFixed(4) : null],
    ["Missing deliveries", result.missing_deliveries !== null && result.missing_deliveries !== undefined ? `${fmtNum(result.missing_deliveries)}` : null],
    ["Write errors", result.write_errors !== null ? `${fmtNum(result.write_errors)}` : null],
    ["Read errors", result.read_errors !== null ? `${fmtNum(result.read_errors)}` : null],
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

function RunRow({ run }: { run: ExperimentRun }) {
  const [open, setOpen] = React.useState(false);
  return (
    <>
      <tr style={{ cursor: "pointer" }} onClick={() => setOpen((o) => !o)}>
        <td style={{ fontFamily: "var(--mono)" }}>#{run.index}</td>
        <td>
          <StatusBadge status={run.status} />
        </td>
        <td style={{ fontFamily: "var(--mono)" }}>
          {run.measurement_start ? fmtTime(run.measurement_start) : run.started_at ? fmtTime(run.started_at) : "—"}
        </td>
        <td style={{ fontFamily: "var(--mono)" }}>
          {run.measurement_end ? fmtTime(run.measurement_end) : run.finished_at ? fmtTime(run.finished_at) : "—"}
        </td>
        <td style={{ fontFamily: "var(--mono)" }}>
          {run.result.receive_rate !== null && run.result.receive_rate !== undefined ? fmtRate(run.result.receive_rate) : "N/A"}
        </td>
        <td style={{ fontFamily: "var(--mono)" }}>
          {run.result.p99_latency_us !== null ? `${fmtNum(run.result.p99_latency_us)}µs` : "N/A"}
        </td>
        <td style={{ fontFamily: "var(--mono)" }}>
          {run.result.delivery_rate !== null && run.result.delivery_rate !== undefined ? run.result.delivery_rate.toFixed(4) : "N/A"}
        </td>
        <td>{open ? "▾" : "▸"}</td>
      </tr>
      {open && (
        <tr>
          <td colSpan={8}>
            <div style={{ display: "flex", gap: 20, flexWrap: "wrap", padding: "8px 4px" }}>
              <div style={{ flex: "1 1 300px", minWidth: 260 }}>
                <div className="panel-sub">Raw result</div>
                <RunResultTable result={run.result} />
                {(run.result.notes?.length ?? 0) > 0 && (
                  <div className="hint" style={{ marginTop: 6 }}>
                    {run.result.notes!.map((n) => (
                      <div key={n}>• {n}</div>
                    ))}
                  </div>
                )}
                {run.error && <div className="error-banner">{run.error}</div>}
              </div>
              <div style={{ flex: "1 1 260px", minWidth: 240 }}>
                <div className="panel-sub">Environment / commit</div>
                {run.environment ? (
                  <div className="env-grid">
                    <div>go {run.environment.go_version}</div>
                    <div>
                      {run.environment.os}/{run.environment.arch}
                    </div>
                    <div>cpu={run.environment.cpu_cores}</div>
                    <div>
                      git: {run.environment.git_commit ? run.environment.git_commit.slice(0, 12) : "N/A"}
                      {run.environment.git_dirty ? " (dirty)" : ""}
                    </div>
                    <div>host={run.environment.hostname ?? "N/A"}</div>
                  </div>
                ) : (
                  <div className="hint">no environment recorded</div>
                )}
                {run.warmup_duration && (
                  <div className="mono-dim" style={{ marginTop: 6 }}>
                    warmup {run.warmup_duration} → measure {run.measurement_duration}
                  </div>
                )}
              </div>
              <div style={{ flex: "1 1 260px", minWidth: 240 }}>
                <div className="panel-sub">Server resources</div>
                {run.resource && run.resource.sampled ? (
                  <div className="env-grid">
                    <div>CPU mean {run.resource.cpu_pct_mean?.toFixed(1) ?? "N/A"}%</div>
                    <div>CPU peak {run.resource.cpu_pct_peak?.toFixed(1) ?? "N/A"}%</div>
                    <div>RSS mean {run.resource.rss_mb_mean ? `${run.resource.rss_mb_mean.toFixed(1)}MB` : "N/A"}</div>
                    <div>RSS peak {run.resource.rss_mb_peak ? `${run.resource.rss_mb_peak.toFixed(1)}MB` : "N/A"}</div>
                    <div>
                      goroutines mean {fmtNum(run.resource.goroutines_mean ?? null)} · GC {run.resource.gc_total ?? "N/A"}
                    </div>
                    <div className="mono-dim">samples {run.resource.sample_count}</div>
                  </div>
                ) : (
                  <div className="hint">not sampled{run.resource?.unavailable_reason ? ` (${run.resource.unavailable_reason})` : ""}</div>
                )}
              </div>
              {run.workload_diagnostics && (
                <div style={{ flex: "1 1 260px", minWidth: 240 }}>
                  <div className="panel-sub">Workload diagnostics</div>
                  <div className="env-grid">
                    <div>dist {run.workload_diagnostics.distribution}</div>
                    <div>
                      largest room share {run.workload_diagnostics.largest_room_share.toFixed(3)}
                    </div>
                    <div>top10 share {run.workload_diagnostics.top_10_percent_room_share.toFixed(3)}</div>
                    <div>
                      room size {run.workload_diagnostics.min_room_size}–{run.workload_diagnostics.max_room_size} (mean {run.workload_diagnostics.mean_room_size.toFixed(1)})
                    </div>
                  </div>
                </div>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

// ---- 主页面 ----

export default function Experiments() {
  const { data: listData, error: listErr } = usePoll(() => api.experiments(100), 2000);
  const { data: presetResp } = usePoll(() => api.presets(), 60000);
  const { data: regimeResp } = usePoll(() => api.regimes(), 60000);
  const presets = presetResp?.presets ?? [];
  const regimes = regimeResp?.regimes ?? [];

  const [form, setForm] = React.useState({
    name: "",
    mode: "regime" as "regime" | "preset",
    regime: "low-fanout",
    preset: "low-fanout",
    architecture: "monolith" as Architecture,
    connections: "",
    rooms: "",
    rate: "",
    duration: "8s",
    warmup: "2s",
    repetitions: "3",
    distribution: "uniform" as WorkloadDist,
    zipf_s: "1.1",
    seed: "1",
    target: "ws://127.0.0.1:18081",
  });
  const [busy, setBusy] = React.useState(false);
  const [formErr, setFormErr] = React.useState<string | null>(null);
  const [selected, setSelected] = React.useState<Experiment | null>(null);

  const experiments: Experiment[] = listData?.experiments ?? [];
  const running = experiments.find((e) => e.status === "running") ?? null;

  // 运行中：1s 轮询详情拿 live 快照
  const { data: detail } = usePoll(
    () => (running ? api.experimentDetail(running.id) : Promise.resolve(null as never)),
    running ? 1000 : 100000,
  );
  const live = detail?.live;
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
      mode: "preset",
      architecture: p.architecture,
      connections: String(p.workload.connections),
      rooms: String(p.workload.rooms),
      rate: String(p.workload.message_rate),
      duration: p.workload.duration,
      target: p.workload.target,
      distribution: p.workload.distribution ?? "uniform",
    }));
  };

  const applyRegime = (name: string) => {
    const r = regimes.find((rr) => rr.name === name);
    if (!r) return;
    setForm((f) => ({
      ...f,
      regime: name,
      mode: "regime",
      architecture: "monolith",
      connections: String(r.workload.connections),
      rooms: String(r.workload.rooms),
      rate: String(r.workload.message_rate),
      duration: r.workload.duration,
      target: r.workload.target,
      distribution: r.workload.distribution ?? "uniform",
      zipf_s: String(r.workload.zipf_s ?? 1.1),
      seed: String(r.workload.seed ?? 1),
    }));
  };

  const runExperiment = async () => {
    setBusy(true);
    setFormErr(null);
    try {
      const explicit = form.connections !== "" && form.rooms !== "" && form.rate !== "";
      const wl = {
        connections: explicit ? Number(form.connections) : 0,
        rooms: explicit ? Number(form.rooms) : 0,
        message_rate: explicit ? Number(form.rate) : 0,
        duration: form.duration,
        target: form.target || "ws://127.0.0.1:18081",
        distribution: form.distribution,
        zipf_s: Number(form.zipf_s || 1.1),
        seed: Number(form.seed || 1),
      };
      const exp = await api.createExperiment({
        name: form.name || undefined,
        architecture: form.architecture,
        regime: form.mode === "regime" ? form.regime : undefined,
        preset: form.mode === "preset" ? form.preset : "custom",
        workload: wl,
        warmup: form.warmup || undefined,
        duration: form.duration,
        repetitions: Number(form.repetitions || 1),
      });
      await api.experimentStart(exp.id);
      setSelected(exp);
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
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            {regimes.map((r) => (
              <button
                key={r.name}
                className={`preset-btn ${form.mode === "regime" && form.regime === r.name ? "active" : ""}`}
                onClick={() => applyRegime(r.name)}
                disabled={!!running}
                title={r.description}
              >
                {r.label}
              </button>
            ))}
            <div className="mono-dim" style={{ alignSelf: "center" }}>
              |
            </div>
            {presets.map((p) => (
              <button
                key={p.name}
                className={`preset-btn ${form.mode === "preset" && form.preset === p.name ? "active" : ""}`}
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
          <div style={{ display: "flex", gap: 12, alignItems: "flex-end", flexWrap: "wrap" }}>
            {field("Experiment name", "name", "200px", false)}
            {field("Target", "target", "230px")}
            {field("Conns", "connections", "90px")}
            {field("Rooms", "rooms", "90px")}
            {field("Rate (msg/s/conn)", "rate", "90px")}
            {field("Warm-up", "warmup", "80px")}
            {field("Measure", "duration", "80px")}
            {field("Repetitions", "repetitions", "80px")}
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              <label className="field-label">Distribution</label>
              <select
                value={form.distribution}
                onChange={(e) => setForm({ ...form, distribution: e.target.value as WorkloadDist })}
                disabled={!!running}
                className="field-input"
              >
                <option value="uniform">uniform</option>
                <option value="hot_room">hot_room</option>
                <option value="zipf">zipf</option>
              </select>
            </div>
            {field("Zipf s", "zipf_s", "70px")}
            {field("Seed", "seed", "70px")}
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
          <div className="hint">
            Warm-up 期间的流量不计入任何统计（measureStart 重置）；测量窗内数据才是结果。repetitions 顺序执行。
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
                RUNNING · rep {live?.repetition ?? "?"}/{live?.repetitions ?? "?"}
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
              <div className="kpi-label">Elapsed</div>
              <div className="kpi-value">
                {live?.elapsed_s !== null && live?.elapsed_s !== undefined ? `${Math.round(live.elapsed_s)}s` : "N/A"}
              </div>
            </div>
          </div>
          <RateChart title={`Loadtest Throughput — ${running.id}`} labelA="send qps" labelB="recv qps" points={points} />
          {running.architecture === "distributed" && overview && (
            <div className="panel">
              <div className="panel-head">Distributed side-channel</div>
              <div className="panel-body">
                <div style={{ display: "flex", gap: 28, flexWrap: "wrap", fontFamily: "var(--mono)", fontSize: 12.5 }}>
                  <div>comet {overview.comet_instances.healthy}/{overview.comet_instances.total} healthy</div>
                  <div>kafka {overview.kafka.available ? "available" : "unavailable"}</div>
                  <div>active conns {fmtNum(overview.active_connections)}</div>
                  <div>health {overview.health}</div>
                </div>
              </div>
            </div>
          )}
        </>
      )}

      {/* ---- 历史 ---- */}
      <div className="panel">
        <div className="panel-head">Recent Experiments</div>
        <div className="panel-body">
          {experiments.length === 0 ? (
            <div className="hint">No experiments yet. Choose a regime above and run one.</div>
          ) : (
            <table className="comp-table">
              <thead>
                <tr>
                  <th>Name / ID</th>
                  <th>Regime</th>
                  <th>Config</th>
                  <th>Reps</th>
                  <th>Status</th>
                  <th>P90 (median)</th>
                  <th>Receive rate (mean)</th>
                  <th>CV</th>
                  <th>Spec</th>
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
                    <td>{e.regime ? regimeLabel[e.regime] ?? e.regime : "—"}</td>
                    <td className="mono-dim">{e.config_label ?? "default"}</td>
                    <td style={{ fontFamily: "var(--mono)" }}>
                      {e.aggregate ? `${e.aggregate.successful_repetitions}/${e.repetitions ?? 1}` : e.runs?.length ?? "—"}
                    </td>
                    <td>
                      <StatusBadge status={e.status} />
                    </td>
                    <td style={{ fontFamily: "var(--mono)" }}>
                      {e.aggregate?.metrics?.["p90_latency_us"]?.median != null
                        ? `${fmtNum(e.aggregate.metrics["p90_latency_us"].median!)}µs`
                        : e.result?.p90_latency_us != null
                        ? `${fmtNum(e.result.p90_latency_us)}µs`
                        : "N/A"}
                    </td>
                    <td style={{ fontFamily: "var(--mono)" }}>
                      {e.aggregate?.metrics?.["receive_rate"]?.mean != null
                        ? fmtRate(e.aggregate.metrics["receive_rate"].mean!)
                        : e.result?.receive_rate != null
                        ? fmtRate(e.result.receive_rate)
                        : "N/A"}
                    </td>
                    <td style={{ fontFamily: "var(--mono)" }}>
                      {e.aggregate?.metrics?.["p90_latency_us"]?.cv != null
                        ? e.aggregate.metrics["p90_latency_us"].cv!.toFixed(3)
                        : "—"}
                    </td>
                    <td className="mono-dim">{e.spec_hash ? e.spec_hash.slice(0, 8) : "—"}</td>
                    <td>
                      <button className="btn small" onClick={() => setSelected(e)} disabled={!!running && e.id === running.id}>
                        Detail
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* ---- 详情 ---- */}
      {selected && <ExperimentDetailPanel exp={selected} onClose={() => setSelected(null)} />}
    </>
  );
}

// ---- 实验详情（Spec / Runs / Aggregate）----

export function ExperimentDetailPanel({ exp, onClose }: { exp: Experiment; onClose?: () => void }) {
  const { data: detail } = usePoll(() => api.experimentDetail(exp.id), exp.status === "running" ? 2000 : 100000);
  const e: Experiment = detail?.experiment ?? exp;
  const live = detail?.live;
  const agg: ExperimentAggregate | null = e.aggregate ?? null;
  const spec: ExperimentSpec | null = e.spec ?? null;

  return (
    <div className="panel">
      <div className="panel-head">
        Experiment {e.id}
        {onClose && (
          <button className="btn small" style={{ float: "right" }} onClick={onClose}>
            close
          </button>
        )}
        <span style={{ float: "right", marginRight: 8 }}>
          <StatusBadge status={e.status} />
        </span>
      </div>
      <div className="panel-body" style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        {/* Spec */}
        <div style={{ display: "flex", gap: 20, flexWrap: "wrap" }}>
          <div style={{ flex: "1 1 420px", minWidth: 300 }}>
            <div className="panel-sub">Spec (immutable)</div>
            <table className="comp-table">
              <tbody>
                <tr>
                  <td>Regime</td>
                  <td>{e.regime ?? "—"}</td>
                </tr>
                <tr>
                  <td>Architecture</td>
                  <td>{e.architecture}</td>
                </tr>
                <tr>
                  <td>Workload</td>
                  <td className="mono">
                    {e.workload.connections}c/{e.workload.rooms}r @{e.workload.message_rate}/s · {e.workload.distribution ?? "uniform"} · seed {e.workload.seed}
                  </td>
                </tr>
                <tr>
                  <td>Window</td>
                  <td className="mono">
                    warmup {e.warmup || "0s"} → measure {e.duration ?? e.workload.duration}
                  </td>
                </tr>
                <tr>
                  <td>Repetitions</td>
                  <td className="mono">{e.repetitions ?? 1}</td>
                </tr>
                <tr>
                  <td>System config</td>
                  <td className="mono">
                    {e.system_config ? (
                      <>
                        {e.system_config.batch_size ? `batch_size=${e.system_config.batch_size} ` : ""}
                        {e.system_config.batch_timeout ? `batch_timeout=${e.system_config.batch_timeout} ` : ""}
                        {e.system_config.workers ? `workers=${e.system_config.workers}` : ""}
                        {Object.keys(e.system_config).length === 0 ? "default" : ""}
                        <span className="mono-dim"> (requires restart)</span>
                      </>
                    ) : (
                      "default"
                    )}
                  </td>
                </tr>
              </tbody>
            </table>
            <div className="mono-dim" style={{ marginTop: 6 }}>
              spec_hash <code>{e.spec_hash ?? "—"}</code>
              {spec && (
                <div>same spec ⇒ same hash; any workload/system change ⇒ different hash</div>
              )}
            </div>
          </div>

          {/* Aggregate */}
          <div style={{ flex: "1 1 520px", minWidth: 320 }}>
            <div className="panel-sub">Aggregate over successful repetitions</div>
            {!agg ? (
              <div className="hint">
                {e.status === "running" ? `running… ${live?.repetition ? `rep ${live.repetition}` : ""}` : "no successful repetitions → no aggregate"}
              </div>
            ) : (
              <>
                <div className="kpi-grid">
                  <MetricAggCard agg={agg.metrics?.["p90_latency_us"]} title="P90 (median)" />
                  <MetricAggCard agg={agg.metrics?.["p99_latency_us"]} title="P99 (median)" />
                  <MetricAggCard agg={agg.metrics?.["receive_rate"]} title="Receive rate" />
                  <MetricAggCard agg={agg.metrics?.["delivery_rate"]} title="Delivery rate" />
                </div>
                <div className="hint" style={{ marginTop: 6 }}>
                  {agg.successful_repetitions} success / {agg.failed_repetitions} fail / {agg.stopped_repetitions} stopped of {agg.total_repetitions} · stability ={" "}
                  <b>{agg.stability}</b> ({agg.stability_note})
                </div>
              </>
            )}
          </div>
        </div>

        {/* Runs */}
        <div>
          <div className="panel-sub">
            Runs ({e.runs?.length ?? 0}) — click to expand
          </div>
          {!e.runs || e.runs.length === 0 ? (
            <div className="hint">no runs recorded</div>
          ) : (
            <table className="comp-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Status</th>
                  <th>Measure start</th>
                  <th>Measure end</th>
                  <th>Receive rate</th>
                  <th>P99</th>
                  <th>Delivery</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {e.runs.map((r) => (
                  <RunRow key={r.id} run={r} />
                ))}
              </tbody>
            </table>
          )}
        </div>

        {e.error && <div className="error-banner">{e.error}</div>}
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
