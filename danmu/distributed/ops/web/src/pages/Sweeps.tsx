import React from "react";
import { api, usePoll, Sweep, Architecture, SweepParam, SweepReport } from "../api";
import { fmtNum, fmtTime } from "../format";
import { StatusBadge } from "./Experiments";

const paramDefs: { name: string; label: string; kind: "system" | "workload" | "dist" }[] = [
  { name: "batch_size", label: "batch_size (server, restart)", kind: "system" },
  { name: "batch_timeout", label: "batch_timeout (server, restart)", kind: "system" },
  { name: "workers", label: "workers (server, restart)", kind: "system" },
  { name: "connections", label: "connections", kind: "workload" },
  { name: "rooms", label: "rooms", kind: "workload" },
  { name: "message_rate", label: "message_rate", kind: "workload" },
  { name: "distribution", label: "distribution", kind: "dist" },
  { name: "zipf_s", label: "zipf_s", kind: "workload" },
];

function estimateRows(params: SweepParam[], regimes: string[], reps: number): { configs: number; runs: number } {
  const combos = params.reduce((acc, p) => acc * Math.max(p.values.length, 1), 1);
  const configs = combos * Math.max(regimes.length, 0);
  return { configs, runs: configs * reps };
}

export default function Sweeps() {
  const { data: listData, error } = usePoll(() => api.sweeps(50), 2500);
  const { data: regimeResp } = usePoll(() => api.regimes(), 60000);
  const regimes = regimeResp?.regimes ?? [];

  const [form, setForm] = React.useState({
    name: "",
    architecture: "monolith" as Architecture,
    regimes: [] as string[],
    repetitions: "5",
    warmup: "2s",
    duration: "8s",
    target: "ws://127.0.0.1:18181",
    params: [
      { name: "batch_size", values: ["2000", "5000", "10000"] },
      { name: "batch_timeout", values: ["5ms", "20ms"] },
    ] as SweepParam[],
  });
  const [busy, setBusy] = React.useState(false);
  const [formErr, setFormErr] = React.useState<string | null>(null);
  const [selectedId, setSelectedId] = React.useState<string | null>(null);

  const sweeps: Sweep[] = listData?.sweeps ?? [];
  const activeId = listData?.active_id ?? null;
  const selected = sweeps.find((s) => s.id === selectedId) ?? null;

  const est = estimateRows(form.params, form.regimes, Number(form.repetitions || 1));

  const toggleRegime = (r: string) =>
    setForm((f) => ({
      ...f,
      regimes: f.regimes.includes(r) ? f.regimes.filter((x) => x !== r) : [...f.regimes, r],
    }));

  const updateParam = (i: number, values: string[]) => {
    setForm((f) => {
      const params = f.params.map((p, idx) => (idx === i ? { ...p, values } : p));
      return { ...f, params };
    });
  };

  const setParamName = (i: number, name: string) => {
    setForm((f) => {
      const params = f.params.map((p, idx) => (idx === i ? { ...p, name } : p));
      return { ...f, params };
    });
  };

  const createAndRun = async () => {
    setBusy(true);
    setFormErr(null);
    try {
      const sw = await api.createSweep({
        name: form.name || undefined,
        architecture: form.architecture,
        regimes: form.regimes.length > 0 ? form.regimes : undefined,
        params: form.params,
        repetitions: Number(form.repetitions || 3),
        warmup: form.warmup || undefined,
        duration: form.duration || undefined,
        target: form.target || undefined,
      });
      await api.sweepStart(sw.id);
      setSelectedId(sw.id);
    } catch (e) {
      setFormErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const action = async (id: string, fn: (id: string) => Promise<void>) => {
    try {
      await fn(id);
    } catch (e) {
      setFormErr(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <>
      <div className="panel">
        <div className="panel-head">New Sweep — deterministic Cartesian product</div>
        <div className="panel-body" style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          <div style={{ display: "flex", gap: 12, alignItems: "flex-end", flexWrap: "wrap" }}>
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              <label className="field-label">Sweep name</label>
              <input
                value={form.name}
                onChange={(e) => setForm({ ...form, name: e.target.value })}
                className="field-input"
              />
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              <label className="field-label">Architecture</label>
              <select
                value={form.architecture}
                onChange={(e) => setForm({ ...form, architecture: e.target.value as Architecture })}
                className="field-input"
              >
                <option value="monolith">monolith</option>
                <option value="distributed">distributed</option>
              </select>
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              <label className="field-label">Warm-up</label>
              <input value={form.warmup} onChange={(e) => setForm({ ...form, warmup: e.target.value })} className="field-input" style={{ fontFamily: "var(--mono)" }} />
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              <label className="field-label">Measure</label>
              <input value={form.duration} onChange={(e) => setForm({ ...form, duration: e.target.value })} className="field-input" style={{ fontFamily: "var(--mono)" }} />
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              <label className="field-label">Repetitions</label>
              <input value={form.repetitions} onChange={(e) => setForm({ ...form, repetitions: e.target.value })} className="field-input" style={{ fontFamily: "var(--mono)" }} />
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              <label className="field-label">Target (controlled server)</label>
              <input value={form.target} onChange={(e) => setForm({ ...form, target: e.target.value })} className="field-input" style={{ fontFamily: "var(--mono)" }} />
            </div>
          </div>

          <div>
            <div className="field-label">Workload regimes</div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 4 }}>
              {regimes.map((r) => (
                <button
                  key={r.name}
                  className={`preset-btn ${form.regimes.includes(r.name) ? "active" : ""}`}
                  onClick={() => toggleRegime(r.name)}
                >
                  {r.label}
                </button>
              ))}
              <span className="mono-dim" style={{ alignSelf: "center" }}>
                {form.regimes.length === 0 ? "all regimes" : `${form.regimes.length} selected`}
              </span>
            </div>
          </div>

          <div>
            <div className="field-label">Parameter dimensions (Cartesian product)</div>
            {form.params.map((p, i) => (
              <div key={i} style={{ display: "flex", gap: 8, alignItems: "center", marginTop: 6, flexWrap: "wrap" }}>
                <select value={p.name} onChange={(e) => setParamName(i, e.target.value)} className="field-input" style={{ width: 240 }}>
                  {paramDefs.map((d) => (
                    <option key={d.name} value={d.name}>
                      {d.label}
                    </option>
                  ))}
                </select>
                <input
                  value={p.values.join(",")}
                  onChange={(e) => updateParam(i, e.target.value.split(/[,\s]+/).filter(Boolean))}
                  className="field-input"
                  style={{ fontFamily: "var(--mono)", width: 260 }}
                  placeholder="2000,5000,10000"
                />
                <button
                  className="btn small danger"
                  onClick={() => setForm((f) => ({ ...f, params: f.params.filter((_, idx) => idx !== i) }))}
                >
                  remove
                </button>
              </div>
            ))}
            <div style={{ marginTop: 6 }}>
              <button
                className="btn small"
                onClick={() => setForm((f) => ({ ...f, params: [...f.params, { name: "batch_timeout", values: [] }] }))}
              >
                + dimension
              </button>
            </div>
          </div>

          <div className="hint">
            Estimated: <b>{est.configs}</b> configs × <b>{Number(form.repetitions || 1)}</b> reps ={" "}
            <b>{est.runs} runs</b> (max configs 32 / max runs 120; sequential, no parallel benchmark)
            {form.params.some((p) => ["batch_size", "batch_timeout", "workers"].includes(p.name)) &&
              " · system params restart a controlled monolith server per config"}
          </div>
          {formErr && <div className="error-banner">{formErr}</div>}
          <div>
            <button className="btn" onClick={createAndRun} disabled={busy}>
              {busy ? "…" : "Create & Run Sweep"}
            </button>
          </div>
        </div>
      </div>

      {/* 历史 */}
      <div className="panel">
        <div className="panel-head">Sweeps</div>
        <div className="panel-body">
          {error && <div className="error-banner">{error}</div>}
          {sweeps.length === 0 ? (
            <div className="hint">No sweeps yet.</div>
          ) : (
            <table className="comp-table">
              <thead>
                <tr>
                  <th>Name / ID</th>
                  <th>Arch</th>
                  <th>Status</th>
                  <th>Regimes</th>
                  <th>Runs</th>
                  <th>Progress</th>
                  <th>Created</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {sweeps.map((s) => {
                  const done = s.plan.filter((u) => u.done).length;
                  const isActive = s.id === activeId;
                  return (
                    <tr key={s.id} className={selectedId === s.id ? "row-selected" : ""}>
                      <td>
                        <div>{s.name}</div>
                        <div className="mono-dim">{s.id}</div>
                      </td>
                      <td>{s.architecture}</td>
                      <td>
                        <StatusBadge status={s.status} />
                      </td>
                      <td className="mono-dim">{s.regimes.join(", ")}</td>
                      <td style={{ fontFamily: "var(--mono)" }}>
                        {s.total_runs} ({s.config_count} configs)
                      </td>
                      <td style={{ fontFamily: "var(--mono)" }}>
                        {done}/{s.plan.length}
                      </td>
                      <td className="mono-dim">{fmtTime(s.created_at)}</td>
                      <td style={{ whiteSpace: "nowrap" }}>
                        <button className="btn small" onClick={() => setSelectedId(s.id)}>
                          Detail
                        </button>{" "}
                        {isActive ? (
                          <button className="btn small danger" onClick={() => action(s.id, api.sweepStop)}>
                            Stop
                          </button>
                        ) : (
                          s.status !== "completed" && (
                            <button className="btn small" onClick={() => action(s.id, api.sweepStart)}>
                              {s.status === "running" ? "Resume" : "Run"}
                            </button>
                          )
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {selected && <SweepDetail sweep={selected} />}
    </>
  );
}

function SweepDetail({ sweep: initial }: { sweep: Sweep }) {
  const { data } = usePoll(() => api.sweepReport(initial.id), initial.status === "running" ? 2000 : 100000);
  const s: Sweep = data?.sweep ?? initial;
  const report: SweepReport | null = data?.report ?? s.report ?? null;

  const bestOf = (rg: string) => report?.best_per_regime?.[rg];

  return (
    <div className="panel">
      <div className="panel-head">
        Sweep {s.id} <StatusBadge status={s.status} />
        {s.status === "running" && (
          <span className="hint"> {s.plan.filter((u) => u.done).length}/{s.plan.length} units done</span>
        )}
      </div>
      <div className="panel-body" style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        <div className="mono-dim">
          target {s.target} · warmup {s.warmup || "0s"} → measure {s.duration} · reps {s.repetitions} · total {s.total_runs} runs
        </div>

        {report && (
          <>
            <div>
              <div className="panel-sub">Best static configuration per regime</div>
              <table className="comp-table">
                <thead>
                  <tr>
                    <th>Regime</th>
                    {report.configs.map((c) => (
                      <th key={c}>{c}</th>
                    ))}
                    <th>Best</th>
                  </tr>
                </thead>
                <tbody>
                  {report.regimes.map((rg) => {
                    const b = bestOf(rg);
                    return (
                      <tr key={rg}>
                        <td>{rg}</td>
                        {report.configs.map((c) => {
                          const r = s.results?.find((x) => x.regime === rg && x.config === c);
                          return (
                            <td key={c} style={{ fontFamily: "var(--mono)", fontSize: 12 }}>
                              {r ? (
                                <>
                                  {r.throughput?.mean != null ? fmtNum(r.throughput.mean) : "—"}
                                  <div className="mono-dim">
                                    p99 {r.p99?.median != null ? `${fmtNum(r.p99.median)}µs` : "—"} · cv{" "}
                                    {r.throughput?.cv != null ? r.throughput.cv.toFixed(2) : "—"}
                                  </div>
                                </>
                              ) : (
                                "—"
                              )}
                            </td>
                          );
                        })}
                        <td style={{ fontFamily: "var(--mono)" }}>
                          {b ? (
                            <>
                              <b>{b.config}</b>
                              {!b.feasible && <span className="na-cell"> (no feasible config)</span>}
                            </>
                          ) : (
                            "—"
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            <div>
              <div className="panel-sub">Dominance</div>
              <div className="hint">
                <b style={{ color: report.domination.static_optimum_shifts ? "var(--amber)" : "var(--green)" }}>
                  {report.domination.static_optimum_shifts ? "STATIC OPTIMUM SHIFTS ACROSS REGIMES" : "NO EVIDENCE OF REGIME-DEPENDENT STATIC OPTIMUM"}
                </b>{" "}
                — {report.domination.conclusion}
              </div>
            </div>

            <div>
              <div className="panel-sub">Adaptive-Control Gate (offline, no controller)</div>
              <div className="hint">
                <b style={{ color: report.adaptive_gate.go ? "var(--green)" : "var(--red)" }}>
                  {report.adaptive_gate.verdict}
                </b>
                <ul style={{ margin: "6px 0" }}>
                  <li>A · optimum shifts across regimes: {String(report.adaptive_gate.condition_a_shifts)}</li>
                  <li>B · best improves over default: {String(report.adaptive_gate.condition_b_improves)}</li>
                  <li>C · low benchmark variance: {String(report.adaptive_gate.condition_c_low_variance)}</li>
                  <li>D · tunable system param: {String(report.adaptive_gate.condition_d_tunable_param)}</li>
                </ul>
                {report.adaptive_gate.evidence.map((ev) => (
                  <div key={ev}>• {ev}</div>
                ))}
              </div>
            </div>
          </>
        )}

        <div>
          <div className="panel-sub">Execution plan</div>
          <table className="comp-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Config</th>
                <th>Regime</th>
                <th>Status</th>
                <th>Experiment</th>
              </tr>
            </thead>
            <tbody>
              {s.plan.map((u, i) => (
                <tr key={i}>
                  <td className="mono-dim">{i + 1}</td>
                  <td className="mono">{u.label}</td>
                  <td>{u.regime}</td>
                  <td>
                    {u.done ? <StatusBadge status={u.status ?? "completed"} /> : <span className="mono-dim">pending</span>}
                  </td>
                  <td>
                    {u.experiment_id ? (
                      <a href={`#/experiments`} className="mono-dim">
                        {u.experiment_id}
                      </a>
                    ) : (
                      "—"
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
