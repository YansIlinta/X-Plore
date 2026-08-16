import React from "react";
import { api, usePoll, LoadtestStatus } from "../api";
import { fmtNum, fmtTime } from "../format";
import RateChart, { SeriesPoint } from "../components/RateChart";

const MAX_POINTS = 240;

// Load Test：UI → Ops API → loadtest 子进程。只允许一个压测同时跑；
// 二进制不存在（如容器内）时明确显示不可用，不假装在压测。
export default function LoadTest() {
  const [status, setStatus] = React.useState<LoadtestStatus | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [points, setPoints] = React.useState<SeriesPoint[]>([]);
  const [form, setForm] = React.useState({
    server: "ws://localhost:8080",
    conns: "1000",
    rooms: "10",
    rate: "1",
    duration: "30s",
  });

  const pollInterval = status?.running ? 1000 : 2000;
  const { data } = usePoll(api.loadtestStatus, pollInterval);
  React.useEffect(() => {
    if (data) setStatus(data);
  }, [data]);

  React.useEffect(() => {
    if (!status?.latest) return;
    const l = status.latest;
    const t = status.elapsed_s ?? 0;
    setPoints((prev) => {
      const next = [
        ...prev,
        { t: Date.now() / 1000, a: l.send_qps, b: l.recv_qps },
      ];
      return next.length > MAX_POINTS ? next.slice(next.length - MAX_POINTS) : next;
    });
    void t;
  }, [status?.latest]);

  const start = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.loadtestStart({
        server: form.server,
        conns: Number(form.conns),
        rooms: Number(form.rooms),
        rate: Number(form.rate),
        duration: form.duration,
      });
      setPoints([]);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const stop = async () => {
    setBusy(true);
    await api.loadtestStop();
    setBusy(false);
  };

  if (!status) return <div className="hint">连接中…</div>;

  const l = status.latest;
  const rep = status.report?.summary as Record<string, unknown> | undefined;

  const field = (label: string, key: string, width = "100%") => (
    <div style={{ display: "flex", flexDirection: "column", gap: 4, width }}>
      <label style={{ fontSize: 10.5, color: "var(--text-faint)", textTransform: "uppercase", letterSpacing: "0.06em" }}>{label}</label>
      <input
        value={form[key as keyof typeof form]}
        onChange={(e) => setForm({ ...form, [key]: e.target.value })}
        disabled={status.running}
        style={{
          background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)",
          borderRadius: 4, padding: "7px 10px", fontFamily: "var(--mono)", fontSize: 12.5,
        }}
      />
    </div>
  );

  return (
    <>
      <div className="panel">
        <div className="panel-head">
          Load Test
          <span style={{ textTransform: "none" }}>
            {status.available ? "" : "loadtest 二进制不可用（需 ../monolith 构建到 bin/loadtest）"}
          </span>
        </div>
        <div className="panel-body" style={{ display: "flex", gap: 12, alignItems: "flex-end", flexWrap: "wrap" }}>
          {field("Server", "server", "320px")}
          {field("Conns", "conns", "110px")}
          {field("Rooms", "rooms", "110px")}
          {field("Rate (msg/s/conn)", "rate", "130px")}
          {field("Duration", "duration", "110px")}
          {status.running ? (
            <button className="btn danger" onClick={stop} disabled={busy}>Stop</button>
          ) : (
            <button className="btn" onClick={start} disabled={busy || !status.available}>
              {busy ? "…" : "Start"}
            </button>
          )}
        </div>
      </div>

      {error && <div className="error-banner">{error}</div>}

      {status.running && (
        <>
          <div className="kpi-grid">
            <div className="kpi">
              <div className="kpi-label">Status</div>
              <div className="kpi-value" style={{ color: "var(--green)", fontSize: 16 }}>RUNNING</div>
              <div className="kpi-sub">started {fmtTime(status.started_at)}</div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Connections</div>
              <div className={`kpi-value ${l ? "" : "na"}`}>
                {l ? `${fmtNum(l.active_conns)} / ${fmtNum(l.target_conns)}` : "N/A"}
              </div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Send / Recv QPS</div>
              <div className={`kpi-value ${l ? "" : "na"}`}>
                {l ? `${fmtNum(l.send_qps)} / ${fmtNum(l.recv_qps)}` : "N/A"}
              </div>
            </div>
            <div className="kpi">
              <div className="kpi-label">E2E p50/p90/p99</div>
              <div className={`kpi-value ${l ? "" : "na"}`} style={{ fontSize: 15 }}>
                {l ? `${fmtNum(l.e2e_p50_us)} / ${fmtNum(l.e2e_p90_us)} / ${fmtNum(l.e2e_p99_us)} μs` : "N/A"}
              </div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Errors (w / r)</div>
              <div className={`kpi-value ${l ? "" : "na"}`}>
                {l ? `${fmtNum(l.write_errors)} / ${fmtNum(l.read_errors)}` : "N/A"}
              </div>
            </div>
            <div className="kpi">
              <div className="kpi-label">Elapsed</div>
              <div className="kpi-value">{status.elapsed_s !== null ? `${status.elapsed_s.toFixed(0)}s` : "N/A"}</div>
            </div>
          </div>
          <RateChart title="Loadtest Throughput (send / recv)" labelA="send qps" labelB="recv qps" points={points} />
        </>
      )}

      {!status.running && status.err && <div className="error-banner">{status.err}</div>}

      {!status.running && rep && (
        <div className="panel">
          <div className="panel-head">Last Report</div>
          <div className="panel-body">
            <pre className="json-dump">{JSON.stringify(rep, null, 2)}</pre>
          </div>
        </div>
      )}
    </>
  );
}
