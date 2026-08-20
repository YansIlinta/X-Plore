import React from "react";
import { api, usePoll, Experiment, CompareResp } from "../api";
import { fmtNum } from "../format";

// Compare：挑两个历史实验，逐指标对比。只有语义相同的指标才计算 delta；
// 一侧为 N/A（null）时该行 Delta/Δ% 都是 N/A。Summary 是后端规则化、确定性的文本。
export default function Compare() {
  const { data } = usePoll(() => api.experiments(200), 5000);
  const experiments: Experiment[] = data?.experiments ?? [];

  const [left, setLeft] = React.useState<string>("");
  const [right, setRight] = React.useState<string>("");
  const [result, setResult] = React.useState<CompareResp | null>(null);
  const [err, setErr] = React.useState<string | null>(null);

  React.useEffect(() => {
    // 默认选最近两个 completed
    const done = experiments.filter((e) => e.status === "completed");
    if (done.length >= 2 && !left && !right) {
      setLeft(done[1].id);
      setRight(done[0].id);
    }
  }, [experiments, left, right]);

  const compare = async (l: string, r: string) => {
    if (!l || !r) return;
    setErr(null);
    try {
      setResult(await api.compare(l, r));
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  };

  React.useEffect(() => {
    if (left && right) void compare(left, right);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [left, right]);

  if (!data) return <div className="hint">连接中…</div>;

  const selectOf = (label: string, value: string, set: (v: string) => void, other: string) => (
    <div style={{ display: "flex", flexDirection: "column", gap: 4, width: "48%" }}>
      <label className="field-label">{label}</label>
      <select value={value} onChange={(e) => set(e.target.value)} className="field-input">
        <option value="">— select —</option>
        {experiments
          .filter((e) => e.id !== other)
          .map((e) => (
            <option key={e.id} value={e.id}>
              {e.name} · {e.architecture} · {e.status} · {e.workload.connections}c
            </option>
          ))}
      </select>
    </div>
  );

  return (
    <>
      <div className="panel">
        <div className="panel-head">Compare two experiments</div>
        <div className="panel-body" style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
          {selectOf("Run A (baseline)", left, setLeft, right)}
          {selectOf("Run B", right, setRight, left)}
        </div>
        {err && <div className="error-banner">{err}</div>}
      </div>

      {result && (
        <>
          <div className="panel">
            <div className="panel-head">Metric comparison</div>
            <div className="panel-body">
              <div className="mono-dim" style={{ marginBottom: 6 }}>
                {result.left.name} ({result.left.id}) vs {result.right.name} ({result.right.id})
                {" · "}
                {result.left.workload.connections}c / {result.left.workload.rooms}r→{result.right.workload.connections}c /{" "}
                {result.right.workload.rooms}r
              </div>
              <table className="comp-table">
                <thead>
                  <tr>
                    <th>Metric</th>
                    <th>Run A</th>
                    <th>Run B</th>
                    <th>Delta (B−A)</th>
                    <th>Δ%</th>
                    <th>Verdict</th>
                  </tr>
                </thead>
                <tbody>
                  {result.rows.map((row) => (
                    <tr key={row.metric}>
                      <td>
                        {row.label}
                        <span className="mono-dim" style={{ marginLeft: 6 }}>
                          {row.group}
                        </span>
                      </td>
                      <td className={row.left === null ? "na-cell" : "mono-cell"}>
                        {row.left === null ? "N/A" : cellVal(row.left, row.unit)}
                      </td>
                      <td className={row.right === null ? "na-cell" : "mono-cell"}>
                        {row.right === null ? "N/A" : cellVal(row.right, row.unit)}
                      </td>
                      <td className={row.delta === null ? "na-cell" : "mono-cell"}>
                        {row.delta === null ? "N/A" : `${sign(row.delta)}${fmtNum(Math.abs(row.delta))}`}
                      </td>
                      <td className={row.delta_pct === null ? "na-cell" : "mono-cell"}>
                        {row.delta_pct === null ? "N/A" : `${sign(row.delta_pct)}${Math.abs(row.delta_pct).toFixed(1)}%`}
                      </td>
                      <td>
                        {row.verdict === "better" ? (
                          <span style={{ color: "var(--green)" }}>▲ better</span>
                        ) : row.verdict === "worse" ? (
                          <span style={{ color: "var(--red)" }}>▼ worse</span>
                        ) : row.verdict === "same" ? (
                          <span className="mono-dim">same</span>
                        ) : (
                          <span className="mono-dim">—</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
          <div className="panel">
            <div className="panel-head">Summary (rule-based, deterministic — no LLM)</div>
            <div className="panel-body">
              {result.summary.map((s) => (
                <div key={s} className="summary-line">
                  {s}
                </div>
              ))}
              <div className="summary-net">{result.net}</div>
            </div>
          </div>
        </>
      )}
    </>
  );
}

// cellVal：展示度量值，rate 类（0~1）显示小数，否则整型 + 单位。
function cellVal(v: number, unit: string): string {
  if (unit === "rate") return v.toFixed(3);
  const num = Number.isInteger(v) ? fmtNum(v) : v.toFixed(2);
  return unit && unit !== "rate" ? `${num} ${unit}` : num;
}

function sign(v: number): string {
  return v > 0 ? "+" : "";
}
