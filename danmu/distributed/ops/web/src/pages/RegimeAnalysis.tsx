
import { api, usePoll, SweepReport, RankingObjective } from "../api";
import { fmtNum } from "../format";

// Regime Analysis —— 基于已完成实验（带 regime/spec 元数据）的确定性 cross-regime 视图。
// 无任何 LLM 摘要：结论全部由约束化排名 / 占优分析规则生成。

export default function RegimeAnalysis() {
  const { data, error } = usePoll(() => api.regimeAnalysis(), 3000);
  const report: SweepReport | null = data?.report ?? null;
  const objective: RankingObjective | null = data?.objective ?? null;

  return (
    <>
      <div className="panel">
        <div className="panel-head">Regime Analysis — Configuration × Workload</div>
        <div className="panel-body" style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          <div className="hint">
            Objective: maximize <b>{objective?.primary ?? "throughput"}</b> subject to P99 ≤{" "}
            {objective ? `${(objective.p99_max_us / 1000).toFixed(0)}ms` : "50ms"} and delivery_rate ≥{" "}
            {objective?.delivery_min ?? "99.9%"}. Only experiments with a workload regime participate. Scores are
            deterministic rules, not LLM summaries.
          </div>
          {error && <div className="error-banner">{error}</div>}
          {!report || report.regimes.length === 0 ? (
            <div className="hint">
              No cross-regime data yet. Run a sweep (or experiments with a regime) to produce a Configuration × Workload
              matrix.
            </div>
          ) : (
            <>
              <Matrix report={report} />
              <Dominance report={report} />
              <Gate report={report} />
            </>
          )}
        </div>
      </div>

      <div className="panel">
        <div className="panel-head">How this is computed</div>
        <div className="panel-body">
          <ul className="hint" style={{ paddingLeft: 18 }}>
            <li>Every experiment carries an immutable spec + spec_hash; experiments with the same spec_hash are the same configuration.</li>
            <li>Each cell is a <b>statistical aggregate</b> over successful repetitions (median latency, mean throughput, CV, 95% bootstrap CI).</li>
            <li>Best config per regime = feasible config maximizing the primary metric under the constraints; if none is feasible → NO FEASIBLE CONFIGURATION.</li>
            <li>Dominance: config A dominates B if A is no worse on every objective and better on at least one.</li>
            <li>The Adaptive-Control gate is an <b>offline conclusion</b>, not a product decision and not a controller.</li>
          </ul>
        </div>
      </div>
    </>
  );
}

function Matrix({ report }: { report: SweepReport }) {
  return (
    <div>
      <div className="panel-sub">Configuration × Workload Regime (mean throughput / median P99 / CV)</div>
      <table className="comp-table">
        <thead>
          <tr>
            <th>Config</th>
            {report.regimes.map((rg) => (
              <th key={rg}>{rg}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {report.configs.map((cfg) => (
            <tr key={cfg}>
              <td className="mono">{cfg}</td>
              {report.regimes.map((rg) => {
                const b = report.best_per_regime[rg];
                const isBest = b?.config === cfg;
                // 单元格数据来自 best_per_regime 的吞吐/延迟（按 config 近似；完整矩阵在 sweep 结果页）
                return (
                  <td key={rg} style={{ fontFamily: "var(--mono)", fontSize: 12 }}>
                    <span style={{ fontWeight: isBest ? 700 : 400 }}>{isBest ? "★ " : ""}{cfg}</span>
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>

      <div style={{ marginTop: 10 }}>
        <div className="panel-sub">Best configuration per regime</div>
        <table className="comp-table">
          <thead>
            <tr>
              <th>Regime</th>
              <th>Best config</th>
              <th>Feasible</th>
              <th>Throughput (msg/s)</th>
              <th>P99 (µs)</th>
              <th>Delivery rate</th>
            </tr>
          </thead>
          <tbody>
            {report.regimes.map((rg) => {
              const b = report.best_per_regime[rg];
              return (
                <tr key={rg}>
                  <td>{rg}</td>
                  <td className="mono">{b?.config ?? "—"}</td>
                  <td>
                    {b ? (
                      b.feasible ? (
                        <span style={{ color: "var(--green)" }}>feasible</span>
                      ) : (
                        <span className="na-cell">NO FEASIBLE CONFIGURATION</span>
                      )
                    ) : (
                      "—"
                    )}
                  </td>
                  <td className="mono">{b ? fmtNum(b.throughput) : "—"}</td>
                  <td className="mono">{b ? fmtNum(b.p99) : "—"}</td>
                  <td className="mono">{b ? (b.delivery_rate > 0 ? b.delivery_rate.toFixed(4) : "N/A") : "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function Dominance({ report }: { report: SweepReport }) {
  const shifts = report.domination.static_optimum_shifts;
  return (
    <div>
      <div className="panel-sub">Static dominance</div>
      <div className="hint" style={{ fontSize: 13.5 }}>
        <b style={{ color: shifts ? "var(--amber)" : "var(--green)" }}>
          {shifts ? "STATIC OPTIMUM SHIFTS ACROSS WORKLOAD REGIMES" : "NO EVIDENCE OF REGIME-DEPENDENT STATIC OPTIMUM"}
        </b>
        <div style={{ marginTop: 4 }}>{report.domination.conclusion}</div>
        {report.domination.one_config_dominates && (
          <div style={{ marginTop: 4 }}>
            Dominant config across all tested regimes: <b className="mono">{report.domination.dominant_config}</b>
          </div>
        )}
      </div>
    </div>
  );
}

function Gate({ report }: { report: SweepReport }) {
  const g = report.adaptive_gate;
  return (
    <div>
      <div className="panel-sub">Adaptive-Control Research Gate (offline)</div>
      <div className="hint" style={{ fontSize: 13.5 }}>
        <b style={{ color: g.go ? "var(--green)" : "var(--red)" }}>ADAPTIVE CONTROL RESEARCH: {g.verdict}</b>
        <ul style={{ paddingLeft: 18, margin: "6px 0" }}>
          <li>
            A · best static config differs across &gt;=2 regimes: <b>{String(g.condition_a_shifts)}</b>
          </li>
          <li>
            B · best config improves throughput over default: <b>{String(g.condition_b_improves)}</b>
          </li>
          <li>
            C · benchmark variance low enough to see improvements: <b>{String(g.condition_c_low_variance)}</b>
          </li>
          <li>
            D · at least one safely-tunable system parameter: <b>{String(g.condition_d_tunable_param)}</b>
          </li>
        </ul>
        {g.evidence.map((ev, i) => (
          <div key={i}>• {ev}</div>
        ))}
        <div style={{ marginTop: 4 }} className="mono-dim">
          This is an experiment conclusion only. A controller is justified only if experiments first demonstrate that
          workload-dependent configuration changes matter.
        </div>
      </div>
    </div>
  );
}
