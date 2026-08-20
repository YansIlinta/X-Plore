import { api, usePoll, Claim, ClaimStatus } from "../api";
import { ClaimBadge } from "./Experiments";

// Evidence / Claims：产品最重要的诚信页。VERIFIED 只能来自存储里的实验；
// 架构目标（如百万连接）没有对应实验时永远是 TARGET，绝不自动升级成 benchmark 结果。
const LEGEND: { status: ClaimStatus; meaning: string }[] = [
  {
    status: "VERIFIED",
    meaning: "存在支撑该 claim 的实验（实验存储中跑出的数字，可追溯到 report/commit/环境）。",
  },
  {
    status: "PARTIALLY VERIFIED",
    meaning: "有相关实验但只做到低于目标量级；页面会指出最高已测量到的值。",
  },
  {
    status: "CODE VERIFIED",
    meaning: "代码 / 单测 / 集成测试层面已实现并验证（chaintest / etcdreg 等），但尚未被 benchmark。",
  },
  { status: "TARGET", meaning: "架构目标。除非存在达到阈值的实验，绝不显示 VERIFIED。" },
  { status: "UNKNOWN", meaning: "状态未知（当前仓库未使用，保留语义）。" },
];

export default function Evidence() {
  const { data, error } = usePoll(() => api.evidence(), 5000);
  if (error) return <div className="error-banner">{error}</div>;
  if (!data) return <div className="hint">连接中…</div>;

  const claims = data.claims;

  const groupCount = (s: ClaimStatus) => claims.filter((c) => c.status === s).length;

  return (
    <>
      <div className="panel">
        <div className="panel-head">Evidence & Claims — 只有被实验支撑的才叫 VERIFIED</div>
        <div className="panel-body">
          <div style={{ display: "flex", gap: 28, flexWrap: "wrap" }}>
            {LEGEND.map((l) => (
              <div key={l.status} style={{ maxWidth: 280 }}>
                <ClaimBadge status={l.status} /> <span className="mono-dim">× {groupCount(l.status)}</span>
                <div className="hint" style={{ marginTop: 4 }}>
                  {l.meaning}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="panel">
        <div className="panel-head">Claims</div>
        <div className="panel-body">
          {claims.length === 0 ? (
            <div className="hint">No claims defined.</div>
          ) : (
            <table className="comp-table">
              <thead>
                <tr>
                  <th style={{ width: "30%" }}>Claim</th>
                  <th>Status</th>
                  <th>Evidence</th>
                  <th>Experiment</th>
                  <th>Commit / Date</th>
                  <th>Notes</th>
                </tr>
              </thead>
              <tbody>
                {claims.map((c: Claim) => (
                  <tr key={c.id}>
                    <td>
                      <b>{c.claim}</b>
                      <div className="mono-dim">{c.id}</div>
                    </td>
                    <td>
                      <ClaimBadge status={c.status} />
                    </td>
                    <td>
                      <ul className="evid-list">
                        {c.evidence.map((e) => (
                          <li key={e}>{e}</li>
                        ))}
                      </ul>
                    </td>
                    <td className="mono-cell">{c.experiment_id ?? "—"}</td>
                    <td className="mono-dim">
                      {c.commit ? c.commit.slice(0, 12) : "N/A"}
                      <br />
                      {c.date ?? "N/A"}
                    </td>
                    <td>
                      <div className="hint" style={{ margin: 0 }}>
                        {c.notes}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          <div className="hint" style={{ marginTop: 10 }}>
            说明：本页状态由 /api/evidence 在每次请求时根据实验存储实时计算。跑一个新实验后刷新即可看到
            claim 升级为 VERIFIED（或 PARTIALLY VERIFIED）。目标型 claim（如百万连接）在没有对应实验前始终为
            TARGET。
          </div>
        </div>
      </div>
    </>
  );
}
