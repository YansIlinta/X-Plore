import React from "react";
import { api, usePoll, Trace } from "../api";

const HOP_LABELS: Record<string, string> = {
  "comet.uplink": "Comet 收到上行",
  "logic.produce": "Logic 写 Kafka",
  "job.consume": "Job 消费",
  "job.push": "Job 扇出 PushRoom",
  "comet.deliver": "Comet 投递下行",
};

// Messages：消息 trace。msg_id 采样命中（默认 1/100）的消息在各环节留 span，
// ops 定期汇聚成链路。缺环节=消息在那一段之前停了或采样窗口没覆盖。
function TraceDetail({ t }: { t: Trace }) {
  return (
    <div className="panel-body">
      <div className="kpi-grid" style={{ marginBottom: 12 }}>
        <div className="kpi">
          <div className="kpi-label">End-to-End</div>
          <div className={`kpi-value ${t.duration_ms === 0 && t.spans.length < 2 ? "na" : ""}`}>
            {t.spans.length >= 2 ? `${t.duration_ms.toFixed(1)} ms` : "N/A"}
          </div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Completeness</div>
          <div className="kpi-value" style={{ fontSize: 16 }}>
            {t.complete ? "✓ complete" : `缺 ${(t.missing_hops ?? []).length} 环节`}
          </div>
        </div>
      </div>
      <table className="ops">
        <thead>
          <tr><th>Hop</th><th>Node</th><th>Offset</th><th>Gap</th><th>Detail</th></tr>
        </thead>
        <tbody>
          {t.spans.map((sp, i) => {
            const gap = i === 0 ? 0 : (sp.ts_nano - t.spans[i - 1].ts_nano) / 1e6;
            return (
              <tr key={i}>
                <td>{HOP_LABELS[sp.hop] ?? sp.hop}</td>
                <td>{sp.node}</td>
                <td>{i === 0 ? "0 ms" : `${((sp.ts_nano - t.spans[0].ts_nano) / 1e6).toFixed(2)} ms`}</td>
                <td>{i === 0 ? "—" : `${gap.toFixed(2)} ms`}</td>
                <td style={{ color: "var(--text-dim)" }}>{sp.detail ?? ""}</td>
              </tr>
            );
          })}
          {(t.missing_hops ?? []).map((h) => (
            <tr key={h} style={{ opacity: 0.45 }}>
              <td>{HOP_LABELS[h] ?? h}</td>
              <td colSpan={4} style={{ color: "var(--amber)" }}>缺少 span（消息未到达或采样窗口溢出）</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function Messages() {
  const { data, error } = usePoll(() => api.traces(50), 3000);
  const [openID, setOpenID] = React.useState<string | null>(null);

  if (error) return <div className="error-banner">无法连接 ops 后端：{error}</div>;
  if (!data) return <div className="hint">连接中…</div>;

  const sources = Object.entries(data.sources);

  return (
    <>
      <div className="panel">
        <div className="panel-head">Message Trace（采样消息，最新在前）</div>
        <div className="panel-body flush">
          <table className="ops">
            <thead>
              <tr><th>msg_id</th><th>Room</th><th>E2E</th><th>链路</th><th>First / Last</th></tr>
            </thead>
            <tbody>
              {data.traces.length === 0 && (
                <tr><td colSpan={5} className="hint">
                  暂无 trace。需要消息流量 + 各服务开启 -trace-sample（默认 1/100 采样），
                  且消息要经过完整链路（Kafka→Job→Comet）。
                </td></tr>
              )}
              {data.traces.map((t) => (
                <React.Fragment key={t.msg_id}>
                  <tr
                    className="expandable"
                    onClick={() => setOpenID(openID === t.msg_id ? null : t.msg_id)}
                  >
                    <td>{t.msg_id}</td>
                    <td>{t.room_id ?? "—"}</td>
                    <td>{t.spans.length >= 2 ? `${t.duration_ms.toFixed(1)} ms` : "N/A"}</td>
                    <td>
                      {t.complete ? <span className="tag info">complete</span> : <span className="tag warning">partial</span>}
                    </td>
                    <td style={{ color: "var(--text-dim)" }}>
                      {t.spans.length > 0
                        ? `${HOP_LABELS[t.spans[0].hop] ?? t.spans[0].hop} → ${HOP_LABELS[t.spans[t.spans.length - 1].hop] ?? t.spans[t.spans.length - 1].hop}`
                        : "—"}
                    </td>
                  </tr>
                  {openID === t.msg_id && (
                    <tr><td colSpan={5}><TraceDetail t={t} /></td></tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="panel">
        <div className="panel-head">Trace Sources（各节点自述）</div>
        <div className="panel-body flush">
          <table className="ops">
            <thead><tr><th>Node</th><th>Enabled</th><th>Rate</th><th>Buffered</th><th>Dropped</th></tr></thead>
            <tbody>
              {sources.map(([node, s]) => {
                const st = (s ?? {}) as Record<string, unknown>;
                return (
                  <tr key={node}>
                    <td>{node}</td>
                    <td>{st["enabled"] ? "yes" : "no"}</td>
                    <td>{st["rate"] !== undefined ? `1/${st["rate"]}` : "N/A"}</td>
                    <td>{String(st["buffered"] ?? "N/A")}</td>
                    <td style={{ color: (st["dropped"] as number) > 0 ? "var(--amber)" : undefined }}>
                      {String(st["dropped"] ?? "N/A")}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
