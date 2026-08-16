import { api, usePoll } from "../api";
import { fmtNum, fmtRate } from "../format";

// Kafka：fan-out pipeline 的关键节点。Lag 是链路阻塞的第一诊断指标。
export default function KafkaPage() {
  const { data, error } = usePoll(api.overview, 2000);

  if (error) return <div className="error-banner">无法连接 ops 后端：{error}</div>;
  if (!data) return <div className="hint">连接中…</div>;

  const k = data.kafka;
  const lagEntries = k.available ? Object.entries(k.lag) : [];

  return (
    <>
      <div className="kpi-grid">
        <div className="kpi">
          <div className="kpi-label">Topic</div>
          <div className="kpi-value" style={{ fontSize: 16 }}>{k.topic ?? "N/A"}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Partitions</div>
          <div className={`kpi-value ${k.partitions === undefined ? "na" : ""}`}>
            {k.partitions !== undefined ? k.partitions : "N/A"}
          </div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Produced /s</div>
          <div className={`kpi-value ${k.produced_rate === null ? "na" : ""}`}>{fmtRate(k.produced_rate)}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Consumed /s</div>
          <div className={`kpi-value ${k.consumed_rate === null ? "na" : ""}`}>{fmtRate(k.consumed_rate)}</div>
        </div>
      </div>

      <div className="panel">
        <div className="panel-head">
          Consumer Lag
          <span style={{ textTransform: "none" }}>
            {k.available ? "" : k.err || "Kafka 观测不可用（未配置 broker 或不可达）"}
          </span>
        </div>
        <div className="panel-body flush">
          <table className="ops">
            <thead>
              <tr><th>Consumer Group</th><th>Lag</th><th>状态</th></tr>
            </thead>
            <tbody>
              {lagEntries.length === 0 && (
                <tr><td colSpan={3} className="hint">无 lag 数据（Kafka 不可达或消费组无提交记录）。</td></tr>
              )}
              {lagEntries.map(([group, lag]) => (
                <tr key={group}>
                  <td>{group}</td>
                  <td>{lag === null ? "N/A" : fmtNum(lag)}</td>
                  <td>
                    {lag === null ? (
                      <span className="tag">no commit</span>
                    ) : lag > 10000 ? (
                      <span className="tag error">blocked</span>
                    ) : lag > 1000 ? (
                      <span className="tag warning">backlog</span>
                    ) : (
                      <span className="tag info">healthy</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
