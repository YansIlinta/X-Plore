import { api, usePoll } from "../api";
import { fmtTime } from "../format";

// Events：系统事件流（实例注册/掉线/恢复、registry 状态、健康翻转）。
export default function EventsPage() {
  const { data, error } = usePoll(() => api.events(300), 3000);

  if (error) return <div className="error-banner">无法连接 ops 后端：{error}</div>;
  if (!data) return <div className="hint">连接中…</div>;

  const cls = (level: string) =>
    level === "INFO" ? "tag info" : level === "WARNING" ? "tag warning" : "tag error";

  return (
    <div className="panel">
      <div className="panel-head">System Events <span style={{ textTransform: "none" }}>{data.events.length} 条（上限 500）</span></div>
      <div className="panel-body flush">
        <table className="ops">
          <thead>
            <tr><th>Time</th><th>Level</th><th>Kind</th><th>Message</th></tr>
          </thead>
          <tbody>
            {data.events.length === 0 && <tr><td colSpan={4} className="hint">暂无事件（事件在实例/健康状态变化时产生）。</td></tr>}
            {data.events.map((e, i) => (
              <tr key={i}>
                <td>{fmtTime(e.ts)}</td>
                <td><span className={cls(e.level)}>{e.level}</span></td>
                <td style={{ color: "var(--text-dim)" }}>{e.kind}</td>
                <td>{e.message}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
