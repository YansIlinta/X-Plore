import React from "react";
import { api, usePoll, Instance } from "../api";
import { fmtNum, fmtRate, fmtUptime } from "../format";

// Services：统一服务实例视图。可点行展开原始 stats JSON（实例详情 MVP）。
// component 非空时只显示该组件（侧栏 Comet/Logic/Job/Etcd 子页）。

function statNum(it: Instance, key: string): number | null {
  const v = it.stats?.[key];
  return typeof v === "number" ? v : null;
}

function InstanceRow({ it }: { it: Instance }) {
  const [open, setOpen] = React.useState(false);
  return (
    <>
      <tr className="expandable" onClick={() => setOpen(!open)}>
        <td>
          <span className={`status-dot ${it.healthy ? "up" : "down"}`} />
          {it.http_addr}
          {it.rpc_addr ? <span style={{ color: "var(--text-faint)" }}> · {it.rpc_addr}</span> : null}
        </td>
        <td>{it.healthy ? "Healthy" : `Down${it.err ? ` · ${it.err}` : ""}`}</td>
        <td>{fmtNum(statNum(it, "conn_count"))}</td>
        <td>{fmtNum(statNum(it, "room_count"))}</td>
        <td>{fmtRate(it.msg_in_rate)}</td>
        <td>{fmtRate(it.msg_out_rate)}</td>
        <td>{fmtUptime(it.stats?.["uptime_ms"])}</td>
      </tr>
      {open && (
        <tr>
          <td colSpan={7}>
            {it.stats ? (
              <pre className="json-dump">{JSON.stringify(it.stats, null, 2)}</pre>
            ) : (
              <div className="hint">实例不可达，无 stats。</div>
            )}
          </td>
        </tr>
      )}
    </>
  );
}

export default function Services({ component }: { component?: string }) {
  const { data, error } = usePoll(api.services, 2000);

  if (error) return <div className="error-banner">无法连接 ops 后端：{error}</div>;
  if (!data) return <div className="hint">连接中…</div>;

  const groups = component ? data.services.filter((s) => s.name === component) : data.services;

  if (groups.length === 0) {
    return <div className="hint">etcd 中暂无 {component ?? "任何"} 实例。</div>;
  }

  return (
    <>
      {groups.map((svc) => (
        <div className="panel" key={svc.name}>
          <div className="panel-head">
            {svc.name.toUpperCase()} INSTANCES
            <span style={{ textTransform: "none" }}>
              {svc.instances.filter((i) => i.healthy).length} / {svc.instances.length} healthy
            </span>
          </div>
          <div className="panel-body flush">
            <table className="ops">
              <thead>
                <tr>
                  <th>Instance</th>
                  <th>Status</th>
                  <th>Conns</th>
                  <th>Rooms</th>
                  <th>Msg In</th>
                  <th>Msg Out</th>
                  <th>Uptime</th>
                </tr>
              </thead>
              <tbody>
                {svc.instances.map((it) => (
                  <InstanceRow key={it.http_addr} it={it} />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </>
  );
}
