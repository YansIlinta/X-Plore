import React from "react";
import { api, usePoll } from "../api";
import { fmtRate } from "../format";
import RateChart, { SeriesPoint } from "../components/RateChart";

const MAX_POINTS = 120;

// Traffic：全局与各组件的实时消息吞吐。
export default function Traffic() {
  const { data: ov, error } = usePoll(api.overview, 2000);
  const { data: svc } = usePoll(api.services, 2000);
  const [points, setPoints] = React.useState<SeriesPoint[]>([]);

  React.useEffect(() => {
    if (!ov) return;
    setPoints((prev) => {
      const next = [
        ...prev,
        {
          t: new Date(ov.ts).getTime() / 1000,
          a: ov.kafka.produced_rate,
          b: ov.kafka.consumed_rate,
        },
      ];
      return next.length > MAX_POINTS ? next.slice(next.length - MAX_POINTS) : next;
    });
  }, [ov]);

  if (error) return <div className="error-banner">无法连接 ops 后端：{error}</div>;
  if (!ov) return <div className="hint">连接中…</div>;

  const rows: { name: string; in_: string; out: string; note: string }[] = [];
  for (const s of svc?.services ?? []) {
    for (const it of s.instances) {
      if (s.name === "comet") {
        rows.push({
          name: `comet ${it.http_addr}`,
          in_: fmtRate(it.msg_in_rate),
          out: fmtRate(it.msg_out_rate),
          note: it.healthy ? "" : "DOWN",
        });
      } else if (s.name === "logic") {
        rows.push({
          name: `logic ${it.http_addr}`,
          in_: fmtRate(it.rates?.["onmessage"] ?? null),
          out: "—",
          note: `produce → kafka${it.healthy ? "" : " · DOWN"}`,
        });
      } else if (s.name === "job") {
        rows.push({
          name: `job ${it.http_addr}`,
          in_: fmtRate(it.rates?.["consumed"] ?? null),
          out: fmtRate(it.rates?.["push_ok"] ?? null),
          note: `consume ← kafka${it.healthy ? "" : " · DOWN"}`,
        });
      }
    }
  }

  return (
    <>
      <div className="kpi-grid">
        <div className="kpi">
          <div className="kpi-label">Comet In (client→)</div>
          <div className={`kpi-value ${ov.msg_in_rate === null ? "na" : ""}`}>{fmtRate(ov.msg_in_rate)}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Comet Out (→client)</div>
          <div className={`kpi-value ${ov.msg_out_rate === null ? "na" : ""}`}>{fmtRate(ov.msg_out_rate)}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Kafka Produce</div>
          <div className={`kpi-value ${ov.kafka.produced_rate === null ? "na" : ""}`}>{fmtRate(ov.kafka.produced_rate)}</div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Kafka Consume</div>
          <div className={`kpi-value ${ov.kafka.consumed_rate === null ? "na" : ""}`}>{fmtRate(ov.kafka.consumed_rate)}</div>
        </div>
      </div>

      <RateChart title="Kafka Throughput (produce / consume)" labelA="produce/s" labelB="consume/s" points={points} />

      <div className="panel">
        <div className="panel-head">Per-Instance Traffic</div>
        <div className="panel-body flush">
          <table className="ops">
            <thead>
              <tr>
                <th>Instance</th>
                <th>In</th>
                <th>Out</th>
                <th>Note</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 && (
                <tr><td colSpan={4} className="hint">暂无实例（etcd 中无实例）。</td></tr>
              )}
              {rows.map((r) => (
                <tr key={r.name}>
                  <td>{r.name}</td>
                  <td>{r.in_}</td>
                  <td>{r.out}</td>
                  <td style={{ color: "var(--text-dim)" }}>{r.note}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
