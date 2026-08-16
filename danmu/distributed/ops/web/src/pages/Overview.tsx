import React from "react";
import { api, usePoll } from "../api";
import { fmtNum, fmtRate } from "../format";
import RateChart, { SeriesPoint } from "../components/RateChart";

const MAX_POINTS = 120; // 环形缓冲：只保留最近 120 个采样点

function Kpi({ label, value, sub, na }: { label: string; value: string; sub?: string; na?: boolean }) {
  return (
    <div className="kpi">
      <div className="kpi-label">{label}</div>
      <div className={`kpi-value ${na ? "na" : ""}`}>{value}</div>
      {sub && <div className="kpi-sub">{sub}</div>}
    </div>
  );
}

export default function Overview() {
  const { data, error } = usePoll(api.overview, 2000);
  const [points, setPoints] = React.useState<SeriesPoint[]>([]);

  // 每次拿到新 overview 就追加一个采样点（null 保持 null，曲线留缺口，不伪造）。
  React.useEffect(() => {
    if (!data) return;
    setPoints((prev) => {
      const next = [
        ...prev,
        {
          t: new Date(data.ts).getTime() / 1000,
          a: data.msg_in_rate,
          b: data.msg_out_rate,
        },
      ];
      return next.length > MAX_POINTS ? next.slice(next.length - MAX_POINTS) : next;
    });
  }, [data]);

  if (error) return <div className="error-banner">无法连接 ops 后端：{error}</div>;
  if (!data) return <div className="hint">连接中…</div>;

  const kafkaLag = data.kafka.available ? data.kafka.lag : null;
  const lagEntries = kafkaLag ? Object.entries(kafkaLag) : [];

  return (
    <>
      {data.health !== "healthy" && (
        <div className="error-banner">
          System {data.health}: {data.health_detail.join("；")}
        </div>
      )}

      <div className="kpi-grid">
        <Kpi label="Active Connections" value={fmtNum(data.active_connections)} na={data.active_connections === null} />
        <Kpi label="Msg In Rate" value={fmtRate(data.msg_in_rate)} na={data.msg_in_rate === null} />
        <Kpi label="Msg Out Rate" value={fmtRate(data.msg_out_rate)} na={data.msg_out_rate === null} />
        <Kpi label="Active Rooms" value={fmtNum(data.active_rooms)} na={data.active_rooms === null} />
        <Kpi
          label="Comet Instances"
          value={`${data.comet_instances.healthy} / ${data.comet_instances.total}`}
          sub="healthy / total"
        />
        <Kpi
          label="Kafka Lag"
          value={
            lagEntries.length === 0
              ? "N/A"
              : lagEntries.map(([g, v]) => `${g}: ${v === null ? "N/A" : fmtNum(v)}`).join("  ")
          }
          na={lagEntries.length === 0}
          sub={data.kafka.available ? undefined : data.kafka.err || "kafka observation unavailable"}
        />
      </div>

      <RateChart title="Message Rate (comet in / out)" labelA="msg in/s" labelB="msg out/s" points={points} />

      <div className="panel">
        <div className="panel-head">Health Detail</div>
        <div className="panel-body">
          <table className="ops">
            <tbody>
              {data.health_detail.map((d, i) => (
                <tr key={i}>
                  <td>{d}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
