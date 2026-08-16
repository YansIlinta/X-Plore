import React from "react";
import uPlot from "uplot";

// RateChart：uPlot 实时速率曲线。数据由父组件每轮 poll 追加（keep 最近 N 点）。
export interface SeriesPoint {
  t: number; // unix 秒
  a: number | null;
  b: number | null;
}

export default function RateChart({
  title,
  labelA,
  labelB,
  points,
}: {
  title: string;
  labelA: string;
  labelB: string;
  points: SeriesPoint[];
}) {
  const rootRef = React.useRef<HTMLDivElement>(null);
  const chartRef = React.useRef<uPlot | null>(null);

  React.useEffect(() => {
    if (!rootRef.current) return;
    const opts: uPlot.Options = {
      width: rootRef.current.clientWidth,
      height: 180,
      scales: { x: { time: true } },
      legend: { show: true },
      cursor: { show: true },
      axes: [
        { stroke: "#8b95a1", grid: { stroke: "#232a33", width: 1 }, ticks: { stroke: "#232a33" }, font: "11px monospace" },
        { stroke: "#8b95a1", grid: { stroke: "#232a33", width: 1 }, ticks: { stroke: "#232a33" }, font: "11px monospace" },
      ],
      series: [
        {},
        { label: labelA, stroke: "#4db2d8", width: 1.5 },
        { label: labelB, stroke: "#2fbf71", width: 1.5 },
      ],
    };
    const u = new uPlot(opts, [[], [], []], rootRef.current);
    chartRef.current = u;
    const onResize = () => u.setSize({ width: rootRef.current!.clientWidth, height: 180 });
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      u.destroy();
      chartRef.current = null;
    };
    // 只建一次
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  React.useEffect(() => {
    const u = chartRef.current;
    if (!u) return;
    const xs: number[] = [];
    const a: (number | null)[] = [];
    const b: (number | null)[] = [];
    for (const p of points) {
      xs.push(p.t);
      a.push(p.a);
      b.push(p.b);
    }
    u.setData([xs, a, b]);
  }, [points]);

  return (
    <div className="panel">
      <div className="panel-head">{title}</div>
      <div className="panel-body">
        <div ref={rootRef} />
      </div>
    </div>
  );
}
