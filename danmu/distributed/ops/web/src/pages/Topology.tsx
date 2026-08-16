import React from "react";
import { api, usePoll, Instance } from "../api";
import { fmtNum } from "../format";

// Topology：手绘 SVG 架构图（节点数量少，不引入 graph 库）。
// 布局按消息链路分层：clients → nginx → comet[] → logic[] → kafka → job[] →(回到 comet)，
// etcd 置于右侧，discover 边用点线。

const NODE_W = 176;
const NODE_H = 46;
const CENTER_X = 430;
const ROW_GAP = 92;

interface PlacedNode {
  id: string;
  kind: string;
  label: string;
  healthy: boolean | null;
  x: number;
  y: number;
}

function shortAddr(label: string): { main: string; sub: string } {
  const parts = label.split(" / ");
  if (parts.length === 2) return { main: parts[1], sub: parts[0] };
  return { main: label, sub: "" };
}

export default function Topology() {
  const { data: topo, error } = usePoll(api.topology, 2000);
  const { data: services } = usePoll(api.services, 2000);

  const nodes = React.useMemo<PlacedNode[]>(() => {
    if (!topo) return [];
    const rows: Record<string, typeof topo.nodes> = {};
    for (const n of topo.nodes) {
      const row =
        n.kind === "client" ? 0 :
        n.kind === "nginx" ? 1 :
        n.kind === "comet" ? 2 :
        n.kind === "logic" ? 3 :
        n.kind === "kafka" ? 4 :
        n.kind === "job" ? 5 : -1; // etcd 特殊位置
      if (row >= 0) (rows[row] ??= []).push(n);
    }
    const placed: PlacedNode[] = [];
    for (const rowStr of Object.keys(rows)) {
      const row = Number(rowStr);
      const list = rows[row];
      const totalW = list.length * NODE_W + (list.length - 1) * 44;
      list.forEach((n, i) => {
        placed.push({
          ...n,
          x: CENTER_X - totalW / 2 + i * (NODE_W + 44) + NODE_W / 2,
          y: 34 + row * ROW_GAP,
        });
      });
    }
    // etcd：右侧，与 logic 层对齐
    for (const n of topo.nodes) {
      if (n.kind === "etcd") {
        placed.push({ ...n, x: CENTER_X + 330, y: 34 + 3 * ROW_GAP });
      }
    }
    return placed;
  }, [topo]);

  // 实例附加信息（comet 连接数等），按 http_addr 对上节点 id
  const instById = React.useMemo(() => {
    const m = new Map<string, Instance>();
    for (const s of services?.services ?? []) {
      for (const it of s.instances) m.set(it.http_addr, it);
    }
    return m;
  }, [services]);

  const nodeById = React.useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);

  if (error) return <div className="error-banner">无法连接 ops 后端：{error}</div>;
  if (!topo) return <div className="hint">连接中…</div>;

  const svgH = 34 + 5 * ROW_GAP + NODE_H + 40;

  const edgePath = (from: PlacedNode, to: PlacedNode): string => {
    if (from.y < to.y) {
      // 从上往下：出底入顶
      return `M ${from.x} ${from.y + NODE_H} L ${to.x} ${to.y}`;
    }
    // 从下往上（job→comet）：右侧绕行的弧线
    const off = 70;
    return `M ${from.x + NODE_W / 2} ${from.y + NODE_H / 2}
            C ${from.x + NODE_W / 2 + off} ${from.y + NODE_H / 2},
              ${to.x + NODE_W / 2 + off} ${to.y + NODE_H / 2},
              ${to.x + NODE_W / 2} ${to.y + NODE_H / 2}`;
  };

  return (
    <div className="panel">
      <div className="panel-head">
        Architecture Topology
        <span style={{ textTransform: "none", letterSpacing: 0 }}>
          ws / rpc / produce / consume 实线流动 · discover 点线 · 虚框=未观测
        </span>
      </div>
      <div className="panel-body topo-wrap">
        <svg width="900" height={svgH} viewBox={`0 0 900 ${svgH}`}>
          {topo.edges.map((e, i) => {
            const from = nodeById.get(e.from);
            const to = nodeById.get(e.to);
            if (!from || !to) return null;
            const cls =
              e.kind === "discover"
                ? "topo-edge discover"
                : from.healthy === false || to.healthy === false
                  ? "topo-edge"
                  : "topo-edge flow";
            return <path key={i} d={edgePath(from, to)} className={cls} />;
          })}
          {nodes.map((n) => {
            const { main, sub } = shortAddr(n.label);
            const cls = n.healthy === null ? "unobserved" : n.healthy ? "healthy" : "down";
            const inst = instById.get(n.id);
            const conns = inst?.stats && typeof inst.stats["conn_count"] === "number"
              ? (inst.stats["conn_count"] as number)
              : null;
            return (
              <g key={n.id} className={`topo-node ${cls}`} transform={`translate(${n.x - NODE_W / 2}, ${n.y})`}>
                <rect width={NODE_W} height={NODE_H} rx={6} />
                <text x={10} y={19}>{main}</text>
                <text className="sub" x={10} y={34}>
                  {n.kind.toUpperCase()}
                  {sub ? ` · ${sub}` : ""}
                  {conns !== null ? ` · ${fmtNum(conns)} conns` : ""}
                  {n.healthy === false ? " · DOWN" : ""}
                </text>
              </g>
            );
          })}
        </svg>
      </div>
    </div>
  );
}
