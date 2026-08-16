import React from "react";
import { api, usePoll } from "../api";
import { fmtNum, fmtTime } from "../format";

// Rooms：房间浏览器。房间数据是"按需扇出查询"（不进采集循环），页面打开时每 5s 刷新。
// comet 侧 /api/v1/rooms 只给前 100 个活跃房间，页面上要说明这一点。
export default function Rooms() {
  const [q, setQ] = React.useState("");
  const [detail, setDetail] = React.useState<{ loading: boolean; room: import("../api").RoomView | null; err?: string }>(
    { loading: false, room: null }
  );
  const { data, error } = usePoll(api.rooms, 5000);

  const search = async (id: string) => {
    const id2 = id.trim();
    if (!id2) return;
    setDetail({ loading: true, room: null });
    try {
      const resp = await api.roomDetail(id2);
      setDetail({ loading: false, room: resp.room });
    } catch (e) {
      setDetail({ loading: false, room: null, err: e instanceof Error ? e.message : String(e) });
    }
  };

  if (error) return <div className="error-banner">无法连接 ops 后端：{error}</div>;
  if (!data) return <div className="hint">连接中…</div>;

  return (
    <>
      <div className="panel">
        <div className="panel-head">
          Room Explorer
          <span style={{ textTransform: "none" }}>
            {data.partial ? "⚠ 部分 comet 拉取失败，结果可能不全" : `${data.total} rooms（活跃度降序，每 comet 前 100）`}
          </span>
        </div>
        <div className="panel-body" style={{ display: "flex", gap: 8 }}>
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && search(q)}
            placeholder="Search room id…（Enter 查询）"
            style={{
              flex: 1, background: "var(--bg)", color: "var(--text)",
              border: "1px solid var(--border)", borderRadius: 4,
              padding: "7px 10px", fontFamily: "var(--mono)", fontSize: 12.5,
            }}
          />
          <button className="btn" onClick={() => search(q)}>Search</button>
        </div>
        <div className="panel-body flush">
          <table className="ops">
            <thead>
              <tr><th>Room ID</th><th>Connections</th><th>Comets</th><th>Status</th></tr>
            </thead>
            <tbody>
              {data.rooms.map((r) => (
                <tr key={r.room_id} style={{ cursor: "pointer" }} onClick={() => { setQ(r.room_id); search(r.room_id); }}>
                  <td>{r.room_id}</td>
                  <td>{fmtNum(r.online_count)}</td>
                  <td style={{ color: "var(--text-dim)" }}>{r.comets.join(", ")}</td>
                  <td>{r.is_active ? <span className="tag info">active</span> : <span className="tag">idle</span>}</td>
                </tr>
              ))}
              {data.rooms.length === 0 && <tr><td colSpan={4} className="hint">没有活跃房间。</td></tr>}
            </tbody>
          </table>
        </div>
      </div>

      {detail.loading && <div className="hint">查询中…</div>}
      {detail.err && <div className="error-banner">{detail.err}</div>}
      {detail.room && (
        <div className="panel">
          <div className="panel-head">Room {detail.room.room_id}</div>
          <div className="panel-body">
            <div className="kpi-grid" style={{ marginBottom: 0 }}>
              <div className="kpi">
                <div className="kpi-label">Connections</div>
                <div className="kpi-value">{fmtNum(detail.room.online_count)}</div>
              </div>
              <div className="kpi">
                <div className="kpi-label">Comet Instances</div>
                <div className="kpi-value">{detail.room.comets.join(", ") || "—"}</div>
                <div className="kpi-sub">{fmtTime(data.ts)}</div>
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
