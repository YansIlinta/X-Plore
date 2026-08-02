import { useCallback, useEffect, useRef, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import DanmuLayer from '../components/DanmuLayer'
import Player from '../components/Player'
import {
  DANMU_WS_DEFAULT,
  getProfile,
  getToken,
  getUserId,
  getVideo,
  listComments,
  listDanmu,
  listRelated,
  postComment,
  postDanmu,
  toggleFavorite,
  toggleFollow,
  toggleLike,
  type CommentItem,
  type DanmuItem,
  type VideoInfo,
} from '../api/client'
import { categoryLabel, formatCount, formatDuration, timeAgo } from '../lib/format'

type DanmuMsg = {
  uid?: string
  content?: string
  msg_id?: string
  type?: string
  offset_ms?: number
}

type DisplayLine = { key: string; text: string; offsetMs: number }

function itemKey(d: DanmuItem | DanmuMsg): string {
  const id = (d as DanmuItem).msgId || (d as DanmuItem).msg_id || (d as DanmuMsg).msg_id
  if (id) return id
  const uid = d.uid || '?'
  const content = d.content || ''
  const off = (d as DanmuItem).offsetMs ?? (d as DanmuItem).offset_ms ?? (d as DanmuMsg).offset_ms ?? 0
  return `${uid}:${off}:${content}`
}

export default function Watch() {
  const { id } = useParams()
  const videoRef = useRef<HTMLVideoElement>(null)
  const [info, setInfo] = useState<VideoInfo | null>(null)
  const [err, setErr] = useState('')
  const [lines, setLines] = useState<DisplayLine[]>([])
  const [text, setText] = useState('')
  const [wsOn, setWsOn] = useState(false)
  const [playMs, setPlayMs] = useState(0)
  const [liked, setLiked] = useState(false)
  const [favorited, setFavorited] = useState(false)
  const [likeCount, setLikeCount] = useState(0)
  const [favCount, setFavCount] = useState(0)
  const [comments, setComments] = useState<CommentItem[]>([])
  const [commentText, setCommentText] = useState('')
  const [commentTotal, setCommentTotal] = useState(0)
  const [danmuOn, setDanmuOn] = useState(true)
  const [liveFloat, setLiveFloat] = useState('')
  const [related, setRelated] = useState<VideoInfo[]>([])
  const [following, setFollowing] = useState(false)
  const [fans, setFans] = useState(0)
  const [followBusy, setFollowBusy] = useState(false)
  const [actionErr, setActionErr] = useState('')
  const wsRef = useRef<WebSocket | null>(null)
  const seenRef = useRef(new Set<string>())
  const historyLoadedRef = useRef(false)

  const pushLine = useCallback((key: string, textLine: string, offsetMs: number) => {
    if (seenRef.current.has(key)) return
    seenRef.current.add(key)
    // 滑动窗口,避免无限增长
    if (seenRef.current.size > 3000) {
      const arr = [...seenRef.current]
      seenRef.current = new Set(arr.slice(arr.length - 1500))
    }
    setLines((prev) => [...prev.slice(-200), { key, text: textLine, offsetMs }])
  }, [])

  // 详情 + 评论 + 相关推荐;切视频时重置弹幕状态
  useEffect(() => {
    if (!id) return
    const vid = Number(id)
    setInfo(null)
    setErr('')
    setLines([])
    setPlayMs(0)
    seenRef.current = new Set()
    historyLoadedRef.current = false
    getVideo(vid)
      .then((v) => {
        setInfo(v)
        setLiked(!!v.liked)
        setFavorited(!!v.favorited)
        setLikeCount(v.likeCount ?? v.like_count ?? 0)
        setFavCount(v.favoriteCount ?? v.favorite_count ?? 0)
        setCommentTotal(v.commentCount ?? v.comment_count ?? 0)
      })
      .catch((e) => setErr(e.message))
    listComments(vid)
      .then((r) => {
        setComments(r.list || [])
        setCommentTotal(r.total ?? (r.list || []).length)
      })
      .catch(() => {})
    listRelated(vid)
      .then((r) => setRelated(r.list || []))
      .catch(() => setRelated([]))
  }, [id])

  // UP 主关注状态(social 服务;未启动时静默降级)
  useEffect(() => {
    const uid = info?.userId ?? info?.user_id
    if (!uid) return
    getProfile(uid)
      .then((p) => {
        setFollowing(!!p.following)
        setFans(p.followerCount ?? p.follower_count ?? 0)
      })
      .catch(() => {})
  }, [info])

  // 历史弹幕:整片一次拉入,按 offset 展示
  useEffect(() => {
    if (!info?.id || historyLoadedRef.current) return
    historyLoadedRef.current = true
    listDanmu(info.id, 0, 0, 800)
      .then((r) => {
        for (const d of r.list || []) {
          const off = d.offsetMs ?? d.offset_ms ?? 0
          const uid = d.uid || '?'
          pushLine(itemKey(d), `${uid}: ${d.content || ''}`, off)
        }
      })
      .catch(() => {
        /* 历史可选,失败不挡播放 */
      })
  }, [info, pushLine])

  // 实时弹幕 WS
  useEffect(() => {
    if (!info) return
    const room = info.roomId || info.room_id || String(info.id)
    const hint = info.danmuWsHint || info.danmu_ws_hint || DANMU_WS_DEFAULT
    const token = getToken()
    if (!token) {
      setWsOn(false)
      return
    }
    let url: URL
    try {
      url = new URL(hint)
    } catch {
      setWsOn(false)
      return
    }
    url.searchParams.set('room', room)
    url.searchParams.set('uid', 'jwt')
    url.searchParams.set('token', token)

    const ws = new WebSocket(url.toString())
    wsRef.current = ws
    ws.onopen = () => setWsOn(true)
    ws.onclose = () => setWsOn(false)
    ws.onerror = () => setWsOn(false)
    ws.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data)
        const items: DanmuMsg[] = Array.isArray(data) ? data : [data]
        for (const m of items) {
          if (m.type && m.type !== 'danmu') continue
          if (!m.content) continue
          const off = m.offset_ms ?? 0
          pushLine(itemKey(m), `${m.uid || '?'}: ${m.content}`, off)
          // 实时消息一律飘字(历史靠 playMs 触发)
          setLiveFloat(`${m.content}-${Date.now()}`)
        }
      } catch {
        /* ignore */
      }
    }
    return () => {
      ws.close()
      wsRef.current = null
    }
  }, [info, pushLine])

  function currentOffsetMs(): number {
    const el = videoRef.current
    if (!el || !Number.isFinite(el.currentTime)) return 0
    return Math.max(0, Math.floor(el.currentTime * 1000))
  }

  async function submitComment() {
    if (!info || !commentText.trim() || !getToken()) return
    try {
      await postComment(info.id, commentText.trim())
      setCommentText('')
      const r = await listComments(info.id)
      setComments(r.list || [])
      setCommentTotal(r.total ?? (r.list || []).length)
    } catch (e) {
      setActionErr(e instanceof Error ? e.message : String(e))
    }
  }

  async function sendDanmu() {
    const content = text.trim()
    if (!content || !info) return
    const offsetMs = currentOffsetMs()
    setText('')
    setLiveFloat(`${content}-${Date.now()}`)
    pushLine(`local-${Date.now()}`, `me: ${content}`, offsetMs)

    const ws = wsRef.current
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'danmu', content, client_ts: Date.now(), offset_ms: offsetMs }))
    }
    if (getToken()) {
      try {
        await postDanmu(info.id, content, offsetMs)
      } catch (e) {
        console.warn('post danmu failed', e)
      }
    }
  }

  async function onFollow() {
    const uid = info?.userId ?? info?.user_id
    if (!uid || !getToken() || followBusy) return
    setFollowBusy(true)
    try {
      const r = await toggleFollow(uid)
      setFollowing(r.following)
      setFans(r.followerCount ?? r.follower_count ?? 0)
    } catch (e) {
      setActionErr(e instanceof Error ? e.message : String(e))
    } finally {
      setFollowBusy(false)
    }
  }

  // 侧栏:当前进度窗口内的历史 + 最近实时
  const windowMs = 8000
  const visible =
    playMs <= 0
      ? lines.slice(-40)
      : lines
          .filter((l) => l.offsetMs >= playMs - windowMs && l.offsetMs <= playMs + 2000)
          .slice(-40)

  if (err) {
    return (
      <main className="page">
        <div className="empty-state">
          <h2>视频加载失败</h2>
          <p>{err}</p>
          <Link className="btn" to="/">
            回首页
          </Link>
        </div>
      </main>
    )
  }

  if (!info) {
    return (
      <main className="page wide">
        <div className="watch-layout">
          <div>
            <div className="sk" style={{ aspectRatio: '16 / 9', borderRadius: 12 }} />
            <div className="sk sk-line" style={{ width: '40%', marginTop: 16 }} />
          </div>
          <div className="sk" style={{ height: 200, borderRadius: 12 }} />
        </div>
      </main>
    )
  }

  const ready = info.status === 'ready' && !!(info.playlistUrl || info.playlist_url)
  const src = ready ? info.playlistUrl || info.playlist_url : undefined
  const upId = info.userId ?? info.user_id
  const upName = info.author || `用户${upId ?? ''}`
  const isSelf = getUserId() === upId
  const loggedIn = !!getToken()

  return (
    <main className="page wide">
      <div className="watch-layout">
        {/* 左:播放器 + 信息 + 互动 + 评论 */}
        <section>
          <h1 className="watch-title">{info.title}</h1>
          <div className="watch-stats stat">
            <span>{formatCount(info.playCount ?? info.play_count)}播放</span>
            <span>{categoryLabel(info.category)}</span>
            <span>房间 {info.roomId || info.room_id}</span>
          </div>

          <div style={{ marginTop: 12 }}>
            <Player
              src={src}
              videoRef={videoRef}
              onTimeMs={setPlayMs}
              danmuOn={danmuOn}
              onToggleDanmu={setDanmuOn}
              placeholder={
                <p>
                  当前状态:{info.status || 'unknown'}
                  <br />
                  <span className="muted">转码中或未就绪,稍后刷新。历史弹幕仍可查看。</span>
                </p>
              }
            >
              <DanmuLayer playMs={playMs} cues={lines} enabled={danmuOn && ready} liveText={liveFloat} />
            </Player>
          </div>

          <div className="danmu-send">
            <span className={`ws-dot ${wsOn ? 'on' : ''}`} title={wsOn ? '弹幕通道已连接' : '弹幕通道未连接'} />
            <input
              value={text}
              onChange={(e) => setText(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && void sendDanmu()}
              placeholder={loggedIn ? '发条弹幕,和大家一起看' : '登录后发送弹幕'}
              disabled={!loggedIn}
              maxLength={100}
            />
            <button type="button" className="btn" onClick={() => void sendDanmu()} disabled={!loggedIn}>
              发送
            </button>
          </div>

          <div className="action-bar">
            <button
              type="button"
              className={`action-btn ${liked ? 'on' : ''}`}
              disabled={!loggedIn}
              onClick={async () => {
                try {
                  const r = await toggleLike(info.id)
                  setLiked(!!r.liked)
                  setLikeCount(r.likeCount ?? r.like_count ?? 0)
                } catch (e) {
                  setActionErr(e instanceof Error ? e.message : String(e))
                }
              }}
            >
              👍 <span className="num">{formatCount(likeCount)}</span>
            </button>
            <button
              type="button"
              className={`action-btn ${favorited ? 'on' : ''}`}
              disabled={!loggedIn}
              onClick={async () => {
                try {
                  const r = await toggleFavorite(info.id)
                  setFavorited(!!r.favorited)
                  setFavCount(r.favoriteCount ?? r.favorite_count ?? 0)
                } catch (e) {
                  setActionErr(e instanceof Error ? e.message : String(e))
                }
              }}
            >
              ⭐ <span className="num">{formatCount(favCount)}</span>
            </button>
            <span className="hint" style={{ marginLeft: 'auto' }}>
              评论 <span className="num">{commentTotal}</span>
            </span>
          </div>
          {actionErr && <p className="error-text">{actionErr}</p>}

          {info.description && <p className="desc-box">{info.description}</p>}

          <section className="comments">
            <h3>
              评论<span className="stat">{commentTotal}</span>
            </h3>
            <div className="comment-form">
              <input
                value={commentText}
                onChange={(e) => setCommentText(e.target.value)}
                placeholder={loggedIn ? '善语结善缘,发条友好的评论' : '登录后参与评论'}
                disabled={!loggedIn}
                onKeyDown={(e) => e.key === 'Enter' && void submitComment()}
              />
              <button className="btn" type="button" disabled={!loggedIn} onClick={() => void submitComment()}>
                发布
              </button>
            </div>
            {comments.length === 0 ? (
              <p className="hint">还没有评论,来抢沙发。</p>
            ) : (
              comments.map((c) => {
                const name = c.author || `用户${c.userId ?? c.user_id ?? ''}`
                return (
                  <div className="comment-item" key={c.id}>
                    <div className="comment-face">{name.slice(0, 1)}</div>
                    <div className="comment-body">
                      <div className="comment-author">
                        {name}
                        {c.createdAt || c.created_at ? (
                          <span> · {timeAgo(c.createdAt ?? c.created_at)}</span>
                        ) : null}
                      </div>
                      <div className="comment-text">{c.content}</div>
                    </div>
                  </div>
                )
              })
            )}
          </section>
        </section>

        {/* 右:UP 卡片 + 弹幕列表 + 相关视频 */}
        <aside>
          <div className="up-card" style={{ marginTop: 0 }}>
            <Link to={upId ? `/space/${upId}` : '#'} className="up-face">
              {upName.slice(0, 1)}
            </Link>
            <div className="up-info">
              {upId ? (
                <Link to={`/space/${upId}`} className="up-name">
                  {upName}
                </Link>
              ) : (
                <div className="up-name">{upName}</div>
              )}
              <div className="up-sub stat">{formatCount(fans)} 粉丝</div>
            </div>
            {!isSelf && (
              <button
                type="button"
                className={`follow-btn ${following ? 'on' : ''}`}
                disabled={!loggedIn || followBusy}
                title={loggedIn ? '' : '登录后可关注'}
                onClick={() => void onFollow()}
              >
                {following ? '已关注' : '+ 关注'}
              </button>
            )}
          </div>

          <div className="side-card" style={{ marginTop: 16 }}>
            <h3>
              弹幕
              <span className="hint">{wsOn ? '实时已连接' : loggedIn ? '连接中断' : '未登录'}</span>
            </h3>
            <div className="dm-list">
              {(visible.length ? visible : lines.slice(-40)).map((line) => (
                <div key={line.key} className="dm-line">
                  <time>{formatDuration(line.offsetMs / 1000) || '0:00'}</time>
                  {line.text}
                </div>
              ))}
              {lines.length === 0 && <span className="hint">还没有弹幕</span>}
            </div>
          </div>

          {related.length > 0 && (
            <div className="side-card">
              <h3>相关推荐</h3>
              {related.map((v) => {
                const cover = v.coverUrl || v.cover_url
                const dur = formatDuration(v.durationSec ?? v.duration_sec)
                return (
                  <Link key={v.id} to={`/watch/${v.id}`} className="rel-item">
                    <div className="rel-cover">
                      {cover ? <img src={cover} alt={v.title} loading="lazy" /> : <div className="cover-ph">X</div>}
                      {dur && <span className="dur-badge">{dur}</span>}
                    </div>
                    <div className="rel-info">
                      <div className="rel-title">{v.title}</div>
                      <div className="rel-meta stat">
                        {v.author || 'UP主'} · {formatCount(v.playCount ?? v.play_count)}播放
                      </div>
                    </div>
                  </Link>
                )
              })}
            </div>
          )}
        </aside>
      </div>
    </main>
  )
}
