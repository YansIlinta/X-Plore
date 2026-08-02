import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { getToken, listMyVideos, type VideoInfo } from '../api/client'
import { GridSkeleton } from '../components/VideoCard'
import { categoryLabel, formatCount, formatDuration } from '../lib/format'

const STATUS_LABEL: Record<string, string> = {
  ready: '已发布',
  processing: '转码中',
  uploading: '待上传',
  failed: '转码失败',
}

/** 稿件管理:含非 ready 状态,展示转码进度 */
export default function Mine() {
  const [list, setList] = useState<VideoInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState('')

  useEffect(() => {
    if (!getToken()) return
    listMyVideos()
      .then((r) => setList(r.list || []))
      .catch((e) => setErr(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (!getToken()) {
    return (
      <main className="page narrow">
        <div className="form-card" style={{ textAlign: 'center' }}>
          <h2>登录后查看你的稿件</h2>
          <Link className="btn" to="/login" style={{ marginTop: 18 }}>
            去登录
          </Link>
        </div>
      </main>
    )
  }

  return (
    <main className="page wide">
      <div className="section-head">
        <h2>稿件管理</h2>
        {list.length > 0 && <span className="hint num">{list.length} 个稿件</span>}
        <Link to="/upload" className="btn-ghost more">
          + 新投稿
        </Link>
      </div>

      {err && <p className="error-text">{err}</p>}
      {loading && <GridSkeleton n={8} />}

      {!loading && !err && list.length === 0 && (
        <div className="empty-state">
          <h2>还没有投稿</h2>
          <p>上传第一个视频,转码完成后会出现在首页。</p>
          <Link className="btn" to="/upload">
            去投稿
          </Link>
        </div>
      )}

      {!loading && list.length > 0 && (
        <div className="video-grid">
          {list.map((v) => {
            const cover = v.coverUrl || v.cover_url
            const dur = formatDuration(v.durationSec ?? v.duration_sec)
            return (
              <article key={v.id}>
                <Link to={`/watch/${v.id}`} className="vcard">
                  <div className="vcard-cover">
                    {cover ? (
                      <img src={cover} alt={v.title} loading="lazy" />
                    ) : (
                      <div className="cover-ph">{STATUS_LABEL[v.status] || v.status}</div>
                    )}
                    {dur && <span className="dur-badge">{dur}</span>}
                  </div>
                  <h3 className="vcard-title">{v.title}</h3>
                </Link>
                <div className="vcard-meta">
                  <span className={`tag status-${v.status}`}>{STATUS_LABEL[v.status] || v.status}</span>
                  <span>{categoryLabel(v.category)}</span>
                  {v.status === 'ready' && <span>{formatCount(v.playCount ?? v.play_count)}播放</span>}
                </div>
              </article>
            )
          })}
        </div>
      )}
    </main>
  )
}
