import { Link } from 'react-router-dom'
import type { VideoInfo } from '../api/client'
import { categoryLabel, formatCount, formatDuration } from '../lib/format'

/** 信息流视频卡片:封面 16:9 + 时长角标 + 两行标题 + UP/播放量 */
export function VideoCard({ v }: { v: VideoInfo }) {
  const cover = v.coverUrl || v.cover_url
  const author = v.author || `用户${v.userId ?? v.user_id ?? ''}`
  const uid = v.userId ?? v.user_id
  const dur = formatDuration(v.durationSec ?? v.duration_sec)
  const plays = v.playCount ?? v.play_count ?? 0

  return (
    <article className="vcard-wrap">
      <Link to={`/watch/${v.id}`} className="vcard">
        <div className="vcard-cover">
          {cover ? <img src={cover} alt={v.title} loading="lazy" /> : <div className="cover-ph">暂无封面</div>}
          {dur && <span className="dur-badge">{dur}</span>}
        </div>
        <h3 className="vcard-title">{v.title}</h3>
      </Link>
      <div className="vcard-meta">
        {uid ? (
          <Link className="up" to={`/space/${uid}`}>
            {author}
          </Link>
        ) : (
          <span className="up">{author}</span>
        )}
        <span>·</span>
        <span>{formatCount(plays)}播放</span>
        <span className="tag">{categoryLabel(v.category)}</span>
      </div>
    </article>
  )
}

export function CardSkeleton() {
  return (
    <div aria-hidden>
      <div className="sk sk-cover" />
      <div className="sk sk-line" />
      <div className="sk sk-line short" />
    </div>
  )
}

export function GridSkeleton({ n = 8 }: { n?: number }) {
  return (
    <div className="video-grid">
      {Array.from({ length: n }, (_, i) => (
        <CardSkeleton key={i} />
      ))}
    </div>
  )
}
