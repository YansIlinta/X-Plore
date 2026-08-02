import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import {
  getProfile,
  getToken,
  getUserId,
  listVideos,
  toggleFollow,
  type Profile,
  type VideoInfo,
} from '../api/client'
import { GridSkeleton, VideoCard } from '../components/VideoCard'
import { formatCount } from '../lib/format'

const PAGE_SIZE = 24

/** 个人空间:UP 信息(粉丝/关注 + 关注按钮) + 公开投稿列表 */
export default function Space() {
  const { uid } = useParams()
  const userId = Number(uid)
  const isSelf = getUserId() === userId

  const [profile, setProfile] = useState<Profile | null>(null)
  const [following, setFollowing] = useState(false)
  const [followers, setFollowers] = useState(0)
  const [list, setList] = useState<VideoInfo[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState('')

  useEffect(() => {
    if (!userId) return
    setLoading(true)
    setErr('')
    Promise.all([
      getProfile(userId).then((p) => {
        setProfile(p)
        setFollowing(!!p.following)
        setFollowers(p.followerCount ?? p.follower_count ?? 0)
      }),
      listVideos(1, PAGE_SIZE, '', { userId }).then((r) => {
        setList(r.list || [])
        setTotal(r.total || 0)
      }),
    ])
      .catch((e) => setErr(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [userId])

  async function onFollow() {
    if (!getToken() || busy) return
    setBusy(true)
    try {
      const r = await toggleFollow(userId)
      setFollowing(r.following)
      setFollowers(r.followerCount ?? r.follower_count ?? 0)
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  async function loadMore() {
    const next = page + 1
    const r = await listVideos(next, PAGE_SIZE, '', { userId })
    setList((prev) => [...prev, ...(r.list || [])])
    setPage(next)
  }

  if (!userId) {
    return (
      <main className="page">
        <div className="empty-state">
          <h2>无效的用户</h2>
        </div>
      </main>
    )
  }

  const name = profile?.username || `用户${userId}`

  return (
    <main className="page wide">
      <section className="space-head">
        <div className="up-face lg">{name.slice(0, 1)}</div>
        <div className="space-info">
          <h1 className="space-name">{name}</h1>
          <div className="space-stats stat">
            <span>
              <b>{formatCount(followers)}</b>粉丝
            </span>
            <span>
              <b>{formatCount(profile?.followingCount ?? profile?.following_count ?? 0)}</b>关注
            </span>
            <span>
              <b className="num">{total}</b>投稿
            </span>
          </div>
        </div>
        {!isSelf && (
          <button
            type="button"
            className={`follow-btn ${following ? 'on' : ''}`}
            disabled={!getToken() || busy}
            title={getToken() ? '' : '登录后可关注'}
            onClick={() => void onFollow()}
          >
            {following ? '已关注' : '+ 关注'}
          </button>
        )}
      </section>

      {err && <p className="error-text">{err}</p>}
      {loading && <GridSkeleton n={8} />}

      {!loading && list.length === 0 && (
        <div className="empty-state">
          <h2>TA 还没有公开投稿</h2>
        </div>
      )}

      {!loading && list.length > 0 && (
        <>
          <div className="video-grid">
            {list.map((v) => (
              <VideoCard key={v.id} v={v} />
            ))}
          </div>
          {list.length < total && (
            <div className="load-more">
              <button type="button" className="btn-ghost" onClick={() => void loadMore()}>
                加载更多
              </button>
            </div>
          )}
        </>
      )}
    </main>
  )
}
