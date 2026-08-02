import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { listVideos, type VideoInfo } from '../api/client'
import { GridSkeleton, VideoCard } from '../components/VideoCard'
import { CATEGORIES } from '../lib/format'

const PAGE_SIZE = 24

/** 首页:分区 tab + 热门横排(全部分区时) + 最新信息流(加载更多) */
export default function Home() {
  const [category, setCategory] = useState('')
  const [hot, setHot] = useState<VideoInfo[]>([])
  const [list, setList] = useState<VideoInfo[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [err, setErr] = useState('')

  // 切换分区:重置分页并拉首屏
  useEffect(() => {
    setLoading(true)
    setErr('')
    setPage(1)
    const jobs: Promise<void>[] = [
      listVideos(1, PAGE_SIZE, category).then((r) => {
        setList(r.list || [])
        setTotal(r.total || 0)
      }),
    ]
    if (!category) {
      jobs.push(
        listVideos(1, 8, '', { sort: 'hot' })
          .then((r) => setHot((r.list || []).filter((v) => (v.playCount ?? v.play_count ?? 0) > 0).slice(0, 4)))
          .catch(() => setHot([])),
      )
    } else {
      setHot([])
    }
    Promise.all(jobs)
      .catch((e) => setErr(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [category])

  async function loadMore() {
    setLoadingMore(true)
    try {
      const next = page + 1
      const r = await listVideos(next, PAGE_SIZE, category)
      setList((prev) => [...prev, ...(r.list || [])])
      setPage(next)
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setLoadingMore(false)
    }
  }

  const cats = [{ id: '', label: '全部' }, ...CATEGORIES]

  return (
    <main className="page wide">
      <div className="cat-tabs" role="tablist" aria-label="分区">
        {cats.map((c) => (
          <button
            key={c.id || 'all'}
            type="button"
            role="tab"
            aria-selected={category === c.id}
            className={`chip ${category === c.id ? 'on' : ''}`}
            onClick={() => setCategory(c.id)}
          >
            {c.label}
          </button>
        ))}
      </div>

      {loading && (
        <>
          <div className="section-head">
            <h2>推荐</h2>
          </div>
          <GridSkeleton n={12} />
        </>
      )}

      {!loading && err && (
        <div className="empty-state">
          <h2>加载失败</h2>
          <p>{err}。请确认 video 服务(:8003)已启动。</p>
        </div>
      )}

      {!loading && !err && (
        <>
          {hot.length > 0 && (
            <>
              <div className="section-head">
                <h2>热门</h2>
                <span className="hint">按播放与点赞加权</span>
              </div>
              <div className="video-grid">
                {hot.map((v) => (
                  <VideoCard key={`hot-${v.id}`} v={v} />
                ))}
              </div>
            </>
          )}

          <div className="section-head">
            <h2>{category ? '分区视频' : '最新'}</h2>
            {total > 0 && <span className="hint num">{total} 个稿件</span>}
          </div>

          {list.length === 0 ? (
            <div className="empty-state">
              <h2>这里还是空的</h2>
              <p>成为第一个投稿的人,转码完成后视频会出现在这里。</p>
              <Link className="btn" to="/upload">
                去投稿
              </Link>
            </div>
          ) : (
            <>
              <div className="video-grid">
                {list.map((v) => (
                  <VideoCard key={v.id} v={v} />
                ))}
              </div>
              {list.length < total && (
                <div className="load-more">
                  <button type="button" className="btn-ghost" disabled={loadingMore} onClick={() => void loadMore()}>
                    {loadingMore ? '加载中…' : '加载更多'}
                  </button>
                </div>
              )}
            </>
          )}
        </>
      )}
    </main>
  )
}
