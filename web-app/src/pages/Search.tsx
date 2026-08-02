import { useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { searchVideos, type SearchHit, type VideoInfo } from '../api/client'
import { GridSkeleton, VideoCard } from '../components/VideoCard'
import { CATEGORIES } from '../lib/format'

const PAGE_SIZE = 24

function hitToVideo(h: SearchHit): VideoInfo {
  return {
    id: h.id,
    title: h.title,
    description: h.description,
    category: h.category,
    userId: h.userId ?? h.user_id,
    author: h.author,
    status: 'ready',
    durationSec: h.durationSec ?? h.duration_sec,
    playCount: h.playCount ?? h.play_count,
    coverUrl: h.coverUrl ?? h.cover_url,
  }
}

/** 搜索结果页:/search?q=xxx,调 search 服务 */
export default function Search() {
  const [params] = useSearchParams()
  const q = (params.get('q') || '').trim()
  const [category, setCategory] = useState('')
  const [list, setList] = useState<VideoInfo[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [err, setErr] = useState('')

  useEffect(() => {
    if (!q) return
    setLoading(true)
    setErr('')
    setPage(1)
    searchVideos(q, 1, PAGE_SIZE, category)
      .then((r) => {
        setList((r.list || []).map(hitToVideo))
        setTotal(r.total || 0)
      })
      .catch((e) => setErr(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [q, category])

  async function loadMore() {
    setLoadingMore(true)
    try {
      const next = page + 1
      const r = await searchVideos(q, next, PAGE_SIZE, category)
      setList((prev) => [...prev, ...(r.list || []).map(hitToVideo)])
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
      <div className="section-head">
        <h2>“{q}”的搜索结果</h2>
        {total > 0 && <span className="hint num">共 {total} 个</span>}
      </div>
      <div className="cat-tabs">
        {cats.map((c) => (
          <button
            key={c.id || 'all'}
            type="button"
            className={`chip ${category === c.id ? 'on' : ''}`}
            onClick={() => setCategory(c.id)}
          >
            {c.label}
          </button>
        ))}
      </div>

      {loading && <GridSkeleton n={8} />}

      {!loading && err && (
        <div className="empty-state">
          <h2>搜索失败</h2>
          <p>{err}。请确认 search 服务(:8005)已启动。</p>
        </div>
      )}

      {!loading && !err && list.length === 0 && (
        <div className="empty-state">
          <h2>没有找到相关视频</h2>
          <p>换个关键词试试,或去首页逛逛。</p>
        </div>
      )}

      {!loading && !err && list.length > 0 && (
        <>
          <div className="video-grid" style={{ marginTop: 16 }}>
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
    </main>
  )
}
