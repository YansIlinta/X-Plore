/** 展示格式化:时长 / 万级计数 / 相对时间 */

export function formatDuration(sec?: number): string {
  if (!sec || sec <= 0) return ''
  const s = Math.floor(sec)
  const h = Math.floor(s / 3600)
  const m = Math.floor((s % 3600) / 60)
  const r = s % 60
  if (h > 0) return `${h}:${String(m).padStart(2, '0')}:${String(r).padStart(2, '0')}`
  return `${m}:${String(r).padStart(2, '0')}`
}

export function formatCount(n?: number): string {
  const v = n ?? 0
  if (v >= 100000000) return `${(v / 100000000).toFixed(1)}亿`
  if (v >= 10000) return `${(v / 10000).toFixed(1)}万`
  return String(v)
}

export function timeAgo(unixMs?: number): string {
  if (!unixMs) return ''
  const diff = Date.now() - unixMs
  const min = Math.floor(diff / 60000)
  if (min < 1) return '刚刚'
  if (min < 60) return `${min}分钟前`
  const h = Math.floor(min / 60)
  if (h < 24) return `${h}小时前`
  const d = Math.floor(h / 24)
  if (d < 30) return `${d}天前`
  const dt = new Date(unixMs)
  return `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}-${String(dt.getDate()).padStart(2, '0')}`
}

/** 分区 id → 展示名,全站统一 */
export const CATEGORIES = [
  { id: 'general', label: '综合' },
  { id: 'tech', label: '科技' },
  { id: 'anime', label: '动画' },
  { id: 'music', label: '音乐' },
  { id: 'game', label: '游戏' },
] as const

export function categoryLabel(id?: string): string {
  return CATEGORIES.find((c) => c.id === id)?.label || id || '综合'
}
