const TOKEN_KEY = 'xplore_token'
const USER_KEY = 'xplore_user_id'

/** 开发默认空串走 Vite 同源代理；走 gateway 时设 VITE_API_BASE=http://localhost:8088 */
export const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined)?.replace(/\/$/, '') || ''

/** 弹幕 WS；默认直连 comet，经 gateway 时设 VITE_DANMU_WS=ws://localhost:8088/ws */
export const DANMU_WS_DEFAULT =
  (import.meta.env.VITE_DANMU_WS as string | undefined) || 'ws://localhost:8080/ws'

function apiURL(path: string): string {
  if (path.startsWith('http')) return path
  return `${API_BASE}${path}`
}

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY)
}

export function setAuth(token: string, userId: number) {
  localStorage.setItem(TOKEN_KEY, token)
  localStorage.setItem(USER_KEY, String(userId))
}

export function clearAuth() {
  localStorage.removeItem(TOKEN_KEY)
  localStorage.removeItem(USER_KEY)
}

export function getUserId(): number | null {
  const v = localStorage.getItem(USER_KEY)
  return v ? Number(v) : null
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  if (!headers.has('Content-Type') && init.body) {
    headers.set('Content-Type', 'application/json')
  }
  const token = getToken()
  if (token) headers.set('Authorization', `Bearer ${token}`)

  const res = await fetch(apiURL(path), { ...init, headers })
  if (!res.ok) {
    let msg = res.statusText
    try {
      const body = await res.json()
      msg = body.message || body.reason || JSON.stringify(body)
    } catch {
      /* ignore */
    }
    throw new Error(msg)
  }
  if (res.status === 204) return undefined as T
  return res.json() as Promise<T>
}

export type AuthReply = { token: string; userId?: number; user_id?: number }

export async function register(username: string, password: string) {
  const data = await request<AuthReply>('/v1/register', {
    method: 'POST',
    body: JSON.stringify({ username, password }),
  })
  const uid = data.userId ?? data.user_id ?? 0
  setAuth(data.token, uid)
  return data
}

export async function login(username: string, password: string) {
  const data = await request<AuthReply>('/v1/login', {
    method: 'POST',
    body: JSON.stringify({ username, password }),
  })
  const uid = data.userId ?? data.user_id ?? 0
  setAuth(data.token, uid)
  return data
}

export type VideoInfo = {
  id: number
  title: string
  description?: string
  category?: string
  userId?: number
  user_id?: number
  author?: string
  status: string
  durationSec?: number
  duration_sec?: number
  playCount?: number
  play_count?: number
  coverUrl?: string
  cover_url?: string
  playlistUrl?: string
  playlist_url?: string
  roomId?: string
  room_id?: string
  danmuWsHint?: string
  danmu_ws_hint?: string
  likeCount?: number
  like_count?: number
  commentCount?: number
  comment_count?: number
  favoriteCount?: number
  favorite_count?: number
  liked?: boolean
  favorited?: boolean
}

export async function listVideos(page = 1, size = 20, category = '', opts: { userId?: number; sort?: 'hot' | 'new' } = {}) {
  const q = new URLSearchParams({ page: String(page), size: String(size) })
  if (category) q.set('category', category)
  if (opts.userId) q.set('user_id', String(opts.userId))
  if (opts.sort) q.set('sort', opts.sort)
  return request<{ list: VideoInfo[]; total: number }>(`/v1/videos?${q}`)
}

export async function listRelated(id: number, limit = 12) {
  return request<{ list: VideoInfo[] }>(`/v1/videos/${id}/related?limit=${limit}`)
}

// ---- search 服务(:8005 / gateway /v1/search)----

export type SearchHit = {
  id: number
  title: string
  description?: string
  category?: string
  userId?: number
  user_id?: number
  author?: string
  durationSec?: number
  duration_sec?: number
  playCount?: number
  play_count?: number
  coverUrl?: string
  cover_url?: string
  createdAt?: number
  created_at?: number
}

export async function searchVideos(q: string, page = 1, size = 20, category = '') {
  const p = new URLSearchParams({ q, page: String(page), size: String(size) })
  if (category) p.set('category', category)
  return request<{ list: SearchHit[]; total: number }>(`/v1/search/videos?${p}`)
}

export async function searchSuggest(q: string) {
  return request<{ list: string[] }>(`/v1/search/suggest?q=${encodeURIComponent(q)}`)
}

// ---- social 服务(:8004 / gateway /v1/users)----

export type Profile = {
  id: number
  username: string
  followerCount?: number
  follower_count?: number
  followingCount?: number
  following_count?: number
  following?: boolean
}

export async function getProfile(userId: number) {
  return request<Profile>(`/v1/users/${userId}/profile`)
}

export async function toggleFollow(userId: number) {
  return request<{ following: boolean; followerCount?: number; follower_count?: number }>(
    `/v1/users/${userId}/follow`,
    { method: 'POST', body: JSON.stringify({}) },
  )
}

export async function getVideo(id: number) {
  return request<VideoInfo>(`/v1/videos/${id}`)
}

export async function createVideo(title: string, description: string, category: string) {
  return request<{ id: number; uploadUrl?: string; upload_url?: string; roomId?: string; room_id?: string }>(
    '/v1/videos',
    { method: 'POST', body: JSON.stringify({ title, description, category }) },
  )
}

export async function completeUpload(id: number) {
  return request<{ status: string }>(`/v1/videos/${id}/complete`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
}

export async function listMyVideos(page = 1, size = 50) {
  const q = new URLSearchParams({ page: String(page), size: String(size) })
  return request<{ list: VideoInfo[]; total: number }>(`/v1/me/videos?${q}`)
}

export type DanmuItem = {
  msgId?: string
  msg_id?: string
  uid?: string
  content?: string
  offsetMs?: number
  offset_ms?: number
  createdAt?: number
  created_at?: number
}

export async function listDanmu(videoId: number, fromMs = 0, toMs = 0, limit = 500) {
  const q = new URLSearchParams({
    from_ms: String(fromMs),
    to_ms: String(toMs),
    limit: String(limit),
  })
  return request<{ list: DanmuItem[] }>(`/v1/videos/${videoId}/danmu?${q}`)
}

export async function postDanmu(videoId: number, content: string, offsetMs: number) {
  return request<{ msgId?: string; msg_id?: string; offsetMs?: number; offset_ms?: number }>(
    `/v1/videos/${videoId}/danmu`,
    { method: 'POST', body: JSON.stringify({ content, offset_ms: offsetMs, offsetMs }) },
  )
}

export type CommentItem = {
  id: number
  userId?: number
  user_id?: number
  author?: string
  content: string
  createdAt?: number
  created_at?: number
}

export async function listComments(videoId: number, page = 1, size = 50) {
  const q = new URLSearchParams({ page: String(page), size: String(size) })
  return request<{ list: CommentItem[]; total: number }>(`/v1/videos/${videoId}/comments?${q}`)
}

export async function postComment(videoId: number, content: string) {
  return request<{ id: number }>(`/v1/videos/${videoId}/comments`, {
    method: 'POST',
    body: JSON.stringify({ content }),
  })
}

export async function toggleLike(videoId: number) {
  return request<{ liked: boolean; likeCount?: number; like_count?: number }>(
    `/v1/videos/${videoId}/like`,
    { method: 'POST', body: JSON.stringify({}) },
  )
}

export async function toggleFavorite(videoId: number) {
  return request<{ favorited: boolean; favoriteCount?: number; favorite_count?: number }>(
    `/v1/videos/${videoId}/favorite`,
    { method: 'POST', body: JSON.stringify({}) },
  )
}

/** 浏览器直传 MinIO（预签名 PUT） */
export async function putToPresigned(url: string, file: File, onProgress?: (pct: number) => void) {
  return new Promise<void>((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    xhr.open('PUT', url)
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable && onProgress) onProgress(Math.round((e.loaded / e.total) * 100))
    }
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) resolve()
      else reject(new Error(`上传失败 HTTP ${xhr.status}`))
    }
    xhr.onerror = () => reject(new Error('上传网络错误'))
    xhr.setRequestHeader('Content-Type', file.type || 'application/octet-stream')
    xhr.send(file)
  })
}
