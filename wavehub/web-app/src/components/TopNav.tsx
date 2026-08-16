import { useEffect, useRef, useState } from 'react'
import { Link, NavLink, useNavigate } from 'react-router-dom'
import { clearAuth, getToken, getUserId, searchSuggest } from '../api/client'

/** 顶栏:logo / 全站搜索(联想) / 导航 / 用户菜单 */
export default function TopNav() {
  const nav = useNavigate()
  const token = getToken()
  const uid = getUserId()

  const [q, setQ] = useState('')
  const [suggests, setSuggests] = useState<string[]>([])
  const [showSuggest, setShowSuggest] = useState(false)
  const [menuOpen, setMenuOpen] = useState(false)
  const searchRef = useRef<HTMLDivElement>(null)
  const menuRef = useRef<HTMLDivElement>(null)
  const debounceRef = useRef(0)

  // 输入联想:250ms 防抖调 search 服务;服务未启动时静默降级
  useEffect(() => {
    window.clearTimeout(debounceRef.current)
    const kw = q.trim()
    if (!kw) {
      setSuggests([])
      return
    }
    debounceRef.current = window.setTimeout(() => {
      searchSuggest(kw)
        .then((r) => setSuggests((r.list || []).slice(0, 10)))
        .catch(() => setSuggests([]))
    }, 250)
    return () => window.clearTimeout(debounceRef.current)
  }, [q])

  // 点击外部关闭下拉
  useEffect(() => {
    function onDoc(e: MouseEvent) {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) setShowSuggest(false)
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) setMenuOpen(false)
    }
    document.addEventListener('mousedown', onDoc)
    return () => document.removeEventListener('mousedown', onDoc)
  }, [])

  function submitSearch(kw?: string) {
    const w = (kw ?? q).trim()
    if (!w) return
    setShowSuggest(false)
    setQ(w)
    nav(`/search?q=${encodeURIComponent(w)}`)
  }

  return (
    <header className="topnav">
      <div className="topnav-inner">
        <Link to="/" className="brand" aria-label="X-Plore 首页">
          <span className="brand-mark">X</span>
          X-Plore
        </Link>
        <nav style={{ display: 'flex', gap: 2 }}>
          <NavLink to="/" end className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            首页
          </NavLink>
          <NavLink to="/mine" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            我的
          </NavLink>
        </nav>

        <div className="search-wrap" ref={searchRef}>
          <div className="search-box">
            <input
              value={q}
              placeholder="搜索视频"
              onChange={(e) => {
                setQ(e.target.value)
                setShowSuggest(true)
              }}
              onFocus={() => setShowSuggest(true)}
              onKeyDown={(e) => e.key === 'Enter' && submitSearch()}
              aria-label="搜索视频"
            />
            <button type="button" className="search-btn" onClick={() => submitSearch()} aria-label="搜索">
              搜索
            </button>
          </div>
          {showSuggest && suggests.length > 0 && (
            <div className="suggest-panel">
              {suggests.map((s) => (
                <button key={s} type="button" className="suggest-item" onClick={() => submitSearch(s)}>
                  {s}
                </button>
              ))}
            </div>
          )}
        </div>

        <div className="nav-actions">
          <Link to="/upload" className="btn">
            投稿
          </Link>
          {token && uid ? (
            <div className="user-menu-wrap" ref={menuRef}>
              <button
                type="button"
                className="avatar"
                onClick={() => setMenuOpen((v) => !v)}
                aria-label="用户菜单"
              >
                U{uid}
              </button>
              {menuOpen && (
                <div className="user-menu">
                  <Link className="menu-item" to={`/space/${uid}`} onClick={() => setMenuOpen(false)}>
                    个人空间
                  </Link>
                  <Link className="menu-item" to="/mine" onClick={() => setMenuOpen(false)}>
                    稿件管理
                  </Link>
                  <div className="menu-sep" />
                  <button
                    type="button"
                    className="menu-item danger"
                    onClick={() => {
                      clearAuth()
                      window.location.href = '/'
                    }}
                  >
                    退出登录
                  </button>
                </div>
              )}
            </div>
          ) : (
            <Link to="/login" className="btn-ghost">
              登录
            </Link>
          )}
        </div>
      </div>
    </header>
  )
}
