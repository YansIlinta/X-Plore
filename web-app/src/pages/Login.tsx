import { useState, type FormEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import { login, register } from '../api/client'

export default function Login() {
  const nav = useNavigate()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [mode, setMode] = useState<'login' | 'register'>('login')
  const [err, setErr] = useState('')
  const [busy, setBusy] = useState(false)

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setErr('')
    setBusy(true)
    try {
      if (mode === 'login') await login(username, password)
      else await register(username, password)
      nav('/')
    } catch (ex) {
      setErr(ex instanceof Error ? ex.message : String(ex))
    } finally {
      setBusy(false)
    }
  }

  return (
    <main className="page narrow">
      <div className="form-card">
        <h2>{mode === 'login' ? '登录 X-Plore' : '注册新账号'}</h2>
        <form onSubmit={onSubmit}>
          <label className="field">
            用户名
            <input
              className="input"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              required
              minLength={2}
              autoComplete="username"
            />
          </label>
          <label className="field">
            密码
            <input
              className="input"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              minLength={4}
              autoComplete={mode === 'login' ? 'current-password' : 'new-password'}
            />
          </label>
          {err && <p className="error-text">{err}</p>}
          <button className="btn" disabled={busy} type="submit" style={{ width: '100%', marginTop: 8 }}>
            {busy ? '请稍候…' : mode === 'login' ? '登录' : '注册并登录'}
          </button>
        </form>
        <p style={{ marginTop: 16, textAlign: 'center' }}>
          <button
            type="button"
            className="linkish"
            onClick={() => setMode(mode === 'login' ? 'register' : 'login')}
          >
            {mode === 'login' ? '没有账号?注册一个' : '已有账号?直接登录'}
          </button>
        </p>
      </div>
    </main>
  )
}
