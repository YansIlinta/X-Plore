import { useState, type FormEvent } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { completeUpload, createVideo, getToken, putToPresigned } from '../api/client'
import { CATEGORIES } from '../lib/format'

/** 投稿:预签名直传 MinIO + 转码状态提示 */
export default function Upload() {
  const nav = useNavigate()
  const [title, setTitle] = useState('')
  const [description, setDescription] = useState('')
  const [category, setCategory] = useState('general')
  const [file, setFile] = useState<File | null>(null)
  const [pct, setPct] = useState(0)
  const [status, setStatus] = useState('')
  const [err, setErr] = useState('')
  const [busy, setBusy] = useState(false)

  if (!getToken()) {
    return (
      <main className="page narrow">
        <div className="form-card" style={{ textAlign: 'center' }}>
          <h2>投稿需要登录</h2>
          <p className="hint">登录后即可上传视频,大文件直传对象存储,不经业务服务器。</p>
          <Link className="btn" to="/login" style={{ marginTop: 18 }}>
            去登录
          </Link>
        </div>
      </main>
    )
  }

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    if (!file) {
      setErr('请选择视频文件')
      return
    }
    setErr('')
    setBusy(true)
    setPct(0)
    try {
      setStatus('创建稿件…')
      const created = await createVideo(title, description, category)
      const id = created.id
      const uploadURL = created.uploadUrl || created.upload_url
      if (!uploadURL) throw new Error('未返回上传地址')

      setStatus('直传对象存储…')
      await putToPresigned(uploadURL, file, setPct)

      setStatus('通知转码…')
      await completeUpload(id)
      setStatus('已进入转码队列,可在「我的」查看进度')
      setTimeout(() => nav(`/watch/${id}`), 800)
    } catch (ex) {
      setErr(ex instanceof Error ? ex.message : String(ex))
      setStatus('')
    } finally {
      setBusy(false)
    }
  }

  return (
    <main className="page narrow">
      <div className="form-card">
        <h2>投稿</h2>
        <form onSubmit={onSubmit}>
          <label className="field">
            标题
            <input
              className="input"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              required
              maxLength={80}
              placeholder="起个让人想点进来的标题"
            />
          </label>
          <label className="field">
            简介
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              rows={3}
              maxLength={500}
              placeholder="介绍一下这个视频(选填)"
            />
          </label>
          <label className="field">
            分区
            <select value={category} onChange={(e) => setCategory(e.target.value)}>
              {CATEGORIES.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.label}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            视频文件
            <div className={`file-drop ${file ? 'has-file' : ''}`}>
              {file ? (
                <span>
                  {file.name}
                  <span className="hint num"> · {(file.size / 1024 / 1024).toFixed(1)} MB</span>
                </span>
              ) : (
                '点击选择 mp4 等视频文件'
              )}
              <input
                type="file"
                accept="video/*"
                style={{ display: 'none' }}
                onChange={(e) => setFile(e.target.files?.[0] ?? null)}
              />
            </div>
          </label>
          {pct > 0 && (
            <>
              <div className="progress">
                <div style={{ width: `${pct}%` }} />
              </div>
              <p className="hint num">{pct}%</p>
            </>
          )}
          {status && <p className="hint">{status}</p>}
          {err && <p className="error-text">{err}</p>}
          <button className="btn" type="submit" disabled={busy} style={{ width: '100%', marginTop: 8 }}>
            {busy ? '处理中…' : '上传并投稿'}
          </button>
        </form>
      </div>
    </main>
  )
}
