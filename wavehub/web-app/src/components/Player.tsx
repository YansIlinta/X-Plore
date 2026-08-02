import { useCallback, useEffect, useRef, useState, type ReactNode, type RefObject } from 'react'
import Hls from 'hls.js'
import { formatDuration } from '../lib/format'

type Props = {
  /** HLS m3u8;空表示未就绪 */
  src?: string
  /** 由父级持有,便于取 currentTime 计算弹幕 offset */
  videoRef: RefObject<HTMLVideoElement | null>
  onTimeMs?: (ms: number) => void
  danmuOn: boolean
  onToggleDanmu: (on: boolean) => void
  /** 未就绪时的占位内容 */
  placeholder?: ReactNode
  /** 弹幕层等覆盖物 */
  children?: ReactNode
}

const RATES = [2, 1.5, 1.25, 1, 0.75, 0.5]

/** 类 B 站自定义控制条播放器:进度/缓冲/倍速/音量/弹幕开关/网页全屏/全屏 */
export default function Player({ src, videoRef, onTimeMs, danmuOn, onToggleDanmu, placeholder, children }: Props) {
  const boxRef = useRef<HTMLDivElement>(null)
  const idleTimer = useRef(0)
  const [playing, setPlaying] = useState(false)
  const [cur, setCur] = useState(0)
  const [dur, setDur] = useState(0)
  const [buf, setBuf] = useState(0)
  const [vol, setVol] = useState(1)
  const [muted, setMuted] = useState(false)
  const [rate, setRate] = useState(1)
  const [idle, setIdle] = useState(false)
  const [webFull, setWebFull] = useState(false)
  const [showRate, setShowRate] = useState(false)
  const [showVol, setShowVol] = useState(false)

  // HLS 装载
  useEffect(() => {
    const el = videoRef.current
    if (!el || !src) return
    let hls: Hls | null = null
    if (Hls.isSupported()) {
      hls = new Hls()
      hls.loadSource(src)
      hls.attachMedia(el)
    } else if (el.canPlayType('application/vnd.apple.mpegurl')) {
      el.src = src
    }
    return () => {
      hls?.destroy()
    }
  }, [src, videoRef])

  // media 事件
  useEffect(() => {
    const el = videoRef.current
    if (!el || !src) return
    const onTime = () => {
      setCur(el.currentTime)
      onTimeMs?.(Math.floor(el.currentTime * 1000))
    }
    const onDur = () => setDur(el.duration || 0)
    const onProg = () => {
      const b = el.buffered
      if (b.length) setBuf(b.end(b.length - 1))
    }
    const onPlay = () => setPlaying(true)
    const onPause = () => setPlaying(false)
    el.addEventListener('timeupdate', onTime)
    el.addEventListener('seeked', onTime)
    el.addEventListener('durationchange', onDur)
    el.addEventListener('progress', onProg)
    el.addEventListener('play', onPlay)
    el.addEventListener('pause', onPause)
    return () => {
      el.removeEventListener('timeupdate', onTime)
      el.removeEventListener('seeked', onTime)
      el.removeEventListener('durationchange', onDur)
      el.removeEventListener('progress', onProg)
      el.removeEventListener('play', onPlay)
      el.removeEventListener('pause', onPause)
    }
  }, [src, videoRef, onTimeMs])

  const togglePlay = useCallback(() => {
    const el = videoRef.current
    if (!el) return
    if (el.paused) void el.play()
    else el.pause()
  }, [videoRef])

  // 播放中 2.5s 无操作隐藏控制条
  const wake = useCallback(() => {
    setIdle(false)
    window.clearTimeout(idleTimer.current)
    idleTimer.current = window.setTimeout(() => setIdle(true), 2500)
  }, [])

  useEffect(() => {
    if (!playing) {
      setIdle(false)
      window.clearTimeout(idleTimer.current)
    } else {
      wake()
    }
    return () => window.clearTimeout(idleTimer.current)
  }, [playing, wake])

  // Esc 退网页全屏;空格播放暂停(焦点在播放器内时)
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setWebFull(false)
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [])

  function seekTo(e: React.MouseEvent<HTMLDivElement>) {
    const el = videoRef.current
    if (!el || !dur) return
    const rect = e.currentTarget.getBoundingClientRect()
    const ratio = Math.min(1, Math.max(0, (e.clientX - rect.left) / rect.width))
    el.currentTime = ratio * dur
  }

  function setVolume(v: number) {
    const el = videoRef.current
    setVol(v)
    setMuted(v === 0)
    if (el) {
      el.volume = v
      el.muted = v === 0
    }
  }

  function toggleMute() {
    const el = videoRef.current
    const next = !muted
    setMuted(next)
    if (el) el.muted = next
  }

  function applyRate(r: number) {
    const el = videoRef.current
    setRate(r)
    setShowRate(false)
    if (el) el.playbackRate = r
  }

  function toggleFullscreen() {
    const box = boxRef.current
    if (!box) return
    if (document.fullscreenElement) void document.exitFullscreen()
    else void box.requestFullscreen()
  }

  const curPct = dur ? (cur / dur) * 100 : 0
  const bufPct = dur ? Math.min(100, (buf / dur) * 100) : 0

  return (
    <div
      ref={boxRef}
      className={`player-box ${idle ? 'idle' : ''} ${webFull ? 'web-full' : ''}`}
      onMouseMove={wake}
      onMouseLeave={() => playing && setIdle(true)}
    >
      {src ? (
        <video
          ref={videoRef}
          className="player-video"
          playsInline
          onClick={togglePlay}
          onDoubleClick={toggleFullscreen}
        />
      ) : (
        <div className="player-ph">{placeholder}</div>
      )}

      {children}

      {src && (
        <div className="player-ctrl">
          <div className="progress-rail" onClick={seekTo} role="slider" aria-label="播放进度"
            aria-valuemin={0} aria-valuemax={Math.floor(dur)} aria-valuenow={Math.floor(cur)}>
            <div className="progress-buf" style={{ width: `${bufPct}%` }} />
            <div className="progress-cur" style={{ width: `${curPct}%` }} />
          </div>
          <div className="ctrl-row">
            <button type="button" className="ctrl-btn" onClick={togglePlay} aria-label={playing ? '暂停' : '播放'}>
              {playing ? '⏸' : '▶'}
            </button>
            <span className="ctrl-time">
              {formatDuration(cur) || '0:00'} / {formatDuration(dur) || '0:00'}
            </span>
            <div className="ctrl-spacer" />
            <button
              type="button"
              className={`ctrl-btn ${danmuOn ? 'on' : ''}`}
              onClick={() => onToggleDanmu(!danmuOn)}
              title="弹幕开关"
            >
              弹
            </button>
            <div className="rate-wrap">
              <button type="button" className="ctrl-btn" onClick={() => setShowRate((v) => !v)}>
                {rate === 1 ? '倍速' : `${rate}x`}
              </button>
              {showRate && (
                <div className="pop-menu">
                  {RATES.map((r) => (
                    <button
                      key={r}
                      type="button"
                      className={`ctrl-btn ${r === rate ? 'on' : ''}`}
                      onClick={() => applyRate(r)}
                    >
                      {r}x
                    </button>
                  ))}
                </div>
              )}
            </div>
            <div className="vol-wrap"
              onMouseEnter={() => setShowVol(true)}
              onMouseLeave={() => setShowVol(false)}>
              <button type="button" className="ctrl-btn" onClick={toggleMute} aria-label="音量">
                {muted || vol === 0 ? '🔇' : '🔊'}
              </button>
              {showVol && (
                <div className="pop-menu vol-pop">
                  <input
                    className="vol-slider"
                    type="range"
                    min={0}
                    max={1}
                    step={0.05}
                    value={muted ? 0 : vol}
                    onChange={(e) => setVolume(Number(e.target.value))}
                    aria-label="音量调节"
                  />
                </div>
              )}
            </div>
            <button
              type="button"
              className={`ctrl-btn ${webFull ? 'on' : ''}`}
              onClick={() => setWebFull((v) => !v)}
              title="网页全屏"
            >
              ⬜
            </button>
            <button type="button" className="ctrl-btn" onClick={toggleFullscreen} title="全屏">
              ⛶
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
