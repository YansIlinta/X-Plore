import { useEffect, useRef, useState } from 'react'

export type FloatCue = {
  key: string
  text: string
  offsetMs: number
}

type Props = {
  playMs: number
  cues: FloatCue[]
  enabled?: boolean
  /** 每次变化时立即飞出一条（实时弹幕内容） */
  liveText?: string
  className?: string
}

type LaneItem = {
  key: string
  text: string
  y: number
  x: number
  speed: number
  color: string
}

const COLORS = ['#ffffff', '#a8c7ff', '#ffd6a5', '#bdb2ff', '#caffbf', '#ffc6ff', '#9bf6ff']

function stripUid(text: string) {
  const i = text.indexOf(': ')
  return i > 0 && i < 24 ? text.slice(i + 2) : text
}

/**
 * 叠加在 video 上的轻量飘字层：按 playMs 触发历史 cue，liveText 立即飞出。
 */
export default function DanmuLayer({ playMs, cues, enabled = true, liveText, className }: Props) {
  const wrapRef = useRef<HTMLDivElement>(null)
  const firedRef = useRef(new Set<string>())
  const itemsRef = useRef<LaneItem[]>([])
  const lastPlayRef = useRef(0)
  const lastLiveRef = useRef('')
  const [, setTick] = useState(0)

  useEffect(() => {
    if (playMs + 3000 < lastPlayRef.current) {
      firedRef.current.clear()
    }
    lastPlayRef.current = playMs
  }, [playMs])

  useEffect(() => {
    if (!enabled || !liveText || liveText === lastLiveRef.current) return
    lastLiveRef.current = liveText
    spawn(itemsRef, wrapRef.current, stripUid(liveText))
    setTick((n) => n + 1)
  }, [liveText, enabled])

  useEffect(() => {
    if (!enabled) return
    for (const c of cues) {
      if (firedRef.current.has(c.key)) continue
      // 刚越过 offset：offset ∈ (playMs-500, playMs]
      if (c.offsetMs <= playMs && c.offsetMs > playMs - 500) {
        firedRef.current.add(c.key)
        spawn(itemsRef, wrapRef.current, stripUid(c.text))
      }
    }
  }, [playMs, cues, enabled])

  useEffect(() => {
    if (!enabled) return
    let prev = 0
    let id = 0
    const tick = (now: number) => {
      const dt = prev ? (now - prev) / 1000 : 0
      prev = now
      const next: LaneItem[] = []
      for (const it of itemsRef.current) {
        it.x -= it.speed * dt
        if (it.x > -Math.max(160, it.text.length * 15)) next.push(it)
      }
      itemsRef.current = next.slice(-100)
      if (next.length) setTick((n) => (n + 1) % 1_000_000)
      id = requestAnimationFrame(tick)
    }
    id = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(id)
  }, [enabled])

  if (!enabled) return null

  return (
    <div ref={wrapRef} className={`danmu-layer ${className || ''}`} aria-hidden>
      {itemsRef.current.map((it) => (
        <span
          key={it.key}
          className="danmu-float"
          style={{
            transform: `translate3d(${it.x}px, ${it.y}px, 0)`,
            color: it.color,
          }}
        >
          {it.text}
        </span>
      ))}
    </div>
  )
}

function spawn(
  itemsRef: { current: LaneItem[] },
  wrap: HTMLDivElement | null,
  text: string,
) {
  if (!text) return
  const w = wrap?.clientWidth || 800
  const h = wrap?.clientHeight || 360
  const lanes = Math.max(1, Math.floor((h - 40) / 28))
  const lane = Math.floor(Math.random() * lanes)
  itemsRef.current.push({
    key: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    text,
    y: 10 + lane * 28,
    x: w + 16,
    speed: 90 + Math.random() * 70,
    color: COLORS[Math.floor(Math.random() * COLORS.length)],
  })
}
