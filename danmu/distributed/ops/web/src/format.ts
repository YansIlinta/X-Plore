// 数值格式化：null → N/A（数据真实性约定），大数 K/M 缩写。

export function fmtNum(v: number | null | undefined, digits = 0): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "N/A";
  if (Math.abs(v) >= 1_000_000) return (v / 1_000_000).toFixed(1) + "M";
  if (Math.abs(v) >= 10_000) return (v / 1_000).toFixed(1) + "K";
  return v.toLocaleString("en-US", { maximumFractionDigits: digits });
}

export function fmtRate(v: number | null | undefined): string {
  if (v === null || v === undefined) return "N/A";
  return fmtNum(v, 1) + "/s";
}

export function fmtTime(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleTimeString("en-GB", { hour12: false });
}

export function fmtUptime(ms: unknown): string {
  if (typeof ms !== "number") return "N/A";
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  return h > 0 ? `${h}h${m}m` : m > 0 ? `${m}m${s % 60}s` : `${s}s`;
}
