import type { VendorBar, MetricRow, Provenance, QualityStatus } from "./types.js";

export function sma(values: number[], window: number): number | null {
  if (values.length < window) return null;
  return values.slice(-window).reduce((a, b) => a + b, 0) / window;
}
export function smaAt(values: number[], window: number, offsetFromEnd: number): number | null {
  const end = values.length - offsetFromEnd;
  if (end < window) return null;
  return values.slice(end - window, end).reduce((a, b) => a + b, 0) / window;
}
export function trueRanges(bars: VendorBar[]): number[] {
  const tr: number[] = [];
  for (let i = 1; i < bars.length; i++) {
    const c = bars[i]!, p = bars[i - 1]!;
    tr.push(Math.max(c.high - c.low, Math.abs(c.high - p.close), Math.abs(c.low - p.close)));
  }
  return tr;
}
export function pctReturn(closes: number[], lookback: number): number | null {
  if (closes.length <= lookback) return null;
  const now = closes[closes.length - 1]!, then = closes[closes.length - 1 - lookback]!;
  return then === 0 ? null : now / then - 1;
}
function maxOver(v: number[], w: number): number | null { return v.length < w ? null : Math.max(...v.slice(-w)); }
function minOver(v: number[], w: number): number | null { return v.length < w ? null : Math.min(...v.slice(-w)); }

export function computeMetrics(bars: VendorBar[], prov: Provenance, quality: { status: QualityStatus; issues: string[] }, instrumentId: string): MetricRow {
  const last = bars[bars.length - 1]!;
  const closes = bars.map((b) => b.close);
  const highs = bars.map((b) => b.high);
  const lows = bars.map((b) => b.low);
  const volumes = bars.map((b) => b.volume);

  const ma20 = sma(closes, 20), ma50 = sma(closes, 50), ma150 = sma(closes, 150), ma200 = sma(closes, 200);
  const ma200Prev = smaAt(closes, 200, 1);
  const atr14 = sma(trueRanges(bars), 14); // needs >= 15 bars (14 true ranges)
  const high52w = maxOver(highs, 252), low52w = minOver(lows, 252);

  return {
    ticker: last.ticker, instrumentId, date: last.date, close: last.close,
    dollarVolume: last.close * last.volume,
    ma20, ma50, ma150, ma200,
    avgVolume20: sma(volumes, 20), avgVolume50: sma(volumes, 50),
    atr14, high52w, low52w,
    distanceTo52wHighPct: high52w ? (last.close - high52w) / high52w * 100 : null,
    distanceFrom52wLowPct: low52w ? (last.close - low52w) / low52w * 100 : null,
    return21d: pctReturn(closes, 21), return63d: pctReturn(closes, 63),
    return126d: pctReturn(closes, 126), return252d: pctReturn(closes, 252),
    above20ma: ma20 === null ? null : last.close > ma20,
    above50ma: ma50 === null ? null : last.close > ma50,
    above150ma: ma150 === null ? null : last.close > ma150,
    above200ma: ma200 === null ? null : last.close > ma200,
    ma150Above200: ma150 !== null && ma200 !== null ? ma150 > ma200 : null,
    ma200Rising: ma200 !== null && ma200Prev !== null ? ma200 > ma200Prev : null,
    qualityStatus: quality.status, qualityIssues: quality.issues,
    ...prov,
  };
}
