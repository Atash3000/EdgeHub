import type { VendorBar, QualityStatus } from "./types.js";

export function gradeBar(bar: VendorBar, seenKeys: Set<string>): { status: QualityStatus; issues: string[] } {
  const issues: string[] = [];
  let status: QualityStatus = "OK";
  const reject = (c: string) => { issues.push(c); status = "REJECTED"; };
  const warn = (c: string) => { issues.push(c); if (status === "OK") status = "WARN"; };

  const { open: o, high: h, low: l, close: c, volume: v } = bar;
  const finite = (x: number | null | undefined): x is number => typeof x === "number" && Number.isFinite(x);

  if (!finite(c)) reject("missing_close");
  if (!finite(v)) reject("missing_volume");
  if (!finite(o) || !finite(h) || !finite(l)) reject("invalid_ohlc");
  if ([o, h, l, c].some((p) => finite(p) && p < 0)) reject("negative_price");
  if (finite(v) && v < 0) reject("negative_volume");
  // OHLC ordering invariants — only meaningful when all four are finite numbers.
  if (finite(o) && finite(h) && finite(l) && finite(c) && (h < l || h < o || h < c || l > o || l > c)) {
    reject("inconsistent_ohlc");
  }

  const key = `${bar.ticker}|${bar.date}`;
  if (seenKeys.has(key)) reject("duplicate");
  else seenKeys.add(key);

  if ((status as QualityStatus) !== "REJECTED" && v === 0) warn("zero_volume");
  return { status, issues };
}
