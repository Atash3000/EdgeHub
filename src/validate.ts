import type { VendorBar, QualityStatus } from "./types.js";

export function gradeBar(bar: VendorBar, seenKeys: Set<string>): { status: QualityStatus; issues: string[] } {
  const issues: string[] = [];
  let status: QualityStatus = "OK";
  const reject = (c: string) => { issues.push(c); status = "REJECTED"; };
  const warn = (c: string) => { issues.push(c); if (status === "OK") status = "WARN"; };

  if (bar.close === null || bar.close === undefined || Number.isNaN(bar.close)) reject("missing_close");
  if (bar.volume === null || bar.volume === undefined || Number.isNaN(bar.volume)) reject("missing_volume");
  if ([bar.open, bar.high, bar.low, bar.close].some((p) => p < 0)) reject("negative_price");
  if (bar.volume < 0) reject("negative_volume");

  const key = `${bar.ticker}|${bar.date}`;
  if (seenKeys.has(key)) reject("duplicate");
  else seenKeys.add(key);

  if ((status as QualityStatus) !== "REJECTED" && bar.volume === 0) warn("zero_volume");
  return { status, issues };
}
