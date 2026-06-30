import type { VendorBar } from "./types.js";

export function mergeHistory(stored: VendorBar[], latest: VendorBar): VendorBar[] {
  const byDate = new Map<string, VendorBar>();
  for (const b of stored) byDate.set(b.date, b);
  byDate.set(latest.date, latest); // latest wins on collision
  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

export function hasEnoughHistory(bars: VendorBar[], minSessions: number): boolean {
  return bars.length >= minSessions;
}
