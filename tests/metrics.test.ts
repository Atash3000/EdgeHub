import { describe, it, expect } from "vitest";
import { sma, pctReturn, computeMetrics } from "../src/metrics.js";
import type { VendorBar, Provenance } from "../src/types.js";

const prov: Provenance = { runId: "R", ingestedAt: "x", source: "fake", sourceVersion: "1.0", schemaVersion: "metrics_v1", metricVersion: "1.0", universeVersion: "2026-06-29" };
const ok = { status: "OK" as const, issues: [] as string[] };
const bar = (date: string, close: number, volume = 1000): VendorBar => ({ ticker: "AAPL", date, open: close, high: close + 1, low: close - 1, close, adjustedClose: null, isAdjusted: false, volume, source: "fake", sourceVersion: "1.0", ingestedAt: "x" });

describe("sma", () => {
  it("averages the last N", () => { expect(sma([1, 2, 3, 4], 2)).toBe(3.5); });
  it("is null when short", () => { expect(sma([1, 2], 5)).toBeNull(); });
});
describe("pctReturn", () => {
  it("computes trailing return", () => { expect(pctReturn([100, 110], 1)).toBeCloseTo(0.1); });
});
describe("computeMetrics", () => {
  it("computes ma20 and flags, injects quality", () => {
    const bars: VendorBar[] = [];
    for (let i = 0; i < 25; i++) bars.push(bar(`2026-05-${String(i + 1).padStart(2, "0")}`, 100 + i));
    const row = computeMetrics(bars, prov, ok);
    expect(row.ticker).toBe("AAPL");
    expect(row.close).toBe(124);
    expect(row.ma20).toBeCloseTo(sma(bars.map((b) => b.close), 20)!);
    expect(row.above20ma).toBe(true);
    expect(row.ma200).toBeNull();
    expect(row.qualityStatus).toBe("OK");
    expect(row.runId).toBe("R");
  });
});
