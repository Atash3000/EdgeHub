import { describe, it, expect } from "vitest";
import { mergeHistory, hasEnoughHistory } from "../src/history.js";
import type { VendorBar } from "../src/types.js";

const bar = (date: string, close: number): VendorBar => ({ ticker: "AAPL", date, open: close, high: close, low: close, close, adjustedClose: null, isAdjusted: false, volume: 1, source: "fake", sourceVersion: "1.0", ingestedAt: "x" });

describe("mergeHistory", () => {
  it("appends latest, keeps ascending order", () => {
    const m = mergeHistory([bar("2026-06-26", 1), bar("2026-06-27", 2)], bar("2026-06-29", 3));
    expect(m.map((b) => b.date)).toEqual(["2026-06-26", "2026-06-27", "2026-06-29"]);
  });
  it("replaces a same-date bar with latest", () => {
    const m = mergeHistory([bar("2026-06-29", 1)], bar("2026-06-29", 9));
    expect(m).toHaveLength(1); expect(m[0]!.close).toBe(9);
  });
});
describe("hasEnoughHistory", () => {
  it("is false below the minimum", () => { expect(hasEnoughHistory([bar("2026-06-29", 1)], 200)).toBe(false); });
});
