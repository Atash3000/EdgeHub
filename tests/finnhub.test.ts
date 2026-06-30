import { describe, it, expect } from "vitest";
import { mapCandle } from "../src/providers/finnhub.js";

describe("mapCandle", () => {
  it("maps a finnhub candle to VendorBars marked unadjusted", () => {
    const json = { s: "ok", t: [1750000000], o: [10], h: [12], l: [9], c: [11], v: [1000] };
    const bars = mapCandle("AAPL", json, "2026-06-29T22:30:00Z");
    expect(bars).toHaveLength(1);
    expect(bars[0]).toMatchObject({
      ticker: "AAPL", close: 11, high: 12, low: 9, open: 10, volume: 1000,
      adjustedClose: null, isAdjusted: false, source: "finnhub", sourceVersion: "1.0",
    });
    expect(bars[0]!.date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });
  it("returns [] when status is not ok", () => {
    expect(mapCandle("AAPL", { s: "no_data" }, "x")).toEqual([]);
  });
});
