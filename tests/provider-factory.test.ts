import { describe, it, expect } from "vitest";
import { getProvider } from "../src/providers/factory.js";
import { FakeProvider } from "../src/providers/fake.js";
import type { VendorBar } from "../src/types.js";

const bar = (date: string): VendorBar => ({ ticker: "AAPL", date, open: 1, high: 2, low: 1, close: 2, adjustedClose: null, isAdjusted: false, volume: 10, source: "fake", sourceVersion: "1.0", ingestedAt: "x" });

describe("getProvider", () => {
  it("returns a fake provider", () => { expect(getProvider("fake", {}).name).toBe("fake"); });
  it("throws on unknown provider", () => { expect(() => getProvider("nope", {})).toThrow(/unknown data provider/i); });
  it("returns a finnhub provider when its key is present", () => { expect(getProvider("finnhub", { finnhubToken: "k" }).name).toBe("finnhub"); });
  it("throws when the active finnhub key is missing", () => { expect(() => getProvider("finnhub", {})).toThrow(/finnhub api key/i); });
  it("returns a polygon provider when its key is present", () => { expect(getProvider("polygon", { polygonToken: "k" }).name).toBe("polygon"); });
  it("throws when the active polygon key is missing", () => { expect(() => getProvider("polygon", {})).toThrow(/polygon api key/i); });
});

describe("FakeProvider.getLatestBars", () => {
  it("returns only the bar matching the requested date", async () => {
    const p = new FakeProvider(new Map([["AAPL", [bar("2026-06-26"), bar("2026-06-29")]]]));
    const out = await p.getLatestBars("2026-06-29", ["AAPL"]);
    expect(out.bars).toHaveLength(1);
    expect(out.bars[0]!.date).toBe("2026-06-29");
    expect(out.failures).toEqual([]);
  });
  it("records a failure when the date is missing (no stale fallback)", async () => {
    const p = new FakeProvider(new Map([["AAPL", [bar("2026-06-26")]]]));
    const out = await p.getLatestBars("2026-06-29", ["AAPL"]);
    expect(out.bars).toEqual([]);
    expect(out.failures[0]).toMatchObject({ ticker: "AAPL", reason: "missing_bar_for_date" });
  });
});
