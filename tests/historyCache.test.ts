import { describe, it, expect } from "vitest";
import { historyCacheKey, readHistoryCache, writeHistoryCache } from "../src/historyCache.js";
import type { VendorBar } from "../src/types.js";

const bar = (date: string): VendorBar => ({ ticker: "AAPL", date, open: 1, high: 1, low: 1, close: 1, adjustedClose: null, isAdjusted: false, volume: 1, source: "finnhub", sourceVersion: "1.0", ingestedAt: "x" });

describe("historyCacheKey", () => {
  it("builds the per-ticker key", () => {
    expect(historyCacheKey("finnhub", "AAPL")).toBe("history/finnhub/ticker=AAPL/current.json");
  });
});

describe("readHistoryCache", () => {
  it("returns [] when the object is missing", async () => {
    const s3 = { send: async () => { throw Object.assign(new Error("nope"), { name: "NoSuchKey" }); } } as never;
    expect(await readHistoryCache(s3, "b", "finnhub", "AAPL")).toEqual([]);
  });
});

describe("writeHistoryCache", () => {
  it("trims to the last maxBars and PUTs JSON", async () => {
    let body = "";
    const s3 = { send: async (c: { input: { Body: string } }) => { body = c.input.Body; return {}; } } as never;
    await writeHistoryCache(s3, "b", "finnhub", "AAPL", [bar("2026-06-25"), bar("2026-06-26"), bar("2026-06-29")], 2);
    const parsed = JSON.parse(body) as VendorBar[];
    expect(parsed.map((b) => b.date)).toEqual(["2026-06-26", "2026-06-29"]);
  });
});
