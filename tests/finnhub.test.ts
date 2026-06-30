import { describe, it, expect } from "vitest";
import { mapCandle, FinnhubProvider } from "../src/providers/finnhub.js";

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
  it("returns [] when arrays are uneven (missing/short column)", () => {
    // t has 2 entries but o/h/l/c/v only have 1 — uneven arrays should return no bars
    expect(mapCandle("AAPL", { s: "ok", t: [1, 2], o: [1], h: [2], l: [1], c: [1], v: [1] }, "x")).toEqual([]);
  });
});

describe("FinnhubProvider.getLatestBars", () => {
  // Helper: create a fake fetch that returns a Finnhub candle for a given epoch/timestamp
  const okFetch = (tsSec: number) => async () => ({
    ok: true,
    status: 200,
    json: async () => ({
      s: "ok",
      t: [tsSec],
      o: [10],
      h: [12],
      l: [9],
      c: [11],
      v: [1000],
    }),
  });

  it("exact-date hit: returns bar when fetchFn returns candle for the requested date", async () => {
    const targetDate = "2026-06-29";
    const targetEpoch = Math.floor(new Date("2026-06-29T00:00:00Z").getTime() / 1000);

    const provider = new FinnhubProvider("tok", okFetch(targetEpoch), 600000);
    const result = await provider.getLatestBars(targetDate, ["AAPL"]);

    expect(result.bars).toHaveLength(1);
    expect(result.bars[0]).toMatchObject({
      ticker: "AAPL",
      date: targetDate,
      open: 10,
      high: 12,
      low: 9,
      close: 11,
      volume: 1000,
      source: "finnhub",
    });
    expect(result.failures).toEqual([]);
  });

  it("missing date: records missing_bar_for_date failure when fetchFn returns candle for different date", async () => {
    const requestedDate = "2026-06-29";
    const differentDate = "2026-06-28";
    const differentEpoch = Math.floor(new Date("2026-06-28T00:00:00Z").getTime() / 1000);

    const provider = new FinnhubProvider("tok", okFetch(differentEpoch), 600000);
    const result = await provider.getLatestBars(requestedDate, ["AAPL"]);

    expect(result.bars).toEqual([]);
    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]).toMatchObject({
      ticker: "AAPL",
      date: requestedDate,
      reason: "missing_bar_for_date",
    });
  });

  it("fetch throws: records provider_error failure but continues batch processing", async () => {
    const targetDate = "2026-06-29";
    const targetEpoch = Math.floor(new Date("2026-06-29T00:00:00Z").getTime() / 1000);

    const smartFetch = async (url: string) => {
      // First ticker (AAPL) throws; second ticker (MSFT) succeeds
      if (url.includes("symbol=AAPL")) {
        throw new Error("Network timeout");
      }
      return {
        ok: true,
        status: 200,
        json: async () => ({
          s: "ok",
          t: [targetEpoch],
          o: [200],
          h: [210],
          l: [195],
          c: [205],
          v: [5000],
        }),
      };
    };

    const provider = new FinnhubProvider("tok", smartFetch, 600000);
    const result = await provider.getLatestBars(targetDate, ["AAPL", "MSFT"]);

    // Batch must complete despite AAPL error; MSFT bar should be present
    expect(result.bars).toHaveLength(1);
    expect(result.bars[0]).toMatchObject({
      ticker: "MSFT",
      date: targetDate,
      open: 200,
      close: 205,
      volume: 5000,
    });

    // AAPL error must be recorded with provider_error reason and message
    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]).toMatchObject({
      ticker: "AAPL",
      date: targetDate,
      reason: "provider_error",
      message: "Network timeout",
    });
  });
});
