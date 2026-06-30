import { describe, it, expect } from "vitest";
import { mapAgg, PolygonProvider } from "../src/providers/polygon.js";

describe("mapAgg", () => {
  it("maps an aggregate result to VendorBar", () => {
    const bar = mapAgg(
      "AAPL",
      { t: 1782604800000, o: 294.12, h: 297.78, l: 291.70, c: 296.42, v: 45732573 },
      "x",
    );
    expect(bar.ticker).toBe("AAPL");
    expect(bar.date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(bar.open).toBe(294.12);
    expect(bar.close).toBe(296.42);
    expect(bar.volume).toBe(45732573);
    expect(bar.adjustedClose).toBe(296.42);
    expect(bar.isAdjusted).toBe(true);
    expect(bar.source).toBe("polygon");
  });
});

describe("PolygonProvider.getHistory", () => {
  it("returns mapped bars and empty failures on success", async () => {
    const agg = { t: 1782604800000, o: 1, h: 2, l: 0.5, c: 1.5, v: 1000 };
    const fakeFetch = async (_url: string) => ({
      ok: true,
      status: 200,
      json: async () => ({ status: "OK", results: [agg] }),
    });
    const provider = new PolygonProvider("k", fakeFetch, 600000);
    const result = await provider.getHistory("AAPL", 30, "2026-06-30");
    expect(result.failures).toEqual([]);
    expect(result.bars).toHaveLength(1);
    expect(result.bars[0]!.ticker).toBe("AAPL");
    expect(result.bars[0]!.source).toBe("polygon");
  });

  it("returns provider_error failure when fetch fails", async () => {
    const fakeFetch = async (_url: string) => ({ ok: false, status: 500, json: async () => ({}) });
    const provider = new PolygonProvider("k", fakeFetch, 600000);
    const result = await provider.getHistory("AAPL", 30, "2026-06-30");
    expect(result.bars).toEqual([]);
    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]!.reason).toBe("provider_error");
  });

  it("treats a 200 response with a non-OK status as provider_error (not empty data)", async () => {
    const fakeFetch = async (_url: string) => ({ ok: true, status: 200, json: async () => ({ status: "ERROR", error: "bad" }) });
    const provider = new PolygonProvider("k", fakeFetch, 600000);
    const result = await provider.getHistory("AAPL", 30, "2026-06-30");
    expect(result.bars).toEqual([]);
    expect(result.failures[0]!.reason).toBe("provider_error");
  });
});

describe("PolygonProvider.getLatestBars", () => {
  it("returns bars for present tickers and failures for missing ones", async () => {
    const fakeFetch = async (_url: string) => ({
      ok: true,
      status: 200,
      json: async () => ({
        status: "OK",
        results: [
          { T: "AAPL", t: 1782604800000, o: 294.12, h: 297.78, l: 291.70, c: 296.42, v: 45732573 },
          { T: "MSFT", t: 1782604800000, o: 420.00, h: 425.00, l: 418.00, c: 422.00, v: 20000000 },
        ],
      }),
    });
    const provider = new PolygonProvider("k", fakeFetch, 600000);
    const result = await provider.getLatestBars("2026-06-27", ["AAPL", "MSFT", "ZZZZ"]);
    expect(result.bars).toHaveLength(2);
    expect(result.bars.map((b) => b.ticker).sort()).toEqual(["AAPL", "MSFT"]);
    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]).toMatchObject({ ticker: "ZZZZ", reason: "missing_bar_for_date" });
  });

  it("returns provider_error for all tickers when HTTP 429 and does not throw", async () => {
    const fakeFetch = async (_url: string) => ({ ok: false, status: 429, json: async () => ({}) });
    const provider = new PolygonProvider("k", fakeFetch, 600000);
    const result = await provider.getLatestBars("2026-06-27", ["AAPL", "MSFT"]);
    expect(result.bars).toEqual([]);
    expect(result.failures).toHaveLength(2);
    expect(result.failures.every((f) => f.reason === "provider_error")).toBe(true);
  });

  it("pins each bar's date to the requested date and rejects a non-OK 200 status", async () => {
    const okFetch = async (_url: string) => ({
      ok: true, status: 200,
      json: async () => ({ status: "OK", results: [{ T: "AAPL", t: 1782604800000, o: 1, h: 2, l: 0.5, c: 1.5, v: 1000 }] }),
    });
    const okProvider = new PolygonProvider("k", okFetch, 600000);
    const okRes = await okProvider.getLatestBars("2026-06-27", ["AAPL"]);
    expect(okRes.bars[0]!.date).toBe("2026-06-27"); // pinned to the requested date, not derived from t

    const errFetch = async (_url: string) => ({ ok: true, status: 200, json: async () => ({ status: "NOT_AUTHORIZED" }) });
    const errProvider = new PolygonProvider("k", errFetch, 600000);
    const errRes = await errProvider.getLatestBars("2026-06-27", ["AAPL", "MSFT"]);
    expect(errRes.failures.every((f) => f.reason === "provider_error")).toBe(true);
  });
});
