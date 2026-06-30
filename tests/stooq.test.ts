import { describe, it, expect } from "vitest";
import { stooqSymbol, parseStooqCsv, StooqProvider } from "../src/providers/stooq.js";

const VALID_CSV =
  "Date,Open,High,Low,Close,Volume\n" +
  "2026-06-26,201.07,203.22,200.85,202.40,41203100\n" +
  "2026-06-29,202.10,204.55,201.90,204.12,38771200";

describe("stooqSymbol", () => {
  it("lowercases ticker and appends .us", () => {
    expect(stooqSymbol("AAPL")).toBe("aapl.us");
  });
});

describe("parseStooqCsv", () => {
  it("parses a valid 2-row CSV into 2 VendorBars", () => {
    const bars = parseStooqCsv("AAPL", VALID_CSV, "2026-06-29T22:00:00.000Z");
    expect(bars).toHaveLength(2);
    const bar = bars[1]!;
    expect(bar.ticker).toBe("AAPL");
    expect(bar.date).toBe("2026-06-29");
    expect(bar.close).toBe(204.12);
    expect(bar.volume).toBe(38771200);
    expect(bar.source).toBe("stooq");
    expect(bar.adjustedClose).toBeNull();
    expect(bar.isAdjusted).toBe(false);
  });

  it("returns [] for non-CSV/error body (e.g. N/D)", () => {
    expect(parseStooqCsv("AAPL", "N/D", "x")).toEqual([]);
  });

  it("returns [] for empty body", () => {
    expect(parseStooqCsv("AAPL", "", "x")).toEqual([]);
  });

  it("skips a row with a non-numeric close", () => {
    const csv =
      "Date,Open,High,Low,Close,Volume\n" +
      "2026-06-26,201.07,203.22,200.85,,41203100\n" +
      "2026-06-29,202.10,204.55,201.90,204.12,38771200";
    const bars = parseStooqCsv("AAPL", csv, "x");
    expect(bars).toHaveLength(1);
    expect(bars[0]!.date).toBe("2026-06-29");
  });

  it("skips a row with a non-numeric open", () => {
    const csv =
      "Date,Open,High,Low,Close,Volume\n" +
      "2026-06-26,N/A,203.22,200.85,202.40,41203100";
    const bars = parseStooqCsv("AAPL", csv, "x");
    expect(bars).toHaveLength(0);
  });

  it("handles \\r\\n line endings", () => {
    const csv = "Date,Open,High,Low,Close,Volume\r\n2026-06-29,202.10,204.55,201.90,204.12,38771200";
    const bars = parseStooqCsv("AAPL", csv, "x");
    expect(bars).toHaveLength(1);
    expect(bars[0]!.date).toBe("2026-06-29");
  });
});

describe("StooqProvider.getLatestBars", () => {
  const makeFetch = (csvBySymbol: Record<string, string>) => {
    return async (url: string) => {
      // Extract the symbol from the URL query param s=<symbol>
      const match = url.match(/[?&]s=([^&]+)/);
      const sym = match ? match[1]! : "";
      const csv = csvBySymbol[sym] ?? "N/D";
      return {
        ok: true,
        status: 200,
        text: async () => csv,
      };
    };
  };

  it("returns a bar when the CSV includes the requested date", async () => {
    const fetchFn = makeFetch({ "aapl.us": VALID_CSV });
    const provider = new StooqProvider(fetchFn as any);
    const result = await provider.getLatestBars("2026-06-29", ["AAPL"]);
    expect(result.bars).toHaveLength(1);
    expect(result.bars[0]).toMatchObject({
      ticker: "AAPL",
      date: "2026-06-29",
      close: 204.12,
      source: "stooq",
    });
    expect(result.failures).toEqual([]);
  });

  it("records missing_bar_for_date when CSV lacks the requested date", async () => {
    const csvWithoutDate =
      "Date,Open,High,Low,Close,Volume\n" +
      "2026-06-26,201.07,203.22,200.85,202.40,41203100";
    const fetchFn = makeFetch({ "msft.us": csvWithoutDate });
    const provider = new StooqProvider(fetchFn as any);
    const result = await provider.getLatestBars("2026-06-29", ["MSFT"]);
    expect(result.bars).toEqual([]);
    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]).toMatchObject({
      ticker: "MSFT",
      date: "2026-06-29",
      reason: "missing_bar_for_date",
    });
  });

  it("handles mixed batch: hit for AAPL, missing for MSFT — never throws", async () => {
    const fetchFn = makeFetch({ "aapl.us": VALID_CSV, "msft.us": "N/D" });
    const provider = new StooqProvider(fetchFn as any);
    const result = await provider.getLatestBars("2026-06-29", ["AAPL", "MSFT"]);
    expect(result.bars).toHaveLength(1);
    expect(result.bars[0]!.ticker).toBe("AAPL");
    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]).toMatchObject({ ticker: "MSFT", reason: "missing_bar_for_date" });
  });

  it("records provider_error when fetchFn throws", async () => {
    const throwingFetch = async (_url: string) => { throw new Error("network failure"); };
    const provider = new StooqProvider(throwingFetch as any);
    const result = await provider.getLatestBars("2026-06-29", ["AAPL"]);
    expect(result.bars).toEqual([]);
    expect(result.failures[0]).toMatchObject({
      ticker: "AAPL",
      reason: "provider_error",
      message: "network failure",
    });
  });

  it("records provider_error when fetchFn returns non-ok status", async () => {
    const errorFetch = async (_url: string) => ({ ok: false, status: 503, text: async () => "" });
    const provider = new StooqProvider(errorFetch as any);
    const result = await provider.getLatestBars("2026-06-29", ["AAPL"]);
    expect(result.bars).toEqual([]);
    expect(result.failures[0]).toMatchObject({ ticker: "AAPL", reason: "provider_error" });
    expect(result.failures[0]!.message).toMatch(/HTTP 503/);
  });
});

describe("StooqProvider.getHistory", () => {
  it("returns filtered bars up to endDate", async () => {
    const fetchFn = async (_url: string) => ({
      ok: true,
      status: 200,
      text: async () => VALID_CSV,
    });
    const provider = new StooqProvider(fetchFn as any);
    const result = await provider.getHistory("AAPL", 5, "2026-06-26");
    // bars are filtered to date <= "2026-06-26"
    expect(result.bars).toHaveLength(1);
    expect(result.bars[0]!.date).toBe("2026-06-26");
    expect(result.failures).toEqual([]);
  });

  it("returns provider_error failure on fetch error", async () => {
    const throwingFetch = async (_url: string) => { throw new Error("timeout"); };
    const provider = new StooqProvider(throwingFetch as any);
    const result = await provider.getHistory("AAPL", 10, "2026-06-29");
    expect(result.bars).toEqual([]);
    expect(result.failures[0]).toMatchObject({ ticker: "AAPL", reason: "provider_error", message: "timeout" });
  });
});
