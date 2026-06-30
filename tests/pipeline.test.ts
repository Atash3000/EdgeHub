// tests/pipeline.test.ts
import { describe, it, expect } from "vitest";
import { makeRunId, buildManifest } from "../src/pipeline.js";

describe("makeRunId", () => {
  it("formats a compact UTC timestamp", () => {
    expect(makeRunId(new Date("2026-06-29T22:30:00Z"))).toBe("20260629T223000Z");
  });
});

describe("buildManifest", () => {
  const common = { mode: "daily" as const, runId: "R", tradingDay: "2026-06-29", provider: "fake", universeVersion: "2026-06-29", warnings: 0, rejected: 0, missingBars: 0, runtimeSec: 5 };
  it("SUCCESS when all requested produce rows", () => {
    expect(buildManifest({ ...common, requested: 10, succeeded: 10 }).status).toBe("SUCCESS");
  });
  it("PARTIAL when above the floor but short", () => {
    expect(buildManifest({ ...common, requested: 10, succeeded: 9 }).status).toBe("PARTIAL");
  });
  it("FAILURE below the floor", () => {
    expect(buildManifest({ ...common, requested: 10, succeeded: 5 }).status).toBe("FAILURE");
  });
});

import { runPipeline } from "../src/pipeline.js";
import { FakeProvider } from "../src/providers/fake.js";
import type { VendorBar } from "../src/types.js";

function series(ticker: string, n: number): VendorBar[] {
  const bars: VendorBar[] = [];
  for (let i = 0; i < n; i++) {
    const close = 100 + i;
    bars.push({ ticker, date: `2025-01-${String((i % 28) + 1).padStart(2, "0")}`, open: close, high: close + 1, low: close - 1, close, adjustedClose: null, isAdjusted: false, volume: 1000, source: "fake", sourceVersion: "1.0", ingestedAt: "x" });
  }
  return bars;
}

describe("runPipeline (backfill, fake provider)", () => {
  it("produces a SUCCESS manifest and writes metrics for all tickers", async () => {
    const hist = new Map<string, VendorBar[]>(
      ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "TSLA", "AVGO", "SPY", "QQQ"].map((t) => [t, series(t, 5)]),
    );
    const calls: string[] = [];
    const s3 = { send: async (c: unknown) => { calls.push((c as { constructor: { name: string } }).constructor.name); return {}; } } as never;
    const glue = { send: async () => ({}) } as never;
    const written = new Map<string, VendorBar[]>();
    const m = await runPipeline("backfill", {
      provider: new FakeProvider(hist), s3, glue, bucket: "b", database: "edgehub", tradingDay: "2025-01-05",
      now: () => new Date("2025-01-05T22:30:00Z"),
      readHistory: async () => [], writeHistory: async (t, bars) => { written.set(t, bars); },
      isTradingDay: () => true, calendarCovers: () => true,
    });
    expect(m.status).toBe("SUCCESS");
    expect(m.rowsWritten).toBe(9);
    expect(calls).toContain("PutObjectCommand");
    expect(written.size).toBe(9); // cache refreshed per ticker
  });

  it("registers daily_bars partition even when all bars are REJECTED (zero metrics)", async () => {
    const bar = (date: string, close: number): VendorBar => ({
      ticker: "AAPL", date, open: close, high: close, low: close, close, adjustedClose: null,
      isAdjusted: false, volume: 1000, source: "fake", sourceVersion: "1.0", ingestedAt: "x",
    });
    const hist = new Map([["AAPL", [bar("2025-01-02", 100), bar("2025-01-03", -5)]]]);
    const glueCalls: string[] = [];
    const s3 = { send: async () => ({}) } as never;
    const glue = { send: async (c: unknown) => { glueCalls.push((c as { constructor: { name: string } }).constructor.name); return {}; } } as never;
    const m = await runPipeline("backfill", {
      provider: new FakeProvider(hist), s3, glue, bucket: "b", database: "edgehub", tradingDay: "2025-01-03",
      now: () => new Date("2025-01-03T22:30:00Z"),
      readHistory: async () => [], writeHistory: async () => {},
      isTradingDay: () => true, calendarCovers: () => true,
    });
    expect(m.rowsWritten).toBe(0); // no metrics (the only bar was rejected)
    expect(glueCalls).toContain("BatchCreatePartitionCommand"); // daily_bars partition STILL registered
  });
});

describe("runPipeline (daily, non-trading day)", () => {
  it("returns SKIPPED without fetching", async () => {
    const s3 = { send: async () => ({}) } as never;
    const glue = { send: async () => ({}) } as never;
    const m = await runPipeline("daily", {
      provider: new FakeProvider(new Map()), s3, glue, bucket: "b", database: "edgehub", tradingDay: "2026-07-04",
      now: () => new Date("2026-07-04T22:30:00Z"),
      readHistory: async () => [], writeHistory: async () => {}, isTradingDay: () => false, calendarCovers: () => true,
    });
    expect(m.status).toBe("SKIPPED");
  });
});

describe("runPipeline (daily, uncovered calendar year)", () => {
  it("fails safely with calendar_year_missing and does not advance current", async () => {
    const calls: string[] = [];
    const s3 = { send: async (c: { constructor: { name: string }, input?: { Key?: string } }) => { calls.push(c.input?.Key ?? c.constructor.name); return {}; } } as never;
    const glue = { send: async () => ({}) } as never;
    const m = await runPipeline("daily", {
      provider: new FakeProvider(new Map()), s3, glue, bucket: "b", database: "edgehub", tradingDay: "2027-03-02",
      now: () => new Date("2027-03-02T22:30:00Z"),
      readHistory: async () => [], writeHistory: async () => {}, isTradingDay: () => true, calendarCovers: () => false,
    });
    expect(m.status).toBe("FAILURE");
    expect(m.note).toBe("calendar_year_missing");
    expect(calls.some((k) => k.includes("metadata/current"))).toBe(false); // current not advanced
  });
});
