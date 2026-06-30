import { describe, it, expect } from "vitest";
import { ParquetReader } from "@dsnp/parquetjs";
import { rawKey, metricsKey, RAW_SCHEMA, METRIC_SCHEMA, SECURITIES_SCHEMA, SYMBOL_ALIASES_SCHEMA, toParquet } from "../src/storage.js";

describe("parquet schemas carry instrumentId", () => {
  it("RAW_SCHEMA and METRIC_SCHEMA both define instrumentId", () => {
    expect(RAW_SCHEMA.schema.instrumentId).toBeDefined();
    expect(METRIC_SCHEMA.schema.instrumentId).toBeDefined();
  });
});

describe("parquet round-trip — SECURITIES_SCHEMA", () => {
  it("writes a MISSING_FALLBACK row with all optional fields undefined and reads it back", async () => {
    const rows = [
      {
        instrumentId: "EH:ZZZZ",
        ticker: "ZZZZ",
        active: true,
        identitySource: "EH_TICKER",
        identityConfidence: "LOW",
        referenceStatus: "MISSING_FALLBACK",
        source: "polygon",
        sourceVersion: "1.0",
        asOfDate: "2026-06-30",
        ingestedAt: "x",
        // all optional fields intentionally absent (undefined)
      },
    ];
    const buf = await toParquet(SECURITIES_SCHEMA, rows as unknown as Record<string, unknown>[]);
    const reader = await ParquetReader.openBuffer(buf);
    const cursor = reader.getCursor();
    const read: Record<string, unknown>[] = [];
    let rec: unknown;
    while ((rec = await cursor.next())) read.push(rec as Record<string, unknown>);
    await reader.close();

    expect(read).toHaveLength(1);
    expect(read[0]!.instrumentId).toBe("EH:ZZZZ");
    expect(read[0]!.ticker).toBe("ZZZZ");
    expect(read[0]!.active).toBe(true);
    expect(read[0]!.referenceStatus).toBe("MISSING_FALLBACK");
  });
});

describe("parquet round-trip — SYMBOL_ALIASES_SCHEMA", () => {
  it("round-trips a symbol-aliases row with null validTo and reads back the fields", async () => {
    const raw = {
      instrumentId: "ID1",
      ticker: "AAA",
      validFrom: "2026-06-30",
      validTo: null as null | string,
      source: "polygon",
      sourceVersion: "1.0",
      asOfDate: "2026-06-30",
      confidence: "MEDIUM",
      createdAt: "t",
    };
    // Mirror the conversion writeSymbolAliases performs (null → undefined so Parquet optional works)
    const flat = [{ ...raw, validTo: raw.validTo ?? undefined }];
    const buf = await toParquet(SYMBOL_ALIASES_SCHEMA, flat as unknown as Record<string, unknown>[]);
    const reader = await ParquetReader.openBuffer(buf);
    const cursor = reader.getCursor();
    const read: Record<string, unknown>[] = [];
    let rec: unknown;
    while ((rec = await cursor.next())) read.push(rec as Record<string, unknown>);
    await reader.close();

    expect(read).toHaveLength(1);
    expect(read[0]!.instrumentId).toBe("ID1");
    expect(read[0]!.ticker).toBe("AAA");
    expect(read[0]!.validFrom).toBe("2026-06-30");
    // parquetjs reads an absent optional column back as null (correct: open window => SQL NULL)
    expect(read[0]!.validTo).toBeNull();
  });
});

describe("path builders", () => {
  it("builds a raw key with source + runId", () => {
    expect(rawKey("finnhub", "2026-06-29", "20260629T223000Z")).toBe(
      "raw/finnhub/daily/year=2026/month=06/day=29/runId=20260629T223000Z/part.parquet");
  });
  it("builds a metrics key with runId", () => {
    expect(metricsKey("2026-06-29", "20260629T223000Z")).toBe(
      "metrics/daily/year=2026/month=06/day=29/runId=20260629T223000Z/part.parquet");
  });
});
