import { describe, it, expect } from "vitest";
import { ParquetReader, ParquetSchema } from "@dsnp/parquetjs";
import { toParquet, METRIC_SCHEMA, RAW_SCHEMA } from "../src/storage.js";

describe("parquet round-trip", () => {
  it("writes two metric rows and reads them back with correct types", async () => {
    const rows = [
      { instrumentId: "EH:AAPL", ticker: "AAPL", date: "2026-06-29", close: 11, dollarVolume: 11000, ma20: 10.5, ma50: null, ma150: null, ma200: null, avgVolume20: null, avgVolume50: null, atr14: null, high52w: null, low52w: null, distanceTo52wHighPct: null, distanceFrom52wLowPct: null, return21d: null, return63d: null, return126d: null, return252d: null, above20ma: true, above50ma: null, above150ma: null, above200ma: null, ma150Above200: null, ma200Rising: null, qualityStatus: "OK", qualityIssues: JSON.stringify([]), runId: "R", ingestedAt: "x", source: "fake", sourceVersion: "1.0", schemaVersion: "metrics_v1", metricVersion: "1.0", universeVersion: "2026-06-29" },
      { instrumentId: "EH:MSFT", ticker: "MSFT", date: "2026-06-29", close: 20, dollarVolume: 40000, ma20: null, ma50: null, ma150: null, ma200: null, avgVolume20: null, avgVolume50: null, atr14: null, high52w: null, low52w: null, distanceTo52wHighPct: null, distanceFrom52wLowPct: null, return21d: null, return63d: null, return126d: null, return252d: null, above20ma: null, above50ma: null, above150ma: null, above200ma: null, ma150Above200: null, ma200Rising: null, qualityStatus: "WARN", qualityIssues: JSON.stringify(["zero_volume"]), runId: "R", ingestedAt: "x", source: "fake", sourceVersion: "1.0", schemaVersion: "metrics_v1", metricVersion: "1.0", universeVersion: "2026-06-29" },
    ];
    const buf = await toParquet(METRIC_SCHEMA, rows as unknown as Record<string, unknown>[]);
    const reader = await ParquetReader.openBuffer(buf);
    const cursor = reader.getCursor();
    const read: Record<string, unknown>[] = [];
    let rec: unknown;
    while ((rec = await cursor.next())) read.push(rec as Record<string, unknown>);
    await reader.close();

    expect(read).toHaveLength(2);
    expect(read[0]!.ticker).toBe("AAPL");
    expect(read[0]!.above20ma).toBe(true);
    expect(read[1]!.qualityStatus).toBe("WARN");
    expect(JSON.parse(read[1]!.qualityIssues as string)).toEqual(["zero_volume"]);
  });

  it("stamps SNAPPY compression on every column of the parquet schemas", () => {
    for (const [name, f] of Object.entries(METRIC_SCHEMA.fields)) expect(f.compression, name).toBe("SNAPPY");
    for (const [name, f] of Object.entries(RAW_SCHEMA.fields)) expect(f.compression, name).toBe("SNAPPY");
  });

  it("SNAPPY meaningfully shrinks repetitive data vs UNCOMPRESSED (self-calibrating)", async () => {
    const plainSchema = new ParquetSchema({ id: { type: "UTF8", compression: "UNCOMPRESSED" }, v: { type: "DOUBLE", compression: "UNCOMPRESSED" } });
    const snapSchema = new ParquetSchema({ id: { type: "UTF8", compression: "SNAPPY" }, v: { type: "DOUBLE", compression: "SNAPPY" } });
    const rows = Array.from({ length: 5000 }, () => ({ id: "AAPL", v: 123.45 }));
    const plain = await toParquet(plainSchema, rows as unknown as Record<string, unknown>[]);
    const snap = await toParquet(snapSchema, rows as unknown as Record<string, unknown>[]);
    expect(snap.length).toBeLessThan(plain.length / 2);
  });
});
