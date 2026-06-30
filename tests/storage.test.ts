import { describe, it, expect } from "vitest";
import { rawKey, metricsKey, RAW_SCHEMA, METRIC_SCHEMA } from "../src/storage.js";

describe("parquet schemas carry instrumentId", () => {
  it("RAW_SCHEMA and METRIC_SCHEMA both define instrumentId", () => {
    expect(RAW_SCHEMA.schema.instrumentId).toBeDefined();
    expect(METRIC_SCHEMA.schema.instrumentId).toBeDefined();
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
