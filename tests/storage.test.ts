import { describe, it, expect } from "vitest";
import { rawKey, metricsKey } from "../src/storage.js";

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
