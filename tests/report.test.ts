// tests/report.test.ts
import { describe, it, expect } from "vitest";
import { renderReport } from "../src/report.js";
import type { RunManifest } from "../src/types.js";

const m: RunManifest = { runId: "20260629T223000Z", mode: "daily", tradingDay: "2026-06-29", provider: "finnhub", universeVersion: "2026-06-29", symbolsRequested: 612, symbolsSucceeded: 610, rowsWritten: 610, warnings: 4, rejected: 2, missingBars: 0, securitiesMastered: 9, securitiesResolved: 9, unresolvedTickers: 0, missingReferenceData: 1, aliasRows: 9, runtimeSec: 302, metricVersion: "1.0", schemaVersion: "metrics_v1", status: "PARTIAL" };

describe("renderReport", () => {
  it("includes key run stats", () => {
    const t = renderReport(m);
    expect(t).toContain("EdgeHub Daily Update");
    expect(t).toContain("2026-06-29");
    expect(t).toContain("610");
    expect(t).toContain("PARTIAL");
  });

  it("includes security resolution stats", () => {
    const t = renderReport(m);
    expect(t).toContain("Securities mastered: 9");
    expect(t).toContain("Securities resolved: 9");
    expect(t).toContain("Missing reference (EH fallback): 1");
    expect(t).toContain("Alias rows: 9");
  });
});
