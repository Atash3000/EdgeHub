// tests/validate.test.ts
import { describe, it, expect } from "vitest";
import { gradeBar } from "../src/validate.js";
import type { VendorBar } from "../src/types.js";

const base: VendorBar = { ticker: "AAPL", date: "2026-06-29", open: 10, high: 12, low: 9, close: 11, adjustedClose: null, isAdjusted: false, volume: 1000, source: "fake", sourceVersion: "1.0", ingestedAt: "x" };

describe("gradeBar", () => {
  it("grades a clean bar OK", () => { expect(gradeBar(base, new Set()).status).toBe("OK"); });
  it("rejects a negative price", () => {
    const r = gradeBar({ ...base, close: -1 }, new Set());
    expect(r.status).toBe("REJECTED"); expect(r.issues).toContain("negative_price");
  });
  it("rejects a duplicate ticker/date", () => {
    const seen = new Set<string>(); gradeBar(base, seen);
    const r = gradeBar(base, seen);
    expect(r.status).toBe("REJECTED"); expect(r.issues).toContain("duplicate");
  });
  it("warns on zero volume", () => {
    const r = gradeBar({ ...base, volume: 0 }, new Set());
    expect(r.status).toBe("WARN"); expect(r.issues).toContain("zero_volume");
  });
});
