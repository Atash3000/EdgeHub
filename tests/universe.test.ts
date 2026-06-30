import { describe, it, expect } from "vitest";
import { mergeUniverse } from "../src/universe.js";

describe("mergeUniverse", () => {
  it("merges, upper-cases, and dedupes", () => {
    const r = mergeUniverse([
      { version: "2026-06-29", tickers: ["aapl", "MSFT"] },
      { version: "2026-06-29", tickers: ["msft", "GOOG"] },
    ]);
    expect(r.tickers).toEqual(["AAPL", "MSFT", "GOOG"]);
    expect(r.universeVersion).toBe("2026-06-29");
  });
  it("throws on version mismatch", () => {
    expect(() => mergeUniverse([
      { version: "2026-06-29", tickers: ["AAPL"] },
      { version: "2026-06-30", tickers: ["MSFT"] },
    ])).toThrow(/version mismatch/i);
  });
});
