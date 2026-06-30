import { describe, it, expect } from "vitest";
import { resolveBarsToInstruments } from "../src/resolver.js";
import type { VendorBar, SecurityMasterRow } from "../src/types.js";

const bar = (ticker: string): VendorBar => ({ ticker, date: "2026-06-30", open: 1, high: 1, low: 1, close: 1, adjustedClose: null, isAdjusted: false, volume: 1, source: "polygon", sourceVersion: "1.0", ingestedAt: "x" });
const sec = (instrumentId: string, ticker: string): SecurityMasterRow => ({ instrumentId, ticker, tickerRoot: ticker, active: true, identitySource: "SHARE_CLASS_FIGI", identityConfidence: "HIGH", referenceStatus: "FOUND", source: "polygon", sourceVersion: "1.0", asOfDate: "2026-06-30", ingestedAt: "x" });

describe("resolveBarsToInstruments", () => {
  it("resolves a bar to its instrumentId and keeps the original ticker", () => {
    const { resolved, errors } = resolveBarsToInstruments([bar("AAPL")], [sec("BBG_AAPL", "AAPL")], "2026-06-30", "R", "polygon", "2026-06-30");
    expect(errors).toEqual([]);
    expect(resolved).toHaveLength(1);
    expect(resolved[0]!.instrumentId).toBe("BBG_AAPL");
    expect(resolved[0]!.ticker).toBe("AAPL");
  });

  it("records unresolved_instrument and excludes a bar with no security row", () => {
    const { resolved, errors } = resolveBarsToInstruments([bar("ZZZZ")], [sec("BBG_AAPL", "AAPL")], "2026-06-30", "R", "polygon", "2026-06-30");
    expect(resolved).toEqual([]);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ ticker: "ZZZZ", reason: "unresolved_instrument", runId: "R" });
  });

  it("never mints an id (unknown ticker is excluded, not given EH:)", () => {
    const { resolved } = resolveBarsToInstruments([bar("ZZZZ")], [], "2026-06-30", "R", "polygon", "2026-06-30");
    expect(resolved).toEqual([]);
  });
});
