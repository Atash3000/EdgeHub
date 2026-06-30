import { describe, it, expect } from "vitest";
import { makeInstrumentId, splitTicker } from "../src/identity.js";

describe("splitTicker", () => {
  it("splits a share-class suffix on the dot", () => {
    expect(splitTicker("BRK.A")).toEqual({ tickerRoot: "BRK", tickerSuffix: "A" });
  });
  it("returns the whole ticker as root when there is no suffix", () => {
    expect(splitTicker("AAPL")).toEqual({ tickerRoot: "AAPL" });
  });
});

describe("makeInstrumentId", () => {
  it("prefers share_class_figi (HIGH)", () => {
    const r = makeInstrumentId({ shareClassFigi: "BBG001S5N8V8", compositeFigi: "BBG000B9XRY4", cik: "0000320193", ticker: "AAPL" });
    expect(r).toEqual({ instrumentId: "BBG001S5N8V8", identitySource: "SHARE_CLASS_FIGI", identityConfidence: "HIGH" });
  });
  it("falls back to composite_figi (HIGH)", () => {
    const r = makeInstrumentId({ compositeFigi: "BBG000B9XRY4", cik: "0000320193", ticker: "AAPL" });
    expect(r).toEqual({ instrumentId: "BBG000B9XRY4", identitySource: "COMPOSITE_FIGI", identityConfidence: "HIGH" });
  });
  it("falls back to EH:cik:ticker (MEDIUM)", () => {
    const r = makeInstrumentId({ cik: "0000320193", ticker: "AAPL" });
    expect(r).toEqual({ instrumentId: "EH:0000320193:AAPL", identitySource: "EH_CIK_TICKER", identityConfidence: "MEDIUM" });
  });
  it("falls back to EH:ticker:exchange (LOW)", () => {
    const r = makeInstrumentId({ ticker: "AAPL", primaryExchange: "XNAS" });
    expect(r).toEqual({ instrumentId: "EH:AAPL:XNAS", identitySource: "EH_TICKER_EXCHANGE", identityConfidence: "LOW" });
  });
  it("falls back to EH:ticker (LOW) when nothing else is present", () => {
    const r = makeInstrumentId({ ticker: "AAPL" });
    expect(r).toEqual({ instrumentId: "EH:AAPL", identitySource: "EH_TICKER", identityConfidence: "LOW" });
  });
  it("gives GOOG and GOOGL different ids when share-class figis differ", () => {
    const goog = makeInstrumentId({ shareClassFigi: "BBG009S39JX6", ticker: "GOOG" });
    const googl = makeInstrumentId({ shareClassFigi: "BBG009S3NB30", ticker: "GOOGL" });
    expect(goog.instrumentId).not.toBe(googl.instrumentId);
  });
  it("gives BRK.A and BRK.B different EH ids when figi is absent (full ticker, not root)", () => {
    const a = makeInstrumentId({ cik: "0001067983", ticker: "BRK.A" });
    const b = makeInstrumentId({ cik: "0001067983", ticker: "BRK.B" });
    expect(a.instrumentId).toBe("EH:0001067983:BRK.A");
    expect(b.instrumentId).toBe("EH:0001067983:BRK.B");
    expect(a.instrumentId).not.toBe(b.instrumentId);
    // and via the exchange fallback when cik is also absent:
    expect(makeInstrumentId({ ticker: "BRK.A", primaryExchange: "XNYS" }).instrumentId).toBe("EH:BRK.A:XNYS");
    expect(makeInstrumentId({ ticker: "BRK.B", primaryExchange: "XNYS" }).instrumentId).toBe("EH:BRK.B:XNYS");
  });
});
