import { describe, it, expect } from "vitest";
import { buildSecurityMaster, buildTickerMap, detectIdentityChanges, securitiesKey } from "../src/securityMaster.js";
import type { SecurityMasterRow, SecurityMasterResult } from "../src/types.js";

const found = (ticker: string, figi: string, active = true): SecurityMasterRow => ({
  instrumentId: figi, ticker, tickerRoot: ticker, name: `${ticker} Inc`, active,
  shareClassFigi: figi, identitySource: "SHARE_CLASS_FIGI", identityConfidence: "HIGH",
  referenceStatus: "FOUND", source: "polygon", sourceVersion: "1.0", asOfDate: "2026-06-30", ingestedAt: "x",
});

describe("securitiesKey", () => {
  it("builds the asOf-partitioned key", () => {
    expect(securitiesKey("2026-06-30")).toBe("reference/securities/asOf=2026-06-30/part.parquet");
  });
});

describe("buildSecurityMaster", () => {
  it("keeps one row per universe ticker and mints EH: fallbacks for missing reference data", () => {
    const result: SecurityMasterResult = {
      securities: [found("AAPL", "BBG_AAPL")],
      failures: [{ ticker: "ZZZZ", date: "2026-06-30", reason: "missing_reference_data" }],
    };
    const out = buildSecurityMaster(["AAPL", "ZZZZ"], result, "2026-06-30", "polygon", "1.0", "x");
    expect(out.securities).toHaveLength(2);
    const zzzz = out.securities.find((s) => s.ticker === "ZZZZ")!;
    expect(zzzz.instrumentId).toBe("EH:ZZZZ");
    expect(zzzz.identitySource).toBe("EH_TICKER");
    expect(zzzz.identityConfidence).toBe("LOW");
    expect(zzzz.referenceStatus).toBe("MISSING_FALLBACK");
    expect(zzzz.active).toBe(true);
    expect(out.missingTickers).toEqual(["ZZZZ"]);
    expect(out.emptyMaster).toBe(false);
  });

  it("flags duplicate provider rows for one ticker and keeps the first", () => {
    const result: SecurityMasterResult = { securities: [found("AAPL", "BBG_A1"), found("AAPL", "BBG_A2")], failures: [] };
    const out = buildSecurityMaster(["AAPL"], result, "2026-06-30", "polygon", "1.0", "x");
    expect(out.securities).toHaveLength(1);
    expect(out.securities[0]!.instrumentId).toBe("BBG_A1");
    expect(out.duplicateTickers).toEqual(["AAPL"]);
  });

  it("reports emptyMaster when the provider returned nothing and mints all fallbacks", () => {
    const out = buildSecurityMaster(["AAPL", "MSFT"], { securities: [], failures: [] }, "2026-06-30", "polygon", "1.0", "x");
    expect(out.emptyMaster).toBe(true);
    expect(out.securities.map((s) => s.instrumentId).sort()).toEqual(["EH:AAPL", "EH:MSFT"]);
  });

  it("preserves active=false for delisted rows", () => {
    const result: SecurityMasterResult = { securities: [found("DEAD", "BBG_DEAD", false)], failures: [] };
    const out = buildSecurityMaster(["DEAD"], result, "2026-06-30", "polygon", "1.0", "x");
    expect(out.securities[0]!.active).toBe(false);
  });
});

describe("buildTickerMap", () => {
  it("maps ticker -> row", () => {
    const map = buildTickerMap([found("AAPL", "BBG_AAPL")]);
    expect(map.get("AAPL")!.instrumentId).toBe("BBG_AAPL");
  });
});

describe("detectIdentityChanges", () => {
  it("reports a ticker whose instrumentId changed since the prior snapshot", () => {
    const prior = [found("AAPL", "OLD")];
    const today = [found("AAPL", "NEW")];
    expect(detectIdentityChanges(today, prior)).toEqual([{ ticker: "AAPL", from: "OLD", to: "NEW" }]);
  });
  it("is silent when ids are unchanged or the ticker is new", () => {
    expect(detectIdentityChanges([found("AAPL", "X")], [found("AAPL", "X")])).toEqual([]);
    expect(detectIdentityChanges([found("NEWCO", "X")], [])).toEqual([]);
  });
});
