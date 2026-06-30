import { describe, it, expect } from "vitest";
import { buildSymbolAliases, symbolAliasesKey } from "../src/symbolAliases.js";
import type { SecurityMasterRow, SymbolAliasRow } from "../src/types.js";

const sec = (instrumentId: string, ticker: string): SecurityMasterRow => ({
  instrumentId, ticker, tickerRoot: ticker, active: true,
  identitySource: "SHARE_CLASS_FIGI", identityConfidence: "HIGH", referenceStatus: "FOUND",
  source: "polygon", sourceVersion: "1.0", asOfDate: "2026-06-30", ingestedAt: "x",
});

describe("symbolAliasesKey", () => {
  it("builds the asOf-partitioned key", () => {
    expect(symbolAliasesKey("2026-06-30")).toBe("reference/symbol_aliases/asOf=2026-06-30/part.parquet");
  });
});

describe("buildSymbolAliases", () => {
  it("opens a new alias for a security with no prior alias", () => {
    const { aliases } = buildSymbolAliases([sec("ID1", "AAPL")], [], "2026-06-30", "2026-06-29", "polygon", "1.0", "t");
    expect(aliases).toHaveLength(1);
    expect(aliases[0]).toMatchObject({ instrumentId: "ID1", ticker: "AAPL", validFrom: "2026-06-30", validTo: null, confidence: "MEDIUM" });
  });

  it("carries an unchanged open alias forward without duplicating", () => {
    const prior: SymbolAliasRow[] = [{ instrumentId: "ID1", ticker: "AAPL", validFrom: "2026-06-01", validTo: null, source: "polygon", sourceVersion: "1.0", asOfDate: "2026-06-29", confidence: "MEDIUM", createdAt: "t0" }];
    const { aliases } = buildSymbolAliases([sec("ID1", "AAPL")], prior, "2026-06-30", "2026-06-29", "polygon", "1.0", "t");
    expect(aliases).toHaveLength(1);
    expect(aliases[0]).toMatchObject({ ticker: "AAPL", validFrom: "2026-06-01", validTo: null });
  });

  it("on a forward rename, closes the old ticker and opens the new one", () => {
    const prior: SymbolAliasRow[] = [{ instrumentId: "ID1", ticker: "FB", validFrom: "2012-05-18", validTo: null, source: "polygon", sourceVersion: "1.0", asOfDate: "2026-06-29", confidence: "MEDIUM", createdAt: "t0" }];
    const { aliases } = buildSymbolAliases([sec("ID1", "META")], prior, "2026-06-30", "2026-06-29", "polygon", "1.0", "t");
    const fb = aliases.find((a) => a.ticker === "FB")!;
    const meta = aliases.find((a) => a.ticker === "META")!;
    expect(fb.validTo).toBe("2026-06-29");
    expect(meta).toMatchObject({ validFrom: "2026-06-30", validTo: null });
    expect(aliases).toHaveLength(2);
  });
});
