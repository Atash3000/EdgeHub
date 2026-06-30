import type { IdentitySource, IdentityConfidence } from "./types.js";

/** Split a share-class suffix on the dot: "BRK.A" -> { root: "BRK", suffix: "A" }. */
export function splitTicker(ticker: string): { tickerRoot: string; tickerSuffix?: string } {
  const dot = ticker.indexOf(".");
  if (dot === -1) return { tickerRoot: ticker };
  return { tickerRoot: ticker.slice(0, dot), tickerSuffix: ticker.slice(dot + 1) };
}

/** Pure, deterministic identity. Fallback order is fixed (spec §6). No cross-day pinning. */
export function makeInstrumentId(input: {
  shareClassFigi?: string; compositeFigi?: string;
  cik?: string; ticker?: string; primaryExchange?: string;
}): { instrumentId: string; identitySource: IdentitySource; identityConfidence: IdentityConfidence } {
  if (input.shareClassFigi) return { instrumentId: input.shareClassFigi, identitySource: "SHARE_CLASS_FIGI", identityConfidence: "HIGH" };
  if (input.compositeFigi) return { instrumentId: input.compositeFigi, identitySource: "COMPOSITE_FIGI", identityConfidence: "HIGH" };
  // EH: fallbacks use the FULL ticker (e.g. "BRK.A"), never tickerRoot — share classes must not collide.
  if (input.cik && input.ticker) return { instrumentId: `EH:${input.cik}:${input.ticker}`, identitySource: "EH_CIK_TICKER", identityConfidence: "MEDIUM" };
  if (input.ticker && input.primaryExchange) return { instrumentId: `EH:${input.ticker}:${input.primaryExchange}`, identitySource: "EH_TICKER_EXCHANGE", identityConfidence: "LOW" };
  return { instrumentId: `EH:${input.ticker ?? ""}`, identitySource: "EH_TICKER", identityConfidence: "LOW" };
}
