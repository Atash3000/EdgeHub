import type { VendorBar, SecurityMasterRow, ResolvedVendorBar, ErrorRecord } from "./types.js";
import { buildTickerMap } from "./securityMaster.js";

/** Resolve vendor bars to instrument identity by exact ticker. Never mints — minting is the master's job. */
export function resolveBarsToInstruments(
  bars: VendorBar[], securities: SecurityMasterRow[],
  tradingDay: string, runId: string, source: string, universeVersion: string,
): { resolved: ResolvedVendorBar[]; errors: ErrorRecord[] } {
  const map = buildTickerMap(securities);
  const resolved: ResolvedVendorBar[] = [];
  const errors: ErrorRecord[] = [];
  for (const bar of bars) {
    const sec = map.get(bar.ticker);
    if (!sec) {
      errors.push({
        runId, tradingDay, source, universeVersion, ticker: bar.ticker,
        reason: "unresolved_instrument",
        message: `No security master row found for ticker ${bar.ticker} on ${tradingDay}`,
        createdAt: new Date().toISOString(),
      });
      continue;
    }
    resolved.push({ ...bar, instrumentId: sec.instrumentId });
  }
  return { resolved, errors };
}
