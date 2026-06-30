import type { ProviderResult, SecurityMasterResult } from "../types.js";

export interface MarketDataProvider {
  readonly name: string;
  readonly version: string;
  /** Returns bars whose date === the requested date (never a stale bar) plus a failure per missing/errored ticker. */
  getLatestBars(date: string, tickers: string[]): Promise<ProviderResult>;
  getHistory(ticker: string, lookbackDays: number, endDate?: string): Promise<ProviderResult>;
  listSecurities(asOfDate: string, tickers?: string[]): Promise<SecurityMasterResult>;
}
