import type { MarketDataProvider } from "./provider.js";
import type { VendorBar, ProviderResult, ProviderFailure, SecurityMasterRow, SecurityMasterResult } from "../types.js";

export class FakeProvider implements MarketDataProvider {
  readonly name = "fake";
  readonly version = "1.0";
  constructor(
    private readonly history: Map<string, VendorBar[]>,
    private readonly securities: SecurityMasterRow[] = [],
  ) {}

  async getLatestBars(date: string, tickers: string[]): Promise<ProviderResult> {
    const bars: VendorBar[] = [];
    const failures: ProviderFailure[] = [];
    for (const t of tickers) {
      const match = (this.history.get(t) ?? []).find((b) => b.date === date);
      if (match) bars.push(match);
      else failures.push({ ticker: t, date, reason: "missing_bar_for_date" });
    }
    return { bars, failures };
  }

  async getHistory(ticker: string, lookbackDays: number, endDate?: string): Promise<ProviderResult> {
    let bars = this.history.get(ticker) ?? [];
    if (endDate) bars = bars.filter((b) => b.date <= endDate);
    return { bars: bars.slice(-lookbackDays), failures: [] };
  }

  async listSecurities(asOfDate: string, tickers: string[] = []): Promise<SecurityMasterResult> {
    const byTicker = new Map(this.securities.map((s) => [s.ticker, s]));
    const securities: SecurityMasterRow[] = [];
    const failures: ProviderFailure[] = [];
    for (const t of tickers) {
      const s = byTicker.get(t);
      if (s) securities.push({ ...s, asOfDate });
      else failures.push({ ticker: t, date: asOfDate, reason: "missing_reference_data" });
    }
    return { securities, failures };
  }
}
