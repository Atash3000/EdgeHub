import type { MarketDataProvider } from "./provider.js";
import type { VendorBar, ProviderResult, ProviderFailure } from "../types.js";

export class FakeProvider implements MarketDataProvider {
  readonly name = "fake";
  readonly version = "1.0";
  constructor(private readonly history: Map<string, VendorBar[]>) {}

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

  async getHistory(ticker: string, lookbackDays: number): Promise<ProviderResult> {
    return { bars: (this.history.get(ticker) ?? []).slice(-lookbackDays), failures: [] };
  }
}
