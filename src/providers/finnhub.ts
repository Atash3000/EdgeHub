import type { MarketDataProvider } from "./provider.js";
import type { ProviderResult } from "../types.js";

export class FinnhubProvider implements MarketDataProvider {
  readonly name = "finnhub";
  readonly version = "1.0";
  constructor(_token: string) {}
  async getLatestBars(_d: string, _t: string[]): Promise<ProviderResult> { throw new Error("not implemented"); }
  async getHistory(_t: string, _n: number): Promise<ProviderResult> { throw new Error("not implemented"); }
}
