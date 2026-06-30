import type { MarketDataProvider } from "./provider.js";
import { FakeProvider } from "./fake.js";
import { FinnhubProvider } from "./finnhub.js";
import { StooqProvider } from "./stooq.js";

export function getProvider(name: string, secrets: Record<string, string>): MarketDataProvider {
  switch (name) {
    case "finnhub": return new FinnhubProvider(secrets.finnhubToken ?? "");
    case "fake": return new FakeProvider(new Map());
    case "stooq": return new StooqProvider();
    default: throw new Error(`Unknown data provider: ${name}`);
  }
}
