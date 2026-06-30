import type { MarketDataProvider } from "./provider.js";
import { FakeProvider } from "./fake.js";
import { FinnhubProvider } from "./finnhub.js";
import { PolygonProvider } from "./polygon.js";

export function getProvider(name: string, secrets: Record<string, string>): MarketDataProvider {
  switch (name) {
    case "finnhub": {
      if (!secrets.finnhubToken) throw new Error("missing finnhub api key (/edge-hunter/finnhub/api_key)");
      return new FinnhubProvider(secrets.finnhubToken);
    }
    case "fake": return new FakeProvider(new Map());
    case "polygon": {
      if (!secrets.polygonToken) throw new Error("missing polygon api key (/global/polygon/api-key)");
      return new PolygonProvider(secrets.polygonToken);
    }
    default: throw new Error(`Unknown data provider: ${name}`);
  }
}
