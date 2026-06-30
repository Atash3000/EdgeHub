export interface MetricDef {
  name: string;
  description: string;
  dependsOn: string[];
  window: number | null;
  version: string;
}

export const METRIC_REGISTRY: MetricDef[] = [
  { name: "dollarVolume", description: "close * volume", dependsOn: ["close", "volume"], window: null, version: "1.0" },
  { name: "ma20", description: "20-day SMA of close", dependsOn: ["close"], window: 20, version: "1.0" },
  { name: "ma50", description: "50-day SMA of close", dependsOn: ["close"], window: 50, version: "1.0" },
  { name: "ma150", description: "150-day SMA of close", dependsOn: ["close"], window: 150, version: "1.0" },
  { name: "ma200", description: "200-day SMA of close", dependsOn: ["close"], window: 200, version: "1.0" },
  { name: "avgVolume20", description: "20-day average volume", dependsOn: ["volume"], window: 20, version: "1.0" },
  { name: "avgVolume50", description: "50-day average volume", dependsOn: ["volume"], window: 50, version: "1.0" },
  { name: "atr14", description: "14-day ATR = SMA of last 14 true ranges (requires >= 15 bars)", dependsOn: ["high", "low", "close"], window: 14, version: "1.0" },
  { name: "high52w", description: "highest high over trailing 252 sessions", dependsOn: ["high"], window: 252, version: "1.0" },
  { name: "low52w", description: "lowest low over trailing 252 sessions", dependsOn: ["low"], window: 252, version: "1.0" },
  { name: "distanceTo52wHighPct", description: "percent distance of close to 52w high (<=0)", dependsOn: ["close", "high"], window: 252, version: "1.0" },
  { name: "distanceFrom52wLowPct", description: "percent distance of close above 52w low (>=0)", dependsOn: ["close", "low"], window: 252, version: "1.0" },
  { name: "return21d", description: "close return over trailing 21 sessions", dependsOn: ["close"], window: 21, version: "1.0" },
  { name: "return63d", description: "close return over trailing 63 sessions", dependsOn: ["close"], window: 63, version: "1.0" },
  { name: "return126d", description: "close return over trailing 126 sessions", dependsOn: ["close"], window: 126, version: "1.0" },
  { name: "return252d", description: "close return over trailing 252 sessions", dependsOn: ["close"], window: 252, version: "1.0" },
  { name: "above20ma", description: "close > ma20", dependsOn: ["close"], window: 20, version: "1.0" },
  { name: "above50ma", description: "close > ma50", dependsOn: ["close"], window: 50, version: "1.0" },
  { name: "above150ma", description: "close > ma150", dependsOn: ["close"], window: 150, version: "1.0" },
  { name: "above200ma", description: "close > ma200", dependsOn: ["close"], window: 200, version: "1.0" },
  { name: "ma150Above200", description: "ma150 > ma200", dependsOn: ["close"], window: 200, version: "1.0" },
  { name: "ma200Rising", description: "today ma200 > prior session ma200 (requires 201 bars)", dependsOn: ["close"], window: 201, version: "1.0" },
];
