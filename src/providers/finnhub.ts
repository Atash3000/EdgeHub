import type { MarketDataProvider } from "./provider.js";
import type { VendorBar, ProviderResult, ProviderFailure } from "../types.js";
import { SOURCE_VERSION } from "../types.js";

interface FinnhubCandle { s: string; t?: number[]; o?: number[]; h?: number[]; l?: number[]; c?: number[]; v?: number[]; }
type FetchFn = (url: string) => Promise<{ ok: boolean; status: number; json: () => Promise<unknown> }>;

export function mapCandle(symbol: string, raw: unknown, ingestedAt: string): VendorBar[] {
  const json = raw as FinnhubCandle;
  if (!json || json.s !== "ok" || !Array.isArray(json.t)) return [];
  const n = json.t.length;
  const cols = [json.o, json.h, json.l, json.c, json.v];
  if (cols.some((a) => !Array.isArray(a) || a.length !== n)) return []; // uneven/missing arrays -> no usable data
  const bars: VendorBar[] = [];
  for (let i = 0; i < n; i++) {
    const o = json.o![i], h = json.h![i], l = json.l![i], c = json.c![i], v = json.v![i], t = json.t[i];
    if (![o, h, l, c, v, t].every((x) => typeof x === "number" && Number.isFinite(x))) continue; // skip malformed bar
    bars.push({
      ticker: symbol,
      date: new Date(t! * 1000).toISOString().slice(0, 10),
      open: o!, high: h!, low: l!, close: c!,
      adjustedClose: null, isAdjusted: false,
      volume: v!,
      source: "finnhub", sourceVersion: SOURCE_VERSION, ingestedAt,
    });
  }
  return bars;
}

class RateLimiter {
  private last = 0;
  constructor(private readonly minIntervalMs: number) {}
  async wait(sleep: (ms: number) => Promise<void>, now: () => number): Promise<void> {
    const elapsed = now() - this.last;
    if (elapsed < this.minIntervalMs) await sleep(this.minIntervalMs - elapsed);
    this.last = now();
  }
}
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export class FinnhubProvider implements MarketDataProvider {
  readonly name = "finnhub";
  readonly version = SOURCE_VERSION;
  private readonly limiter: RateLimiter;

  constructor(
    private readonly token: string,
    private readonly fetchFn: FetchFn = fetch as unknown as FetchFn,
    maxPerMinute = 55,
  ) { this.limiter = new RateLimiter(Math.ceil(60000 / maxPerMinute)); }

  private async candle(symbol: string, fromSec: number, toSec: number): Promise<VendorBar[]> {
    await this.limiter.wait(sleep, () => Date.now());
    const url = `https://finnhub.io/api/v1/stock/candle?symbol=${symbol}&resolution=D&from=${fromSec}&to=${toSec}&token=${this.token}`;
    const res = await this.fetchFn(url);
    if (!res.ok) throw new Error(`finnhub ${symbol} HTTP ${res.status}`);
    return mapCandle(symbol, await res.json(), new Date().toISOString());
  }

  async getLatestBars(date: string, tickers: string[]): Promise<ProviderResult> {
    const to = Math.floor(new Date(`${date}T23:59:59Z`).getTime() / 1000);
    const from = to - 5 * 86400;
    const bars: VendorBar[] = [];
    const failures: ProviderFailure[] = [];
    for (const t of tickers) {
      try {
        const candles = await this.candle(t, from, to);
        const match = candles.find((b) => b.date === date); // exact date only — no stale fallback
        if (match) bars.push(match);
        else failures.push({ ticker: t, date, reason: "missing_bar_for_date" });
      } catch (err) {
        failures.push({ ticker: t, date, reason: "provider_error", message: (err as Error).message });
      }
    }
    return { bars, failures }; // one bad ticker never aborts the batch
  }

  async getHistory(ticker: string, lookbackDays: number, endDate?: string): Promise<ProviderResult> {
    const to = endDate
      ? Math.floor(new Date(`${endDate}T23:59:59Z`).getTime() / 1000)
      : Math.floor(Date.now() / 1000);
    const from = to - Math.ceil(lookbackDays * 1.5) * 86400; // pad for weekends/holidays
    try {
      return { bars: await this.candle(ticker, from, to), failures: [] };
    } catch (err) {
      return { bars: [], failures: [{ ticker, date: endDate ?? "", reason: "provider_error", message: (err as Error).message }] };
    }
  }
}
