import type { MarketDataProvider } from "./provider.js";
import type { VendorBar, ProviderResult, ProviderFailure } from "../types.js";
import { SOURCE_VERSION } from "../types.js";

type FetchInit = { headers: Record<string, string> };
type FetchFn = (url: string, init?: FetchInit) => Promise<{ ok: boolean; status: number; json: () => Promise<unknown> }>;

interface Agg { t: number; o: number; h: number; l: number; c: number; v: number; }
interface AggResponse { status?: string; results?: Agg[]; }
interface GroupedResult extends Agg { T: string; }
interface GroupedResponse { status?: string; results?: GroupedResult[]; }

const BASE = "https://api.polygon.io";
const isoDate = (ms: number): string => new Date(ms).toISOString().slice(0, 10);

export function mapAgg(ticker: string, r: Agg, ingestedAt: string): VendorBar {
  return {
    ticker, date: isoDate(r.t),
    open: r.o, high: r.h, low: r.l, close: r.c,
    adjustedClose: r.c, isAdjusted: true, // fetched with adjusted=true
    volume: r.v,
    source: "polygon", sourceVersion: SOURCE_VERSION, ingestedAt,
  };
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

export class PolygonProvider implements MarketDataProvider {
  readonly name = "polygon";
  readonly version = SOURCE_VERSION;
  private readonly limiter: RateLimiter;
  constructor(
    private readonly apiKey: string,
    private readonly fetchFn: FetchFn = fetch as unknown as FetchFn,
    maxPerMinute = 5,
  ) { this.limiter = new RateLimiter(Math.ceil(60000 / maxPerMinute)); }

  private async get(url: string): Promise<unknown> {
    await this.limiter.wait(sleep, () => Date.now());
    // Send the API key as a header, never in the URL/query string (avoids leaking it via logs/proxies).
    const res = await this.fetchFn(url, { headers: { Authorization: `Bearer ${this.apiKey}` } });
    if (!res.ok) throw new Error(`polygon HTTP ${res.status}`); // status only — never the URL
    return res.json();
  }

  async getHistory(ticker: string, lookbackDays: number, endDate?: string): Promise<ProviderResult> {
    const end = endDate ?? new Date().toISOString().slice(0, 10);
    const fromMs = new Date(`${end}T00:00:00Z`).getTime() - Math.ceil(lookbackDays * 1.5) * 86400000;
    const from = new Date(fromMs).toISOString().slice(0, 10);
    const url = `${BASE}/v2/aggs/ticker/${ticker}/range/1/day/${from}/${end}?adjusted=true&sort=asc&limit=50000`;
    try {
      const json = (await this.get(url)) as AggResponse;
      const at = new Date().toISOString();
      const bars = (json.results ?? []).map((r) => mapAgg(ticker, r, at)).filter((b) => b.date <= end);
      return { bars, failures: [] };
    } catch (err) {
      return { bars: [], failures: [{ ticker, date: endDate ?? "", reason: "provider_error", message: (err as Error).message }] };
    }
  }

  async getLatestBars(date: string, tickers: string[]): Promise<ProviderResult> {
    const url = `${BASE}/v2/aggs/grouped/locale/us/market/stocks/${date}?adjusted=true`;
    try {
      const json = (await this.get(url)) as GroupedResponse;
      const at = new Date().toISOString();
      const byTicker = new Map<string, GroupedResult>();
      for (const r of json.results ?? []) byTicker.set(r.T, r);
      const bars: VendorBar[] = [];
      const failures: ProviderFailure[] = [];
      for (const t of tickers) {
        const r = byTicker.get(t);
        if (r) bars.push(mapAgg(t, r, at));
        else failures.push({ ticker: t, date, reason: "missing_bar_for_date" });
      }
      return { bars, failures };
    } catch (err) {
      return { bars: [], failures: tickers.map((t) => ({ ticker: t, date, reason: "provider_error", message: (err as Error).message })) };
    }
  }
}
