import type { MarketDataProvider } from "./provider.js";
import type { VendorBar, ProviderResult, ProviderFailure } from "../types.js";
import { SOURCE_VERSION } from "../types.js";

type FetchFn = (url: string) => Promise<{ ok: boolean; status: number; text: () => Promise<string> }>;

export function stooqSymbol(ticker: string): string {
  return `${ticker.toLowerCase()}.us`;
}

export function parseStooqCsv(ticker: string, csv: string, ingestedAt: string): VendorBar[] {
  const lines = csv.split(/\r?\n/).filter((l) => l.trim() !== "");
  if (lines.length < 2) return [];
  const header = lines[0]!.toLowerCase();
  if (!header.startsWith("date,open,high,low,close")) return [];

  const bars: VendorBar[] = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i]!.split(",");
    if (cols.length < 6) continue;
    const [dateStr, openStr, highStr, lowStr, closeStr, volumeStr] = cols;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr!)) continue;
    const toNum = (s: string | undefined) => (s === undefined || s.trim() === "" ? NaN : Number(s));
    const open = toNum(openStr);
    const high = toNum(highStr);
    const low = toNum(lowStr);
    const close = toNum(closeStr);
    const volume = toNum(volumeStr);
    if (![open, high, low, close, volume].every(Number.isFinite)) continue;
    bars.push({
      ticker,
      date: dateStr!,
      open,
      high,
      low,
      close,
      adjustedClose: null,
      isAdjusted: false,
      volume,
      source: "stooq",
      sourceVersion: SOURCE_VERSION,
      ingestedAt,
    });
  }
  return bars;
}

export class StooqProvider implements MarketDataProvider {
  readonly name = "stooq";
  readonly version = SOURCE_VERSION;

  constructor(private readonly fetchFn: FetchFn = fetch as unknown as FetchFn) {}

  private async fetchCsv(ticker: string, d1: string, d2: string): Promise<VendorBar[]> {
    const sym = stooqSymbol(ticker);
    const url = `https://stooq.com/q/d/l/?s=${sym}&d1=${d1}&d2=${d2}&i=d`;
    const res = await this.fetchFn(url);
    if (!res.ok) throw new Error(`stooq ${ticker} HTTP ${res.status}`);
    return parseStooqCsv(ticker, await res.text(), new Date().toISOString());
  }

  async getHistory(ticker: string, lookbackDays: number, endDate?: string): Promise<ProviderResult> {
    const end = endDate ?? new Date().toISOString().slice(0, 10);
    const d2 = end.replace(/-/g, "");
    const d1Start = new Date(end);
    d1Start.setDate(d1Start.getDate() - Math.ceil(lookbackDays * 1.5));
    const d1 = d1Start.toISOString().slice(0, 10).replace(/-/g, "");
    try {
      const bars = (await this.fetchCsv(ticker, d1, d2)).filter((b) => b.date <= end);
      return { bars, failures: [] };
    } catch (err) {
      return { bars: [], failures: [{ ticker, date: endDate ?? "", reason: "provider_error", message: (err as Error).message }] };
    }
  }

  async getLatestBars(date: string, tickers: string[]): Promise<ProviderResult> {
    const d2 = date.replace(/-/g, "");
    const d1Start = new Date(date);
    d1Start.setDate(d1Start.getDate() - 10);
    const d1 = d1Start.toISOString().slice(0, 10).replace(/-/g, "");
    const bars: VendorBar[] = [];
    const failures: ProviderFailure[] = [];
    for (const ticker of tickers) {
      try {
        const candles = await this.fetchCsv(ticker, d1, d2);
        const match = candles.find((b) => b.date === date);
        if (match) bars.push(match);
        else failures.push({ ticker, date, reason: "missing_bar_for_date" });
      } catch (err) {
        failures.push({ ticker, date, reason: "provider_error", message: (err as Error).message });
      }
    }
    return { bars, failures };
  }
}
