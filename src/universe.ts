import sp500 from "../config/universe/sp500.json" with { type: "json" };
import nasdaq100 from "../config/universe/nasdaq100.json" with { type: "json" };
import watchlist from "../config/universe/watchlist.json" with { type: "json" };

export interface UniverseFile { version: string; tickers: string[]; }
export interface ResolvedUniverse { tickers: string[]; universeVersion: string; }

export function mergeUniverse(files: UniverseFile[]): ResolvedUniverse {
  const versions = new Set(files.map((f) => f.version));
  if (versions.size > 1) throw new Error(`Universe version mismatch: ${[...versions].join(", ")}`);
  const seen = new Set<string>();
  const tickers: string[] = [];
  for (const file of files) {
    for (const raw of file.tickers) {
      const t = raw.toUpperCase();
      if (!seen.has(t)) { seen.add(t); tickers.push(t); }
    }
  }
  return { tickers, universeVersion: files[0]!.version };
}

export function loadUniverse(): ResolvedUniverse {
  return mergeUniverse([sp500, nasdaq100, watchlist]);
}
