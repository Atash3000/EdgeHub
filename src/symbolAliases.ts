import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import type { SecurityMasterRow, SymbolAliasRow } from "./types.js";

export { symbolAliasesKey, writeSymbolAliases } from "./storage.js";

export function aliasesStateKey(): string {
  return "reference/state/symbol_aliases.json";
}

/** Forward-only alias windows (spec §8.2). No historical reconstruction. */
export function buildSymbolAliases(
  securities: SecurityMasterRow[], priorAliases: SymbolAliasRow[],
  asOfDate: string, previousTradingDay: string, source: string, sourceVersion: string, createdAt: string,
): { aliases: SymbolAliasRow[]; conflicts: string[] } {
  const conflicts: string[] = [];
  // Today's open ticker per instrumentId (one security per instrumentId expected).
  const currentTickerById = new Map<string, SecurityMasterRow>();
  for (const s of securities) {
    if (currentTickerById.has(s.instrumentId) && currentTickerById.get(s.instrumentId)!.ticker !== s.ticker) {
      conflicts.push(s.ticker); continue;
    }
    currentTickerById.set(s.instrumentId, s);
  }

  // Clone prior rows so we can close renamed ones; key open rows by instrumentId.
  const out: SymbolAliasRow[] = priorAliases.map((a) => ({ ...a }));
  const priorOpenById = new Map<string, SymbolAliasRow>();
  for (const a of out) if (a.validTo === null) priorOpenById.set(a.instrumentId, a);

  for (const [instrumentId, sec] of currentTickerById) {
    const open = priorOpenById.get(instrumentId);
    if (!open) {
      out.push(makeAlias(sec, asOfDate, source, sourceVersion, createdAt));
    } else if (open.ticker !== sec.ticker) {
      open.validTo = previousTradingDay;                              // close the old ticker
      out.push(makeAlias(sec, asOfDate, source, sourceVersion, createdAt)); // open the new ticker
    } // else unchanged -> carry forward (already in out)
  }

  // De-dupe identical rows by (instrumentId, ticker, validFrom, validTo).
  const seen = new Set<string>();
  const aliases = out.filter((a) => {
    const k = `${a.instrumentId}|${a.ticker}|${a.validFrom}|${a.validTo ?? ""}`;
    if (seen.has(k)) return false; seen.add(k); return true;
  });
  return { aliases, conflicts };
}

function makeAlias(s: SecurityMasterRow, asOfDate: string, source: string, sourceVersion: string, createdAt: string): SymbolAliasRow {
  return {
    instrumentId: s.instrumentId, ticker: s.ticker, tickerRoot: s.tickerRoot, tickerSuffix: s.tickerSuffix,
    primaryExchange: s.primaryExchange, validFrom: asOfDate, validTo: null,
    source, sourceVersion, asOfDate, confidence: "MEDIUM", createdAt,
  };
}

export async function writeAliasesState(s3: S3Client, bucket: string, rows: SymbolAliasRow[]): Promise<void> {
  await s3.send(new PutObjectCommand({
    Bucket: bucket, Key: aliasesStateKey(), Body: JSON.stringify(rows), ContentType: "application/json",
  }));
}

export async function readAliasesState(s3: S3Client, bucket: string): Promise<SymbolAliasRow[]> {
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: aliasesStateKey() }));
    const parsed = JSON.parse(await res.Body!.transformToString());
    return Array.isArray(parsed) ? (parsed as SymbolAliasRow[]) : [];
  } catch { return []; }
}
