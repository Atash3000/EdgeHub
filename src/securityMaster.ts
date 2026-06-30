import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import type { SecurityMasterRow, SecurityMasterResult } from "./types.js";
import { makeInstrumentId, splitTicker } from "./identity.js";

export { securitiesKey, writeSecurities } from "./storage.js";

export function securitiesStateKey(): string {
  return "reference/state/securities.json";
}

/** §4 invariant: exactly one row per universe ticker. Provider rows win; missing tickers are minted EH:<ticker>. */
export function buildSecurityMaster(
  universeTickers: string[], result: SecurityMasterResult,
  asOfDate: string, source: string, sourceVersion: string, ingestedAt: string,
): { securities: SecurityMasterRow[]; missingTickers: string[]; duplicateTickers: string[]; emptyMaster: boolean } {
  const byTicker = new Map<string, SecurityMasterRow>();
  const duplicateTickers: string[] = [];
  for (const s of result.securities) {
    if (byTicker.has(s.ticker)) { duplicateTickers.push(s.ticker); continue; } // keep first
    byTicker.set(s.ticker, { ...s, asOfDate, source, sourceVersion, ingestedAt });
  }
  const missingTickers: string[] = [];
  for (const ticker of universeTickers) {
    if (byTicker.has(ticker)) continue;
    missingTickers.push(ticker);
    const { tickerRoot, tickerSuffix } = splitTicker(ticker);
    const id = makeInstrumentId({ ticker }); // bare-ticker fallback -> EH:<ticker>
    byTicker.set(ticker, {
      instrumentId: id.instrumentId, ticker, tickerRoot, tickerSuffix, active: true,
      identitySource: id.identitySource, identityConfidence: id.identityConfidence, referenceStatus: "MISSING_FALLBACK",
      source, sourceVersion, asOfDate, ingestedAt,
    });
  }
  return {
    securities: [...byTicker.values()],
    missingTickers, duplicateTickers,
    emptyMaster: result.securities.length === 0,
  };
}

export function buildTickerMap(rows: SecurityMasterRow[]): Map<string, SecurityMasterRow> {
  return new Map(rows.map((r) => [r.ticker, r]));
}

/** Non-blocking guard: a universe ticker whose instrumentId differs from the prior snapshot. */
export function detectIdentityChanges(
  today: SecurityMasterRow[], prior: SecurityMasterRow[],
): { ticker: string; from: string; to: string }[] {
  const priorByTicker = new Map(prior.map((r) => [r.ticker, r.instrumentId]));
  const changes: { ticker: string; from: string; to: string }[] = [];
  for (const r of today) {
    const was = priorByTicker.get(r.ticker);
    if (was && was !== r.instrumentId) changes.push({ ticker: r.ticker, from: was, to: r.instrumentId });
  }
  return changes;
}

export async function writeSecuritiesState(s3: S3Client, bucket: string, rows: SecurityMasterRow[]): Promise<void> {
  await s3.send(new PutObjectCommand({
    Bucket: bucket, Key: securitiesStateKey(), Body: JSON.stringify(rows), ContentType: "application/json",
  }));
}

export async function readSecuritiesState(s3: S3Client, bucket: string): Promise<SecurityMasterRow[]> {
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: securitiesStateKey() }));
    const parsed = JSON.parse(await res.Body!.transformToString());
    return Array.isArray(parsed) ? (parsed as SecurityMasterRow[]) : [];
  } catch { return []; }
}
