import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";
import { Writable } from "stream";
import type { RawBarRow, MetricRow, SecurityMasterRow, SymbolAliasRow } from "./types.js";

function parts(date: string): { year: string; month: string; day: string } {
  const [year, month, day] = date.split("-") as [string, string, string];
  return { year, month, day };
}
export function rawKey(source: string, date: string, runId: string): string {
  const { year, month, day } = parts(date);
  return `raw/${source}/daily/year=${year}/month=${month}/day=${day}/runId=${runId}/part.parquet`;
}
export function metricsKey(date: string, runId: string): string {
  const { year, month, day } = parts(date);
  return `metrics/daily/year=${year}/month=${month}/day=${day}/runId=${runId}/part.parquet`;
}

export const RAW_SCHEMA = new ParquetSchema({
  ticker: { type: "UTF8" }, date: { type: "UTF8" },
  open: { type: "DOUBLE" }, high: { type: "DOUBLE" }, low: { type: "DOUBLE" }, close: { type: "DOUBLE" },
  adjustedClose: { type: "DOUBLE", optional: true }, isAdjusted: { type: "BOOLEAN" },
  volume: { type: "DOUBLE" },
  source: { type: "UTF8" }, sourceVersion: { type: "UTF8" }, ingestedAt: { type: "UTF8" },
  runId: { type: "UTF8" }, schemaVersion: { type: "UTF8" }, metricVersion: { type: "UTF8" }, universeVersion: { type: "UTF8" },
});

export const METRIC_SCHEMA = new ParquetSchema({
  ticker: { type: "UTF8" }, date: { type: "UTF8" }, close: { type: "DOUBLE" }, dollarVolume: { type: "DOUBLE" },
  ma20: { type: "DOUBLE", optional: true }, ma50: { type: "DOUBLE", optional: true },
  ma150: { type: "DOUBLE", optional: true }, ma200: { type: "DOUBLE", optional: true },
  avgVolume20: { type: "DOUBLE", optional: true }, avgVolume50: { type: "DOUBLE", optional: true },
  atr14: { type: "DOUBLE", optional: true },
  high52w: { type: "DOUBLE", optional: true }, low52w: { type: "DOUBLE", optional: true },
  distanceTo52wHighPct: { type: "DOUBLE", optional: true }, distanceFrom52wLowPct: { type: "DOUBLE", optional: true },
  return21d: { type: "DOUBLE", optional: true }, return63d: { type: "DOUBLE", optional: true },
  return126d: { type: "DOUBLE", optional: true }, return252d: { type: "DOUBLE", optional: true },
  above20ma: { type: "BOOLEAN", optional: true }, above50ma: { type: "BOOLEAN", optional: true },
  above150ma: { type: "BOOLEAN", optional: true }, above200ma: { type: "BOOLEAN", optional: true },
  ma150Above200: { type: "BOOLEAN", optional: true }, ma200Rising: { type: "BOOLEAN", optional: true },
  qualityStatus: { type: "UTF8" }, qualityIssues: { type: "UTF8" },
  runId: { type: "UTF8" }, ingestedAt: { type: "UTF8" }, source: { type: "UTF8" }, sourceVersion: { type: "UTF8" },
  schemaVersion: { type: "UTF8" }, metricVersion: { type: "UTF8" }, universeVersion: { type: "UTF8" },
});

export const SECURITIES_SCHEMA = new ParquetSchema({
  instrumentId: { type: "UTF8" }, ticker: { type: "UTF8" },
  tickerRoot: { type: "UTF8", optional: true }, tickerSuffix: { type: "UTF8", optional: true },
  name: { type: "UTF8", optional: true }, market: { type: "UTF8", optional: true },
  locale: { type: "UTF8", optional: true }, type: { type: "UTF8", optional: true },
  currencyName: { type: "UTF8", optional: true },
  cik: { type: "UTF8", optional: true }, compositeFigi: { type: "UTF8", optional: true },
  shareClassFigi: { type: "UTF8", optional: true }, primaryExchange: { type: "UTF8", optional: true },
  active: { type: "BOOLEAN" },
  listDate: { type: "UTF8", optional: true }, delistedUtc: { type: "UTF8", optional: true },
  lastUpdatedUtc: { type: "UTF8", optional: true },
  identitySource: { type: "UTF8" }, identityConfidence: { type: "UTF8" }, referenceStatus: { type: "UTF8" },
  source: { type: "UTF8" }, sourceVersion: { type: "UTF8" }, asOfDate: { type: "UTF8" }, ingestedAt: { type: "UTF8" },
});

export const SYMBOL_ALIASES_SCHEMA = new ParquetSchema({
  instrumentId: { type: "UTF8" }, ticker: { type: "UTF8" },
  tickerRoot: { type: "UTF8", optional: true }, tickerSuffix: { type: "UTF8", optional: true },
  primaryExchange: { type: "UTF8", optional: true },
  validFrom: { type: "UTF8" }, validTo: { type: "UTF8", optional: true },
  source: { type: "UTF8" }, sourceVersion: { type: "UTF8" }, asOfDate: { type: "UTF8" },
  confidence: { type: "UTF8" }, createdAt: { type: "UTF8" },
});

export function securitiesKey(asOfDate: string): string {
  return `reference/securities/asOf=${asOfDate}/part.parquet`;
}

export function symbolAliasesKey(asOfDate: string): string {
  return `reference/symbol_aliases/asOf=${asOfDate}/part.parquet`;
}

export async function writeSymbolAliases(s3: S3Client, bucket: string, asOfDate: string, rows: SymbolAliasRow[]): Promise<string> {
  const key = symbolAliasesKey(asOfDate);
  const flat = rows.map((r) => ({ ...r, validTo: r.validTo ?? undefined }));
  const body = await toParquet(SYMBOL_ALIASES_SCHEMA, flat as unknown as Record<string, unknown>[]);
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
  return key;
}

export async function writeSecurities(s3: S3Client, bucket: string, asOfDate: string, rows: SecurityMasterRow[]): Promise<string> {
  const key = securitiesKey(asOfDate);
  const body = await toParquet(SECURITIES_SCHEMA, rows as unknown as Record<string, unknown>[]);
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
  return key;
}

export async function toParquet(schema: ParquetSchema, rows: Record<string, unknown>[]): Promise<Buffer> {
  const chunks: Buffer[] = [];
  const stream = new Writable({
    write(chunk, encoding, callback) {
      chunks.push(chunk);
      callback();
    }
  });
  const writer = await ParquetWriter.openStream(schema, stream as never);
  for (const row of rows) await writer.appendRow(row);
  await writer.close();
  return Buffer.concat(chunks);
}

export async function writeRaw(s3: S3Client, bucket: string, rows: RawBarRow[], source: string, date: string, runId: string): Promise<string> {
  const key = rawKey(source, date, runId);
  const body = await toParquet(RAW_SCHEMA, rows as unknown as Record<string, unknown>[]);
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
  return key;
}

export async function writeMetrics(s3: S3Client, bucket: string, rows: MetricRow[], date: string, runId: string): Promise<string> {
  const key = metricsKey(date, runId);
  const flat = rows.map((r) => ({ ...r, qualityIssues: JSON.stringify(r.qualityIssues) })); // JSON string, not comma-joined
  const body = await toParquet(METRIC_SCHEMA, flat as unknown as Record<string, unknown>[]);
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
  return key;
}
