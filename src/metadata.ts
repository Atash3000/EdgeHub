import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import type { RunManifest } from "./types.js";

function parts(date: string) {
  const [year, month, day] = date.split("-") as [string, string, string];
  return { year, month, day };
}
export function manifestKey(date: string, runId: string): string {
  const { year, month, day } = parts(date);
  return `metadata/runs/year=${year}/month=${month}/day=${day}/runId=${runId}/manifest.json`;
}
export function currentKey(date: string): string {
  const { year, month, day } = parts(date);
  return `metadata/current/daily_metrics/year=${year}/month=${month}/day=${day}.json`;
}
export function universeKey(date: string): string { return `metadata/universe/${date}.json`; }

async function putJson(s3: S3Client, bucket: string, key: string, value: unknown): Promise<void> {
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: JSON.stringify(value, null, 2), ContentType: "application/json" }));
}
export async function writeManifest(s3: S3Client, bucket: string, m: RunManifest): Promise<void> {
  await putJson(s3, bucket, manifestKey(m.tradingDay, m.runId), m);
}
export async function markCurrent(s3: S3Client, bucket: string, m: RunManifest): Promise<void> {
  await putJson(s3, bucket, currentKey(m.tradingDay), { runId: m.runId, status: m.status, rowCount: m.rowsWritten });
}
export async function snapshotUniverse(s3: S3Client, bucket: string, date: string, universe: { universeVersion: string; tickers: string[] }): Promise<void> {
  await putJson(s3, bucket, universeKey(date), universe);
}
