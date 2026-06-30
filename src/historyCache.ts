import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import type { VendorBar } from "./types.js";

export function historyCacheKey(source: string, ticker: string): string {
  return `history/${source}/ticker=${ticker}/current.json`;
}

export async function readHistoryCache(s3: S3Client, bucket: string, source: string, ticker: string): Promise<VendorBar[]> {
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: historyCacheKey(source, ticker) }));
    const text = await res.Body!.transformToString();
    const parsed = JSON.parse(text);
    return Array.isArray(parsed) ? (parsed as VendorBar[]) : [];
  } catch {
    return []; // miss -> pipeline backfills
  }
}

export async function writeHistoryCache(s3: S3Client, bucket: string, source: string, ticker: string, bars: VendorBar[], maxBars = 400): Promise<void> {
  const trimmed = bars.slice(-maxBars);
  await s3.send(new PutObjectCommand({
    Bucket: bucket, Key: historyCacheKey(source, ticker),
    Body: JSON.stringify(trimmed), ContentType: "application/json",
  }));
}
