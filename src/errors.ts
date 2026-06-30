import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import type { ErrorRecord } from "./types.js";

export function errorsKey(date: string, runId: string): string {
  const [year, month, day] = date.split("-") as [string, string, string];
  return `errors/year=${year}/month=${month}/day=${day}/runId=${runId}/errors.json`;
}

export async function writeErrors(s3: S3Client, bucket: string, date: string, runId: string, errors: ErrorRecord[]): Promise<void> {
  if (errors.length === 0) return;
  await s3.send(new PutObjectCommand({
    Bucket: bucket, Key: errorsKey(date, runId),
    Body: JSON.stringify(errors, null, 2), ContentType: "application/json",
  }));
}
