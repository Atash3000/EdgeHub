import { S3Client } from "@aws-sdk/client-s3";
import { GlueClient } from "@aws-sdk/client-glue";
import { SSMClient } from "@aws-sdk/client-ssm";
import type { RunMode } from "./types.js";
import { getProvider } from "./providers/factory.js";
import { loadConfig } from "./config.js";
import { runPipeline } from "./pipeline.js";
import { readHistoryCache, writeHistoryCache } from "./historyCache.js";
import { isTradingDay, calendarCoversYear, previousTradingDay } from "./calendar.js";
import { renderReport, sendTelegram } from "./report.js";

export function parseEvent(event: { mode?: string; tradingDay?: string }, now: Date): { mode: RunMode; tradingDay: string } {
  const mode: RunMode = event.mode === "backfill" ? "backfill" : "daily";
  const tradingDay = event.tradingDay ?? now.toISOString().slice(0, 10);
  return { mode, tradingDay };
}

export async function handler(event: { mode?: string; tradingDay?: string } = {}): Promise<{ status: string }> {
  const region = process.env.AWS_REGION ?? "us-east-1";
  const bucket = process.env.BUCKET_NAME!;
  const database = process.env.GLUE_DATABASE ?? "edgehub";
  const providerName = process.env.DATA_PROVIDER ?? "finnhub";

  const s3 = new S3Client({ region });
  const glue = new GlueClient({ region });
  const ssm = new SSMClient({ region });

  const config = await loadConfig(ssm);
  const provider = getProvider(providerName, config);
  const { mode, tradingDay } = parseEvent(event, new Date());

  const manifest = await runPipeline(mode, {
    provider, s3, glue, bucket, database, tradingDay, now: () => new Date(),
    readHistory: (t) => readHistoryCache(s3, bucket, provider.name, t),
    writeHistory: (t, bars) => writeHistoryCache(s3, bucket, provider.name, t, bars),
    isTradingDay: (d) => isTradingDay(d),
    previousTradingDay: (d) => previousTradingDay(d),
    calendarCovers: (d) => calendarCoversYear(d),
  });

  if (config.telegramBotToken && config.telegramChatId) {
    try {
      await sendTelegram(config.telegramBotToken, config.telegramChatId, renderReport(manifest));
    } catch (err) {
      console.error("telegram notification failed (ingestion already completed):", (err as Error).message);
    }
  } else {
    console.log("EdgeHub report (no telegram configured):\n" + renderReport(manifest));
  }
  return { status: manifest.status };
}
