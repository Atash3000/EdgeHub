import type { RunManifest } from "./types.js";

type FetchFn = (url: string, init: { method: string; headers: Record<string, string>; body: string }) => Promise<{ ok: boolean; status: number }>;

export function renderReport(m: RunManifest): string {
  const lines = [
    `EdgeHub Daily Update`,
    `Date: ${m.tradingDay}`,
    `Provider: ${m.provider}`,
    `Universe: ${m.symbolsRequested} (v${m.universeVersion})`,
    `Downloaded: ${m.symbolsSucceeded}`,
    `Rows: ${m.rowsWritten}`,
    `Warnings: ${m.warnings}`,
    `Rejected: ${m.rejected}`,
    `Missing bars: ${m.missingBars}`,
    `Metric Version: ${m.metricVersion}`,
    `Runtime: ${m.runtimeSec}s`,
    `Status: ${m.status}`,
  ];
  if (m.note) lines.push(`Note: ${m.note}`);
  return lines.join("\n");
}

export async function sendTelegram(botToken: string, chatId: string, text: string, fetchFn: FetchFn = fetch as unknown as FetchFn): Promise<void> {
  const res = await fetchFn(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ chat_id: chatId, text }),
  });
  if (!res.ok) throw new Error(`telegram HTTP ${res.status}`);
}
