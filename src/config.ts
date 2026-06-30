import { SSMClient, GetParametersCommand } from "@aws-sdk/client-ssm";

export interface ParamRef { Name?: string; Value?: string }
export interface ParamNames { finnhubApiKey: string; telegramBotToken: string; telegramChatId: string }

export const DEFAULT_PARAM_NAMES: ParamNames = {
  finnhubApiKey: "/edge-hunter/finnhub/api_key",
  telegramBotToken: "/edge-hub/telegram/api-key",
  telegramChatId: "/edge-hub/telegram/chat-id",
};

/** Pure selector — unit-testable without AWS. Requires the finnhub api key; telegram is optional. */
export function selectConfig(params: ParamRef[], names: ParamNames): Record<string, string> {
  const byName = new Map(params.map((p) => [p.Name ?? "", p.Value ?? ""]));
  const finnhubToken = byName.get(names.finnhubApiKey);
  if (!finnhubToken) throw new Error(`missing required SSM parameter ${names.finnhubApiKey}`);
  const config: Record<string, string> = { finnhubToken };
  const bot = byName.get(names.telegramBotToken);
  const chat = byName.get(names.telegramChatId);
  if (bot) config.telegramBotToken = bot;
  if (chat) config.telegramChatId = chat;
  return config;
}

/** Fetch config from SSM Parameter Store. Reads the finnhub key + (optional) telegram bot token & chat id. */
export async function loadConfig(ssm: SSMClient, names: ParamNames = DEFAULT_PARAM_NAMES): Promise<Record<string, string>> {
  const res = await ssm.send(new GetParametersCommand({
    Names: [names.finnhubApiKey, names.telegramBotToken, names.telegramChatId],
    WithDecryption: true,
  }));
  return selectConfig(res.Parameters ?? [], names);
}
