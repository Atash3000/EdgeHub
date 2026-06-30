import { SSMClient, GetParametersCommand } from "@aws-sdk/client-ssm";

export interface ParamRef { Name?: string; Value?: string }
export interface ParamNames { finnhubApiKey: string; telegramBotToken: string; telegramChatId: string; polygonApiKey: string; }

export const DEFAULT_PARAM_NAMES: ParamNames = {
  finnhubApiKey: "/edge-hunter/finnhub/api_key",
  telegramBotToken: "/edge-hub/telegram/api-key",
  telegramChatId: "/edge-hub/telegram/chat-id",
  polygonApiKey: "/global/polygon/api-key",
};

/** Pure selector — unit-testable without AWS. Loads whatever provider/notification keys are present.
 *  No key is required here; the ACTIVE provider's required key is validated by the factory (getProvider),
 *  so a single-provider setup (e.g. polygon-only) does not need the others' keys. */
export function selectConfig(params: ParamRef[], names: ParamNames): Record<string, string> {
  const byName = new Map(params.map((p) => [p.Name ?? "", p.Value ?? ""]));
  const config: Record<string, string> = {};
  const finnhub = byName.get(names.finnhubApiKey);
  const poly = byName.get(names.polygonApiKey);
  const bot = byName.get(names.telegramBotToken);
  const chat = byName.get(names.telegramChatId);
  if (finnhub) config.finnhubToken = finnhub;
  if (poly) config.polygonToken = poly;
  if (bot) config.telegramBotToken = bot;
  if (chat) config.telegramChatId = chat;
  return config;
}

/** Fetch config from SSM Parameter Store — reads the provider keys (finnhub, polygon) and the optional
 *  telegram bot token & chat id; whichever exist are returned. */
export async function loadConfig(ssm: SSMClient, names: ParamNames = DEFAULT_PARAM_NAMES): Promise<Record<string, string>> {
  const res = await ssm.send(new GetParametersCommand({
    Names: [names.finnhubApiKey, names.telegramBotToken, names.telegramChatId, names.polygonApiKey],
    WithDecryption: true,
  }));
  return selectConfig(res.Parameters ?? [], names);
}
