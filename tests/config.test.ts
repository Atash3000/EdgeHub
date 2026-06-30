import { describe, it, expect } from "vitest";
import { selectConfig, DEFAULT_PARAM_NAMES } from "../src/config.js";

describe("selectConfig", () => {
  it("returns finnhubToken when api key is present (no telegram keys)", () => {
    const result = selectConfig(
      [{ Name: "/edge-hunter/finnhub/api_key", Value: "abc" }],
      DEFAULT_PARAM_NAMES
    );
    expect(result).toEqual({ finnhubToken: "abc" });
  });

  it("throws when api key is missing", () => {
    expect(() => selectConfig([], DEFAULT_PARAM_NAMES)).toThrow(/missing required SSM parameter/);
  });

  it("includes telegram creds when both are present", () => {
    const result = selectConfig(
      [
        { Name: "/edge-hunter/finnhub/api_key", Value: "abc" },
        { Name: "/edge-hub/telegram/api-key", Value: "bot" },
        { Name: "/edge-hub/telegram/chat_id", Value: "123" },
      ],
      DEFAULT_PARAM_NAMES
    );
    expect(result.telegramBotToken).toBe("bot");
    expect(result.telegramChatId).toBe("123");
  });
});
