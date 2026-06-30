// tests/errors.test.ts
import { describe, it, expect } from "vitest";
import { errorsKey, writeErrors } from "../src/errors.js";
import type { ErrorRecord } from "../src/types.js";

describe("errorsKey", () => {
  it("builds the partitioned errors key", () => {
    expect(errorsKey("2026-06-29", "20260629T223000Z")).toBe(
      "errors/year=2026/month=06/day=29/runId=20260629T223000Z/errors.json");
  });
});

describe("writeErrors", () => {
  it("skips the PUT when there are no errors", async () => {
    let called = false;
    const s3 = { send: async () => { called = true; return {}; } } as never;
    await writeErrors(s3, "b", "2026-06-29", "R", []);
    expect(called).toBe(false);
  });
  it("PUTs the error records as JSON", async () => {
    let body = "";
    const s3 = { send: async (c: { input: { Body: string } }) => { body = c.input.Body; return {}; } } as never;
    const errs: ErrorRecord[] = [{ runId: "R", tradingDay: "2026-06-29", source: "finnhub", universeVersion: "2026-06-29", ticker: "AAPL", reason: "provider_error", message: "HTTP 500", createdAt: "2026-06-29T22:30:00Z" }];
    await writeErrors(s3, "b", "2026-06-29", "R", errs);
    expect(JSON.parse(body)).toHaveLength(1);
    expect(JSON.parse(body)[0].reason).toBe("provider_error");
  });
});
