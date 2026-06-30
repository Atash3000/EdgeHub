import { describe, it, expect } from "vitest";
import { parseEvent } from "../src/handler.js";

describe("parseEvent", () => {
  it("defaults to daily mode and today's date", () => {
    const r = parseEvent({}, new Date("2026-06-29T22:30:00Z"));
    expect(r.mode).toBe("daily"); expect(r.tradingDay).toBe("2026-06-29");
  });
  it("honors explicit backfill mode and date", () => {
    const r = parseEvent({ mode: "backfill", tradingDay: "2026-06-01" }, new Date());
    expect(r.mode).toBe("backfill"); expect(r.tradingDay).toBe("2026-06-01");
  });
});
