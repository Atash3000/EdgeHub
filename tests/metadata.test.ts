import { describe, it, expect } from "vitest";
import { manifestKey, currentKey, universeKey } from "../src/metadata.js";

describe("metadata keys", () => {
  it("manifest key", () => { expect(manifestKey("2026-06-29", "20260629T223000Z")).toBe("metadata/runs/year=2026/month=06/day=29/runId=20260629T223000Z/manifest.json"); });
  it("current key", () => { expect(currentKey("2026-06-29")).toBe("metadata/current/daily_metrics/year=2026/month=06/day=29.json"); });
  it("universe key", () => { expect(universeKey("2026-06-29")).toBe("metadata/universe/2026-06-29.json"); });
});
