import { describe, it, expect } from "vitest";
import { SCHEMA_VERSION, RAW_SCHEMA_VERSION, METRIC_VERSION, SOURCE_VERSION } from "../src/types.js";

describe("version constants", () => {
  it("exposes the v2 schema ids and unchanged versions", () => {
    expect(SCHEMA_VERSION).toBe("metrics_v2");
    expect(RAW_SCHEMA_VERSION).toBe("dailyBars_v2");
    expect(METRIC_VERSION).toBe("1.0");
    expect(SOURCE_VERSION).toBe("1.0");
  });
});
