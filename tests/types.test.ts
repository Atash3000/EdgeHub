import { describe, it, expect } from "vitest";
import { SCHEMA_VERSION, RAW_SCHEMA_VERSION, METRIC_VERSION, SOURCE_VERSION } from "../src/types.js";

describe("version constants", () => {
  it("exposes the schema ids and versions", () => {
    expect(SCHEMA_VERSION).toBe("metrics_v1");
    expect(RAW_SCHEMA_VERSION).toBe("dailyBars_v1");
    expect(METRIC_VERSION).toBe("1.0");
    expect(SOURCE_VERSION).toBe("1.0");
  });
});
