import { describe, it, expect } from "vitest";
import { METRIC_REGISTRY } from "../config/metrics.js";

describe("metric registry", () => {
  it("declares ma200 with window and dependency", () => {
    const ma200 = METRIC_REGISTRY.find((m) => m.name === "ma200");
    expect(ma200).toBeDefined();
    expect(ma200!.window).toBe(200);
    expect(ma200!.dependsOn).toContain("close");
  });
  it("has unique metric names", () => {
    const names = METRIC_REGISTRY.map((m) => m.name);
    expect(new Set(names).size).toBe(names.length);
  });
});
