import { describe, it, expect } from "vitest";
import { partitionValues } from "../src/glue.js";

describe("partitionValues", () => {
  it("splits a date", () => { expect(partitionValues("2026-06-29")).toEqual(["2026", "06", "29"]); });
});
