import { describe, it, expect } from "vitest";
import { parseSecret } from "../src/secrets.js";

describe("parseSecret", () => {
  it("parses the secret blob", () => {
    expect(parseSecret('{"finnhubToken":"abc"}').finnhubToken).toBe("abc");
  });
  it("throws on malformed JSON", () => { expect(() => parseSecret("not json")).toThrow(); });
});
