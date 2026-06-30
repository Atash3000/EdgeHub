// tests/calendar.test.ts
import { describe, it, expect } from "vitest";
import { isWeekend, isTradingDay, previousTradingDay, calendarCoversYear } from "../src/calendar.js";

const holidays = new Set(["2026-07-03"]); // observed Independence Day

describe("calendar", () => {
  it("detects weekends", () => {
    expect(isWeekend("2026-06-27")).toBe(true);  // Saturday
    expect(isWeekend("2026-06-29")).toBe(false); // Monday
  });
  it("treats holidays and weekends as non-trading", () => {
    expect(isTradingDay("2026-07-03", holidays)).toBe(false);
    expect(isTradingDay("2026-06-27", holidays)).toBe(false);
    expect(isTradingDay("2026-06-29", holidays)).toBe(true);
  });
  it("walks back over a weekend to the prior trading day", () => {
    expect(previousTradingDay("2026-06-29", holidays)).toBe("2026-06-26"); // Mon -> Fri
  });
});

describe("calendarCoversYear", () => {
  const covered = new Set(["2026"]);
  it("is true for a covered year", () => { expect(calendarCoversYear("2026-06-29", covered)).toBe(true); });
  it("is false for an uncovered year", () => { expect(calendarCoversYear("2027-01-04", covered)).toBe(false); });
});
