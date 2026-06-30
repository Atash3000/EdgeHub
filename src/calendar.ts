import holidays from "../config/calendar/holidays.json" with { type: "json" };

const DEFAULT_HOLIDAYS = new Set<string>(holidays.dates);
const DEFAULT_COVERED = new Set<string>(holidays.coveredYears);

export function isWeekend(date: string): boolean {
  // Parse as UTC noon to avoid timezone rollover.
  const day = new Date(`${date}T12:00:00Z`).getUTCDay();
  return day === 0 || day === 6;
}

export function isTradingDay(date: string, holidaySet: Set<string> = DEFAULT_HOLIDAYS): boolean {
  return !isWeekend(date) && !holidaySet.has(date);
}

export function previousTradingDay(date: string, holidaySet: Set<string> = DEFAULT_HOLIDAYS): string {
  const d = new Date(`${date}T12:00:00Z`);
  do {
    d.setUTCDate(d.getUTCDate() - 1);
  } while (!isTradingDay(d.toISOString().slice(0, 10), holidaySet));
  return d.toISOString().slice(0, 10);
}

/** Guards against an uncovered year silently treating real holidays as trading days. */
export function calendarCoversYear(date: string, coveredYears: Set<string> = DEFAULT_COVERED): boolean {
  return coveredYears.has(date.slice(0, 4));
}
