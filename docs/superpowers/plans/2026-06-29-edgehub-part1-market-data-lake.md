# EdgeHub Part 1 — Market Data Lake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a serverless daily pipeline that downloads US equity OHLCV, computes versioned market metrics, stores them as partitioned Parquet in S3, catalogs them in Glue for Athena, and reports health via Telegram — deployed to AWS exclusively from GitHub.

**Architecture:** A single, internally-modular TypeScript Lambda (`edgehub-daily-collector`) runs the pipeline (calendar check → universe → download → validate → store raw → compute metrics → store metrics → Glue partition → manifest → Telegram). EventBridge Scheduler triggers it daily at 6:30 PM America/New_York. Infrastructure is AWS SAM, deployed via GitHub Actions using OIDC. The vendor sits behind a `MarketDataProvider` interface returning `VendorBar`s; the pipeline enriches them with provenance into `RawBarRow`s.

**Tech Stack:** TypeScript, Node.js 24 (latest LTS), AWS SAM, AWS SDK v3 (S3/Glue/Secrets Manager), `@dsnp/parquetjs`, Vitest, GitHub Actions.

## Global Constraints

- Runtime: **Node.js 24** (latest stable LTS; Lambda `nodejs24.x`), TypeScript bundled by SAM esbuild.
- Region: **us-east-1**. Stack: **edgehub**. Bucket: **edgehub-data**.
- Derived data is **metrics** (never "features"): dir `metrics/`, table `daily_metrics`, field `metricVersion`, env `METRIC_VERSION`.
- Version constants: `SCHEMA_VERSION = "metrics_v1"`, `RAW_SCHEMA_VERSION = "dailyBars_v1"`, `METRIC_VERSION = "1.0"`, `SOURCE_VERSION = "1.0"`.
- Active vendor via env `DATA_PROVIDER` (default `finnhub`). `source`/`sourceVersion` come from `provider.name`/`provider.version` — never a literal.
- **Provider returns `VendorBar` (vendor data only). The pipeline enriches to `RawBarRow` with full provenance** (`runId, ingestedAt, source, sourceVersion, schemaVersion, metricVersion, universeVersion`) before writing. Providers never know `runId`/`universeVersion`.
- Every raw row, metric row, report, and error file carries full provenance.
- **Never fabricate adjusted prices.** `adjustedClose: number | null` + `isAdjusted: boolean`. Finnhub free candles → `adjustedClose=null, isAdjusted=false`.
- **Never substitute a stale bar for a missing day.** If the exact `tradingDay` bar is absent, record a per-ticker `missing_bar_for_date` failure and write no today row.
- **Market calendar gates daily runs.** Non-trading days are skipped (manifest `SKIPPED`), not processed.
- Immutability: every write under `runId=<UTC timestamp>`. Authoritative run recorded in `metadata/current/`, advanced only on `SUCCESS` or accepted `PARTIAL` (success rate ≥ floor) — never on `FAILURE`/`SKIPPED`.
- **History cache**: per-ticker trailing bars at `history/{source}/ticker={t}/current.json`, rebuildable from immutable raw (which remains the source of truth).
- Quality: `OK | WARN | REJECTED`; WARN stored, REJECTED excluded + logged to `errors/`. `qualityIssues` stored as a **JSON string**. Nothing silently dropped.
- Deploys only from GitHub Actions on push to `main` via OIDC. No laptop deploys.
- `labels/` and `corporate_actions/` are reserved (unused) for Part 2. No trading/strategy/AI logic in Part 1.
- TDD: failing test → watch it fail → minimal impl → watch it pass → commit.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `package.json`, `tsconfig.json`, `vitest.config.ts` | Project + test setup |
| `src/types.ts` | Shared types (`VendorBar`, `RawBarRow`, `MetricRow`, `RunManifest`, `Provenance`) + constants |
| `config/metrics.ts` | Metric registry |
| `schemas/dailyBars_v1.json`, `schemas/metrics_v1.json` | Schema registry |
| `config/calendar/holidays.json`, `src/calendar.ts` | Market calendar |
| `config/universe/*.json`, `src/universe.ts` | Versioned universe |
| `src/providers/provider.ts` / `factory.ts` / `finnhub.ts` / `fake.ts` | Vendor abstraction |
| `src/validate.ts` | Quality grading |
| `src/metrics.ts` | Metric computation (quality injected) |
| `src/secrets.ts` | Secrets Manager loader |
| `src/storage.ts` | Raw/metric Parquet writers + path builders |
| `src/history.ts` | Merge logic + sufficiency check |
| `src/historyCache.ts` | Per-ticker trailing-bar cache I/O |
| `src/glue.ts` | Glue partition registration |
| `src/metadata.ts` | Manifest + current pointer + universe snapshot |
| `src/errors.ts` | Structured error-record writer (`errors/`) |
| `src/report.ts` | Telegram report |
| `src/pipeline.ts` | Orchestration, runId, manifest, current policy |
| `src/handler.ts` | Lambda entrypoint |
| `template.yaml`, `samconfig.toml` | SAM infra + deploy config |
| `.github/workflows/ci.yml`, `deploy.yml` | CI + CD |
| `docs/DATA_DICTIONARY.md`, `docs/BOOTSTRAP.md` | Docs |

---

## Task 1: Project scaffolding

**Files:** Create `package.json`, `tsconfig.json`, `vitest.config.ts`, `.gitignore`

**Interfaces:** Produces `npm test`, `npm run typecheck`, `npm run build` used by all later tasks + CI.

- [ ] **Step 1: Create `package.json`**

```json
{
  "name": "edgehub",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "engines": { "node": ">=22" },
  "scripts": {
    "typecheck": "tsc --noEmit",
    "test": "vitest run",
    "test:watch": "vitest",
    "build": "tsc -p tsconfig.json",
    "invoke:local": "sam local invoke EdgeHubCollector -e events/daily.json"
  },
  "dependencies": {
    "@aws-sdk/client-s3": "^3.600.0",
    "@aws-sdk/client-glue": "^3.600.0",
    "@aws-sdk/client-secrets-manager": "^3.600.0",
    "@dsnp/parquetjs": "^1.8.0"
  },
  "devDependencies": {
    "@types/node": "^20.14.0",
    "typescript": "^5.5.0",
    "vitest": "^2.0.0",
    "esbuild": "^0.23.0"
  }
}
```

- [ ] **Step 2: Create `tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "Bundler",
    "lib": ["ES2022"],
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "resolveJsonModule": true,
    "outDir": "dist",
    "rootDir": "."
  },
  "include": ["src/**/*", "config/**/*", "tests/**/*"]
}
```

- [ ] **Step 3: Create `vitest.config.ts`**

```ts
import { defineConfig } from "vitest/config";
export default defineConfig({ test: { include: ["tests/**/*.test.ts"], environment: "node" } });
```

- [ ] **Step 4: Create `.gitignore`**

```
node_modules/
dist/
.aws-sam/
*.log
```

- [ ] **Step 5: Install and verify** — Run: `npm install && npm run typecheck` — Expected: clean install; `tsc` exits 0.

- [ ] **Step 6: Commit**

```bash
git add package.json tsconfig.json vitest.config.ts .gitignore package-lock.json
git commit -m "chore: scaffold TypeScript + Vitest project"
```

---

## Task 2: Shared types and version constants

**Files:** Create `src/types.ts`; Test `tests/types.test.ts`

**Interfaces:**
- Produces `VendorBar` (provider output), `RawBarRow` (stored raw = VendorBar + provenance), `Provenance`, `MetricRow`, `RunManifest`, `QualityStatus`, `RunMode`, and constants. Every later task imports from here.

- [ ] **Step 1: Write the failing test**

```ts
// tests/types.test.ts
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
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/types.test.ts` — Expected: FAIL (module not found).

- [ ] **Step 3: Create `src/types.ts`**

```ts
export const SCHEMA_VERSION = "metrics_v1";
export const RAW_SCHEMA_VERSION = "dailyBars_v1";
export const METRIC_VERSION = "1.0";
export const SOURCE_VERSION = "1.0";

export type RunMode = "daily" | "backfill";
export type QualityStatus = "OK" | "WARN" | "REJECTED";

/** What a MarketDataProvider returns: vendor data only, no pipeline provenance. */
export interface VendorBar {
  ticker: string;
  date: string; // YYYY-MM-DD
  open: number;
  high: number;
  low: number;
  close: number;
  adjustedClose: number | null; // null when the vendor feed is unadjusted
  isAdjusted: boolean;
  volume: number;
  source: string;
  sourceVersion: string;
  ingestedAt: string; // ISO 8601, set by the provider at fetch time
}

export interface Provenance {
  runId: string;
  ingestedAt: string;
  source: string;
  sourceVersion: string;
  schemaVersion: string;
  metricVersion: string;
  universeVersion: string;
}

/** A per-ticker fetch failure — collected, never thrown, so one bad ticker can't kill the run. */
export interface ProviderFailure {
  ticker: string;
  date: string;
  reason: string;   // e.g. "provider_error" | "missing_bar_for_date"
  message?: string;
}

/** Providers return found bars AND the failures, so the pipeline can record both. */
export interface ProviderResult {
  bars: VendorBar[];
  failures: ProviderFailure[];
}

/** A single recorded error/warning written to errors/. */
export interface ErrorRecord {
  runId: string;
  tradingDay: string;
  source: string;
  universeVersion: string;
  ticker: string;
  reason: string;   // provider_error | missing_bar_for_date | rejected | warn | pipeline_error | calendar_year_missing
  message?: string;
  createdAt: string;
}

/** Stored raw row = vendor bar enriched with full provenance by the pipeline. */
export interface RawBarRow extends VendorBar {
  runId: string;
  schemaVersion: string; // RAW_SCHEMA_VERSION
  metricVersion: string;
  universeVersion: string;
}

export interface MetricRow extends Provenance {
  ticker: string;
  date: string;
  close: number;
  dollarVolume: number;
  ma20: number | null;
  ma50: number | null;
  ma150: number | null;
  ma200: number | null;
  avgVolume20: number | null;
  avgVolume50: number | null;
  atr14: number | null;
  high52w: number | null;
  low52w: number | null;
  distanceTo52wHighPct: number | null;
  distanceFrom52wLowPct: number | null;
  return21d: number | null;
  return63d: number | null;
  return126d: number | null;
  return252d: number | null;
  above20ma: boolean | null;
  above50ma: boolean | null;
  above150ma: boolean | null;
  above200ma: boolean | null;
  ma150Above200: boolean | null;
  ma200Rising: boolean | null;
  qualityStatus: QualityStatus;
  qualityIssues: string[];
}

export interface RunManifest {
  runId: string;
  mode: RunMode;
  tradingDay: string;
  provider: string;
  universeVersion: string;
  symbolsRequested: number;
  symbolsSucceeded: number;
  rowsWritten: number;
  warnings: number;
  rejected: number;
  missingBars: number;
  runtimeSec: number;
  metricVersion: string;
  schemaVersion: string;
  status: "SUCCESS" | "PARTIAL" | "FAILURE" | "SKIPPED";
  note?: string; // reason for SKIPPED/FAILURE early-exit (e.g. "calendar_year_missing")
}
```

- [ ] **Step 4: Run test to verify it passes** — Run: `npx vitest run tests/types.test.ts` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/types.ts tests/types.test.ts
git commit -m "feat: add shared types (VendorBar/RawBarRow/MetricRow) and constants"
```

---

## Task 3: Metric registry + schema registry

**Files:** Create `config/metrics.ts`, `schemas/dailyBars_v1.json`, `schemas/metrics_v1.json`; Test `tests/metrics-registry.test.ts`

**Interfaces:** Produces `METRIC_REGISTRY: MetricDef[]` (`{ name, description, dependsOn, window, version }`). Consumed by `metrics.ts` and the data dictionary.

- [ ] **Step 1: Write the failing test**

```ts
// tests/metrics-registry.test.ts
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
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/metrics-registry.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `config/metrics.ts`**

```ts
export interface MetricDef {
  name: string;
  description: string;
  dependsOn: string[];
  window: number | null;
  version: string;
}

export const METRIC_REGISTRY: MetricDef[] = [
  { name: "dollarVolume", description: "close * volume", dependsOn: ["close", "volume"], window: null, version: "1.0" },
  { name: "ma20", description: "20-day SMA of close", dependsOn: ["close"], window: 20, version: "1.0" },
  { name: "ma50", description: "50-day SMA of close", dependsOn: ["close"], window: 50, version: "1.0" },
  { name: "ma150", description: "150-day SMA of close", dependsOn: ["close"], window: 150, version: "1.0" },
  { name: "ma200", description: "200-day SMA of close", dependsOn: ["close"], window: 200, version: "1.0" },
  { name: "avgVolume20", description: "20-day average volume", dependsOn: ["volume"], window: 20, version: "1.0" },
  { name: "avgVolume50", description: "50-day average volume", dependsOn: ["volume"], window: 50, version: "1.0" },
  { name: "atr14", description: "14-day ATR = SMA of last 14 true ranges (requires >= 15 bars)", dependsOn: ["high", "low", "close"], window: 14, version: "1.0" },
  { name: "high52w", description: "highest high over trailing 252 sessions", dependsOn: ["high"], window: 252, version: "1.0" },
  { name: "low52w", description: "lowest low over trailing 252 sessions", dependsOn: ["low"], window: 252, version: "1.0" },
  { name: "distanceTo52wHighPct", description: "percent distance of close to 52w high (<=0)", dependsOn: ["close", "high"], window: 252, version: "1.0" },
  { name: "distanceFrom52wLowPct", description: "percent distance of close above 52w low (>=0)", dependsOn: ["close", "low"], window: 252, version: "1.0" },
  { name: "return21d", description: "close return over trailing 21 sessions", dependsOn: ["close"], window: 21, version: "1.0" },
  { name: "return63d", description: "close return over trailing 63 sessions", dependsOn: ["close"], window: 63, version: "1.0" },
  { name: "return126d", description: "close return over trailing 126 sessions", dependsOn: ["close"], window: 126, version: "1.0" },
  { name: "return252d", description: "close return over trailing 252 sessions", dependsOn: ["close"], window: 252, version: "1.0" },
  { name: "above20ma", description: "close > ma20", dependsOn: ["close"], window: 20, version: "1.0" },
  { name: "above50ma", description: "close > ma50", dependsOn: ["close"], window: 50, version: "1.0" },
  { name: "above150ma", description: "close > ma150", dependsOn: ["close"], window: 150, version: "1.0" },
  { name: "above200ma", description: "close > ma200", dependsOn: ["close"], window: 200, version: "1.0" },
  { name: "ma150Above200", description: "ma150 > ma200", dependsOn: ["close"], window: 200, version: "1.0" },
  { name: "ma200Rising", description: "today ma200 > prior session ma200 (requires 201 bars)", dependsOn: ["close"], window: 201, version: "1.0" },
];
```

- [ ] **Step 4: Create `schemas/dailyBars_v1.json`**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "dailyBars_v1",
  "type": "object",
  "required": ["ticker", "date", "open", "high", "low", "close", "isAdjusted", "volume", "source", "sourceVersion", "ingestedAt", "runId", "schemaVersion", "metricVersion", "universeVersion"],
  "properties": {
    "ticker": { "type": "string" },
    "date": { "type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$" },
    "open": { "type": "number" },
    "high": { "type": "number" },
    "low": { "type": "number" },
    "close": { "type": "number" },
    "adjustedClose": { "type": ["number", "null"] },
    "isAdjusted": { "type": "boolean" },
    "volume": { "type": "number" },
    "source": { "type": "string" },
    "sourceVersion": { "type": "string" },
    "ingestedAt": { "type": "string" },
    "runId": { "type": "string" },
    "schemaVersion": { "type": "string" },
    "metricVersion": { "type": "string" },
    "universeVersion": { "type": "string" }
  }
}
```

- [ ] **Step 5: Create `schemas/metrics_v1.json`**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "metrics_v1",
  "type": "object",
  "required": ["ticker", "date", "close", "qualityStatus", "runId", "schemaVersion", "metricVersion", "universeVersion"],
  "properties": {
    "ticker": { "type": "string" },
    "date": { "type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$" },
    "close": { "type": "number" },
    "qualityStatus": { "type": "string", "enum": ["OK", "WARN", "REJECTED"] },
    "qualityIssues": { "type": "string", "description": "JSON-encoded string array" },
    "runId": { "type": "string" },
    "schemaVersion": { "type": "string" },
    "metricVersion": { "type": "string" },
    "universeVersion": { "type": "string" }
  }
}
```

- [ ] **Step 6: Run test to verify it passes** — Run: `npx vitest run tests/metrics-registry.test.ts` — Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add config/metrics.ts schemas/ tests/metrics-registry.test.ts
git commit -m "feat: add metric registry and schema registry"
```

---

## Task 4: Market calendar

**Files:** Create `config/calendar/holidays.json`, `src/calendar.ts`; Test `tests/calendar.test.ts`

**Interfaces:**
- Produces `isWeekend(date): boolean`, `isTradingDay(date: string, holidays?: Set<string>): boolean`, `previousTradingDay(date: string, holidays?: Set<string>): string`. Holidays are a committed static list (a real exchange calendar is Part 2).

- [ ] **Step 1: Write the failing test**

```ts
// tests/calendar.test.ts
import { describe, it, expect } from "vitest";
import { isWeekend, isTradingDay, previousTradingDay } from "../src/calendar.js";

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
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/calendar.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `config/calendar/holidays.json`**

```json
{
  "coveredYears": ["2026"],
  "dates": [
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-11-26", "2026-12-25"
  ]
}
```

> When the year rolls over, add the next year's holidays + extend `coveredYears` in a PR. Until then the
> pipeline fails *safely* on an uncovered year (Step 5b) rather than guessing.

- [ ] **Step 4: Create `src/calendar.ts`**

```ts
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
```

- [ ] **Step 5: Run tests to verify they pass** — Run: `npx vitest run tests/calendar.test.ts` — Expected: PASS (all three).

- [ ] **Step 5b: Add the coverage test**

Append to `tests/calendar.test.ts`:

```ts
import { calendarCoversYear } from "../src/calendar.js";

describe("calendarCoversYear", () => {
  const covered = new Set(["2026"]);
  it("is true for a covered year", () => { expect(calendarCoversYear("2026-06-29", covered)).toBe(true); });
  it("is false for an uncovered year", () => { expect(calendarCoversYear("2027-01-04", covered)).toBe(false); });
});
```

Run: `npx vitest run tests/calendar.test.ts` — Expected: PASS (all five).

- [ ] **Step 6: Commit**

```bash
git add src/calendar.ts config/calendar/ tests/calendar.test.ts
git commit -m "feat: add market calendar (trading-day awareness)"
```

---

## Task 5: Universe loader (versioned)

**Files:** Create `src/universe.ts`, `config/universe/sp500.json`, `config/universe/nasdaq100.json`, `config/universe/watchlist.json`; Test `tests/universe.test.ts`

**Interfaces:** Produces `mergeUniverse(files): { tickers: string[]; universeVersion: string }` and `loadUniverse()`. Tickers upper-cased, merged, deduped; version shared across files.

- [ ] **Step 1: Write the failing test**

```ts
// tests/universe.test.ts
import { describe, it, expect } from "vitest";
import { mergeUniverse } from "../src/universe.js";

describe("mergeUniverse", () => {
  it("merges, upper-cases, and dedupes", () => {
    const r = mergeUniverse([
      { version: "2026-06-29", tickers: ["aapl", "MSFT"] },
      { version: "2026-06-29", tickers: ["msft", "GOOG"] },
    ]);
    expect(r.tickers).toEqual(["AAPL", "MSFT", "GOOG"]);
    expect(r.universeVersion).toBe("2026-06-29");
  });
  it("throws on version mismatch", () => {
    expect(() => mergeUniverse([
      { version: "2026-06-29", tickers: ["AAPL"] },
      { version: "2026-06-30", tickers: ["MSFT"] },
    ])).toThrow(/version mismatch/i);
  });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/universe.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create the universe JSON files**

`config/universe/sp500.json`:
```json
{ "version": "2026-06-29", "tickers": ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA"] }
```
`config/universe/nasdaq100.json`:
```json
{ "version": "2026-06-29", "tickers": ["AAPL", "MSFT", "NVDA", "TSLA", "AVGO"] }
```
`config/universe/watchlist.json`:
```json
{ "version": "2026-06-29", "tickers": ["SPY", "QQQ"] }
```

> Seed lists. Expanding to full constituents is a later data-entry PR; the loader is unchanged.

- [ ] **Step 4: Create `src/universe.ts`**

```ts
import sp500 from "../config/universe/sp500.json" with { type: "json" };
import nasdaq100 from "../config/universe/nasdaq100.json" with { type: "json" };
import watchlist from "../config/universe/watchlist.json" with { type: "json" };

export interface UniverseFile { version: string; tickers: string[]; }
export interface ResolvedUniverse { tickers: string[]; universeVersion: string; }

export function mergeUniverse(files: UniverseFile[]): ResolvedUniverse {
  const versions = new Set(files.map((f) => f.version));
  if (versions.size > 1) throw new Error(`Universe version mismatch: ${[...versions].join(", ")}`);
  const seen = new Set<string>();
  const tickers: string[] = [];
  for (const file of files) {
    for (const raw of file.tickers) {
      const t = raw.toUpperCase();
      if (!seen.has(t)) { seen.add(t); tickers.push(t); }
    }
  }
  return { tickers, universeVersion: files[0]!.version };
}

export function loadUniverse(): ResolvedUniverse {
  return mergeUniverse([sp500, nasdaq100, watchlist]);
}
```

- [ ] **Step 5: Run tests to verify they pass** — Run: `npx vitest run tests/universe.test.ts` — Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/universe.ts config/universe/ tests/universe.test.ts
git commit -m "feat: add versioned universe loader"
```

---

## Task 6: Provider interface, factory, and fake provider

**Files:** Create `src/providers/provider.ts`, `src/providers/factory.ts`, `src/providers/fake.ts`; Test `tests/provider-factory.test.ts`

**Interfaces:**
- `interface MarketDataProvider { readonly name: string; readonly version: string; getLatestBars(date, tickers): Promise<ProviderResult>; getHistory(ticker, lookbackDays): Promise<ProviderResult> }`. `getLatestBars` returns ONLY bars whose date equals the requested `date` (no stale fallback) and records a `ProviderFailure` for any ticker with no exact-date bar or a fetch error.
- `getProvider(name, secrets): MarketDataProvider`.
- `class FakeProvider` constructed with `Map<string, VendorBar[]>`.

- [ ] **Step 1: Write the failing test**

```ts
// tests/provider-factory.test.ts
import { describe, it, expect } from "vitest";
import { getProvider } from "../src/providers/factory.js";
import { FakeProvider } from "../src/providers/fake.js";
import type { VendorBar } from "../src/types.js";

const bar = (date: string): VendorBar => ({ ticker: "AAPL", date, open: 1, high: 2, low: 1, close: 2, adjustedClose: null, isAdjusted: false, volume: 10, source: "fake", sourceVersion: "1.0", ingestedAt: "x" });

describe("getProvider", () => {
  it("returns a fake provider", () => { expect(getProvider("fake", {}).name).toBe("fake"); });
  it("throws on unknown provider", () => { expect(() => getProvider("nope", {})).toThrow(/unknown data provider/i); });
});

describe("FakeProvider.getLatestBars", () => {
  it("returns only the bar matching the requested date", async () => {
    const p = new FakeProvider(new Map([["AAPL", [bar("2026-06-26"), bar("2026-06-29")]]]));
    const out = await p.getLatestBars("2026-06-29", ["AAPL"]);
    expect(out.bars).toHaveLength(1);
    expect(out.bars[0]!.date).toBe("2026-06-29");
    expect(out.failures).toEqual([]);
  });
  it("records a failure when the date is missing (no stale fallback)", async () => {
    const p = new FakeProvider(new Map([["AAPL", [bar("2026-06-26")]]]));
    const out = await p.getLatestBars("2026-06-29", ["AAPL"]);
    expect(out.bars).toEqual([]);
    expect(out.failures[0]).toMatchObject({ ticker: "AAPL", reason: "missing_bar_for_date" });
  });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/provider-factory.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/providers/provider.ts`**

```ts
import type { ProviderResult } from "../types.js";

export interface MarketDataProvider {
  readonly name: string;
  readonly version: string;
  /** Returns bars whose date === the requested date (never a stale bar) plus a failure per missing/errored ticker. */
  getLatestBars(date: string, tickers: string[]): Promise<ProviderResult>;
  getHistory(ticker: string, lookbackDays: number): Promise<ProviderResult>;
}
```

- [ ] **Step 4: Create `src/providers/fake.ts`**

```ts
import type { MarketDataProvider } from "./provider.js";
import type { VendorBar, ProviderResult, ProviderFailure } from "../types.js";

export class FakeProvider implements MarketDataProvider {
  readonly name = "fake";
  readonly version = "1.0";
  constructor(private readonly history: Map<string, VendorBar[]>) {}

  async getLatestBars(date: string, tickers: string[]): Promise<ProviderResult> {
    const bars: VendorBar[] = [];
    const failures: ProviderFailure[] = [];
    for (const t of tickers) {
      const match = (this.history.get(t) ?? []).find((b) => b.date === date);
      if (match) bars.push(match);
      else failures.push({ ticker: t, date, reason: "missing_bar_for_date" });
    }
    return { bars, failures };
  }

  async getHistory(ticker: string, lookbackDays: number): Promise<ProviderResult> {
    return { bars: (this.history.get(ticker) ?? []).slice(-lookbackDays), failures: [] };
  }
}
```

- [ ] **Step 5: Create `src/providers/factory.ts`**

```ts
import type { MarketDataProvider } from "./provider.js";
import { FakeProvider } from "./fake.js";
import { FinnhubProvider } from "./finnhub.js";

export function getProvider(name: string, secrets: Record<string, string>): MarketDataProvider {
  switch (name) {
    case "finnhub": return new FinnhubProvider(secrets.finnhubToken ?? "");
    case "fake": return new FakeProvider(new Map());
    default: throw new Error(`Unknown data provider: ${name}`);
  }
}
```

> `finnhub.ts` arrives in Task 7. Create this temporary stub now so the factory compiles, then replace it:
> `src/providers/finnhub.ts`:
> ```ts
> import type { MarketDataProvider } from "./provider.js";
> import type { ProviderResult } from "../types.js";
> export class FinnhubProvider implements MarketDataProvider {
>   readonly name = "finnhub";
>   readonly version = "1.0";
>   constructor(_token: string) {}
>   async getLatestBars(_d: string, _t: string[]): Promise<ProviderResult> { throw new Error("not implemented"); }
>   async getHistory(_t: string, _n: number): Promise<ProviderResult> { throw new Error("not implemented"); }
> }
> ```

- [ ] **Step 6: Run tests to verify they pass** — Run: `npx vitest run tests/provider-factory.test.ts` — Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/providers/ tests/provider-factory.test.ts
git commit -m "feat: add provider interface, factory, and fake provider"
```

---

## Task 7: Finnhub provider (unadjusted, no stale fallback, rate-limited)

**Files:** Modify `src/providers/finnhub.ts` (replace stub); Test `tests/finnhub.test.ts`

**Interfaces:**
- `mapCandle(symbol, json, ingestedAt): VendorBar[]` — sets `adjustedClose=null, isAdjusted=false` (Finnhub free candles are unadjusted).
- `class FinnhubProvider` with injectable `fetchFn` + rate limiter (≤ `maxPerMinute`, default 55). `getLatestBars` returns only the exact-date bar (no fallback).

- [ ] **Step 1: Write the failing test**

```ts
// tests/finnhub.test.ts
import { describe, it, expect } from "vitest";
import { mapCandle } from "../src/providers/finnhub.js";

describe("mapCandle", () => {
  it("maps a finnhub candle to VendorBars marked unadjusted", () => {
    const json = { s: "ok", t: [1750000000], o: [10], h: [12], l: [9], c: [11], v: [1000] };
    const bars = mapCandle("AAPL", json, "2026-06-29T22:30:00Z");
    expect(bars).toHaveLength(1);
    expect(bars[0]).toMatchObject({
      ticker: "AAPL", close: 11, high: 12, low: 9, open: 10, volume: 1000,
      adjustedClose: null, isAdjusted: false, source: "finnhub", sourceVersion: "1.0",
    });
    expect(bars[0]!.date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });
  it("returns [] when status is not ok", () => {
    expect(mapCandle("AAPL", { s: "no_data" }, "x")).toEqual([]);
  });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/finnhub.test.ts` — Expected: FAIL.

- [ ] **Step 3: Replace `src/providers/finnhub.ts`**

```ts
import type { MarketDataProvider } from "./provider.js";
import type { VendorBar, ProviderResult, ProviderFailure } from "../types.js";
import { SOURCE_VERSION } from "../types.js";

interface FinnhubCandle { s: string; t?: number[]; o?: number[]; h?: number[]; l?: number[]; c?: number[]; v?: number[]; }
type FetchFn = (url: string) => Promise<{ ok: boolean; status: number; json: () => Promise<unknown> }>;

export function mapCandle(symbol: string, raw: unknown, ingestedAt: string): VendorBar[] {
  const json = raw as FinnhubCandle;
  if (!json || json.s !== "ok" || !json.t) return [];
  const bars: VendorBar[] = [];
  for (let i = 0; i < json.t.length; i++) {
    bars.push({
      ticker: symbol,
      date: new Date(json.t[i]! * 1000).toISOString().slice(0, 10),
      open: json.o![i]!, high: json.h![i]!, low: json.l![i]!, close: json.c![i]!,
      adjustedClose: null,   // Finnhub free candles are unadjusted; never fabricate this
      isAdjusted: false,
      volume: json.v![i]!,
      source: "finnhub", sourceVersion: SOURCE_VERSION, ingestedAt,
    });
  }
  return bars;
}

class RateLimiter {
  private last = 0;
  constructor(private readonly minIntervalMs: number) {}
  async wait(sleep: (ms: number) => Promise<void>, now: () => number): Promise<void> {
    const elapsed = now() - this.last;
    if (elapsed < this.minIntervalMs) await sleep(this.minIntervalMs - elapsed);
    this.last = now();
  }
}
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export class FinnhubProvider implements MarketDataProvider {
  readonly name = "finnhub";
  readonly version = SOURCE_VERSION;
  private readonly limiter: RateLimiter;

  constructor(
    private readonly token: string,
    private readonly fetchFn: FetchFn = fetch as unknown as FetchFn,
    maxPerMinute = 55,
  ) { this.limiter = new RateLimiter(Math.ceil(60000 / maxPerMinute)); }

  private async candle(symbol: string, fromSec: number, toSec: number): Promise<VendorBar[]> {
    await this.limiter.wait(sleep, () => Date.now());
    const url = `https://finnhub.io/api/v1/stock/candle?symbol=${symbol}&resolution=D&from=${fromSec}&to=${toSec}&token=${this.token}`;
    const res = await this.fetchFn(url);
    if (!res.ok) throw new Error(`finnhub ${symbol} HTTP ${res.status}`);
    return mapCandle(symbol, await res.json(), new Date().toISOString());
  }

  async getLatestBars(date: string, tickers: string[]): Promise<ProviderResult> {
    const to = Math.floor(new Date(`${date}T23:59:59Z`).getTime() / 1000);
    const from = to - 5 * 86400;
    const bars: VendorBar[] = [];
    const failures: ProviderFailure[] = [];
    for (const t of tickers) {
      try {
        const candles = await this.candle(t, from, to);
        const match = candles.find((b) => b.date === date); // exact date only — no stale fallback
        if (match) bars.push(match);
        else failures.push({ ticker: t, date, reason: "missing_bar_for_date" });
      } catch (err) {
        failures.push({ ticker: t, date, reason: "provider_error", message: (err as Error).message });
      }
    }
    return { bars, failures }; // one bad ticker never aborts the batch
  }

  async getHistory(ticker: string, lookbackDays: number): Promise<ProviderResult> {
    const to = Math.floor(Date.now() / 1000);
    const from = to - Math.ceil(lookbackDays * 1.5) * 86400; // pad for weekends/holidays
    try {
      return { bars: await this.candle(ticker, from, to), failures: [] };
    } catch (err) {
      return { bars: [], failures: [{ ticker, date: "", reason: "provider_error", message: (err as Error).message }] };
    }
  }
}
```

- [ ] **Step 4: Run tests to verify they pass** — Run: `npx vitest run tests/finnhub.test.ts tests/provider-factory.test.ts` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/providers/finnhub.ts tests/finnhub.test.ts
git commit -m "feat: implement Finnhub provider (unadjusted, exact-date, rate-limited)"
```

---

## Task 8: Quality validation

**Files:** Create `src/validate.ts`; Test `tests/validate.test.ts`

**Interfaces:** `gradeBar(bar: VendorBar, seenKeys: Set<string>): { status: QualityStatus; issues: string[] }`. `seenKeys` accumulates `"ticker|date"` across the batch for duplicate detection. **Called once per bar at the pipeline batch stage**, not inside metric computation.

- [ ] **Step 1: Write the failing test**

```ts
// tests/validate.test.ts
import { describe, it, expect } from "vitest";
import { gradeBar } from "../src/validate.js";
import type { VendorBar } from "../src/types.js";

const base: VendorBar = { ticker: "AAPL", date: "2026-06-29", open: 10, high: 12, low: 9, close: 11, adjustedClose: null, isAdjusted: false, volume: 1000, source: "fake", sourceVersion: "1.0", ingestedAt: "x" };

describe("gradeBar", () => {
  it("grades a clean bar OK", () => { expect(gradeBar(base, new Set()).status).toBe("OK"); });
  it("rejects a negative price", () => {
    const r = gradeBar({ ...base, close: -1 }, new Set());
    expect(r.status).toBe("REJECTED"); expect(r.issues).toContain("negative_price");
  });
  it("rejects a duplicate ticker/date", () => {
    const seen = new Set<string>(); gradeBar(base, seen);
    const r = gradeBar(base, seen);
    expect(r.status).toBe("REJECTED"); expect(r.issues).toContain("duplicate");
  });
  it("warns on zero volume", () => {
    const r = gradeBar({ ...base, volume: 0 }, new Set());
    expect(r.status).toBe("WARN"); expect(r.issues).toContain("zero_volume");
  });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/validate.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/validate.ts`**

```ts
import type { VendorBar, QualityStatus } from "./types.js";

export function gradeBar(bar: VendorBar, seenKeys: Set<string>): { status: QualityStatus; issues: string[] } {
  const issues: string[] = [];
  let status: QualityStatus = "OK";
  const reject = (c: string) => { issues.push(c); status = "REJECTED"; };
  const warn = (c: string) => { issues.push(c); if (status === "OK") status = "WARN"; };

  if (bar.close === null || bar.close === undefined || Number.isNaN(bar.close)) reject("missing_close");
  if (bar.volume === null || bar.volume === undefined || Number.isNaN(bar.volume)) reject("missing_volume");
  if ([bar.open, bar.high, bar.low, bar.close].some((p) => p < 0)) reject("negative_price");
  if (bar.volume < 0) reject("negative_volume");

  const key = `${bar.ticker}|${bar.date}`;
  if (seenKeys.has(key)) reject("duplicate");
  else seenKeys.add(key);

  if (status !== "REJECTED" && bar.volume === 0) warn("zero_volume");
  return { status, issues };
}
```

- [ ] **Step 4: Run tests to verify they pass** — Run: `npx vitest run tests/validate.test.ts` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/validate.ts tests/validate.test.ts
git commit -m "feat: add OK/WARN/REJECTED quality grading"
```

---

## Task 9: Metric computation (quality injected)

**Files:** Create `src/metrics.ts`; Test `tests/metrics.test.ts`

**Interfaces:**
- `computeMetrics(bars: VendorBar[], prov: Provenance, quality: { status: QualityStatus; issues: string[] }): MetricRow`. `bars` is ONE ticker's trailing history sorted ascending; last element is the current session. Quality is computed by the pipeline (Task 8) and **injected** — `computeMetrics` does not grade. Helper exports `sma`, `smaAt`, `trueRanges`, `pctReturn`.

- [ ] **Step 1: Write the failing test**

```ts
// tests/metrics.test.ts
import { describe, it, expect } from "vitest";
import { sma, pctReturn, computeMetrics } from "../src/metrics.js";
import type { VendorBar, Provenance } from "../src/types.js";

const prov: Provenance = { runId: "R", ingestedAt: "x", source: "fake", sourceVersion: "1.0", schemaVersion: "metrics_v1", metricVersion: "1.0", universeVersion: "2026-06-29" };
const ok = { status: "OK" as const, issues: [] as string[] };
const bar = (date: string, close: number, volume = 1000): VendorBar => ({ ticker: "AAPL", date, open: close, high: close + 1, low: close - 1, close, adjustedClose: null, isAdjusted: false, volume, source: "fake", sourceVersion: "1.0", ingestedAt: "x" });

describe("sma", () => {
  it("averages the last N", () => { expect(sma([1, 2, 3, 4], 2)).toBe(3.5); });
  it("is null when short", () => { expect(sma([1, 2], 5)).toBeNull(); });
});
describe("pctReturn", () => {
  it("computes trailing return", () => { expect(pctReturn([100, 110], 1)).toBeCloseTo(0.1); });
});
describe("computeMetrics", () => {
  it("computes ma20 and flags, injects quality", () => {
    const bars: VendorBar[] = [];
    for (let i = 0; i < 25; i++) bars.push(bar(`2026-05-${String(i + 1).padStart(2, "0")}`, 100 + i));
    const row = computeMetrics(bars, prov, ok);
    expect(row.ticker).toBe("AAPL");
    expect(row.close).toBe(124);
    expect(row.ma20).toBeCloseTo(sma(bars.map((b) => b.close), 20)!);
    expect(row.above20ma).toBe(true);
    expect(row.ma200).toBeNull();
    expect(row.qualityStatus).toBe("OK");
    expect(row.runId).toBe("R");
  });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/metrics.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/metrics.ts`**

```ts
import type { VendorBar, MetricRow, Provenance, QualityStatus } from "./types.js";

export function sma(values: number[], window: number): number | null {
  if (values.length < window) return null;
  return values.slice(-window).reduce((a, b) => a + b, 0) / window;
}
export function smaAt(values: number[], window: number, offsetFromEnd: number): number | null {
  const end = values.length - offsetFromEnd;
  if (end < window) return null;
  return values.slice(end - window, end).reduce((a, b) => a + b, 0) / window;
}
export function trueRanges(bars: VendorBar[]): number[] {
  const tr: number[] = [];
  for (let i = 1; i < bars.length; i++) {
    const c = bars[i]!, p = bars[i - 1]!;
    tr.push(Math.max(c.high - c.low, Math.abs(c.high - p.close), Math.abs(c.low - p.close)));
  }
  return tr;
}
export function pctReturn(closes: number[], lookback: number): number | null {
  if (closes.length <= lookback) return null;
  const now = closes[closes.length - 1]!, then = closes[closes.length - 1 - lookback]!;
  return then === 0 ? null : now / then - 1;
}
function maxOver(v: number[], w: number): number | null { return v.length < w ? null : Math.max(...v.slice(-w)); }
function minOver(v: number[], w: number): number | null { return v.length < w ? null : Math.min(...v.slice(-w)); }

export function computeMetrics(bars: VendorBar[], prov: Provenance, quality: { status: QualityStatus; issues: string[] }): MetricRow {
  const last = bars[bars.length - 1]!;
  const closes = bars.map((b) => b.close);
  const highs = bars.map((b) => b.high);
  const lows = bars.map((b) => b.low);
  const volumes = bars.map((b) => b.volume);

  const ma20 = sma(closes, 20), ma50 = sma(closes, 50), ma150 = sma(closes, 150), ma200 = sma(closes, 200);
  const ma200Prev = smaAt(closes, 200, 1);
  const atr14 = sma(trueRanges(bars), 14); // needs >= 15 bars (14 true ranges)
  const high52w = maxOver(highs, 252), low52w = minOver(lows, 252);

  return {
    ticker: last.ticker, date: last.date, close: last.close,
    dollarVolume: last.close * last.volume,
    ma20, ma50, ma150, ma200,
    avgVolume20: sma(volumes, 20), avgVolume50: sma(volumes, 50),
    atr14, high52w, low52w,
    distanceTo52wHighPct: high52w ? (last.close - high52w) / high52w * 100 : null,
    distanceFrom52wLowPct: low52w ? (last.close - low52w) / low52w * 100 : null,
    return21d: pctReturn(closes, 21), return63d: pctReturn(closes, 63),
    return126d: pctReturn(closes, 126), return252d: pctReturn(closes, 252),
    above20ma: ma20 === null ? null : last.close > ma20,
    above50ma: ma50 === null ? null : last.close > ma50,
    above150ma: ma150 === null ? null : last.close > ma150,
    above200ma: ma200 === null ? null : last.close > ma200,
    ma150Above200: ma150 !== null && ma200 !== null ? ma150 > ma200 : null,
    ma200Rising: ma200 !== null && ma200Prev !== null ? ma200 > ma200Prev : null,
    qualityStatus: quality.status, qualityIssues: quality.issues,
    ...prov,
  };
}
```

- [ ] **Step 4: Run tests to verify they pass** — Run: `npx vitest run tests/metrics.test.ts` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/metrics.ts tests/metrics.test.ts
git commit -m "feat: compute market metrics with injected quality"
```

---

## Task 10: Secrets loader

**Files:** Create `src/secrets.ts`; Test `tests/secrets.test.ts`

**Interfaces:** `parseSecret(json): Record<string,string>`, `loadSecrets(client, secretName): Promise<Record<string,string>>` (client injected).

- [ ] **Step 1: Write the failing test**

```ts
// tests/secrets.test.ts
import { describe, it, expect } from "vitest";
import { parseSecret } from "../src/secrets.js";

describe("parseSecret", () => {
  it("parses the secret blob", () => {
    expect(parseSecret('{"finnhubToken":"abc"}').finnhubToken).toBe("abc");
  });
  it("throws on malformed JSON", () => { expect(() => parseSecret("not json")).toThrow(); });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/secrets.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/secrets.ts`**

```ts
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";

export function parseSecret(json: string): Record<string, string> {
  const obj = JSON.parse(json);
  if (typeof obj !== "object" || obj === null) throw new Error("secret is not an object");
  return obj as Record<string, string>;
}

export async function loadSecrets(client: SecretsManagerClient, secretName: string): Promise<Record<string, string>> {
  const res = await client.send(new GetSecretValueCommand({ SecretId: secretName }));
  if (!res.SecretString) throw new Error("secret has no SecretString");
  return parseSecret(res.SecretString);
}
```

- [ ] **Step 4: Run tests to verify they pass** — Run: `npx vitest run tests/secrets.test.ts` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/secrets.ts tests/secrets.test.ts
git commit -m "feat: add Secrets Manager loader"
```

---

## Task 11: S3 storage (Parquet writers + path builders)

**Files:** Create `src/storage.ts`; Test `tests/storage.test.ts`

**Interfaces:**
- `rawKey(source, date, runId)`, `metricsKey(date, runId)`.
- `toParquet(schema, rows): Promise<Buffer>` (exported for the smoke test in Task 12).
- `RAW_SCHEMA`, `METRIC_SCHEMA` (exported Parquet schemas).
- `writeRaw(s3, bucket, rows: RawBarRow[], source, date, runId)`, `writeMetrics(s3, bucket, rows: MetricRow[], date, runId)`. `qualityIssues` is stored as a JSON string. S3 client injected.

- [ ] **Step 1: Write the failing test (path builders)**

```ts
// tests/storage.test.ts
import { describe, it, expect } from "vitest";
import { rawKey, metricsKey } from "../src/storage.js";

describe("path builders", () => {
  it("builds a raw key with source + runId", () => {
    expect(rawKey("finnhub", "2026-06-29", "20260629T223000Z")).toBe(
      "raw/finnhub/daily/year=2026/month=06/day=29/runId=20260629T223000Z/part.parquet");
  });
  it("builds a metrics key with runId", () => {
    expect(metricsKey("2026-06-29", "20260629T223000Z")).toBe(
      "metrics/daily/year=2026/month=06/day=29/runId=20260629T223000Z/part.parquet");
  });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/storage.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/storage.ts`**

```ts
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";
import type { RawBarRow, MetricRow } from "./types.js";

function parts(date: string): { year: string; month: string; day: string } {
  const [year, month, day] = date.split("-") as [string, string, string];
  return { year, month, day };
}
export function rawKey(source: string, date: string, runId: string): string {
  const { year, month, day } = parts(date);
  return `raw/${source}/daily/year=${year}/month=${month}/day=${day}/runId=${runId}/part.parquet`;
}
export function metricsKey(date: string, runId: string): string {
  const { year, month, day } = parts(date);
  return `metrics/daily/year=${year}/month=${month}/day=${day}/runId=${runId}/part.parquet`;
}

export const RAW_SCHEMA = new ParquetSchema({
  ticker: { type: "UTF8" }, date: { type: "UTF8" },
  open: { type: "DOUBLE" }, high: { type: "DOUBLE" }, low: { type: "DOUBLE" }, close: { type: "DOUBLE" },
  adjustedClose: { type: "DOUBLE", optional: true }, isAdjusted: { type: "BOOLEAN" },
  volume: { type: "DOUBLE" },
  source: { type: "UTF8" }, sourceVersion: { type: "UTF8" }, ingestedAt: { type: "UTF8" },
  runId: { type: "UTF8" }, schemaVersion: { type: "UTF8" }, metricVersion: { type: "UTF8" }, universeVersion: { type: "UTF8" },
});

export const METRIC_SCHEMA = new ParquetSchema({
  ticker: { type: "UTF8" }, date: { type: "UTF8" }, close: { type: "DOUBLE" }, dollarVolume: { type: "DOUBLE" },
  ma20: { type: "DOUBLE", optional: true }, ma50: { type: "DOUBLE", optional: true },
  ma150: { type: "DOUBLE", optional: true }, ma200: { type: "DOUBLE", optional: true },
  avgVolume20: { type: "DOUBLE", optional: true }, avgVolume50: { type: "DOUBLE", optional: true },
  atr14: { type: "DOUBLE", optional: true },
  high52w: { type: "DOUBLE", optional: true }, low52w: { type: "DOUBLE", optional: true },
  distanceTo52wHighPct: { type: "DOUBLE", optional: true }, distanceFrom52wLowPct: { type: "DOUBLE", optional: true },
  return21d: { type: "DOUBLE", optional: true }, return63d: { type: "DOUBLE", optional: true },
  return126d: { type: "DOUBLE", optional: true }, return252d: { type: "DOUBLE", optional: true },
  above20ma: { type: "BOOLEAN", optional: true }, above50ma: { type: "BOOLEAN", optional: true },
  above150ma: { type: "BOOLEAN", optional: true }, above200ma: { type: "BOOLEAN", optional: true },
  ma150Above200: { type: "BOOLEAN", optional: true }, ma200Rising: { type: "BOOLEAN", optional: true },
  qualityStatus: { type: "UTF8" }, qualityIssues: { type: "UTF8" },
  runId: { type: "UTF8" }, ingestedAt: { type: "UTF8" }, source: { type: "UTF8" }, sourceVersion: { type: "UTF8" },
  schemaVersion: { type: "UTF8" }, metricVersion: { type: "UTF8" }, universeVersion: { type: "UTF8" },
});

export async function toParquet(schema: ParquetSchema, rows: Record<string, unknown>[]): Promise<Buffer> {
  const chunks: Buffer[] = [];
  const writer = await ParquetWriter.openStream(schema, {
    write: (c: Buffer) => chunks.push(c), end: () => {},
  } as never);
  for (const row of rows) await writer.appendRow(row);
  await writer.close();
  return Buffer.concat(chunks);
}

export async function writeRaw(s3: S3Client, bucket: string, rows: RawBarRow[], source: string, date: string, runId: string): Promise<string> {
  const key = rawKey(source, date, runId);
  const body = await toParquet(RAW_SCHEMA, rows as unknown as Record<string, unknown>[]);
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
  return key;
}

export async function writeMetrics(s3: S3Client, bucket: string, rows: MetricRow[], date: string, runId: string): Promise<string> {
  const key = metricsKey(date, runId);
  const flat = rows.map((r) => ({ ...r, qualityIssues: JSON.stringify(r.qualityIssues) })); // JSON string, not comma-joined
  const body = await toParquet(METRIC_SCHEMA, flat as unknown as Record<string, unknown>[]);
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
  return key;
}
```

- [ ] **Step 4: Run tests to verify they pass** — Run: `npx vitest run tests/storage.test.ts` — Expected: PASS.

- [ ] **Step 5: Typecheck** — Run: `npm run typecheck` — Expected: exits 0. (The `as never` cast on the writer sink is intentional; keep it.)

- [ ] **Step 6: Commit**

```bash
git add src/storage.ts tests/storage.test.ts
git commit -m "feat: add S3 Parquet storage (provenance, JSON qualityIssues)"
```

---

## Task 12: Parquet round-trip smoke test (early integration)

**Files:** Test `tests/parquet-smoke.test.ts`

**Interfaces:** Consumes `toParquet`, `METRIC_SCHEMA` from Task 11. Proves a written Parquet buffer can be re-read with the expected schema and values — before any logic depends on it.

- [ ] **Step 1: Write the smoke test**

```ts
// tests/parquet-smoke.test.ts
import { describe, it, expect } from "vitest";
import { ParquetReader } from "@dsnp/parquetjs";
import { toParquet, METRIC_SCHEMA } from "../src/storage.js";

describe("parquet round-trip", () => {
  it("writes two metric rows and reads them back with correct types", async () => {
    const rows = [
      { ticker: "AAPL", date: "2026-06-29", close: 11, dollarVolume: 11000, ma20: 10.5, ma50: null, ma150: null, ma200: null, avgVolume20: null, avgVolume50: null, atr14: null, high52w: null, low52w: null, distanceTo52wHighPct: null, distanceFrom52wLowPct: null, return21d: null, return63d: null, return126d: null, return252d: null, above20ma: true, above50ma: null, above150ma: null, above200ma: null, ma150Above200: null, ma200Rising: null, qualityStatus: "OK", qualityIssues: JSON.stringify([]), runId: "R", ingestedAt: "x", source: "fake", sourceVersion: "1.0", schemaVersion: "metrics_v1", metricVersion: "1.0", universeVersion: "2026-06-29" },
      { ticker: "MSFT", date: "2026-06-29", close: 20, dollarVolume: 40000, ma20: null, ma50: null, ma150: null, ma200: null, avgVolume20: null, avgVolume50: null, atr14: null, high52w: null, low52w: null, distanceTo52wHighPct: null, distanceFrom52wLowPct: null, return21d: null, return63d: null, return126d: null, return252d: null, above20ma: null, above50ma: null, above150ma: null, above200ma: null, ma150Above200: null, ma200Rising: null, qualityStatus: "WARN", qualityIssues: JSON.stringify(["zero_volume"]), runId: "R", ingestedAt: "x", source: "fake", sourceVersion: "1.0", schemaVersion: "metrics_v1", metricVersion: "1.0", universeVersion: "2026-06-29" },
    ];
    const buf = await toParquet(METRIC_SCHEMA, rows as unknown as Record<string, unknown>[]);
    const reader = await ParquetReader.openBuffer(buf);
    const cursor = reader.getCursor();
    const read: Record<string, unknown>[] = [];
    let rec: unknown;
    while ((rec = await cursor.next())) read.push(rec as Record<string, unknown>);
    await reader.close();

    expect(read).toHaveLength(2);
    expect(read[0]!.ticker).toBe("AAPL");
    expect(read[0]!.above20ma).toBe(true);
    expect(read[1]!.qualityStatus).toBe("WARN");
    expect(JSON.parse(read[1]!.qualityIssues as string)).toEqual(["zero_volume"]);
  });
});
```

- [ ] **Step 2: Run the smoke test** — Run: `npx vitest run tests/parquet-smoke.test.ts` — Expected: PASS. If `@dsnp/parquetjs` cannot round-trip these types, STOP and resolve the library issue here — before building dependent logic. (The Glue/Athena column types in Task 20 mirror this schema, so a green round-trip is strong evidence Athena will read it.)

- [ ] **Step 3: Commit**

```bash
git add tests/parquet-smoke.test.ts
git commit -m "test: add Parquet write/read round-trip smoke test"
```

---

## Task 13: History merge logic

**Files:** Create `src/history.ts`; Test `tests/history.test.ts`

**Interfaces:** `mergeHistory(stored: VendorBar[], latest: VendorBar): VendorBar[]` (ascending, de-duped on date, latest wins). `hasEnoughHistory(bars, minSessions): boolean`.

- [ ] **Step 1: Write the failing test**

```ts
// tests/history.test.ts
import { describe, it, expect } from "vitest";
import { mergeHistory, hasEnoughHistory } from "../src/history.js";
import type { VendorBar } from "../src/types.js";

const bar = (date: string, close: number): VendorBar => ({ ticker: "AAPL", date, open: close, high: close, low: close, close, adjustedClose: null, isAdjusted: false, volume: 1, source: "fake", sourceVersion: "1.0", ingestedAt: "x" });

describe("mergeHistory", () => {
  it("appends latest, keeps ascending order", () => {
    const m = mergeHistory([bar("2026-06-26", 1), bar("2026-06-27", 2)], bar("2026-06-29", 3));
    expect(m.map((b) => b.date)).toEqual(["2026-06-26", "2026-06-27", "2026-06-29"]);
  });
  it("replaces a same-date bar with latest", () => {
    const m = mergeHistory([bar("2026-06-29", 1)], bar("2026-06-29", 9));
    expect(m).toHaveLength(1); expect(m[0]!.close).toBe(9);
  });
});
describe("hasEnoughHistory", () => {
  it("is false below the minimum", () => { expect(hasEnoughHistory([bar("2026-06-29", 1)], 200)).toBe(false); });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/history.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/history.ts`**

```ts
import type { VendorBar } from "./types.js";

export function mergeHistory(stored: VendorBar[], latest: VendorBar): VendorBar[] {
  const byDate = new Map<string, VendorBar>();
  for (const b of stored) byDate.set(b.date, b);
  byDate.set(latest.date, latest); // latest wins on collision
  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

export function hasEnoughHistory(bars: VendorBar[], minSessions: number): boolean {
  return bars.length >= minSessions;
}
```

- [ ] **Step 4: Run tests to verify they pass** — Run: `npx vitest run tests/history.test.ts` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/history.ts tests/history.test.ts
git commit -m "feat: add history merge and sufficiency check"
```

---

## Task 14: Per-ticker history cache (S3 JSON)

**Files:** Create `src/historyCache.ts`; Test `tests/historyCache.test.ts`

**Interfaces:**
- `historyCacheKey(source, ticker): string` → `history/{source}/ticker={ticker}/current.json`.
- `readHistoryCache(s3, bucket, source, ticker): Promise<VendorBar[]>` — returns `[]` on any miss/error.
- `writeHistoryCache(s3, bucket, source, ticker, bars, maxBars=400): Promise<void>` — trims to the last `maxBars`. The cache is rebuildable from immutable raw, so it is not the source of truth.

- [ ] **Step 1: Write the failing test**

```ts
// tests/historyCache.test.ts
import { describe, it, expect } from "vitest";
import { historyCacheKey, readHistoryCache, writeHistoryCache } from "../src/historyCache.js";
import type { VendorBar } from "../src/types.js";

const bar = (date: string): VendorBar => ({ ticker: "AAPL", date, open: 1, high: 1, low: 1, close: 1, adjustedClose: null, isAdjusted: false, volume: 1, source: "finnhub", sourceVersion: "1.0", ingestedAt: "x" });

describe("historyCacheKey", () => {
  it("builds the per-ticker key", () => {
    expect(historyCacheKey("finnhub", "AAPL")).toBe("history/finnhub/ticker=AAPL/current.json");
  });
});

describe("readHistoryCache", () => {
  it("returns [] when the object is missing", async () => {
    const s3 = { send: async () => { throw Object.assign(new Error("nope"), { name: "NoSuchKey" }); } } as never;
    expect(await readHistoryCache(s3, "b", "finnhub", "AAPL")).toEqual([]);
  });
});

describe("writeHistoryCache", () => {
  it("trims to the last maxBars and PUTs JSON", async () => {
    let body = "";
    const s3 = { send: async (c: { input: { Body: string } }) => { body = c.input.Body; return {}; } } as never;
    await writeHistoryCache(s3, "b", "finnhub", "AAPL", [bar("2026-06-25"), bar("2026-06-26"), bar("2026-06-29")], 2);
    const parsed = JSON.parse(body) as VendorBar[];
    expect(parsed.map((b) => b.date)).toEqual(["2026-06-26", "2026-06-29"]);
  });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/historyCache.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/historyCache.ts`**

```ts
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import type { VendorBar } from "./types.js";

export function historyCacheKey(source: string, ticker: string): string {
  return `history/${source}/ticker=${ticker}/current.json`;
}

export async function readHistoryCache(s3: S3Client, bucket: string, source: string, ticker: string): Promise<VendorBar[]> {
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: historyCacheKey(source, ticker) }));
    const text = await res.Body!.transformToString();
    const parsed = JSON.parse(text);
    return Array.isArray(parsed) ? (parsed as VendorBar[]) : [];
  } catch {
    return []; // miss -> pipeline backfills
  }
}

export async function writeHistoryCache(s3: S3Client, bucket: string, source: string, ticker: string, bars: VendorBar[], maxBars = 400): Promise<void> {
  const trimmed = bars.slice(-maxBars);
  await s3.send(new PutObjectCommand({
    Bucket: bucket, Key: historyCacheKey(source, ticker),
    Body: JSON.stringify(trimmed), ContentType: "application/json",
  }));
}
```

- [ ] **Step 4: Run tests to verify they pass** — Run: `npx vitest run tests/historyCache.test.ts` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/historyCache.ts tests/historyCache.test.ts
git commit -m "feat: add per-ticker history cache (S3 JSON, rebuildable)"
```

---

## Task 15: Glue partition registration

**Files:** Create `src/glue.ts`; Test `tests/glue.test.ts`

**Interfaces:** `partitionValues(date) -> [year, month, day]`; `addPartition(glue, db, table, bucket, prefix, date)` calls `BatchCreatePartition`, treating "already exists" as success. Glue client injected.

- [ ] **Step 1: Write the failing test**

```ts
// tests/glue.test.ts
import { describe, it, expect } from "vitest";
import { partitionValues } from "../src/glue.js";

describe("partitionValues", () => {
  it("splits a date", () => { expect(partitionValues("2026-06-29")).toEqual(["2026", "06", "29"]); });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/glue.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/glue.ts`**

```ts
import { GlueClient, BatchCreatePartitionCommand } from "@aws-sdk/client-glue";

export function partitionValues(date: string): [string, string, string] {
  const [y, m, d] = date.split("-") as [string, string, string];
  return [y, m, d];
}

export async function addPartition(glue: GlueClient, database: string, table: string, bucket: string, prefix: string, date: string): Promise<void> {
  const [year, month, day] = partitionValues(date);
  const location = `s3://${bucket}/${prefix}/year=${year}/month=${month}/day=${day}/`;
  try {
    await glue.send(new BatchCreatePartitionCommand({
      DatabaseName: database, TableName: table,
      PartitionInputList: [{ Values: [year, month, day], StorageDescriptor: { Location: location } }],
    }));
  } catch (err) {
    if (((err as { name?: string }).name ?? "").includes("AlreadyExists")) return; // idempotent
    throw err;
  }
}
```

- [ ] **Step 4: Run tests to verify they pass** — Run: `npx vitest run tests/glue.test.ts` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/glue.ts tests/glue.test.ts
git commit -m "feat: add idempotent Glue partition registration"
```

---

## Task 16: Metadata (manifest, current pointer, universe snapshot)

**Files:** Create `src/metadata.ts`; Test `tests/metadata.test.ts`

**Interfaces:** `manifestKey(date, runId)`, `currentKey(date)`, `universeKey(date)`; `writeManifest(s3, bucket, m)`, `markCurrent(s3, bucket, m)` (caller gates on status), `snapshotUniverse(s3, bucket, date, universe)`.

- [ ] **Step 1: Write the failing test**

```ts
// tests/metadata.test.ts
import { describe, it, expect } from "vitest";
import { manifestKey, currentKey, universeKey } from "../src/metadata.js";

describe("metadata keys", () => {
  it("manifest key", () => { expect(manifestKey("2026-06-29", "20260629T223000Z")).toBe("metadata/runs/year=2026/month=06/day=29/runId=20260629T223000Z/manifest.json"); });
  it("current key", () => { expect(currentKey("2026-06-29")).toBe("metadata/current/daily_metrics/year=2026/month=06/day=29.json"); });
  it("universe key", () => { expect(universeKey("2026-06-29")).toBe("metadata/universe/2026-06-29.json"); });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/metadata.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/metadata.ts`**

```ts
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import type { RunManifest } from "./types.js";

function parts(date: string) {
  const [year, month, day] = date.split("-") as [string, string, string];
  return { year, month, day };
}
export function manifestKey(date: string, runId: string): string {
  const { year, month, day } = parts(date);
  return `metadata/runs/year=${year}/month=${month}/day=${day}/runId=${runId}/manifest.json`;
}
export function currentKey(date: string): string {
  const { year, month, day } = parts(date);
  return `metadata/current/daily_metrics/year=${year}/month=${month}/day=${day}.json`;
}
export function universeKey(date: string): string { return `metadata/universe/${date}.json`; }

async function putJson(s3: S3Client, bucket: string, key: string, value: unknown): Promise<void> {
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: JSON.stringify(value, null, 2), ContentType: "application/json" }));
}
export async function writeManifest(s3: S3Client, bucket: string, m: RunManifest): Promise<void> {
  await putJson(s3, bucket, manifestKey(m.tradingDay, m.runId), m);
}
export async function markCurrent(s3: S3Client, bucket: string, m: RunManifest): Promise<void> {
  await putJson(s3, bucket, currentKey(m.tradingDay), { runId: m.runId, status: m.status, rowCount: m.rowsWritten });
}
export async function snapshotUniverse(s3: S3Client, bucket: string, date: string, universe: { universeVersion: string; tickers: string[] }): Promise<void> {
  await putJson(s3, bucket, universeKey(date), universe);
}
```

- [ ] **Step 4: Run tests to verify they pass** — Run: `npx vitest run tests/metadata.test.ts` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/metadata.ts tests/metadata.test.ts
git commit -m "feat: add manifest, current pointer, and universe snapshot writers"
```

---

## Task 16b: Error sink

**Files:** Create `src/errors.ts`; Test `tests/errors.test.ts`

**Interfaces:**
- `errorsKey(date, runId): string` → `errors/year=.../month=.../day=.../runId=.../errors.json`.
- `writeErrors(s3, bucket, date, runId, errors: ErrorRecord[]): Promise<void>` — no-op when `errors` is empty. Records provider failures, `missing_bar_for_date`, rejected rows, warnings, and pipeline errors. Nothing is silently dropped.

- [ ] **Step 1: Write the failing test**

```ts
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
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/errors.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/errors.ts`**

```ts
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import type { ErrorRecord } from "./types.js";

export function errorsKey(date: string, runId: string): string {
  const [year, month, day] = date.split("-") as [string, string, string];
  return `errors/year=${year}/month=${month}/day=${day}/runId=${runId}/errors.json`;
}

export async function writeErrors(s3: S3Client, bucket: string, date: string, runId: string, errors: ErrorRecord[]): Promise<void> {
  if (errors.length === 0) return;
  await s3.send(new PutObjectCommand({
    Bucket: bucket, Key: errorsKey(date, runId),
    Body: JSON.stringify(errors, null, 2), ContentType: "application/json",
  }));
}
```

- [ ] **Step 4: Run tests to verify they pass** — Run: `npx vitest run tests/errors.test.ts` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/errors.ts tests/errors.test.ts
git commit -m "feat: add structured error sink (errors/ writer)"
```

---

## Task 17: Telegram report

**Files:** Create `src/report.ts`; Test `tests/report.test.ts`

**Interfaces:** `renderReport(m: RunManifest): string`, `sendTelegram(botToken, chatId, text, fetchFn?)`.

- [ ] **Step 1: Write the failing test**

```ts
// tests/report.test.ts
import { describe, it, expect } from "vitest";
import { renderReport } from "../src/report.js";
import type { RunManifest } from "../src/types.js";

const m: RunManifest = { runId: "20260629T223000Z", mode: "daily", tradingDay: "2026-06-29", provider: "finnhub", universeVersion: "2026-06-29", symbolsRequested: 612, symbolsSucceeded: 610, rowsWritten: 610, warnings: 4, rejected: 2, missingBars: 0, runtimeSec: 302, metricVersion: "1.0", schemaVersion: "metrics_v1", status: "PARTIAL" };

describe("renderReport", () => {
  it("includes key run stats", () => {
    const t = renderReport(m);
    expect(t).toContain("EdgeHub Daily Update");
    expect(t).toContain("2026-06-29");
    expect(t).toContain("610");
    expect(t).toContain("PARTIAL");
  });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/report.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/report.ts`**

```ts
import type { RunManifest } from "./types.js";

type FetchFn = (url: string, init: { method: string; headers: Record<string, string>; body: string }) => Promise<{ ok: boolean; status: number }>;

export function renderReport(m: RunManifest): string {
  const lines = [
    `EdgeHub Daily Update`,
    `Date: ${m.tradingDay}`,
    `Provider: ${m.provider}`,
    `Universe: ${m.symbolsRequested} (v${m.universeVersion})`,
    `Downloaded: ${m.symbolsSucceeded}`,
    `Rows: ${m.rowsWritten}`,
    `Warnings: ${m.warnings}`,
    `Rejected: ${m.rejected}`,
    `Missing bars: ${m.missingBars}`,
    `Metric Version: ${m.metricVersion}`,
    `Runtime: ${m.runtimeSec}s`,
    `Status: ${m.status}`,
  ];
  if (m.note) lines.push(`Note: ${m.note}`);
  return lines.join("\n");
}

export async function sendTelegram(botToken: string, chatId: string, text: string, fetchFn: FetchFn = fetch as unknown as FetchFn): Promise<void> {
  const res = await fetchFn(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ chat_id: chatId, text }),
  });
  if (!res.ok) throw new Error(`telegram HTTP ${res.status}`);
}
```

- [ ] **Step 4: Run tests to verify they pass** — Run: `npx vitest run tests/report.test.ts` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/report.ts tests/report.test.ts
git commit -m "feat: add Telegram report rendering and sending"
```

---

## Task 18: Pipeline orchestration

**Files:** Create `src/pipeline.ts`; Test `tests/pipeline.test.ts`

**Interfaces:**
- `makeRunId(now: Date): string` → `YYYYMMDDTHHMMSSZ`.
- `enrichRaw(bar: VendorBar, ctx: { runId: string; universeVersion: string }): RawBarRow`.
- `buildManifest(args): RunManifest` — status: `SKIPPED` when not a trading day; `FAILURE` when success rate < floor (0.90) or zero; `PARTIAL` when below requested but ≥ floor; `SUCCESS` when all requested produced rows.
- `runPipeline(mode, deps): Promise<RunManifest>` where `Deps = { provider, s3, glue, bucket, database, tradingDay, now, readHistory, writeHistory, isTradingDay, calendarCovers }`. Collects every per-ticker failure into `ErrorRecord[]` and writes them via `writeErrors`; fails safely (`FAILURE` / `calendar_year_missing`) when the year isn't covered; never writes a stale today row; advances `current` only on `SUCCESS`/`PARTIAL`.

- [ ] **Step 1: Write the failing test (runId + manifest)**

```ts
// tests/pipeline.test.ts
import { describe, it, expect } from "vitest";
import { makeRunId, buildManifest } from "../src/pipeline.js";

describe("makeRunId", () => {
  it("formats a compact UTC timestamp", () => {
    expect(makeRunId(new Date("2026-06-29T22:30:00Z"))).toBe("20260629T223000Z");
  });
});

describe("buildManifest", () => {
  const common = { mode: "daily" as const, runId: "R", tradingDay: "2026-06-29", provider: "fake", universeVersion: "2026-06-29", warnings: 0, rejected: 0, missingBars: 0, runtimeSec: 5 };
  it("SUCCESS when all requested produce rows", () => {
    expect(buildManifest({ ...common, requested: 10, succeeded: 10 }).status).toBe("SUCCESS");
  });
  it("PARTIAL when above the floor but short", () => {
    expect(buildManifest({ ...common, requested: 10, succeeded: 9 }).status).toBe("PARTIAL");
  });
  it("FAILURE below the floor", () => {
    expect(buildManifest({ ...common, requested: 10, succeeded: 5 }).status).toBe("FAILURE");
  });
});
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/pipeline.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/pipeline.ts`**

```ts
import { S3Client } from "@aws-sdk/client-s3";
import { GlueClient } from "@aws-sdk/client-glue";
import type { MarketDataProvider } from "./providers/provider.js";
import type { VendorBar, RawBarRow, MetricRow, RunManifest, RunMode, Provenance, ErrorRecord } from "./types.js";
import { SCHEMA_VERSION, RAW_SCHEMA_VERSION, METRIC_VERSION } from "./types.js";
import { loadUniverse } from "./universe.js";
import { gradeBar } from "./validate.js";
import { computeMetrics } from "./metrics.js";
import { mergeHistory, hasEnoughHistory } from "./history.js";
import { writeRaw, writeMetrics } from "./storage.js";
import { addPartition } from "./glue.js";
import { writeManifest, markCurrent, snapshotUniverse } from "./metadata.js";
import { writeErrors } from "./errors.js";

export function makeRunId(now: Date): string {
  return now.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
}

export function enrichRaw(bar: VendorBar, ctx: { runId: string; universeVersion: string }): RawBarRow {
  return { ...bar, runId: ctx.runId, schemaVersion: RAW_SCHEMA_VERSION, metricVersion: METRIC_VERSION, universeVersion: ctx.universeVersion };
}

const MIN_SUCCESS_RATE = 0.9;

export interface BuildManifestArgs {
  mode: RunMode; runId: string; tradingDay: string; provider: string; universeVersion: string;
  requested: number; succeeded: number; warnings: number; rejected: number; missingBars: number; runtimeSec: number;
}
export function buildManifest(a: BuildManifestArgs): RunManifest {
  const rate = a.requested === 0 ? 0 : a.succeeded / a.requested;
  const status: RunManifest["status"] =
    a.succeeded === 0 || rate < MIN_SUCCESS_RATE ? "FAILURE" : a.succeeded < a.requested ? "PARTIAL" : "SUCCESS";
  return {
    runId: a.runId, mode: a.mode, tradingDay: a.tradingDay, provider: a.provider, universeVersion: a.universeVersion,
    symbolsRequested: a.requested, symbolsSucceeded: a.succeeded, rowsWritten: a.succeeded,
    warnings: a.warnings, rejected: a.rejected, missingBars: a.missingBars, runtimeSec: a.runtimeSec,
    metricVersion: METRIC_VERSION, schemaVersion: SCHEMA_VERSION, status,
  };
}

export interface Deps {
  provider: MarketDataProvider;
  s3: S3Client;
  glue: GlueClient;
  bucket: string;
  database: string;
  tradingDay: string;
  now: () => Date;
  readHistory: (ticker: string) => Promise<VendorBar[]>;
  writeHistory: (ticker: string, bars: VendorBar[]) => Promise<void>;
  isTradingDay: (date: string) => boolean;
  calendarCovers: (date: string) => boolean;
}

const MIN_SESSIONS = 200;
const LOOKBACK_DAYS = 400;

export async function runPipeline(mode: RunMode, deps: Deps): Promise<RunManifest> {
  const start = deps.now().getTime();
  const runId = makeRunId(deps.now());
  const { tickers, universeVersion } = loadUniverse();

  const earlyExit = (status: RunManifest["status"], note: string): RunManifest => ({
    runId, mode, tradingDay: deps.tradingDay, provider: deps.provider.name, universeVersion,
    symbolsRequested: tickers.length, symbolsSucceeded: 0, rowsWritten: 0, warnings: 0, rejected: 0,
    missingBars: 0, runtimeSec: 0, metricVersion: METRIC_VERSION, schemaVersion: SCHEMA_VERSION, status, note,
  });

  // Fail safely if the calendar does not cover this year — never guess holidays.
  if (mode === "daily" && !deps.calendarCovers(deps.tradingDay)) {
    const manifest = earlyExit("FAILURE", "calendar_year_missing");
    await writeManifest(deps.s3, deps.bucket, manifest);
    await writeErrors(deps.s3, deps.bucket, deps.tradingDay, runId, [{
      runId, tradingDay: deps.tradingDay, source: deps.provider.name, universeVersion, ticker: "*",
      reason: "calendar_year_missing", createdAt: deps.now().toISOString(),
    }]);
    return manifest; // do NOT advance current
  }

  // Calendar gate: skip non-trading days for scheduled daily runs.
  if (mode === "daily" && !deps.isTradingDay(deps.tradingDay)) {
    const manifest = earlyExit("SKIPPED", "non_trading_day");
    await writeManifest(deps.s3, deps.bucket, manifest); // do NOT advance current
    return manifest;
  }

  await snapshotUniverse(deps.s3, deps.bucket, deps.tradingDay, { universeVersion, tickers });

  const prov: Provenance = {
    runId, ingestedAt: deps.now().toISOString(), source: deps.provider.name, sourceVersion: deps.provider.version,
    schemaVersion: SCHEMA_VERSION, metricVersion: METRIC_VERSION, universeVersion,
  };

  const rawToStore: RawBarRow[] = [];
  const metricRows: MetricRow[] = [];
  const errors: ErrorRecord[] = [];
  const seen = new Set<string>();
  let warnings = 0, rejected = 0, missingBars = 0;

  const recordError = (ticker: string, reason: string, message?: string) =>
    errors.push({ runId, tradingDay: deps.tradingDay, source: deps.provider.name, universeVersion, ticker, reason, message, createdAt: deps.now().toISOString() });

  // Daily mode fetches all latest bars in one resilient batch call.
  const latestByTicker = new Map<string, VendorBar>();
  if (mode === "daily") {
    const result = await deps.provider.getLatestBars(deps.tradingDay, tickers);
    for (const b of result.bars) latestByTicker.set(b.ticker, b);
    for (const f of result.failures) recordError(f.ticker, f.reason, f.message);
  }

  for (const ticker of tickers) {
    try {
      let bars: VendorBar[];
      if (mode === "backfill") {
        const r = await deps.provider.getHistory(ticker, LOOKBACK_DAYS);
        for (const f of r.failures) recordError(f.ticker || ticker, f.reason, f.message);
        bars = r.bars;
        if (bars.length === 0) { missingBars++; continue; }
      } else {
        const latest = latestByTicker.get(ticker);
        if (!latest) { missingBars++; continue; } // failure already recorded from the batch result
        const stored = await deps.readHistory(ticker);
        bars = mergeHistory(stored, latest);
        if (!hasEnoughHistory(bars, MIN_SESSIONS)) {
          const r = await deps.provider.getHistory(ticker, LOOKBACK_DAYS);
          for (const f of r.failures) recordError(f.ticker || ticker, f.reason, f.message);
          bars = mergeHistory(r.bars, latest);
        }
      }

      const today = bars[bars.length - 1];
      if (!today || (mode === "daily" && today.date !== deps.tradingDay)) {
        missingBars++; recordError(ticker, "missing_bar_for_date"); continue; // NO fake row
      }

      const grade = gradeBar(today, seen); // single batch-level grading (shared seen set)
      rawToStore.push(enrichRaw(today, { runId, universeVersion }));
      if (grade.status === "REJECTED") { rejected++; recordError(ticker, "rejected", grade.issues.join(",")); continue; }
      if (grade.status === "WARN") { warnings++; recordError(ticker, "warn", grade.issues.join(",")); }

      metricRows.push(computeMetrics(bars, prov, grade));
      await deps.writeHistory(ticker, bars); // refresh cache for next run
    } catch (err) {
      recordError(ticker, "pipeline_error", (err as Error).message); // per-ticker failure is non-fatal
      continue;
    }
  }

  if (rawToStore.length > 0) {
    await writeRaw(deps.s3, deps.bucket, rawToStore, deps.provider.name, deps.tradingDay, runId);
  }
  if (metricRows.length > 0) {
    await writeMetrics(deps.s3, deps.bucket, metricRows, deps.tradingDay, runId);
    await addPartition(deps.glue, deps.database, "daily_bars", deps.bucket, `raw/${deps.provider.name}/daily`, deps.tradingDay);
    await addPartition(deps.glue, deps.database, "daily_metrics", deps.bucket, "metrics/daily", deps.tradingDay);
  }
  await writeErrors(deps.s3, deps.bucket, deps.tradingDay, runId, errors); // no-op if empty

  const runtimeSec = Math.round((deps.now().getTime() - start) / 1000);
  const manifest = buildManifest({
    mode, runId, tradingDay: deps.tradingDay, provider: deps.provider.name, universeVersion,
    requested: tickers.length, succeeded: metricRows.length, warnings, rejected, missingBars, runtimeSec,
  });

  await writeManifest(deps.s3, deps.bucket, manifest);
  if (manifest.status === "SUCCESS" || manifest.status === "PARTIAL") {
    await markCurrent(deps.s3, deps.bucket, manifest); // accepted runs only; never on FAILURE/SKIPPED
  }
  return manifest;
}
```

- [ ] **Step 4: Add an end-to-end backfill test with the fake provider**

Append to `tests/pipeline.test.ts`:

```ts
import { runPipeline } from "../src/pipeline.js";
import { FakeProvider } from "../src/providers/fake.js";
import type { VendorBar } from "../src/types.js";

function series(ticker: string, n: number): VendorBar[] {
  const bars: VendorBar[] = [];
  for (let i = 0; i < n; i++) {
    const close = 100 + i;
    bars.push({ ticker, date: `2025-01-${String((i % 28) + 1).padStart(2, "0")}`, open: close, high: close + 1, low: close - 1, close, adjustedClose: null, isAdjusted: false, volume: 1000, source: "fake", sourceVersion: "1.0", ingestedAt: "x" });
  }
  return bars;
}

describe("runPipeline (backfill, fake provider)", () => {
  it("produces a SUCCESS manifest and writes metrics for all tickers", async () => {
    const hist = new Map<string, VendorBar[]>(
      ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "TSLA", "AVGO", "SPY", "QQQ"].map((t) => [t, series(t, 5)]),
    );
    const calls: string[] = [];
    const s3 = { send: async (c: unknown) => { calls.push((c as { constructor: { name: string } }).constructor.name); return {}; } } as never;
    const glue = { send: async () => ({}) } as never;
    const written = new Map<string, VendorBar[]>();
    const m = await runPipeline("backfill", {
      provider: new FakeProvider(hist), s3, glue, bucket: "b", database: "edgehub", tradingDay: "2025-01-05",
      now: () => new Date("2025-01-05T22:30:00Z"),
      readHistory: async () => [], writeHistory: async (t, bars) => { written.set(t, bars); },
      isTradingDay: () => true, calendarCovers: () => true,
    });
    expect(m.status).toBe("SUCCESS");
    expect(m.rowsWritten).toBe(9);
    expect(calls).toContain("PutObjectCommand");
    expect(written.size).toBe(9); // cache refreshed per ticker
  });
});

describe("runPipeline (daily, non-trading day)", () => {
  it("returns SKIPPED without fetching", async () => {
    const s3 = { send: async () => ({}) } as never;
    const glue = { send: async () => ({}) } as never;
    const m = await runPipeline("daily", {
      provider: new FakeProvider(new Map()), s3, glue, bucket: "b", database: "edgehub", tradingDay: "2026-07-04",
      now: () => new Date("2026-07-04T22:30:00Z"),
      readHistory: async () => [], writeHistory: async () => {}, isTradingDay: () => false, calendarCovers: () => true,
    });
    expect(m.status).toBe("SKIPPED");
  });
});

describe("runPipeline (daily, uncovered calendar year)", () => {
  it("fails safely with calendar_year_missing and does not advance current", async () => {
    const calls: string[] = [];
    const s3 = { send: async (c: { constructor: { name: string }, input?: { Key?: string } }) => { calls.push(c.input?.Key ?? c.constructor.name); return {}; } } as never;
    const glue = { send: async () => ({}) } as never;
    const m = await runPipeline("daily", {
      provider: new FakeProvider(new Map()), s3, glue, bucket: "b", database: "edgehub", tradingDay: "2027-03-02",
      now: () => new Date("2027-03-02T22:30:00Z"),
      readHistory: async () => [], writeHistory: async () => {}, isTradingDay: () => true, calendarCovers: () => false,
    });
    expect(m.status).toBe("FAILURE");
    expect(m.note).toBe("calendar_year_missing");
    expect(calls.some((k) => k.includes("metadata/current"))).toBe(false); // current not advanced
  });
});
```

- [ ] **Step 5: Run tests to verify they pass** — Run: `npx vitest run tests/pipeline.test.ts` — Expected: PASS (runId, manifest, backfill, skipped).

- [ ] **Step 6: Commit**

```bash
git add src/pipeline.ts tests/pipeline.test.ts
git commit -m "feat: orchestrate pipeline (calendar gate, no stale rows, current policy)"
```

---

## Task 19: Lambda handler

**Files:** Create `src/handler.ts`, `events/daily.json`, `events/backfill.json`; Test `tests/handler.test.ts`

**Interfaces:** `parseEvent(event, now): { mode; tradingDay }` and `handler`. Wires real AWS clients, the calendar, and the history cache (`readHistory`/`writeHistory`) into `runPipeline`.

- [ ] **Step 1: Write the failing test**

```ts
// tests/handler.test.ts
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
```

- [ ] **Step 2: Run test to verify it fails** — Run: `npx vitest run tests/handler.test.ts` — Expected: FAIL.

- [ ] **Step 3: Create `src/handler.ts`**

```ts
import { S3Client } from "@aws-sdk/client-s3";
import { GlueClient } from "@aws-sdk/client-glue";
import { SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import type { RunMode } from "./types.js";
import { getProvider } from "./providers/factory.js";
import { loadSecrets } from "./secrets.js";
import { runPipeline } from "./pipeline.js";
import { readHistoryCache, writeHistoryCache } from "./historyCache.js";
import { isTradingDay, calendarCoversYear } from "./calendar.js";
import { renderReport, sendTelegram } from "./report.js";

export function parseEvent(event: { mode?: string; tradingDay?: string }, now: Date): { mode: RunMode; tradingDay: string } {
  const mode: RunMode = event.mode === "backfill" ? "backfill" : "daily";
  const tradingDay = event.tradingDay ?? now.toISOString().slice(0, 10);
  return { mode, tradingDay };
}

export async function handler(event: { mode?: string; tradingDay?: string } = {}): Promise<{ status: string }> {
  const region = process.env.AWS_REGION ?? "us-east-1";
  const bucket = process.env.BUCKET_NAME!;
  const database = process.env.GLUE_DATABASE ?? "edgehub";
  const providerName = process.env.DATA_PROVIDER ?? "finnhub";
  const secretName = process.env.SECRET_NAME!;

  const s3 = new S3Client({ region });
  const glue = new GlueClient({ region });
  const sm = new SecretsManagerClient({ region });

  const secrets = await loadSecrets(sm, secretName);
  const provider = getProvider(providerName, secrets);
  const { mode, tradingDay } = parseEvent(event, new Date());

  const manifest = await runPipeline(mode, {
    provider, s3, glue, bucket, database, tradingDay, now: () => new Date(),
    readHistory: (t) => readHistoryCache(s3, bucket, provider.name, t),
    writeHistory: (t, bars) => writeHistoryCache(s3, bucket, provider.name, t, bars),
    isTradingDay: (d) => isTradingDay(d),
    calendarCovers: (d) => calendarCoversYear(d),
  });

  await sendTelegram(secrets.telegramBotToken!, secrets.telegramChatId!, renderReport(manifest));
  return { status: manifest.status };
}
```

- [ ] **Step 4: Create event fixtures**

`events/daily.json`:
```json
{ "mode": "daily" }
```
`events/backfill.json`:
```json
{ "mode": "backfill", "tradingDay": "2026-06-29" }
```

- [ ] **Step 5: Run full suite + typecheck** — Run: `npx vitest run && npm run typecheck` — Expected: ALL tests PASS; typecheck exits 0.

- [ ] **Step 6: Commit**

```bash
git add src/handler.ts events/ tests/handler.test.ts
git commit -m "feat: add Lambda handler wiring calendar + history cache"
```

---

## Task 20: SAM template + deploy config

**Files:** Create `template.yaml`, `samconfig.toml`

**Interfaces:** Deployable stack (bucket, Lambda, EventBridge Scheduler, Glue DB + 2 tables, Secret declaration, IAM). Verified via `sam validate` / `sam build`.

- [ ] **Step 1: Create `template.yaml`**

```yaml
AWSTemplateFormatVersion: "2010-09-09"
Transform: AWS::Serverless-2016-10-31
Description: EdgeHub Part 1 - Market Data Lake

Globals:
  Function:
    Timeout: 900
    MemorySize: 1024
    Runtime: nodejs24.x

Resources:
  DataBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "edgehub-data-${AWS::AccountId}-${AWS::Region}"  # globally-unique
      VersioningConfiguration: { Status: Enabled }

  EdgeHubSecret:
    Type: AWS::SecretsManager::Secret
    Properties:
      Name: edgehub/secrets
      Description: "Finnhub + Telegram credentials (value set manually once)"

  GlueDatabase:
    Type: AWS::Glue::Database
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseInput: { Name: edgehub }

  EdgeHubCollector:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: edgehub-daily-collector
      Handler: src/handler.handler
      ReservedConcurrentExecutions: 1   # one run at a time — prevents history-cache write races
      Environment:
        Variables:
          BUCKET_NAME: !Ref DataBucket
          GLUE_DATABASE: edgehub
          DATA_PROVIDER: finnhub
          SECRET_NAME: edgehub/secrets
          SCHEMA_VERSION: metrics_v1
          METRIC_VERSION: "1.0"
          SOURCE_VERSION: "1.0"
      Policies:
        - S3CrudPolicy: { BucketName: !Ref DataBucket }
        - Statement:
            - Effect: Allow
              Action: [glue:BatchCreatePartition, glue:GetPartition, glue:GetTable]
              Resource: "*"
            - Effect: Allow
              Action: secretsmanager:GetSecretValue
              Resource: !Ref EdgeHubSecret
    Metadata:
      BuildMethod: esbuild
      BuildProperties:
        Format: esm
        Target: node24
        EntryPoints: [src/handler.ts]

  SchedulerRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal: { Service: scheduler.amazonaws.com }
            Action: sts:AssumeRole
      Policies:
        - PolicyName: InvokeCollector
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Effect: Allow
                Action: lambda:InvokeFunction
                Resource: !GetAtt EdgeHubCollector.Arn

  CollectorSchedule:
    Type: AWS::Scheduler::Schedule
    Properties:
      Name: edgehub-daily-630pm-et
      # 6:30 PM ET per project-plan.md. If the vendor lags, missing bars are recorded and `current`
      # is NOT advanced, so a too-early run is safe; bump to cron(30 19 ...) for 7:30 PM if lag recurs.
      ScheduleExpression: cron(30 18 ? * MON-FRI *)
      ScheduleExpressionTimezone: America/New_York
      FlexibleTimeWindow: { Mode: "OFF" }
      Target:
        Arn: !GetAtt EdgeHubCollector.Arn
        RoleArn: !GetAtt SchedulerRole.Arn
        Input: '{"mode":"daily"}'

  DailyBarsTable:
    Type: AWS::Glue::Table
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseName: !Ref GlueDatabase
      TableInput:
        Name: daily_bars
        TableType: EXTERNAL_TABLE
        PartitionKeys: [{ Name: year, Type: string }, { Name: month, Type: string }, { Name: day, Type: string }]
        Parameters: { classification: parquet }
        StorageDescriptor:
          Location: !Sub "s3://${DataBucket}/raw/finnhub/daily/"
          InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
          OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
          SerdeInfo: { SerializationLibrary: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe }
          Columns:
            - { Name: ticker, Type: string }
            - { Name: date, Type: string }
            - { Name: open, Type: double }
            - { Name: high, Type: double }
            - { Name: low, Type: double }
            - { Name: close, Type: double }
            - { Name: adjustedClose, Type: double }
            - { Name: isAdjusted, Type: boolean }
            - { Name: volume, Type: double }
            - { Name: source, Type: string }
            - { Name: sourceVersion, Type: string }
            - { Name: ingestedAt, Type: string }
            - { Name: runId, Type: string }
            - { Name: schemaVersion, Type: string }
            - { Name: metricVersion, Type: string }
            - { Name: universeVersion, Type: string }

  DailyMetricsTable:
    Type: AWS::Glue::Table
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseName: !Ref GlueDatabase
      TableInput:
        Name: daily_metrics
        TableType: EXTERNAL_TABLE
        PartitionKeys: [{ Name: year, Type: string }, { Name: month, Type: string }, { Name: day, Type: string }]
        Parameters: { classification: parquet }
        StorageDescriptor:
          Location: !Sub "s3://${DataBucket}/metrics/daily/"
          InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
          OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
          SerdeInfo: { SerializationLibrary: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe }
          Columns:
            - { Name: ticker, Type: string }
            - { Name: date, Type: string }
            - { Name: close, Type: double }
            - { Name: dollarVolume, Type: double }
            - { Name: ma20, Type: double }
            - { Name: ma50, Type: double }
            - { Name: ma150, Type: double }
            - { Name: ma200, Type: double }
            - { Name: avgVolume20, Type: double }
            - { Name: avgVolume50, Type: double }
            - { Name: atr14, Type: double }
            - { Name: high52w, Type: double }
            - { Name: low52w, Type: double }
            - { Name: distanceTo52wHighPct, Type: double }
            - { Name: distanceFrom52wLowPct, Type: double }
            - { Name: return21d, Type: double }
            - { Name: return63d, Type: double }
            - { Name: return126d, Type: double }
            - { Name: return252d, Type: double }
            - { Name: above20ma, Type: boolean }
            - { Name: above50ma, Type: boolean }
            - { Name: above150ma, Type: boolean }
            - { Name: above200ma, Type: boolean }
            - { Name: ma150Above200, Type: boolean }
            - { Name: ma200Rising, Type: boolean }
            - { Name: qualityStatus, Type: string }
            - { Name: qualityIssues, Type: string }
            - { Name: runId, Type: string }
            - { Name: ingestedAt, Type: string }
            - { Name: source, Type: string }
            - { Name: sourceVersion, Type: string }
            - { Name: schemaVersion, Type: string }
            - { Name: metricVersion, Type: string }
            - { Name: universeVersion, Type: string }

Outputs:
  CollectorArn:
    Value: !GetAtt EdgeHubCollector.Arn
```

- [ ] **Step 2: Create `samconfig.toml`**

```toml
version = 0.1

[default.global.parameters]
stack_name = "edgehub"

[default.build.parameters]
cached = true
parallel = true

[default.deploy.parameters]
region = "us-east-1"
capabilities = "CAPABILITY_IAM"
confirm_changeset = false
resolve_s3 = true
fail_on_empty_changeset = false
```

- [ ] **Step 3: Validate** — Run: `sam validate --lint` — Expected: "valid SAM Template". (If SAM CLI is absent locally, CI Task 22 runs it; note and proceed.)

- [ ] **Step 4: Build** — Run: `sam build` — Expected: "Build Succeeded" (esbuild bundles `src/handler.ts`).

- [ ] **Step 5: Commit**

```bash
git add template.yaml samconfig.toml
git commit -m "feat: add SAM template and deploy config"
```

---

## Task 21: Data dictionary + bootstrap docs

**Files:** Create `docs/DATA_DICTIONARY.md`, `docs/BOOTSTRAP.md`

- [ ] **Step 1: Create `docs/DATA_DICTIONARY.md`**

````markdown
# EdgeHub Data Dictionary

Generated from `config/metrics.ts` (the metric registry). Add a metric there → add its row here.

## daily_bars (raw)

| Column | Type | Meaning |
|--------|------|---------|
| ticker | string | Symbol |
| date | string | Trading day, YYYY-MM-DD |
| open/high/low/close | double | OHLC (unadjusted) |
| adjustedClose | double/null | Adjusted close; **null on Finnhub free tier** |
| isAdjusted | boolean | Whether adjustedClose reflects splits/dividends (false for Finnhub free) |
| volume | double | Share volume |
| source / sourceVersion | string | Provider + version |
| ingestedAt | string | ISO ingestion timestamp |
| runId, schemaVersion, metricVersion, universeVersion | string | Provenance |

> **Adjustment caveat:** Part 1 stores **unadjusted** Finnhub candles (`isAdjusted=false`, `adjustedClose=null`).
> Do not treat `close` as split/dividend-adjusted. True adjustment + corporate actions arrive in Part 2.

## daily_metrics

Identity & provenance: ticker, date, runId, source, sourceVersion, schemaVersion, metricVersion, universeVersion.

| Metric | Window | Depends on | Meaning |
|--------|--------|-----------|---------|
| dollarVolume | — | close, volume | close × volume |
| ma20 / ma50 / ma150 / ma200 | 20/50/150/200 | close | Simple moving averages |
| avgVolume20 / avgVolume50 | 20/50 | volume | Average volume |
| atr14 | 14 | high, low, close | **SMA of the last 14 true ranges; requires ≥ 15 bars** |
| high52w / low52w | 252 | high / low | 52-week extremes |
| distanceTo52wHighPct | 252 | close, high | % distance to 52w high (≤ 0) |
| distanceFrom52wLowPct | 252 | close, low | % distance above 52w low (≥ 0) |
| return21d / 63d / 126d / 252d | 21/63/126/252 | close | Trailing returns |
| above20ma / 50 / 150 / 200ma | — | close | close above the MA |
| ma150Above200 | — | close | ma150 > ma200 |
| ma200Rising | — | close | ma200 today > prior session (needs 201 bars) |
| qualityStatus | — | — | OK / WARN / REJECTED |
| qualityIssues | — | — | **JSON-encoded string array** of issue codes |

Any metric is `null` when its window exceeds available history.

## Querying rule (IMPORTANT — reruns)

A date partition can contain **multiple `runId`s** (same-day reruns are immutable, not overwrites):

```
metrics/daily/year=2026/month=06/day=29/runId=R1/...
metrics/daily/year=2026/month=06/day=29/runId=R2/...
```

A naive `SELECT ... WHERE year=... AND month=... AND day=...` returns **all** runs → duplicate rows.
Consumers MUST resolve the accepted run from `metadata/current/daily_metrics/<...>.json` and filter:

```sql
SELECT * FROM edgehub.daily_metrics
WHERE year='2026' AND month='06' AND day='29'
  AND runId = '<runId from metadata/current>';
```

> Future convenience (Part 2, optional): a `metrics_current/` prefix holding only the accepted run per
> day, so consumers can query without the runId filter. Immutable history stays in `metrics/`.

## Adjustment caveat (repeat)

Part 1 metrics are computed from **unadjusted** Finnhub candles. Splits/dividends can distort MAs,
returns, and 52-week stats. True adjustment + corporate actions arrive in Part 2.
````

- [ ] **Step 2: Create `docs/BOOTSTRAP.md`**

````markdown
# EdgeHub Bootstrap (one-time manual setup)

Run once before the first GitHub deploy — these create the trust + secret GitHub Actions cannot create for itself.

## 1. GitHub OIDC provider + deploy role

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1

aws iam create-role --role-name edgehub-deploy \
  --assume-role-policy-document file://trust-policy.json

# Simple path for a personal project; tighten later.
aws iam attach-role-policy --role-name edgehub-deploy --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
aws iam attach-role-policy --role-name edgehub-deploy --policy-arn arn:aws:iam::aws:policy/IAMFullAccess
```

`trust-policy.json`:
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com" },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": { "token.actions.githubusercontent.com:aud": "sts.amazonaws.com" },
      "StringLike": { "token.actions.githubusercontent.com:sub": "repo:Atash3000/EdgeHub:ref:refs/heads/main" }
    }
  }]
}
```

Add the role ARN as GitHub repo **variable** `AWS_DEPLOY_ROLE_ARN` (Settings → Secrets and variables → Actions → Variables).

## 2. Secret value

```bash
aws secretsmanager put-secret-value --secret-id edgehub/secrets \
  --secret-string '{"finnhubToken":"<FINNHUB>","telegramBotToken":"<BOT>","telegramChatId":"<CHAT_ID>"}'
```

## 3. First backfill (after first successful deploy)

```bash
aws lambda invoke --function-name edgehub-daily-collector \
  --payload '{"mode":"backfill"}' --cli-binary-format raw-in-base64-out /dev/stdout
```

## 4. Verify Athena

In the Athena console (workgroup with an S3 results location set), run:
```sql
-- runId comes from metadata/current/daily_metrics/<...>.json (see Data Dictionary "Querying rule")
SELECT ticker, close, ma200, return252d FROM edgehub.daily_metrics
WHERE runId = '<runId from metadata/current>' LIMIT 20;
```
Expect rows for the accepted run.
````

- [ ] **Step 3: Commit**

```bash
git add docs/DATA_DICTIONARY.md docs/BOOTSTRAP.md
git commit -m "docs: add data dictionary (adjustment caveat) and bootstrap guide"
```

---

## Task 22: CI workflow

**Files:** Create `.github/workflows/ci.yml`

- [ ] **Step 1: Create `.github/workflows/ci.yml`**

```yaml
name: CI
on:
  pull_request:
    branches: [main]

jobs:
  build-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: "24", cache: npm }
      - run: npm ci
      - run: npm run typecheck
      - run: npm test
      - uses: aws-actions/setup-sam@v2
        with: { use-installer: true }
      - run: sam validate --lint
      - run: sam build
```

- [ ] **Step 2: Verify locally** — Run: `npm ci && npm run typecheck && npm test` — Expected: clean install, typecheck 0, all tests PASS.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add PR build-and-test workflow"
```

---

## Task 23: Deploy workflow (GitHub → AWS via OIDC)

**Files:** Create `.github/workflows/deploy.yml`

**Interfaces:** Push-to-main deploy via OIDC. Consumes repo variable `AWS_DEPLOY_ROLE_ARN` (BOOTSTRAP.md).

- [ ] **Step 1: Create `.github/workflows/deploy.yml`**

```yaml
name: Deploy
on:
  push:
    branches: [main]

permissions:
  id-token: write
  contents: read

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: "24", cache: npm }
      - run: npm ci
      - run: npm run typecheck && npm test
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_DEPLOY_ROLE_ARN }}
          aws-region: us-east-1
      - uses: aws-actions/setup-sam@v2
        with: { use-installer: true }
      - run: sam build
      - run: sam deploy --no-confirm-changeset --no-fail-on-empty-changeset
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/deploy.yml
git commit -m "ci: add OIDC deploy-on-main workflow"
```

- [ ] **Step 3: Push and verify** — Run: `git push origin main` — Expected: the **Deploy** workflow assumes the role via OIDC and `sam deploy` creates/updates the `edgehub` stack. Verify in GitHub Actions + CloudFormation. (Requires Task 21 bootstrap first.)

---

## Self-Review

**1. Spec + review-feedback coverage (rounds 1–4):**
- VendorBar vs RawBarRow split → Tasks 2, 6, 7, 18 (`enrichRaw`). ✓
- No fake adjustedClose; `isAdjusted`/null → Tasks 2, 7, 21. ✓
- No stale-bar fallback; `missing_bar_for_date` → Tasks 6, 7 (exact-date), 18. ✓
- History cache replaces S3 scan → Tasks 14, 18, 19. ✓
- Market calendar gate → Tasks 4, 18, 19. ✓
- qualityIssues as JSON → Tasks 11, 12, 21. ✓
- Early Parquet smoke test → Task 12. ✓
- current advances only on SUCCESS/PARTIAL → Tasks 16, 18. ✓
- Duplicate detection at batch level, quality injected → Tasks 8, 9, 18. ✓
- ATR ≥15-bar note → Tasks 3, 9, 21. ✓
- **Round 4 #1 — ProviderResult { bars, failures }; one bad ticker can't kill the run** → Tasks 2, 6, 7, 18. ✓
- **Round 4 #2 — dedicated `errors.ts` writer; failures/rejects/warnings/missing recorded** → Task 16b, 18. ✓
- **Round 4 #3 — Athena rerun rule (filter by metadata/current runId)** → Task 21 (data dictionary + bootstrap). ✓
- **Round 4 #4 — calendar coverage guard (`calendar_year_missing` → FAILURE, no current advance)** → Tasks 4, 18, 19. ✓
- **Round 4 #5 — ReservedConcurrentExecutions: 1** → Task 20. ✓
- **Round 4 + — account/region-suffixed bucket name; 6:30 PM lag note** → Task 20. ✓
- SAM/single Lambda/OIDC/us-east-1/metrics naming/Parquet/registry/Glue tables/Telegram/Scheduler → Tasks 20, 1, 23, 3, 11, 15, 17. ✓
- Universe versioning + snapshot, runId immutability, manifest → Tasks 5, 16, 18. ✓

**2. Placeholder scan:** No "TBD"/"add error handling"/"similar to Task N". Every code step shows complete code. ✓

**3. Type consistency:** `VendorBar`/`RawBarRow`/`MetricRow`/`Provenance`/`RunManifest`/`ProviderResult`/`ProviderFailure`/`ErrorRecord` defined in Task 2, used unchanged downstream. `MarketDataProvider` returns `ProviderResult` consistently (Tasks 6/7/18). `computeMetrics(bars, prov, quality)` matches caller in Task 18. `gradeBar(bar, seen)` consistent Tasks 8/18. `Deps` fields `readHistory`/`writeHistory`/`isTradingDay`/`calendarCovers` (Task 18) match handler wiring (Task 19). `RunManifest.note` set by early-exits (Task 18) and rendered (Task 17). runId paths identical in storage (11), metadata (16), errors (16b). ✓

**Note on reserved prefixes:** `labels/` and `corporate_actions/` need no task — S3 has no real folders; the first Part 2 write creates them. Documented, not omitted.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-29-edgehub-part1-market-data-lake.md`.
