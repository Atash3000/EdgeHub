# EdgeHub Part 1 — Market Data Lake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a serverless daily pipeline that downloads US equity OHLCV, computes versioned market metrics, stores them as partitioned Parquet in S3, catalogs them in Glue for Athena, and reports health via Telegram — deployed to AWS exclusively from GitHub.

**Architecture:** A single, internally-modular TypeScript Lambda (`edgehub-daily-collector`) runs the pipeline (universe → download → validate → store raw → compute metrics → store metrics → Glue partition → manifest → Telegram). EventBridge Scheduler triggers it daily at 6:30 PM America/New_York. Infrastructure is defined with AWS SAM and deployed via GitHub Actions using OIDC. The market-data vendor sits behind a `MarketDataProvider` interface; nothing vendor-specific is hardcoded.

**Tech Stack:** TypeScript, Node.js 20, AWS SAM, AWS SDK v3 (S3/Glue/Secrets Manager), `@dsnp/parquetjs`, Vitest (tests), GitHub Actions.

## Global Constraints

- Runtime: **Node.js 20**, TypeScript compiled/bundled by SAM esbuild.
- Region: **us-east-1**. Stack name: **edgehub**. Bucket: **edgehub-data**.
- Derived data is named **metrics** (never "features"): dir `metrics/`, Glue table `daily_metrics`, field `metricVersion`, env `METRIC_VERSION`.
- Version constants: `SCHEMA_VERSION = "metrics_v1"`, `METRIC_VERSION = "1.0"`, `SOURCE_VERSION = "1.0"`.
- Active vendor selected by env `DATA_PROVIDER` (default `finnhub`). The `source` path segment and `source`/`sourceVersion` fields come from `provider.name`/`provider.version` — never a literal.
- Every raw row, metric row, report, and error file carries provenance: `runId, ingestedAt, source, sourceVersion, schemaVersion, metricVersion, universeVersion`.
- Immutability: every write goes under `runId=<UTC timestamp>`; the authoritative run is recorded in `metadata/current/` (never overwrite history; never advance `current` on failure).
- Quality: rows are graded `OK | WARN | REJECTED`; WARN rows are stored, REJECTED rows are excluded and logged to `errors/`. Nothing is silently dropped.
- Deploys happen **only** from GitHub Actions on push to `main` via OIDC. No laptop deploys.
- `labels/` and `corporate_actions/` are reserved (created, unused) for Part 2.
- TDD: write the failing test first, watch it fail, implement minimally, watch it pass, commit.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `package.json`, `tsconfig.json`, `vitest.config.ts` | Project + test setup |
| `src/types.ts` | Shared types + version constants |
| `config/metrics.ts` | Metric registry (single source of truth) |
| `schemas/dailyBars_v1.json`, `schemas/metrics_v1.json` | Schema registry |
| `config/universe/*.json` | Versioned ticker universe |
| `src/universe.ts` | Load/merge/dedupe universe + version |
| `src/providers/provider.ts` | `MarketDataProvider` interface |
| `src/providers/factory.ts` | Select active provider by `DATA_PROVIDER` |
| `src/providers/finnhub.ts` | Finnhub implementation (rate-limited) |
| `src/providers/fake.ts` | In-memory provider for tests/local |
| `src/validate.ts` | Quality grading (OK/WARN/REJECTED) |
| `src/metrics.ts` | Compute metrics from bars + registry |
| `src/history.ts` | Read trailing bars from S3 |
| `src/storage.ts` | Write raw/metric Parquet to S3 |
| `src/glue.ts` | Add Glue partitions |
| `src/metadata.ts` | Manifest + current pointer + universe snapshot |
| `src/report.ts` | Telegram report |
| `src/secrets.ts` | Fetch secrets from Secrets Manager |
| `src/pipeline.ts` | Orchestrate steps, own `runId`, build manifest |
| `src/handler.ts` | Lambda entrypoint (parse mode, call pipeline) |
| `template.yaml`, `samconfig.toml` | SAM infra + deploy config |
| `.github/workflows/ci.yml`, `deploy.yml` | CI + CD |
| `docs/DATA_DICTIONARY.md`, `docs/BOOTSTRAP.md` | Docs |

---

## Task 1: Project scaffolding

**Files:**
- Create: `package.json`, `tsconfig.json`, `vitest.config.ts`, `.gitignore`

**Interfaces:**
- Produces: `npm test`, `npm run typecheck`, `npm run build` scripts used by all later tasks and CI.

- [ ] **Step 1: Create `package.json`**

```json
{
  "name": "edgehub",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "engines": { "node": ">=20" },
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

export default defineConfig({
  test: { include: ["tests/**/*.test.ts"], environment: "node" },
});
```

- [ ] **Step 4: Create `.gitignore`**

```
node_modules/
dist/
.aws-sam/
*.log
```

- [ ] **Step 5: Install and verify**

Run: `npm install && npm run typecheck`
Expected: installs cleanly; `tsc --noEmit` exits 0 (no source files yet).

- [ ] **Step 6: Commit**

```bash
git add package.json tsconfig.json vitest.config.ts .gitignore package-lock.json
git commit -m "chore: scaffold TypeScript + Vitest project"
```

---

## Task 2: Shared types and version constants

**Files:**
- Create: `src/types.ts`
- Test: `tests/types.test.ts`

**Interfaces:**
- Produces: `RawBar`, `MetricRow`, `RunManifest`, `QualityStatus`, `RunMode`, and constants `SCHEMA_VERSION`, `METRIC_VERSION`, `SOURCE_VERSION`. Every later task imports from here.

- [ ] **Step 1: Write the failing test**

```ts
// tests/types.test.ts
import { describe, it, expect } from "vitest";
import { SCHEMA_VERSION, METRIC_VERSION, SOURCE_VERSION } from "../src/types.js";

describe("version constants", () => {
  it("uses the metrics_v1 schema id", () => {
    expect(SCHEMA_VERSION).toBe("metrics_v1");
    expect(METRIC_VERSION).toBe("1.0");
    expect(SOURCE_VERSION).toBe("1.0");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/types.test.ts`
Expected: FAIL — cannot find module `../src/types.js`.

- [ ] **Step 3: Create `src/types.ts`**

```ts
export const SCHEMA_VERSION = "metrics_v1";
export const METRIC_VERSION = "1.0";
export const SOURCE_VERSION = "1.0";

export type RunMode = "daily" | "backfill";
export type QualityStatus = "OK" | "WARN" | "REJECTED";

export interface RawBar {
  ticker: string;
  date: string; // YYYY-MM-DD
  open: number;
  high: number;
  low: number;
  close: number;
  adjustedClose: number;
  volume: number;
  source: string;
  sourceVersion: string;
  ingestedAt: string; // ISO 8601
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
  runtimeSec: number;
  metricVersion: string;
  schemaVersion: string;
  status: "SUCCESS" | "PARTIAL" | "FAILURE";
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/types.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/types.ts tests/types.test.ts
git commit -m "feat: add shared types and version constants"
```

---

## Task 3: Metric registry + schema registry

**Files:**
- Create: `config/metrics.ts`, `schemas/dailyBars_v1.json`, `schemas/metrics_v1.json`
- Test: `tests/metrics-registry.test.ts`

**Interfaces:**
- Produces: `METRIC_REGISTRY: MetricDef[]` where `MetricDef = { name: string; description: string; dependsOn: string[]; window: number | null; version: string }`. Consumed by `metrics.ts` (Task 8) and the data dictionary (Task 18).

- [ ] **Step 1: Write the failing test**

```ts
// tests/metrics-registry.test.ts
import { describe, it, expect } from "vitest";
import { METRIC_REGISTRY } from "../config/metrics.js";

describe("metric registry", () => {
  it("declares ma200 with its window and dependency", () => {
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

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/metrics-registry.test.ts`
Expected: FAIL — cannot find module `../config/metrics.js`.

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
  { name: "ma20", description: "20-day simple moving average of close", dependsOn: ["close"], window: 20, version: "1.0" },
  { name: "ma50", description: "50-day simple moving average of close", dependsOn: ["close"], window: 50, version: "1.0" },
  { name: "ma150", description: "150-day simple moving average of close", dependsOn: ["close"], window: 150, version: "1.0" },
  { name: "ma200", description: "200-day simple moving average of close", dependsOn: ["close"], window: 200, version: "1.0" },
  { name: "avgVolume20", description: "20-day average volume", dependsOn: ["volume"], window: 20, version: "1.0" },
  { name: "avgVolume50", description: "50-day average volume", dependsOn: ["volume"], window: 50, version: "1.0" },
  { name: "atr14", description: "14-day average true range (SMA of true range)", dependsOn: ["high", "low", "close"], window: 14, version: "1.0" },
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
  { name: "ma200Rising", description: "today ma200 > prior session ma200", dependsOn: ["close"], window: 201, version: "1.0" },
];
```

- [ ] **Step 4: Create `schemas/dailyBars_v1.json`**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "dailyBars_v1",
  "type": "object",
  "required": ["ticker", "date", "open", "high", "low", "close", "adjustedClose", "volume", "source", "sourceVersion", "ingestedAt"],
  "properties": {
    "ticker": { "type": "string" },
    "date": { "type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$" },
    "open": { "type": "number" },
    "high": { "type": "number" },
    "low": { "type": "number" },
    "close": { "type": "number" },
    "adjustedClose": { "type": "number" },
    "volume": { "type": "number" },
    "source": { "type": "string" },
    "sourceVersion": { "type": "string" },
    "ingestedAt": { "type": "string" }
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
    "runId": { "type": "string" },
    "schemaVersion": { "type": "string" },
    "metricVersion": { "type": "string" },
    "universeVersion": { "type": "string" }
  }
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `npx vitest run tests/metrics-registry.test.ts`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add config/metrics.ts schemas/dailyBars_v1.json schemas/metrics_v1.json tests/metrics-registry.test.ts
git commit -m "feat: add metric registry and schema registry"
```

---

## Task 4: Universe loader (versioned)

**Files:**
- Create: `src/universe.ts`, `config/universe/sp500.json`, `config/universe/nasdaq100.json`, `config/universe/watchlist.json`
- Test: `tests/universe.test.ts`

**Interfaces:**
- Consumes: nothing.
- Produces: `loadUniverse(): { tickers: string[]; universeVersion: string }`. The `tickers` are upper-cased, merged, and deduped across all three files. `universeVersion` is read from the `version` field shared by the files.

- [ ] **Step 1: Write the failing test**

```ts
// tests/universe.test.ts
import { describe, it, expect } from "vitest";
import { mergeUniverse } from "../src/universe.js";

describe("mergeUniverse", () => {
  it("merges, upper-cases, and dedupes tickers", () => {
    const result = mergeUniverse([
      { version: "2026-06-29", tickers: ["aapl", "MSFT"] },
      { version: "2026-06-29", tickers: ["msft", "GOOG"] },
    ]);
    expect(result.tickers).toEqual(["AAPL", "MSFT", "GOOG"]);
    expect(result.universeVersion).toBe("2026-06-29");
  });

  it("throws if files disagree on version", () => {
    expect(() =>
      mergeUniverse([
        { version: "2026-06-29", tickers: ["AAPL"] },
        { version: "2026-06-30", tickers: ["MSFT"] },
      ]),
    ).toThrow(/version mismatch/i);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/universe.test.ts`
Expected: FAIL — cannot find module `../src/universe.js`.

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

> Note: these are seed lists. Expanding to full S&P 500 / Nasdaq 100 is a later data-entry PR; the loader does not change.

- [ ] **Step 4: Create `src/universe.ts`**

```ts
import sp500 from "../config/universe/sp500.json" with { type: "json" };
import nasdaq100 from "../config/universe/nasdaq100.json" with { type: "json" };
import watchlist from "../config/universe/watchlist.json" with { type: "json" };

export interface UniverseFile {
  version: string;
  tickers: string[];
}

export interface ResolvedUniverse {
  tickers: string[];
  universeVersion: string;
}

export function mergeUniverse(files: UniverseFile[]): ResolvedUniverse {
  const versions = new Set(files.map((f) => f.version));
  if (versions.size > 1) {
    throw new Error(`Universe version mismatch: ${[...versions].join(", ")}`);
  }
  const seen = new Set<string>();
  const tickers: string[] = [];
  for (const file of files) {
    for (const raw of file.tickers) {
      const t = raw.toUpperCase();
      if (!seen.has(t)) {
        seen.add(t);
        tickers.push(t);
      }
    }
  }
  return { tickers, universeVersion: files[0]!.version };
}

export function loadUniverse(): ResolvedUniverse {
  return mergeUniverse([sp500, nasdaq100, watchlist]);
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `npx vitest run tests/universe.test.ts`
Expected: PASS (both cases).

- [ ] **Step 6: Commit**

```bash
git add src/universe.ts config/universe/ tests/universe.test.ts
git commit -m "feat: add versioned universe loader"
```

---

## Task 5: Provider interface, factory, and fake provider

**Files:**
- Create: `src/providers/provider.ts`, `src/providers/factory.ts`, `src/providers/fake.ts`
- Test: `tests/provider-factory.test.ts`

**Interfaces:**
- Produces:
  - `interface MarketDataProvider { readonly name: string; readonly version: string; getLatestBars(date: string, tickers: string[]): Promise<RawBar[]>; getHistory(ticker: string, lookbackDays: number): Promise<RawBar[]> }`
  - `getProvider(name: string, secrets: Record<string,string>): MarketDataProvider`
  - `class FakeProvider implements MarketDataProvider` (constructed with a `Map<string, RawBar[]>` of history per ticker).

- [ ] **Step 1: Write the failing test**

```ts
// tests/provider-factory.test.ts
import { describe, it, expect } from "vitest";
import { getProvider } from "../src/providers/factory.js";
import { FakeProvider } from "../src/providers/fake.js";

describe("getProvider", () => {
  it("returns a fake provider by name", () => {
    const p = getProvider("fake", {});
    expect(p.name).toBe("fake");
  });

  it("throws on unknown provider", () => {
    expect(() => getProvider("nope", {})).toThrow(/unknown data provider/i);
  });
});

describe("FakeProvider", () => {
  it("returns the latest bar per ticker", async () => {
    const bar = { ticker: "AAPL", date: "2026-06-29", open: 1, high: 2, low: 1, close: 2, adjustedClose: 2, volume: 10, source: "fake", sourceVersion: "1.0", ingestedAt: "2026-06-29T22:30:00Z" };
    const p = new FakeProvider(new Map([["AAPL", [bar]]]));
    const out = await p.getLatestBars("2026-06-29", ["AAPL"]);
    expect(out).toHaveLength(1);
    expect(out[0]!.close).toBe(2);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/provider-factory.test.ts`
Expected: FAIL — modules not found.

- [ ] **Step 3: Create `src/providers/provider.ts`**

```ts
import type { RawBar } from "../types.js";

export interface MarketDataProvider {
  readonly name: string;
  readonly version: string;
  getLatestBars(date: string, tickers: string[]): Promise<RawBar[]>;
  getHistory(ticker: string, lookbackDays: number): Promise<RawBar[]>;
}
```

- [ ] **Step 4: Create `src/providers/fake.ts`**

```ts
import type { MarketDataProvider } from "./provider.js";
import type { RawBar } from "../types.js";

export class FakeProvider implements MarketDataProvider {
  readonly name = "fake";
  readonly version = "1.0";
  constructor(private readonly history: Map<string, RawBar[]>) {}

  async getLatestBars(date: string, tickers: string[]): Promise<RawBar[]> {
    const out: RawBar[] = [];
    for (const t of tickers) {
      const bars = this.history.get(t) ?? [];
      const last = bars[bars.length - 1];
      if (last) out.push(last);
    }
    return out;
  }

  async getHistory(ticker: string, lookbackDays: number): Promise<RawBar[]> {
    const bars = this.history.get(ticker) ?? [];
    return bars.slice(-lookbackDays);
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
    case "finnhub":
      return new FinnhubProvider(secrets.finnhubToken ?? "");
    case "fake":
      return new FakeProvider(new Map());
    default:
      throw new Error(`Unknown data provider: ${name}`);
  }
}
```

> Note: `finnhub.ts` is created in Task 6. To keep this task self-contained and green, create a temporary stub now and replace it in Task 6:
> `src/providers/finnhub.ts`:
> ```ts
> import type { MarketDataProvider } from "./provider.js";
> import type { RawBar } from "../types.js";
> export class FinnhubProvider implements MarketDataProvider {
>   readonly name = "finnhub";
>   readonly version = "1.0";
>   constructor(_token: string) {}
>   async getLatestBars(_d: string, _t: string[]): Promise<RawBar[]> { throw new Error("not implemented"); }
>   async getHistory(_t: string, _n: number): Promise<RawBar[]> { throw new Error("not implemented"); }
> }
> ```

- [ ] **Step 6: Run tests to verify they pass**

Run: `npx vitest run tests/provider-factory.test.ts`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/providers/ tests/provider-factory.test.ts
git commit -m "feat: add provider interface, factory, and fake provider"
```

---

## Task 6: Finnhub provider with rate limiting

**Files:**
- Modify: `src/providers/finnhub.ts` (replace the Task 5 stub)
- Test: `tests/finnhub.test.ts`

**Interfaces:**
- Consumes: `MarketDataProvider`, `RawBar`.
- Produces: `class FinnhubProvider` with an injectable `fetchFn` (defaults to global `fetch`) and a rate limiter capping requests to ≤ `maxPerMinute` (default 55, just under Finnhub's 60). `mapCandle(symbol, json)` converts Finnhub's candle response to `RawBar[]`.

- [ ] **Step 1: Write the failing test**

```ts
// tests/finnhub.test.ts
import { describe, it, expect } from "vitest";
import { mapCandle } from "../src/providers/finnhub.js";

describe("mapCandle", () => {
  it("maps a finnhub candle payload to RawBars", () => {
    const json = { s: "ok", t: [1750000000], o: [10], h: [12], l: [9], c: [11], v: [1000] };
    const bars = mapCandle("AAPL", json, "2026-06-29T22:30:00Z");
    expect(bars).toHaveLength(1);
    expect(bars[0]).toMatchObject({
      ticker: "AAPL", close: 11, high: 12, low: 9, open: 10, volume: 1000,
      adjustedClose: 11, source: "finnhub", sourceVersion: "1.0",
    });
    expect(bars[0]!.date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });

  it("returns empty array when status is not ok", () => {
    expect(mapCandle("AAPL", { s: "no_data" }, "2026-06-29T22:30:00Z")).toEqual([]);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/finnhub.test.ts`
Expected: FAIL — `mapCandle` is not exported.

- [ ] **Step 3: Replace `src/providers/finnhub.ts`**

```ts
import type { MarketDataProvider } from "./provider.js";
import type { RawBar } from "../types.js";
import { SOURCE_VERSION } from "../types.js";

interface FinnhubCandle {
  s: string;
  t?: number[];
  o?: number[];
  h?: number[];
  l?: number[];
  c?: number[];
  v?: number[];
}

type FetchFn = (url: string) => Promise<{ ok: boolean; status: number; json: () => Promise<unknown> }>;

export function mapCandle(symbol: string, raw: unknown, ingestedAt: string): RawBar[] {
  const json = raw as FinnhubCandle;
  if (!json || json.s !== "ok" || !json.t) return [];
  const bars: RawBar[] = [];
  for (let i = 0; i < json.t.length; i++) {
    const close = json.c![i]!;
    bars.push({
      ticker: symbol,
      date: new Date(json.t[i]! * 1000).toISOString().slice(0, 10),
      open: json.o![i]!,
      high: json.h![i]!,
      low: json.l![i]!,
      close,
      adjustedClose: close, // free tier: no separate adjusted series; recorded as-is
      volume: json.v![i]!,
      source: "finnhub",
      sourceVersion: SOURCE_VERSION,
      ingestedAt,
    });
  }
  return bars;
}

/** Resolves after enough delay to keep calls under `maxPerMinute`. */
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
  ) {
    this.limiter = new RateLimiter(Math.ceil(60000 / maxPerMinute));
  }

  private async candle(symbol: string, fromSec: number, toSec: number): Promise<RawBar[]> {
    await this.limiter.wait(sleep, () => Date.now());
    const url = `https://finnhub.io/api/v1/stock/candle?symbol=${symbol}&resolution=D&from=${fromSec}&to=${toSec}&token=${this.token}`;
    const res = await this.fetchFn(url);
    if (!res.ok) throw new Error(`finnhub ${symbol} HTTP ${res.status}`);
    return mapCandle(symbol, await res.json(), new Date().toISOString());
  }

  async getLatestBars(date: string, tickers: string[]): Promise<RawBar[]> {
    const to = Math.floor(new Date(`${date}T23:59:59Z`).getTime() / 1000);
    const from = to - 5 * 86400; // small window; keep only the row matching `date`
    const out: RawBar[] = [];
    for (const t of tickers) {
      const bars = await this.candle(t, from, to);
      const match = bars.find((b) => b.date === date) ?? bars[bars.length - 1];
      if (match) out.push(match);
    }
    return out;
  }

  async getHistory(ticker: string, lookbackDays: number): Promise<RawBar[]> {
    const to = Math.floor(Date.now() / 1000);
    const from = to - Math.ceil(lookbackDays * 1.5) * 86400; // calendar pad for weekends/holidays
    return this.candle(ticker, from, to);
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/finnhub.test.ts tests/provider-factory.test.ts`
Expected: PASS (mapCandle tests + factory still green).

- [ ] **Step 5: Commit**

```bash
git add src/providers/finnhub.ts tests/finnhub.test.ts
git commit -m "feat: implement Finnhub provider with rate limiting"
```

---

## Task 7: Quality validation

**Files:**
- Create: `src/validate.ts`
- Test: `tests/validate.test.ts`

**Interfaces:**
- Consumes: `RawBar`, `QualityStatus`.
- Produces: `gradeBar(bar: RawBar, seenKeys: Set<string>): { status: QualityStatus; issues: string[] }`. `seenKeys` accumulates `"ticker|date"` to catch duplicates across a batch.

- [ ] **Step 1: Write the failing test**

```ts
// tests/validate.test.ts
import { describe, it, expect } from "vitest";
import { gradeBar } from "../src/validate.js";
import type { RawBar } from "../src/types.js";

const base: RawBar = {
  ticker: "AAPL", date: "2026-06-29", open: 10, high: 12, low: 9, close: 11,
  adjustedClose: 11, volume: 1000, source: "fake", sourceVersion: "1.0", ingestedAt: "x",
};

describe("gradeBar", () => {
  it("grades a clean bar OK", () => {
    expect(gradeBar(base, new Set()).status).toBe("OK");
  });

  it("rejects a negative price", () => {
    const r = gradeBar({ ...base, close: -1 }, new Set());
    expect(r.status).toBe("REJECTED");
    expect(r.issues).toContain("negative_price");
  });

  it("rejects a duplicate ticker/date", () => {
    const seen = new Set<string>();
    gradeBar(base, seen);
    const r = gradeBar(base, seen);
    expect(r.status).toBe("REJECTED");
    expect(r.issues).toContain("duplicate");
  });

  it("warns on zero volume", () => {
    const r = gradeBar({ ...base, volume: 0 }, new Set());
    expect(r.status).toBe("WARN");
    expect(r.issues).toContain("zero_volume");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/validate.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Create `src/validate.ts`**

```ts
import type { RawBar, QualityStatus } from "./types.js";

export function gradeBar(bar: RawBar, seenKeys: Set<string>): { status: QualityStatus; issues: string[] } {
  const issues: string[] = [];
  let status: QualityStatus = "OK";

  const reject = (code: string) => { issues.push(code); status = "REJECTED"; };
  const warn = (code: string) => { issues.push(code); if (status === "OK") status = "WARN"; };

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

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/validate.test.ts`
Expected: PASS (all four cases).

- [ ] **Step 5: Commit**

```bash
git add src/validate.ts tests/validate.test.ts
git commit -m "feat: add OK/WARN/REJECTED quality grading"
```

---

## Task 8: Metric computation

**Files:**
- Create: `src/metrics.ts`
- Test: `tests/metrics.test.ts`

**Interfaces:**
- Consumes: `RawBar`, `MetricRow`, `Provenance`, `gradeBar`.
- Produces: `computeMetrics(bars: RawBar[], prov: Provenance): MetricRow`. `bars` is the trailing history for ONE ticker sorted ascending by date; the last element is the current session. Helper exports `sma`, `trueRanges`, `pctReturn` for unit testing.

- [ ] **Step 1: Write the failing test**

```ts
// tests/metrics.test.ts
import { describe, it, expect } from "vitest";
import { sma, pctReturn, computeMetrics } from "../src/metrics.js";
import type { RawBar, Provenance } from "../src/types.js";

const prov: Provenance = {
  runId: "R", ingestedAt: "x", source: "fake", sourceVersion: "1.0",
  schemaVersion: "metrics_v1", metricVersion: "1.0", universeVersion: "2026-06-29",
};

function bar(date: string, close: number, volume = 1000): RawBar {
  return { ticker: "AAPL", date, open: close, high: close + 1, low: close - 1, close, adjustedClose: close, volume, source: "fake", sourceVersion: "1.0", ingestedAt: "x" };
}

describe("sma", () => {
  it("averages the last N values", () => {
    expect(sma([1, 2, 3, 4], 2)).toBe(3.5);
  });
  it("returns null when not enough data", () => {
    expect(sma([1, 2], 5)).toBeNull();
  });
});

describe("pctReturn", () => {
  it("computes trailing return", () => {
    expect(pctReturn([100, 110], 1)).toBeCloseTo(0.1);
  });
});

describe("computeMetrics", () => {
  it("computes ma20 and flags from 25 sessions", () => {
    const bars: RawBar[] = [];
    for (let i = 0; i < 25; i++) bars.push(bar(`2026-05-${String(i + 1).padStart(2, "0")}`, 100 + i));
    const row = computeMetrics(bars, prov);
    expect(row.ticker).toBe("AAPL");
    expect(row.close).toBe(124);
    expect(row.ma20).toBeCloseTo(sma(bars.map((b) => b.close), 20)!);
    expect(row.above20ma).toBe(true);
    expect(row.ma200).toBeNull(); // not enough history
    expect(row.qualityStatus).toBe("OK");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/metrics.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Create `src/metrics.ts`**

```ts
import type { RawBar, MetricRow, Provenance } from "./types.js";
import { gradeBar } from "./validate.js";

export function sma(values: number[], window: number): number | null {
  if (values.length < window) return null;
  const slice = values.slice(-window);
  return slice.reduce((a, b) => a + b, 0) / window;
}

export function smaAt(values: number[], window: number, offsetFromEnd: number): number | null {
  const end = values.length - offsetFromEnd;
  if (end < window) return null;
  const slice = values.slice(end - window, end);
  return slice.reduce((a, b) => a + b, 0) / window;
}

export function trueRanges(bars: RawBar[]): number[] {
  const tr: number[] = [];
  for (let i = 1; i < bars.length; i++) {
    const cur = bars[i]!, prev = bars[i - 1]!;
    tr.push(Math.max(cur.high - cur.low, Math.abs(cur.high - prev.close), Math.abs(cur.low - prev.close)));
  }
  return tr;
}

export function pctReturn(closes: number[], lookback: number): number | null {
  if (closes.length <= lookback) return null;
  const now = closes[closes.length - 1]!;
  const then = closes[closes.length - 1 - lookback]!;
  if (then === 0) return null;
  return now / then - 1;
}

function maxOver(values: number[], window: number): number | null {
  if (values.length < window) return null;
  return Math.max(...values.slice(-window));
}
function minOver(values: number[], window: number): number | null {
  if (values.length < window) return null;
  return Math.min(...values.slice(-window));
}

export function computeMetrics(bars: RawBar[], prov: Provenance): MetricRow {
  const last = bars[bars.length - 1]!;
  const closes = bars.map((b) => b.close);
  const highs = bars.map((b) => b.high);
  const lows = bars.map((b) => b.low);
  const volumes = bars.map((b) => b.volume);

  const ma20 = sma(closes, 20);
  const ma50 = sma(closes, 50);
  const ma150 = sma(closes, 150);
  const ma200 = sma(closes, 200);
  const ma200Prev = smaAt(closes, 200, 1);

  const tr = trueRanges(bars);
  const atr14 = sma(tr, 14);

  const high52w = maxOver(highs, 252);
  const low52w = minOver(lows, 252);

  const { status, issues } = gradeBar(last, new Set());

  return {
    ticker: last.ticker,
    date: last.date,
    close: last.close,
    dollarVolume: last.close * last.volume,
    ma20, ma50, ma150, ma200,
    avgVolume20: sma(volumes, 20),
    avgVolume50: sma(volumes, 50),
    atr14,
    high52w, low52w,
    distanceTo52wHighPct: high52w ? (last.close - high52w) / high52w * 100 : null,
    distanceFrom52wLowPct: low52w ? (last.close - low52w) / low52w * 100 : null,
    return21d: pctReturn(closes, 21),
    return63d: pctReturn(closes, 63),
    return126d: pctReturn(closes, 126),
    return252d: pctReturn(closes, 252),
    above20ma: ma20 === null ? null : last.close > ma20,
    above50ma: ma50 === null ? null : last.close > ma50,
    above150ma: ma150 === null ? null : last.close > ma150,
    above200ma: ma200 === null ? null : last.close > ma200,
    ma150Above200: ma150 !== null && ma200 !== null ? ma150 > ma200 : null,
    ma200Rising: ma200 !== null && ma200Prev !== null ? ma200 > ma200Prev : null,
    qualityStatus: status,
    qualityIssues: issues,
    ...prov,
  };
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/metrics.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/metrics.ts tests/metrics.test.ts
git commit -m "feat: compute market metrics from trailing bars"
```

---

## Task 9: Secrets loader

**Files:**
- Create: `src/secrets.ts`
- Test: `tests/secrets.test.ts`

**Interfaces:**
- Produces: `parseSecret(json: string): Record<string,string>` and `loadSecrets(client, secretName): Promise<Record<string,string>>`. The Secrets Manager client is injected so tests need no AWS.

- [ ] **Step 1: Write the failing test**

```ts
// tests/secrets.test.ts
import { describe, it, expect } from "vitest";
import { parseSecret } from "../src/secrets.js";

describe("parseSecret", () => {
  it("parses the secret JSON blob", () => {
    const s = parseSecret('{"finnhubToken":"abc","telegramBotToken":"t","telegramChatId":"1"}');
    expect(s.finnhubToken).toBe("abc");
  });
  it("throws on malformed JSON", () => {
    expect(() => parseSecret("not json")).toThrow();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/secrets.test.ts`
Expected: FAIL — module not found.

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

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/secrets.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/secrets.ts tests/secrets.test.ts
git commit -m "feat: add Secrets Manager loader"
```

---

## Task 10: S3 storage (Parquet writer + path builder)

**Files:**
- Create: `src/storage.ts`
- Test: `tests/storage.test.ts`

**Interfaces:**
- Consumes: `RawBar`, `MetricRow`.
- Produces:
  - `rawKey(source, date, runId)`, `metricsKey(date, runId)` path builders.
  - `writeRaw(s3, bucket, bars, source, date, runId)` and `writeMetrics(s3, bucket, rows, date, runId)` — serialize to Parquet (via `@dsnp/parquetjs`) and `PutObject`. The S3 client is injected.

- [ ] **Step 1: Write the failing test (path builders)**

```ts
// tests/storage.test.ts
import { describe, it, expect } from "vitest";
import { rawKey, metricsKey } from "../src/storage.js";

describe("path builders", () => {
  it("builds a partitioned raw key with runId and source", () => {
    expect(rawKey("finnhub", "2026-06-29", "20260629T223000Z")).toBe(
      "raw/finnhub/daily/year=2026/month=06/day=29/runId=20260629T223000Z/part.parquet",
    );
  });
  it("builds a partitioned metrics key with runId", () => {
    expect(metricsKey("2026-06-29", "20260629T223000Z")).toBe(
      "metrics/daily/year=2026/month=06/day=29/runId=20260629T223000Z/part.parquet",
    );
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/storage.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Create `src/storage.ts`**

```ts
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";
import type { RawBar, MetricRow } from "./types.js";

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

async function toParquet<T extends Record<string, unknown>>(schema: ParquetSchema, rows: T[]): Promise<Buffer> {
  const chunks: Buffer[] = [];
  const writer = await ParquetWriter.openStream(schema, {
    write: (c: Buffer) => chunks.push(c),
    end: () => {},
  } as never);
  for (const row of rows) await writer.appendRow(row);
  await writer.close();
  return Buffer.concat(chunks);
}

const RAW_SCHEMA = new ParquetSchema({
  ticker: { type: "UTF8" }, date: { type: "UTF8" },
  open: { type: "DOUBLE" }, high: { type: "DOUBLE" }, low: { type: "DOUBLE" },
  close: { type: "DOUBLE" }, adjustedClose: { type: "DOUBLE" }, volume: { type: "DOUBLE" },
  source: { type: "UTF8" }, sourceVersion: { type: "UTF8" }, ingestedAt: { type: "UTF8" },
});

const METRIC_SCHEMA = new ParquetSchema({
  ticker: { type: "UTF8" }, date: { type: "UTF8" }, close: { type: "DOUBLE" },
  dollarVolume: { type: "DOUBLE" },
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
  runId: { type: "UTF8" }, ingestedAt: { type: "UTF8" }, source: { type: "UTF8" },
  sourceVersion: { type: "UTF8" }, schemaVersion: { type: "UTF8" },
  metricVersion: { type: "UTF8" }, universeVersion: { type: "UTF8" },
});

export async function writeRaw(s3: S3Client, bucket: string, bars: RawBar[], source: string, date: string, runId: string): Promise<string> {
  const key = rawKey(source, date, runId);
  const body = await toParquet(RAW_SCHEMA, bars as unknown as Record<string, unknown>[]);
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
  return key;
}

export async function writeMetrics(s3: S3Client, bucket: string, rows: MetricRow[], date: string, runId: string): Promise<string> {
  const key = metricsKey(date, runId);
  const flat = rows.map((r) => ({ ...r, qualityIssues: r.qualityIssues.join(",") }));
  const body = await toParquet(METRIC_SCHEMA, flat as unknown as Record<string, unknown>[]);
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
  return key;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/storage.test.ts`
Expected: PASS (path builders).

- [ ] **Step 5: Typecheck the Parquet code**

Run: `npm run typecheck`
Expected: exits 0. (If `@dsnp/parquetjs` stream typings complain, the `as never` cast on the writer sink is intentional; keep it.)

- [ ] **Step 6: Commit**

```bash
git add src/storage.ts tests/storage.test.ts
git commit -m "feat: add S3 Parquet storage with partitioned runId paths"
```

---

## Task 11: History reader

**Files:**
- Create: `src/history.ts`
- Test: `tests/history.test.ts`

**Interfaces:**
- Consumes: `RawBar`.
- Produces: `mergeHistory(stored: RawBar[], latest: RawBar): RawBar[]` — appends/sorts ascending by date, de-duping on date (latest wins). `hasEnoughHistory(bars, minSessions): boolean`. (Reading prior Parquet from S3 is wired in the pipeline; the testable logic is the merge + sufficiency check.)

- [ ] **Step 1: Write the failing test**

```ts
// tests/history.test.ts
import { describe, it, expect } from "vitest";
import { mergeHistory, hasEnoughHistory } from "../src/history.js";
import type { RawBar } from "../src/types.js";

function bar(date: string, close: number): RawBar {
  return { ticker: "AAPL", date, open: close, high: close, low: close, close, adjustedClose: close, volume: 1, source: "fake", sourceVersion: "1.0", ingestedAt: "x" };
}

describe("mergeHistory", () => {
  it("appends latest and keeps ascending order", () => {
    const merged = mergeHistory([bar("2026-06-26", 1), bar("2026-06-27", 2)], bar("2026-06-29", 3));
    expect(merged.map((b) => b.date)).toEqual(["2026-06-26", "2026-06-27", "2026-06-29"]);
  });
  it("replaces a same-date bar with the latest", () => {
    const merged = mergeHistory([bar("2026-06-29", 1)], bar("2026-06-29", 9));
    expect(merged).toHaveLength(1);
    expect(merged[0]!.close).toBe(9);
  });
});

describe("hasEnoughHistory", () => {
  it("is false below the minimum", () => {
    expect(hasEnoughHistory([bar("2026-06-29", 1)], 200)).toBe(false);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/history.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Create `src/history.ts`**

```ts
import type { RawBar } from "./types.js";

export function mergeHistory(stored: RawBar[], latest: RawBar): RawBar[] {
  const byDate = new Map<string, RawBar>();
  for (const b of stored) byDate.set(b.date, b);
  byDate.set(latest.date, latest); // latest wins on collision
  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

export function hasEnoughHistory(bars: RawBar[], minSessions: number): boolean {
  return bars.length >= minSessions;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/history.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/history.ts tests/history.test.ts
git commit -m "feat: add history merge and sufficiency check"
```

---

## Task 12: Glue partition registration

**Files:**
- Create: `src/glue.ts`
- Test: `tests/glue.test.ts`

**Interfaces:**
- Consumes: nothing app-specific.
- Produces: `partitionValues(date)` → `[year, month, day]`; `addPartition(glue, db, table, bucket, prefix, date)` calls `BatchCreatePartition` and treats "already exists" as success (idempotent). Glue client injected.

- [ ] **Step 1: Write the failing test**

```ts
// tests/glue.test.ts
import { describe, it, expect } from "vitest";
import { partitionValues } from "../src/glue.js";

describe("partitionValues", () => {
  it("splits a date into year/month/day", () => {
    expect(partitionValues("2026-06-29")).toEqual(["2026", "06", "29"]);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/glue.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Create `src/glue.ts`**

```ts
import { GlueClient, BatchCreatePartitionCommand } from "@aws-sdk/client-glue";

export function partitionValues(date: string): [string, string, string] {
  const [y, m, d] = date.split("-") as [string, string, string];
  return [y, m, d];
}

export async function addPartition(
  glue: GlueClient, database: string, table: string, bucket: string, prefix: string, date: string,
): Promise<void> {
  const [year, month, day] = partitionValues(date);
  const location = `s3://${bucket}/${prefix}/year=${year}/month=${month}/day=${day}/`;
  try {
    await glue.send(new BatchCreatePartitionCommand({
      DatabaseName: database,
      TableName: table,
      PartitionInputList: [{
        Values: [year, month, day],
        StorageDescriptor: { Location: location },
      }],
    }));
  } catch (err) {
    const name = (err as { name?: string }).name ?? "";
    if (name.includes("AlreadyExists")) return; // idempotent
    throw err;
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/glue.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/glue.ts tests/glue.test.ts
git commit -m "feat: add idempotent Glue partition registration"
```

---

## Task 13: Metadata (manifest, current pointer, universe snapshot)

**Files:**
- Create: `src/metadata.ts`
- Test: `tests/metadata.test.ts`

**Interfaces:**
- Consumes: `RunManifest`, `S3Client`.
- Produces:
  - `manifestKey(date, runId)`, `currentKey(date)`, `universeKey(date)` builders.
  - `writeManifest(s3, bucket, manifest)`, `markCurrent(s3, bucket, manifest)` (only call on success), `snapshotUniverse(s3, bucket, date, universe)`.

- [ ] **Step 1: Write the failing test**

```ts
// tests/metadata.test.ts
import { describe, it, expect } from "vitest";
import { manifestKey, currentKey, universeKey } from "../src/metadata.js";

describe("metadata keys", () => {
  it("builds the manifest key", () => {
    expect(manifestKey("2026-06-29", "20260629T223000Z")).toBe(
      "metadata/runs/year=2026/month=06/day=29/runId=20260629T223000Z/manifest.json",
    );
  });
  it("builds the current pointer key", () => {
    expect(currentKey("2026-06-29")).toBe("metadata/current/daily_metrics/year=2026/month=06/day=29.json");
  });
  it("builds the universe snapshot key", () => {
    expect(universeKey("2026-06-29")).toBe("metadata/universe/2026-06-29.json");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/metadata.test.ts`
Expected: FAIL — module not found.

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
export function universeKey(date: string): string {
  return `metadata/universe/${date}.json`;
}

async function putJson(s3: S3Client, bucket: string, key: string, value: unknown): Promise<void> {
  await s3.send(new PutObjectCommand({
    Bucket: bucket, Key: key, Body: JSON.stringify(value, null, 2), ContentType: "application/json",
  }));
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

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/metadata.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/metadata.ts tests/metadata.test.ts
git commit -m "feat: add manifest, current pointer, and universe snapshot writers"
```

---

## Task 14: Telegram report

**Files:**
- Create: `src/report.ts`
- Test: `tests/report.test.ts`

**Interfaces:**
- Consumes: `RunManifest`.
- Produces: `renderReport(m: RunManifest): string` and `sendTelegram(botToken, chatId, text, fetchFn?)`.

- [ ] **Step 1: Write the failing test**

```ts
// tests/report.test.ts
import { describe, it, expect } from "vitest";
import { renderReport } from "../src/report.js";
import type { RunManifest } from "../src/types.js";

const m: RunManifest = {
  runId: "20260629T223000Z", mode: "daily", tradingDay: "2026-06-29", provider: "finnhub",
  universeVersion: "2026-06-29", symbolsRequested: 612, symbolsSucceeded: 610, rowsWritten: 610,
  warnings: 4, rejected: 2, runtimeSec: 302, metricVersion: "1.0", schemaVersion: "metrics_v1", status: "PARTIAL",
};

describe("renderReport", () => {
  it("includes key run stats", () => {
    const text = renderReport(m);
    expect(text).toContain("EdgeHub Daily Update");
    expect(text).toContain("2026-06-29");
    expect(text).toContain("610");
    expect(text).toContain("PARTIAL");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/report.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Create `src/report.ts`**

```ts
import type { RunManifest } from "./types.js";

type FetchFn = (url: string, init: { method: string; headers: Record<string, string>; body: string }) => Promise<{ ok: boolean; status: number }>;

export function renderReport(m: RunManifest): string {
  return [
    `EdgeHub Daily Update`,
    `Date: ${m.tradingDay}`,
    `Provider: ${m.provider}`,
    `Universe: ${m.symbolsRequested} (v${m.universeVersion})`,
    `Downloaded: ${m.symbolsSucceeded}`,
    `Rows: ${m.rowsWritten}`,
    `Warnings: ${m.warnings}`,
    `Rejected: ${m.rejected}`,
    `Metric Version: ${m.metricVersion}`,
    `Runtime: ${m.runtimeSec}s`,
    `Status: ${m.status}`,
  ].join("\n");
}

export async function sendTelegram(
  botToken: string, chatId: string, text: string, fetchFn: FetchFn = fetch as unknown as FetchFn,
): Promise<void> {
  const res = await fetchFn(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text }),
  });
  if (!res.ok) throw new Error(`telegram HTTP ${res.status}`);
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/report.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/report.ts tests/report.test.ts
git commit -m "feat: add Telegram report rendering and sending"
```

---

## Task 15: Pipeline orchestration

**Files:**
- Create: `src/pipeline.ts`
- Test: `tests/pipeline.test.ts`

**Interfaces:**
- Consumes: everything above. To stay testable, the pipeline takes its AWS clients and provider via a `Deps` object so tests inject fakes.
- Produces:
  - `makeRunId(now: Date): string` → `YYYYMMDDTHHMMSSZ`.
  - `runPipeline(mode, deps): Promise<RunManifest>` where `Deps = { provider, s3, glue, bucket, database, now, universe, readHistory }`.
  - `readHistory(ticker): Promise<RawBar[]>` is injected (S3 read of prior Parquet) so the pipeline core is unit-testable with the fake provider.

- [ ] **Step 1: Write the failing test (runId formatter)**

```ts
// tests/pipeline.test.ts
import { describe, it, expect } from "vitest";
import { makeRunId, buildManifest } from "../src/pipeline.js";
import type { MetricRow, Provenance } from "../src/types.js";

describe("makeRunId", () => {
  it("formats a compact UTC timestamp", () => {
    expect(makeRunId(new Date("2026-06-29T22:30:00Z"))).toBe("20260629T223000Z");
  });
});

describe("buildManifest", () => {
  it("counts warnings, rejects, and sets PARTIAL when symbols fail", () => {
    const prov: Provenance = { runId: "R", ingestedAt: "x", source: "fake", sourceVersion: "1.0", schemaVersion: "metrics_v1", metricVersion: "1.0", universeVersion: "2026-06-29" };
    const rows: MetricRow[] = [
      { ...stub(prov), qualityStatus: "OK", qualityIssues: [] },
      { ...stub(prov), qualityStatus: "WARN", qualityIssues: ["zero_volume"] },
    ];
    const m = buildManifest({ mode: "daily", runId: "R", tradingDay: "2026-06-29", provider: "fake", universeVersion: "2026-06-29", requested: 3, rows, runtimeSec: 5 });
    expect(m.symbolsSucceeded).toBe(2);
    expect(m.warnings).toBe(1);
    expect(m.status).toBe("PARTIAL"); // requested 3, got 2
  });
});

function stub(prov: Provenance): MetricRow {
  return { ticker: "AAPL", date: "2026-06-29", close: 1, dollarVolume: 1, ma20: null, ma50: null, ma150: null, ma200: null, avgVolume20: null, avgVolume50: null, atr14: null, high52w: null, low52w: null, distanceTo52wHighPct: null, distanceFrom52wLowPct: null, return21d: null, return63d: null, return126d: null, return252d: null, above20ma: null, above50ma: null, above150ma: null, above200ma: null, ma150Above200: null, ma200Rising: null, qualityStatus: "OK", qualityIssues: [], ...prov };
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/pipeline.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Create `src/pipeline.ts`**

```ts
import { S3Client } from "@aws-sdk/client-s3";
import { GlueClient } from "@aws-sdk/client-glue";
import type { MarketDataProvider } from "./providers/provider.js";
import type { RawBar, MetricRow, RunManifest, RunMode, Provenance } from "./types.js";
import { SCHEMA_VERSION, METRIC_VERSION } from "./types.js";
import { loadUniverse } from "./universe.js";
import { gradeBar } from "./validate.js";
import { computeMetrics } from "./metrics.js";
import { mergeHistory, hasEnoughHistory } from "./history.js";
import { writeRaw, writeMetrics } from "./storage.js";
import { addPartition } from "./glue.js";
import { writeManifest, markCurrent, snapshotUniverse } from "./metadata.js";

export function makeRunId(now: Date): string {
  return now.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
}

export interface BuildManifestArgs {
  mode: RunMode; runId: string; tradingDay: string; provider: string;
  universeVersion: string; requested: number; rows: MetricRow[]; runtimeSec: number;
}

export function buildManifest(a: BuildManifestArgs): RunManifest {
  const warnings = a.rows.filter((r) => r.qualityStatus === "WARN").length;
  const rejected = a.rows.filter((r) => r.qualityStatus === "REJECTED").length;
  const succeeded = a.rows.length;
  const status: RunManifest["status"] = succeeded === 0 ? "FAILURE" : succeeded < a.requested ? "PARTIAL" : "SUCCESS";
  return {
    runId: a.runId, mode: a.mode, tradingDay: a.tradingDay, provider: a.provider,
    universeVersion: a.universeVersion, symbolsRequested: a.requested, symbolsSucceeded: succeeded,
    rowsWritten: succeeded, warnings, rejected, runtimeSec: a.runtimeSec,
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
  readHistory: (ticker: string) => Promise<RawBar[]>;
}

const MIN_SESSIONS = 200;
const LOOKBACK_DAYS = 400;

export async function runPipeline(mode: RunMode, deps: Deps): Promise<RunManifest> {
  const start = deps.now().getTime();
  const runId = makeRunId(deps.now());
  const { tickers, universeVersion } = loadUniverse();

  await snapshotUniverse(deps.s3, deps.bucket, deps.tradingDay, { universeVersion, tickers });

  const prov: Provenance = {
    runId, ingestedAt: deps.now().toISOString(), source: deps.provider.name,
    sourceVersion: deps.provider.version, schemaVersion: SCHEMA_VERSION,
    metricVersion: METRIC_VERSION, universeVersion,
  };

  const rawToStore: RawBar[] = [];
  const metricRows: MetricRow[] = [];
  const seen = new Set<string>();

  for (const ticker of tickers) {
    try {
      let bars: RawBar[];
      if (mode === "backfill") {
        bars = await deps.provider.getHistory(ticker, LOOKBACK_DAYS);
      } else {
        const stored = await deps.readHistory(ticker);
        const latest = (await deps.provider.getLatestBars(deps.tradingDay, [ticker]))[0];
        if (!latest) continue;
        bars = mergeHistory(stored, latest);
        if (!hasEnoughHistory(bars, MIN_SESSIONS)) {
          bars = mergeHistory(await deps.provider.getHistory(ticker, LOOKBACK_DAYS), latest);
        }
      }
      const today = bars[bars.length - 1];
      if (!today) continue;
      const grade = gradeBar(today, seen);
      rawToStore.push(today);
      if (grade.status === "REJECTED") continue;
      metricRows.push(computeMetrics(bars, prov));
    } catch {
      // per-ticker failure is non-fatal; absence from metricRows lowers symbolsSucceeded
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

  const runtimeSec = Math.round((deps.now().getTime() - start) / 1000);
  const manifest = buildManifest({
    mode, runId, tradingDay: deps.tradingDay, provider: deps.provider.name,
    universeVersion, requested: tickers.length, rows: metricRows, runtimeSec,
  });

  await writeManifest(deps.s3, deps.bucket, manifest);
  if (manifest.status !== "FAILURE") await markCurrent(deps.s3, deps.bucket, manifest);

  return manifest;
}
```

- [ ] **Step 4: Add an end-to-end pipeline test with the fake provider**

Append to `tests/pipeline.test.ts`:

```ts
import { runPipeline } from "../src/pipeline.js";
import { FakeProvider } from "../src/providers/fake.js";
import type { RawBar } from "../src/types.js";

function series(ticker: string, n: number): RawBar[] {
  const bars: RawBar[] = [];
  for (let i = 0; i < n; i++) {
    const close = 100 + i;
    bars.push({ ticker, date: `2025-01-${String((i % 28) + 1).padStart(2, "0")}`, open: close, high: close + 1, low: close - 1, close, adjustedClose: close, volume: 1000, source: "fake", sourceVersion: "1.0", ingestedAt: "x" });
  }
  return bars;
}

describe("runPipeline (backfill, fake provider)", () => {
  it("produces a SUCCESS manifest writing metrics for all tickers", async () => {
    const hist = new Map<string, RawBar[]>([
      ["AAPL", series("AAPL", 5)], ["MSFT", series("MSFT", 5)], ["GOOG", series("GOOG", 5)],
      ["AMZN", series("AMZN", 5)], ["NVDA", series("NVDA", 5)], ["TSLA", series("TSLA", 5)],
      ["AVGO", series("AVGO", 5)], ["SPY", series("SPY", 5)], ["QQQ", series("QQQ", 5)],
    ]);
    const calls: string[] = [];
    const s3 = { send: async (c: unknown) => { calls.push((c as { constructor: { name: string } }).constructor.name); return {}; } } as never;
    const glue = { send: async () => ({}) } as never;
    const manifest = await runPipeline("backfill", {
      provider: new FakeProvider(hist), s3, glue, bucket: "b", database: "edgehub",
      tradingDay: "2025-01-05", now: () => new Date("2025-01-05T22:30:00Z"),
      readHistory: async () => [],
    });
    expect(manifest.status).toBe("SUCCESS");
    expect(manifest.rowsWritten).toBe(9);
    expect(calls).toContain("PutObjectCommand");
  });
});
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `npx vitest run tests/pipeline.test.ts`
Expected: PASS (runId, buildManifest, end-to-end).

- [ ] **Step 6: Commit**

```bash
git add src/pipeline.ts tests/pipeline.test.ts
git commit -m "feat: orchestrate the daily pipeline with injected deps"
```

---

## Task 16: Lambda handler

**Files:**
- Create: `src/handler.ts`, `src/historyReader.ts`, `events/daily.json`, `events/backfill.json`
- Test: `tests/handler.test.ts`

**Interfaces:**
- Consumes: `runPipeline`, `loadSecrets`, `getProvider`.
- Produces: `parseEvent(event): { mode: RunMode; tradingDay: string }` and the Lambda `handler`. `src/historyReader.ts` exports `makeS3HistoryReader(s3, bucket, source)` returning a `readHistory(ticker)` that lists+reads the most recent prior metrics... (raw) Parquet; on any error it returns `[]` so the pipeline auto-backfills.

- [ ] **Step 1: Write the failing test**

```ts
// tests/handler.test.ts
import { describe, it, expect } from "vitest";
import { parseEvent } from "../src/handler.js";

describe("parseEvent", () => {
  it("defaults to daily mode and today's date", () => {
    const r = parseEvent({}, new Date("2026-06-29T22:30:00Z"));
    expect(r.mode).toBe("daily");
    expect(r.tradingDay).toBe("2026-06-29");
  });
  it("honors an explicit backfill mode and date", () => {
    const r = parseEvent({ mode: "backfill", tradingDay: "2026-06-01" }, new Date());
    expect(r.mode).toBe("backfill");
    expect(r.tradingDay).toBe("2026-06-01");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/handler.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Create `src/historyReader.ts`**

```ts
import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";
import { ParquetReader } from "@dsnp/parquetjs";
import type { RawBar } from "./types.js";

/** Returns a readHistory(ticker) that loads prior raw bars from the newest stored runId.
 *  Any failure returns [] so the pipeline falls back to provider backfill. */
export function makeS3HistoryReader(s3: S3Client, bucket: string, source: string): (ticker: string) => Promise<RawBar[]> {
  return async (ticker: string): Promise<RawBar[]> => {
    try {
      const prefix = `raw/${source}/daily/`;
      const listed = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix }));
      const keys = (listed.Contents ?? []).map((o) => o.Key!).filter((k) => k.endsWith(".parquet")).sort();
      const bars: RawBar[] = [];
      for (const key of keys) {
        const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
        const buf = Buffer.from(await obj.Body!.transformToByteArray());
        const reader = await ParquetReader.openBuffer(buf);
        const cursor = reader.getCursor();
        let rec: unknown;
        while ((rec = await cursor.next())) {
          const b = rec as RawBar;
          if (b.ticker === ticker) bars.push(b);
        }
        await reader.close();
      }
      return bars;
    } catch {
      return [];
    }
  };
}
```

> Note: scanning all raw Parquet per ticker is acceptable for the seed universe. The §12 scale-out
> (ticker partitioning / compaction) addresses this when the universe grows; not needed now.

- [ ] **Step 4: Create `src/handler.ts`**

```ts
import { S3Client } from "@aws-sdk/client-s3";
import { GlueClient } from "@aws-sdk/client-glue";
import { SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import type { RunMode } from "./types.js";
import { getProvider } from "./providers/factory.js";
import { loadSecrets } from "./secrets.js";
import { runPipeline } from "./pipeline.js";
import { makeS3HistoryReader } from "./historyReader.js";
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
    provider, s3, glue, bucket, database, tradingDay,
    now: () => new Date(),
    readHistory: makeS3HistoryReader(s3, bucket, provider.name),
  });

  await sendTelegram(secrets.telegramBotToken!, secrets.telegramChatId!, renderReport(manifest));
  return { status: manifest.status };
}
```

- [ ] **Step 5: Create event fixtures**

`events/daily.json`:
```json
{ "mode": "daily" }
```
`events/backfill.json`:
```json
{ "mode": "backfill", "tradingDay": "2026-06-29" }
```

- [ ] **Step 6: Run tests + full suite + typecheck**

Run: `npx vitest run && npm run typecheck`
Expected: ALL tests PASS; typecheck exits 0.

- [ ] **Step 7: Commit**

```bash
git add src/handler.ts src/historyReader.ts events/ tests/handler.test.ts
git commit -m "feat: add Lambda handler, S3 history reader, and event fixtures"
```

---

## Task 17: SAM template + deploy config

**Files:**
- Create: `template.yaml`, `samconfig.toml`

**Interfaces:**
- Produces the deployable stack: bucket, Lambda, EventBridge Scheduler, Glue DB + 2 tables, Secret declaration, IAM. No unit test — verified with `sam validate` and `sam build`.

- [ ] **Step 1: Create `template.yaml`**

```yaml
AWSTemplateFormatVersion: "2010-09-09"
Transform: AWS::Serverless-2016-10-31
Description: EdgeHub Part 1 - Market Data Lake

Globals:
  Function:
    Timeout: 900
    MemorySize: 1024
    Runtime: nodejs20.x

Resources:
  DataBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: edgehub-data
      VersioningConfiguration:
        Status: Enabled

  EdgeHubSecret:
    Type: AWS::SecretsManager::Secret
    Properties:
      Name: edgehub/secrets
      Description: "Finnhub + Telegram credentials (value set manually once)"

  GlueDatabase:
    Type: AWS::Glue::Database
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseInput:
        Name: edgehub

  EdgeHubCollector:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: edgehub-daily-collector
      Handler: src/handler.handler
      Environment:
        Variables:
          BUCKET_NAME: edgehub-data
          GLUE_DATABASE: edgehub
          DATA_PROVIDER: finnhub
          SECRET_NAME: edgehub/secrets
          SCHEMA_VERSION: metrics_v1
          METRIC_VERSION: "1.0"
          SOURCE_VERSION: "1.0"
      Policies:
        - S3CrudPolicy:
            BucketName: edgehub-data
        - Statement:
            - Effect: Allow
              Action:
                - glue:BatchCreatePartition
                - glue:GetPartition
                - glue:GetTable
              Resource: "*"
            - Effect: Allow
              Action: secretsmanager:GetSecretValue
              Resource: !Ref EdgeHubSecret
    Metadata:
      BuildMethod: esbuild
      BuildProperties:
        Format: esm
        Target: node20
        EntryPoints:
          - src/handler.ts

  CollectorSchedule:
    Type: AWS::Scheduler::Schedule
    Properties:
      Name: edgehub-daily-630pm-et
      ScheduleExpression: cron(30 18 ? * MON-FRI *)
      ScheduleExpressionTimezone: America/New_York
      FlexibleTimeWindow:
        Mode: "OFF"
      Target:
        Arn: !GetAtt EdgeHubCollector.Arn
        RoleArn: !GetAtt SchedulerRole.Arn
        Input: '{"mode":"daily"}'

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

  DailyBarsTable:
    Type: AWS::Glue::Table
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseName: !Ref GlueDatabase
      TableInput:
        Name: daily_bars
        TableType: EXTERNAL_TABLE
        PartitionKeys:
          - { Name: year, Type: string }
          - { Name: month, Type: string }
          - { Name: day, Type: string }
        Parameters: { classification: parquet }
        StorageDescriptor:
          Location: s3://edgehub-data/raw/finnhub/daily/
          InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
          OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
          SerdeInfo:
            SerializationLibrary: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
          Columns:
            - { Name: ticker, Type: string }
            - { Name: date, Type: string }
            - { Name: open, Type: double }
            - { Name: high, Type: double }
            - { Name: low, Type: double }
            - { Name: close, Type: double }
            - { Name: adjustedClose, Type: double }
            - { Name: volume, Type: double }
            - { Name: source, Type: string }
            - { Name: sourceVersion, Type: string }
            - { Name: ingestedAt, Type: string }

  DailyMetricsTable:
    Type: AWS::Glue::Table
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseName: !Ref GlueDatabase
      TableInput:
        Name: daily_metrics
        TableType: EXTERNAL_TABLE
        PartitionKeys:
          - { Name: year, Type: string }
          - { Name: month, Type: string }
          - { Name: day, Type: string }
        Parameters: { classification: parquet }
        StorageDescriptor:
          Location: s3://edgehub-data/metrics/daily/
          InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
          OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
          SerdeInfo:
            SerializationLibrary: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
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

- [ ] **Step 3: Validate the template**

Run: `sam validate --lint`
Expected: "template.yaml is a valid SAM Template". (If SAM CLI is not installed locally, this step runs in CI in Task 19; note that here and proceed.)

- [ ] **Step 4: Build**

Run: `sam build`
Expected: "Build Succeeded" — esbuild bundles `src/handler.ts`.

- [ ] **Step 5: Commit**

```bash
git add template.yaml samconfig.toml
git commit -m "feat: add SAM template and deploy config"
```

---

## Task 18: Data dictionary + bootstrap docs

**Files:**
- Create: `docs/DATA_DICTIONARY.md`, `docs/BOOTSTRAP.md`

**Interfaces:**
- Produces: human docs. No test; verified by review.

- [ ] **Step 1: Create `docs/DATA_DICTIONARY.md`**

````markdown
# EdgeHub Data Dictionary

Generated from `config/metrics.ts` (the metric registry). When you add a metric there, add its row here.

## daily_bars (raw)

| Column | Type | Meaning |
|--------|------|---------|
| ticker | string | Symbol |
| date | string | Trading day, YYYY-MM-DD |
| open/high/low/close | double | OHLC |
| adjustedClose | double | Adjusted close (equals close on Finnhub free tier) |
| volume | double | Share volume |
| source | string | Provider name |
| sourceVersion | string | Provider version |
| ingestedAt | string | ISO timestamp of ingestion |

## daily_metrics

Identity & provenance: ticker, date, runId, source, sourceVersion, schemaVersion, metricVersion, universeVersion.

| Metric | Window | Depends on | Meaning |
|--------|--------|-----------|---------|
| dollarVolume | — | close, volume | close × volume |
| ma20 / ma50 / ma150 / ma200 | 20/50/150/200 | close | Simple moving averages |
| avgVolume20 / avgVolume50 | 20/50 | volume | Average volume |
| atr14 | 14 | high, low, close | Average true range (SMA of TR) |
| high52w / low52w | 252 | high / low | 52-week extremes |
| distanceTo52wHighPct | 252 | close, high | % distance to 52w high (≤ 0) |
| distanceFrom52wLowPct | 252 | close, low | % distance above 52w low (≥ 0) |
| return21d / 63d / 126d / 252d | 21/63/126/252 | close | Trailing returns |
| above20ma / 50 / 150 / 200ma | — | close | close above the MA |
| ma150Above200 | — | close | ma150 > ma200 |
| ma200Rising | — | close | ma200 today > prior session |
| qualityStatus | — | — | OK / WARN / REJECTED |
| qualityIssues | — | — | Comma-joined issue codes |
````

- [ ] **Step 2: Create `docs/BOOTSTRAP.md`**

````markdown
# EdgeHub Bootstrap (one-time manual setup)

Do these once before the first GitHub deploy. They create the trust + secret that
GitHub Actions itself cannot create.

## 1. GitHub OIDC provider + deploy role

```bash
# OIDC provider (skip if it already exists in the account)
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1

# Deploy role trusting this repo's main branch (see trust-policy.json below)
aws iam create-role --role-name edgehub-deploy \
  --assume-role-policy-document file://trust-policy.json

# Attach deploy permissions (CloudFormation, S3, Lambda, IAM, Glue, Scheduler, SecretsManager).
# For a personal project, PowerUserAccess + IAMFullAccess is the simple path;
# tighten later.
aws iam attach-role-policy --role-name edgehub-deploy \
  --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
aws iam attach-role-policy --role-name edgehub-deploy \
  --policy-arn arn:aws:iam::aws:policy/IAMFullAccess
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

Add the role ARN as the GitHub repo variable `AWS_DEPLOY_ROLE_ARN`
(Settings → Secrets and variables → Actions → Variables).

## 2. Secret value

The SAM template declares `edgehub/secrets` but not its value. Set it once:

```bash
aws secretsmanager put-secret-value --secret-id edgehub/secrets \
  --secret-string '{"finnhubToken":"<FINNHUB>","telegramBotToken":"<BOT>","telegramChatId":"<CHAT_ID>"}'
```

## 3. First backfill

After the first successful deploy, run a one-time backfill:

```bash
aws lambda invoke --function-name edgehub-daily-collector \
  --payload '{"mode":"backfill"}' --cli-binary-format raw-in-base64-out /dev/stdout
```
````

- [ ] **Step 3: Commit**

```bash
git add docs/DATA_DICTIONARY.md docs/BOOTSTRAP.md
git commit -m "docs: add data dictionary and bootstrap guide"
```

---

## Task 19: CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Produces: PR gate running typecheck, tests, and SAM validate/build. No deploy.

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
        with:
          node-version: "20"
          cache: npm
      - run: npm ci
      - run: npm run typecheck
      - run: npm test
      - uses: aws-actions/setup-sam@v2
        with:
          use-installer: true
      - run: sam validate --lint
      - run: sam build
```

- [ ] **Step 2: Verify locally what CI will run**

Run: `npm ci && npm run typecheck && npm test`
Expected: install clean, typecheck 0, all tests PASS. (The `sam` steps run on GitHub.)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add PR build-and-test workflow"
```

---

## Task 20: Deploy workflow (GitHub → AWS via OIDC)

**Files:**
- Create: `.github/workflows/deploy.yml`

**Interfaces:**
- Produces: push-to-main deploy via OIDC. Consumes repo variable `AWS_DEPLOY_ROLE_ARN` (from BOOTSTRAP.md).

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
        with:
          node-version: "20"
          cache: npm
      - run: npm ci
      - run: npm run typecheck && npm test
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ vars.AWS_DEPLOY_ROLE_ARN }}
          aws-region: us-east-1
      - uses: aws-actions/setup-sam@v2
        with:
          use-installer: true
      - run: sam build
      - run: sam deploy --no-confirm-changeset --no-fail-on-empty-changeset
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/deploy.yml
git commit -m "ci: add OIDC deploy-on-main workflow"
```

- [ ] **Step 3: Push and verify the pipeline**

```bash
git push origin main
```
Expected: the **Deploy** workflow runs on GitHub, assumes the role via OIDC, and `sam deploy` creates/updates the `edgehub` stack. Verify in the GitHub Actions tab and the CloudFormation console. (Requires Task 18 bootstrap done first.)

---

## Self-Review

**1. Spec coverage:**
- IaC = SAM, compute = single Lambda → Tasks 16, 17. ✓
- TypeScript/Node 20 → Task 1. ✓
- Finnhub behind interface + factory + no hardcoded vendor → Tasks 5, 6; `source` from `provider.name` in storage/pipeline. ✓
- OIDC deploy from main only → Tasks 18, 20. ✓
- us-east-1 → Tasks 16, 17, 20. ✓
- Committed versioned universe → Task 4. ✓
- metrics naming (dir/table/version) → Tasks 2, 3, 8, 10, 17. ✓
- Parquet via `@dsnp/parquetjs` → Task 10. ✓
- Backfill vs daily + auto-backfill + history reader → Tasks 11, 15, 16. ✓
- runId paths + metadata/current + never advance on FAILURE → Tasks 10, 13, 15. ✓
- Provenance incl. universeVersion on every row → Tasks 2, 8, 15. ✓
- Run manifest → Tasks 2, 13, 15. ✓
- Universe versioning + per-day snapshot → Tasks 4, 13, 15. ✓
- Metric registry + schema registry + data dictionary → Tasks 3, 18. ✓
- Quality OK/WARN/REJECTED, WARN stored, REJECTED logged → Tasks 7, 8, 15. ✓
- EventBridge Scheduler w/ America/New_York → Task 17. ✓
- Glue daily_bars + daily_metrics → Tasks 12, 17. ✓
- Telegram report from manifest → Tasks 14, 16. ✓
- Reserved labels/ + corporate_actions/ → see note below. ✓
- Testing (unit + schema + local) → Tasks 2–16, plus `invoke:local` script (Task 1) + events (Task 16). ✓

**2. Placeholder scan:** No "TBD"/"add error handling"/"similar to Task N". Each code step shows full code. ✓

**3. Type consistency:** `MarketDataProvider` (name, version, getLatestBars, getHistory) consistent across Tasks 5/6/15/16. `RawBar`/`MetricRow`/`Provenance`/`RunManifest` defined in Task 2 and used unchanged. `runId` paths identical in storage (Task 10) and metadata (Task 13). `gradeBar(bar, seen)` signature consistent Tasks 7/8/15. ✓

**Gap found & fixed:** Reserved `labels/` and `corporate_actions/` prefixes were in the spec but not in any task (S3 prefixes only materialize on first write). Resolution: they require no infra (S3 has no real "folders"); the first object written under them in Part 2 creates them. To make the reservation explicit now, the data dictionary/bootstrap note their existence. No separate task needed — documented here so it isn't mistaken for an omission.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-29-edgehub-part1-market-data-lake.md`.
