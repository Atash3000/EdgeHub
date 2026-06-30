# EdgeHub Part 1.5a — Security Master + Instrument Identity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an identity layer (`instrumentId`, a scoped security master, a symbol-alias table, and ticker→instrument resolution) on top of EdgeHub Part 1, without changing Part 1 behavior.

**Architecture:** Additive. Each trading day the pipeline asks the provider for point-in-time reference data for the universe tickers, builds a security master (one row per universe ticker, minting an `EH:<ticker>` fallback when reference data is missing), derives forward symbol aliases, resolves each fetched bar to an `instrumentId`, and stamps `instrumentId` onto raw bars + metric rows. The per-ticker history cache is re-keyed from `ticker` to `instrumentId`. Facts stay skinny; the rich dimension lives in `reference/securities/` and `reference/symbol_aliases/`.

**Tech Stack:** TypeScript (ES2022, strict), Node.js 24 Lambda, `@dsnp/parquetjs`, AWS SDK v3 (`@aws-sdk/client-s3`, `-glue`, `-ssm`), AWS SAM (`template.yaml`), Vitest. Polygon.io reference-tickers + grouped-daily endpoints.

## Global Constraints

- Source spec: `docs/superpowers/specs/2026-06-30-edgehub-part1.5a-security-master-design.md`. Where this plan and the spec differ, the spec's §2 decisions win.
- **D1 — scoped to universe:** the committed `config/universe/*.json` stays the ingest list; build the master only for those tickers; do **not** page Polygon's full catalog.
- **D2 — never drop a universe stock:** guarantee exactly one security-master row per universe ticker; mint `EH:<ticker>` / `MISSING_FALLBACK` when reference data is missing; the resolver only excludes genuinely non-universe bars.
- **D3 — pure deterministic `makeInstrumentId`:** no cross-day pinning; add `identitySource`/`identityConfidence`; `identity_changed` is a non-blocking warning; no restamping.
- **D4 — in-place schema bump:** add `instrumentId` to the existing `daily_bars`/`daily_metrics` tables and Parquet schemas; bump `dailyBars_v1→v2`, `metrics_v1→v2`; no parallel `*_v2` tables.
- Provider method signature is future-ready: `listSecurities(asOfDate: string, tickers?: string[])`; 1.5a always passes `tickers`.
- `identitySource→identityConfidence`: `SHARE_CLASS_FIGI`/`COMPOSITE_FIGI`→`HIGH`; `EH_CIK_TICKER`→`MEDIUM`; `EH_TICKER_EXCHANGE`/`EH_TICKER`→`LOW`.
- `instrumentId` fallback chain: `share_class_figi → composite_figi → EH:<cik>:<ticker> → EH:<ticker>:<primaryExchange> → EH:<ticker>`. **The `EH:` fallbacks use the full `ticker` (e.g. `BRK.A`), never `tickerRoot` — otherwise share classes (`BRK.A`/`BRK.B`) collide when FIGI is absent.** `tickerRoot`/`tickerSuffix` are descriptive metadata only, never identity keys.
- TDD throughout: write the failing test first, watch it fail, implement minimally, watch it pass, commit. After **every** task `npm run typecheck` and `npm test` must both pass.
- Commit messages end with: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- Branch: `part-1.5a-security-master` (already checked out).
- All `import` paths use the `.js` extension (ESM/NodeNext), matching the existing code.

---

## File Structure

**New source files:**
- `src/identity.ts` — pure `makeInstrumentId` + `splitTicker`.
- `src/securityMaster.ts` — build/write/read the security master; ticker map; identity-change detection; `SECURITIES_SCHEMA`'s home is `storage.ts` (see below).
- `src/symbolAliases.ts` — forward-only alias windows; write/read state.
- `src/resolver.ts` — `resolveBarsToInstruments`.

**Modified source files:**
- `src/types.ts` — new interfaces/enums; `instrumentId` on `RawBarRow`/`MetricRow`; v2 version constants; new `RunManifest` fields.
- `src/providers/provider.ts` — add `listSecurities` to the interface.
- `src/providers/polygon.ts`, `src/providers/finnhub.ts`, `src/providers/fake.ts` — implement `listSecurities`.
- `src/storage.ts` — `instrumentId` in `RAW_SCHEMA`/`METRIC_SCHEMA`; add `SECURITIES_SCHEMA`, `SYMBOL_ALIASES_SCHEMA`, `writeSecurities`, `writeSymbolAliases`, key builders.
- `src/historyCache.ts` — re-key from `ticker` to `instrumentId`.
- `src/metrics.ts` — `computeMetrics` accepts `instrumentId`.
- `src/metadata.ts` — `snapshotUniverse` stores securities.
- `src/pipeline.ts` — wire the identity steps; new manifest fields.
- `src/report.ts` — resolution stats lines.
- `src/glue.ts` — `addAsOfPartition`.
- `template.yaml` — `instrumentId` column on existing tables; new `securities` + `symbol_aliases` Glue tables.

**New schema-registry files:**
- `schemas/dailyBars_v2.json`, `schemas/metrics_v2.json`.

**New / updated tests:**
- New: `tests/identity.test.ts`, `tests/securityMaster.test.ts`, `tests/symbolAliases.test.ts`, `tests/resolver.test.ts`.
- Updated: `tests/types.test.ts`, `tests/historyCache.test.ts`, `tests/polygon.test.ts`, `tests/storage.test.ts`, `tests/metrics.test.ts`, `tests/pipeline.test.ts`, `tests/report.test.ts`.

---

## Task 1: Types, version bump, and v2 schema registry

**Files:**
- Modify: `src/types.ts`
- Create: `schemas/dailyBars_v2.json`, `schemas/metrics_v2.json`
- Test: `tests/types.test.ts`

**Interfaces:**
- Consumes: nothing.
- Produces: `IdentitySource`, `IdentityConfidence`, `ReferenceStatus`, `SecurityMasterRow`, `SymbolAliasRow`, `ResolvedVendorBar`, `SecurityMasterResult` (exported types); `RawBarRow`/`MetricRow` gain `instrumentId: string`; `RunManifest` gains `securitiesMastered`, `securitiesResolved`, `unresolvedTickers`, `missingReferenceData`, `aliasRows: number`; constants `SCHEMA_VERSION="metrics_v2"`, `RAW_SCHEMA_VERSION="dailyBars_v2"`.

> Note: adding `instrumentId` to `RawBarRow`/`MetricRow` and the manifest fields here is safe **only because** their construction sites (`enrichRaw`, `computeMetrics`, `buildManifest`) are updated in this same task's last step is **not** true — those live in Task 9/10. To keep the build green, this task adds `instrumentId` as **required** to the interfaces AND immediately updates the three construction sites with a temporary literal so the project compiles. Those temporaries are replaced in Task 9. See Step 4.

- [ ] **Step 1: Update the version-constant test to v2**

Replace the body of `tests/types.test.ts`:

```typescript
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
```

- [ ] **Step 2: Run it to verify it fails**

Run: `npx vitest run tests/types.test.ts`
Expected: FAIL — `expected 'metrics_v1' to be 'metrics_v2'`.

- [ ] **Step 3: Bump the version constants and add the new types in `src/types.ts`**

Change the top four constants:

```typescript
export const SCHEMA_VERSION = "metrics_v2";
export const RAW_SCHEMA_VERSION = "dailyBars_v2";
export const METRIC_VERSION = "1.0";
export const SOURCE_VERSION = "1.0";
```

Add these exported types (place them after the `Provenance` interface):

```typescript
export type IdentitySource =
  | "SHARE_CLASS_FIGI" | "COMPOSITE_FIGI"
  | "EH_CIK_TICKER" | "EH_TICKER_EXCHANGE" | "EH_TICKER";
export type IdentityConfidence = "HIGH" | "MEDIUM" | "LOW";
export type ReferenceStatus = "FOUND" | "MISSING_FALLBACK";

export interface SecurityMasterRow {
  instrumentId: string;
  ticker: string;
  tickerRoot?: string;
  tickerSuffix?: string;
  name?: string;
  market?: string;
  locale?: string;
  type?: string;
  currencyName?: string;
  cik?: string;
  compositeFigi?: string;
  shareClassFigi?: string;
  primaryExchange?: string;
  active: boolean;
  listDate?: string;
  delistedUtc?: string;
  lastUpdatedUtc?: string;
  identitySource: IdentitySource;
  identityConfidence: IdentityConfidence;
  referenceStatus: ReferenceStatus;
  source: string;
  sourceVersion: string;
  asOfDate: string;
  ingestedAt: string;
}

export interface SymbolAliasRow {
  instrumentId: string;
  ticker: string;
  tickerRoot?: string;
  tickerSuffix?: string;
  primaryExchange?: string;
  validFrom: string;
  validTo: string | null;
  source: string;
  sourceVersion: string;
  asOfDate: string;
  confidence: "HIGH" | "MEDIUM" | "LOW";
  createdAt: string;
}

export interface ResolvedVendorBar extends VendorBar {
  instrumentId: string;
}

export interface SecurityMasterResult {
  securities: SecurityMasterRow[];
  failures: ProviderFailure[];
}
```

Add `instrumentId` to `RawBarRow` (first field) and `MetricRow` (first field after `extends Provenance`):

```typescript
export interface RawBarRow extends VendorBar {
  instrumentId: string;
  runId: string;
  schemaVersion: string;
  metricVersion: string;
  universeVersion: string;
}

export interface MetricRow extends Provenance {
  instrumentId: string;
  ticker: string;
  date: string;
  // ...all existing fields unchanged...
```

Extend the `ErrorRecord.reason` doc comment to list the new reasons (comment only — `reason` stays `string`):

```typescript
  reason: string;   // provider_error | missing_bar_for_date | rejected | warn | pipeline_error | calendar_year_missing | unresolved_instrument | duplicate_instrument_ticker | missing_reference_data | security_master_empty | alias_conflict | identity_changed
```

Add the five numeric fields to `RunManifest` (after `missingBars`):

```typescript
  missingBars: number;
  securitiesMastered: number;
  securitiesResolved: number;
  unresolvedTickers: number;
  missingReferenceData: number;
  aliasRows: number;
```

- [ ] **Step 4: Keep the build green with temporary construction-site fills**

Adding required fields breaks three call sites. Apply the minimal temporary fills (replaced in Task 9):

In `src/pipeline.ts`, `enrichRaw` — add `instrumentId` (temporary `bar.ticker`):

```typescript
export function enrichRaw(bar: VendorBar, ctx: { runId: string; universeVersion: string }): RawBarRow {
  return { instrumentId: bar.ticker, ...bar, runId: ctx.runId, schemaVersion: RAW_SCHEMA_VERSION, metricVersion: METRIC_VERSION, universeVersion: ctx.universeVersion };
}
```

In `src/metrics.ts`, `computeMetrics` return object — add `instrumentId: last.ticker,` as the first property after `ticker: last.ticker,`.

In `src/pipeline.ts`, `buildManifest` return object and the `earlyExit` object — add the five new fields set to `0`:

```typescript
    securitiesMastered: 0, securitiesResolved: 0, unresolvedTickers: 0, missingReferenceData: 0, aliasRows: 0,
```

(Add to **both** the `buildManifest` return and the `earlyExit` literal in `runPipeline`.)

- [ ] **Step 5: Create `schemas/dailyBars_v2.json`**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "dailyBars_v2",
  "type": "object",
  "required": ["instrumentId", "ticker", "date", "open", "high", "low", "close", "isAdjusted", "volume", "source", "sourceVersion", "ingestedAt", "runId", "schemaVersion", "metricVersion", "universeVersion"],
  "properties": {
    "instrumentId": { "type": "string" },
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

- [ ] **Step 6: Create `schemas/metrics_v2.json`**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "metrics_v2",
  "type": "object",
  "required": ["instrumentId", "ticker", "date", "close", "qualityStatus", "runId", "schemaVersion", "metricVersion", "universeVersion"],
  "properties": {
    "instrumentId": { "type": "string" },
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

- [ ] **Step 7: Verify the whole suite is green**

Run: `npm run typecheck && npm test`
Expected: PASS (all existing tests + the updated `types.test.ts`). If any other test asserted the literal `"metrics_v1"`/`"dailyBars_v1"`, fix it now: `grep -rn "metrics_v1\|dailyBars_v1" tests/` and update the assertion to v2 (fixtures that merely *set* a `schemaVersion` string value do not need changing).

- [ ] **Step 8: Commit**

```bash
git add src/types.ts src/pipeline.ts src/metrics.ts schemas/dailyBars_v2.json schemas/metrics_v2.json tests/types.test.ts
git commit -m "feat(types): add identity types, instrumentId on fact rows, v2 schema bump

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Identity helper (`makeInstrumentId`, `splitTicker`)

**Files:**
- Create: `src/identity.ts`
- Test: `tests/identity.test.ts`

**Interfaces:**
- Consumes: `IdentitySource`, `IdentityConfidence` from `../src/types.js`.
- Produces:
  - `splitTicker(ticker: string): { tickerRoot: string; tickerSuffix?: string }`
  - `makeInstrumentId(input: { shareClassFigi?: string; compositeFigi?: string; cik?: string; ticker?: string; primaryExchange?: string }): { instrumentId: string; identitySource: IdentitySource; identityConfidence: IdentityConfidence }` — note: no `tickerRoot` param; the `EH:` fallbacks use the full `ticker`.

- [ ] **Step 1: Write the failing test**

Create `tests/identity.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { makeInstrumentId, splitTicker } from "../src/identity.js";

describe("splitTicker", () => {
  it("splits a share-class suffix on the dot", () => {
    expect(splitTicker("BRK.A")).toEqual({ tickerRoot: "BRK", tickerSuffix: "A" });
  });
  it("returns the whole ticker as root when there is no suffix", () => {
    expect(splitTicker("AAPL")).toEqual({ tickerRoot: "AAPL" });
  });
});

describe("makeInstrumentId", () => {
  it("prefers share_class_figi (HIGH)", () => {
    const r = makeInstrumentId({ shareClassFigi: "BBG001S5N8V8", compositeFigi: "BBG000B9XRY4", cik: "0000320193", ticker: "AAPL" });
    expect(r).toEqual({ instrumentId: "BBG001S5N8V8", identitySource: "SHARE_CLASS_FIGI", identityConfidence: "HIGH" });
  });
  it("falls back to composite_figi (HIGH)", () => {
    const r = makeInstrumentId({ compositeFigi: "BBG000B9XRY4", cik: "0000320193", ticker: "AAPL" });
    expect(r).toEqual({ instrumentId: "BBG000B9XRY4", identitySource: "COMPOSITE_FIGI", identityConfidence: "HIGH" });
  });
  it("falls back to EH:cik:ticker (MEDIUM)", () => {
    const r = makeInstrumentId({ cik: "0000320193", ticker: "AAPL" });
    expect(r).toEqual({ instrumentId: "EH:0000320193:AAPL", identitySource: "EH_CIK_TICKER", identityConfidence: "MEDIUM" });
  });
  it("falls back to EH:ticker:exchange (LOW)", () => {
    const r = makeInstrumentId({ ticker: "AAPL", primaryExchange: "XNAS" });
    expect(r).toEqual({ instrumentId: "EH:AAPL:XNAS", identitySource: "EH_TICKER_EXCHANGE", identityConfidence: "LOW" });
  });
  it("falls back to EH:ticker (LOW) when nothing else is present", () => {
    const r = makeInstrumentId({ ticker: "AAPL" });
    expect(r).toEqual({ instrumentId: "EH:AAPL", identitySource: "EH_TICKER", identityConfidence: "LOW" });
  });
  it("gives GOOG and GOOGL different ids when share-class figis differ", () => {
    const goog = makeInstrumentId({ shareClassFigi: "BBG009S39JX6", ticker: "GOOG" });
    const googl = makeInstrumentId({ shareClassFigi: "BBG009S3NB30", ticker: "GOOGL" });
    expect(goog.instrumentId).not.toBe(googl.instrumentId);
  });
  it("gives BRK.A and BRK.B different EH ids when figi is absent (full ticker, not root)", () => {
    const a = makeInstrumentId({ cik: "0001067983", ticker: "BRK.A" });
    const b = makeInstrumentId({ cik: "0001067983", ticker: "BRK.B" });
    expect(a.instrumentId).toBe("EH:0001067983:BRK.A");
    expect(b.instrumentId).toBe("EH:0001067983:BRK.B");
    expect(a.instrumentId).not.toBe(b.instrumentId);
    // and via the exchange fallback when cik is also absent:
    expect(makeInstrumentId({ ticker: "BRK.A", primaryExchange: "XNYS" }).instrumentId).toBe("EH:BRK.A:XNYS");
    expect(makeInstrumentId({ ticker: "BRK.B", primaryExchange: "XNYS" }).instrumentId).toBe("EH:BRK.B:XNYS");
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `npx vitest run tests/identity.test.ts`
Expected: FAIL — cannot find module `../src/identity.js`.

- [ ] **Step 3: Implement `src/identity.ts`**

```typescript
import type { IdentitySource, IdentityConfidence } from "./types.js";

/** Split a share-class suffix on the dot: "BRK.A" -> { root: "BRK", suffix: "A" }. */
export function splitTicker(ticker: string): { tickerRoot: string; tickerSuffix?: string } {
  const dot = ticker.indexOf(".");
  if (dot === -1) return { tickerRoot: ticker };
  return { tickerRoot: ticker.slice(0, dot), tickerSuffix: ticker.slice(dot + 1) };
}

/** Pure, deterministic identity. Fallback order is fixed (spec §6). No cross-day pinning. */
export function makeInstrumentId(input: {
  shareClassFigi?: string; compositeFigi?: string;
  cik?: string; ticker?: string; primaryExchange?: string;
}): { instrumentId: string; identitySource: IdentitySource; identityConfidence: IdentityConfidence } {
  if (input.shareClassFigi) return { instrumentId: input.shareClassFigi, identitySource: "SHARE_CLASS_FIGI", identityConfidence: "HIGH" };
  if (input.compositeFigi) return { instrumentId: input.compositeFigi, identitySource: "COMPOSITE_FIGI", identityConfidence: "HIGH" };
  // EH: fallbacks use the FULL ticker (e.g. "BRK.A"), never tickerRoot — share classes must not collide.
  if (input.cik && input.ticker) return { instrumentId: `EH:${input.cik}:${input.ticker}`, identitySource: "EH_CIK_TICKER", identityConfidence: "MEDIUM" };
  if (input.ticker && input.primaryExchange) return { instrumentId: `EH:${input.ticker}:${input.primaryExchange}`, identitySource: "EH_TICKER_EXCHANGE", identityConfidence: "LOW" };
  return { instrumentId: `EH:${input.ticker ?? ""}`, identitySource: "EH_TICKER", identityConfidence: "LOW" };
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run tests/identity.test.ts`
Expected: PASS (all cases).

- [ ] **Step 5: Verify the whole suite + commit**

```bash
npm run typecheck && npm test
git add src/identity.ts tests/identity.test.ts
git commit -m "feat(identity): pure makeInstrumentId + splitTicker

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Provider reference method (`listSecurities`)

**Files:**
- Modify: `src/providers/provider.ts`, `src/providers/polygon.ts`, `src/providers/finnhub.ts`, `src/providers/fake.ts`
- Test: `tests/polygon.test.ts`

**Interfaces:**
- Consumes: `SecurityMasterResult`, `SecurityMasterRow`, `ProviderFailure` from `../types.js`; `makeInstrumentId`, `splitTicker` from `../identity.js`.
- Produces: `MarketDataProvider.listSecurities(asOfDate: string, tickers?: string[]): Promise<SecurityMasterResult>` implemented by `PolygonProvider`, `FinnhubProvider`, `FakeProvider`. `FakeProvider`'s constructor gains an optional second arg `securities?: SecurityMasterRow[]`.

- [ ] **Step 1: Write the failing Polygon test**

Append to `tests/polygon.test.ts`:

```typescript
import type { SecurityMasterRow } from "../src/types.js";

describe("PolygonProvider.listSecurities", () => {
  it("maps reference fields and assigns a FIGI-based instrumentId", async () => {
    const fakeFetch = async (_url: string) => ({
      ok: true, status: 200,
      json: async () => ({ status: "OK", results: [{
        ticker: "AAPL", name: "Apple Inc.", market: "stocks", locale: "us", type: "CS",
        currency_name: "usd", cik: "0000320193", composite_figi: "BBG000B9XRY4",
        share_class_figi: "BBG001S5N8V8", primary_exchange: "XNAS", active: true,
        list_date: "1980-12-12", last_updated_utc: "2026-06-30T00:00:00Z",
      }] }),
    });
    const provider = new PolygonProvider("k", fakeFetch, 600000);
    const res = await provider.listSecurities("2026-06-30", ["AAPL"]);
    expect(res.failures).toEqual([]);
    expect(res.securities).toHaveLength(1);
    const s = res.securities[0]!;
    expect(s.ticker).toBe("AAPL");
    expect(s.name).toBe("Apple Inc.");
    expect(s.cik).toBe("0000320193");
    expect(s.instrumentId).toBe("BBG001S5N8V8");
    expect(s.identitySource).toBe("SHARE_CLASS_FIGI");
    expect(s.referenceStatus).toBe("FOUND");
    expect(s.active).toBe(true);
    expect(s.asOfDate).toBe("2026-06-30");
  });

  it("returns a missing_reference_data failure when a ticker has no result", async () => {
    const fakeFetch = async (_url: string) => ({ ok: true, status: 200, json: async () => ({ status: "OK", results: [] }) });
    const provider = new PolygonProvider("k", fakeFetch, 600000);
    const res = await provider.listSecurities("2026-06-30", ["ZZZZ"]);
    expect(res.securities).toEqual([]);
    expect(res.failures).toHaveLength(1);
    expect(res.failures[0]).toMatchObject({ ticker: "ZZZZ", reason: "missing_reference_data" });
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `npx vitest run tests/polygon.test.ts`
Expected: FAIL — `provider.listSecurities is not a function`.

- [ ] **Step 3: Add `listSecurities` to the interface**

In `src/providers/provider.ts`:

```typescript
import type { ProviderResult, SecurityMasterResult } from "../types.js";

export interface MarketDataProvider {
  readonly name: string;
  readonly version: string;
  getLatestBars(date: string, tickers: string[]): Promise<ProviderResult>;
  getHistory(ticker: string, lookbackDays: number, endDate?: string): Promise<ProviderResult>;
  listSecurities(asOfDate: string, tickers?: string[]): Promise<SecurityMasterResult>;
}
```

- [ ] **Step 4: Implement `listSecurities` in `src/providers/polygon.ts`**

Add imports at the top (extend the existing type import):

```typescript
import type { VendorBar, ProviderResult, ProviderFailure, SecurityMasterRow, SecurityMasterResult } from "../types.js";
import { makeInstrumentId, splitTicker } from "../identity.js";
```

Add the Polygon reference response shape near the other interfaces:

```typescript
interface RefTicker {
  ticker: string; name?: string; market?: string; locale?: string; type?: string;
  currency_name?: string; cik?: string; composite_figi?: string; share_class_figi?: string;
  primary_exchange?: string; active?: boolean; list_date?: string; delisted_utc?: string; last_updated_utc?: string;
}
interface RefResponse { status?: string; results?: RefTicker[]; }
```

Add the method to the `PolygonProvider` class:

```typescript
  async listSecurities(asOfDate: string, tickers: string[] = []): Promise<SecurityMasterResult> {
    const securities: SecurityMasterRow[] = [];
    const failures: ProviderFailure[] = [];
    const at = new Date().toISOString();
    for (const ticker of tickers) {
      const url = `${BASE}/v3/reference/tickers?ticker=${encodeURIComponent(ticker)}&date=${asOfDate}&limit=1`;
      try {
        const json = (await this.get(url)) as RefResponse;
        ensureOk(json.status);
        const r = (json.results ?? [])[0];
        if (!r) { failures.push({ ticker, date: asOfDate, reason: "missing_reference_data" }); continue; }
        const { tickerRoot, tickerSuffix } = splitTicker(r.ticker); // metadata only
        const id = makeInstrumentId({
          shareClassFigi: r.share_class_figi, compositeFigi: r.composite_figi,
          cik: r.cik, ticker: r.ticker, primaryExchange: r.primary_exchange,
        });
        securities.push({
          instrumentId: id.instrumentId, ticker: r.ticker, tickerRoot, tickerSuffix,
          name: r.name, market: r.market, locale: r.locale, type: r.type, currencyName: r.currency_name,
          cik: r.cik, compositeFigi: r.composite_figi, shareClassFigi: r.share_class_figi,
          primaryExchange: r.primary_exchange, active: r.active ?? true,
          listDate: r.list_date, delistedUtc: r.delisted_utc, lastUpdatedUtc: r.last_updated_utc,
          identitySource: id.identitySource, identityConfidence: id.identityConfidence, referenceStatus: "FOUND",
          source: this.name, sourceVersion: this.version, asOfDate, ingestedAt: at,
        });
      } catch (err) {
        failures.push({ ticker, date: asOfDate, reason: "provider_error", message: (err as Error).message });
      }
    }
    return { securities, failures };
  }
```

- [ ] **Step 5: Implement a stub `listSecurities` in `src/providers/finnhub.ts`**

Finnhub is inactive but must satisfy the interface. Add the import and method:

```typescript
import type { /* existing */ SecurityMasterResult } from "../types.js";
```

```typescript
  async listSecurities(asOfDate: string, tickers: string[] = []): Promise<SecurityMasterResult> {
    // Finnhub free tier has no point-in-time reference master; report all as missing so the
    // pipeline mints EH: fallbacks. (Finnhub is not the active provider.)
    return { securities: [], failures: tickers.map((t) => ({ ticker: t, date: asOfDate, reason: "missing_reference_data" })) };
  }
```

- [ ] **Step 6: Implement `listSecurities` in `src/providers/fake.ts`**

```typescript
import type { VendorBar, ProviderResult, ProviderFailure, SecurityMasterRow, SecurityMasterResult } from "../types.js";

export class FakeProvider implements MarketDataProvider {
  readonly name = "fake";
  readonly version = "1.0";
  constructor(
    private readonly history: Map<string, VendorBar[]>,
    private readonly securities: SecurityMasterRow[] = [],
  ) {}

  async listSecurities(asOfDate: string, tickers: string[] = []): Promise<SecurityMasterResult> {
    const byTicker = new Map(this.securities.map((s) => [s.ticker, s]));
    const securities: SecurityMasterRow[] = [];
    const failures: ProviderFailure[] = [];
    for (const t of tickers) {
      const s = byTicker.get(t);
      if (s) securities.push({ ...s, asOfDate });
      else failures.push({ ticker: t, date: asOfDate, reason: "missing_reference_data" });
    }
    return { securities, failures };
  }

  // ...existing getLatestBars / getHistory unchanged...
}
```

- [ ] **Step 7: Run the Polygon test + full suite**

Run: `npx vitest run tests/polygon.test.ts && npm run typecheck && npm test`
Expected: PASS. (Existing `FakeProvider` callers still compile — the second constructor arg is optional.)

- [ ] **Step 8: Commit**

```bash
git add src/providers/provider.ts src/providers/polygon.ts src/providers/finnhub.ts src/providers/fake.ts tests/polygon.test.ts
git commit -m "feat(providers): add listSecurities (polygon reference, finnhub stub, fake)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Security master storage + build + identity-change detection

**Files:**
- Create: `src/securityMaster.ts`
- Modify: `src/storage.ts` (add `SECURITIES_SCHEMA`, `securitiesKey`, `writeSecurities`)
- Test: `tests/securityMaster.test.ts`

**Interfaces:**
- Consumes: `SecurityMasterRow`, `SecurityMasterResult` from `../types.js`; `makeInstrumentId`, `splitTicker` from `./identity.js`; `SECURITIES_SCHEMA`, `toParquet` from `./storage.js`; `@aws-sdk/client-s3`.
- Produces:
  - `securitiesKey(asOfDate: string): string` → `reference/securities/asOf=<asOfDate>/part.parquet`
  - `securitiesStateKey(): string` → `reference/state/securities.json`
  - `buildSecurityMaster(universeTickers: string[], result: SecurityMasterResult, asOfDate: string, source: string, sourceVersion: string, ingestedAt: string): { securities: SecurityMasterRow[]; missingTickers: string[]; duplicateTickers: string[]; emptyMaster: boolean }`
  - `buildTickerMap(rows: SecurityMasterRow[]): Map<string, SecurityMasterRow>`
  - `detectIdentityChanges(today: SecurityMasterRow[], prior: SecurityMasterRow[]): { ticker: string; from: string; to: string }[]`
  - `writeSecurities(s3, bucket, asOfDate, rows): Promise<string>` (in `storage.ts`)
  - `writeSecuritiesState(s3, bucket, rows): Promise<void>`, `readSecuritiesState(s3, bucket): Promise<SecurityMasterRow[]>`

- [ ] **Step 1: Write the failing test**

Create `tests/securityMaster.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { buildSecurityMaster, buildTickerMap, detectIdentityChanges, securitiesKey } from "../src/securityMaster.js";
import type { SecurityMasterRow, SecurityMasterResult } from "../src/types.js";

const found = (ticker: string, figi: string, active = true): SecurityMasterRow => ({
  instrumentId: figi, ticker, tickerRoot: ticker, name: `${ticker} Inc`, active,
  shareClassFigi: figi, identitySource: "SHARE_CLASS_FIGI", identityConfidence: "HIGH",
  referenceStatus: "FOUND", source: "polygon", sourceVersion: "1.0", asOfDate: "2026-06-30", ingestedAt: "x",
});

describe("securitiesKey", () => {
  it("builds the asOf-partitioned key", () => {
    expect(securitiesKey("2026-06-30")).toBe("reference/securities/asOf=2026-06-30/part.parquet");
  });
});

describe("buildSecurityMaster", () => {
  it("keeps one row per universe ticker and mints EH: fallbacks for missing reference data", () => {
    const result: SecurityMasterResult = {
      securities: [found("AAPL", "BBG_AAPL")],
      failures: [{ ticker: "ZZZZ", date: "2026-06-30", reason: "missing_reference_data" }],
    };
    const out = buildSecurityMaster(["AAPL", "ZZZZ"], result, "2026-06-30", "polygon", "1.0", "x");
    expect(out.securities).toHaveLength(2);
    const zzzz = out.securities.find((s) => s.ticker === "ZZZZ")!;
    expect(zzzz.instrumentId).toBe("EH:ZZZZ");
    expect(zzzz.identitySource).toBe("EH_TICKER");
    expect(zzzz.identityConfidence).toBe("LOW");
    expect(zzzz.referenceStatus).toBe("MISSING_FALLBACK");
    expect(zzzz.active).toBe(true);
    expect(out.missingTickers).toEqual(["ZZZZ"]);
    expect(out.emptyMaster).toBe(false);
  });

  it("flags duplicate provider rows for one ticker and keeps the first", () => {
    const result: SecurityMasterResult = { securities: [found("AAPL", "BBG_A1"), found("AAPL", "BBG_A2")], failures: [] };
    const out = buildSecurityMaster(["AAPL"], result, "2026-06-30", "polygon", "1.0", "x");
    expect(out.securities).toHaveLength(1);
    expect(out.securities[0]!.instrumentId).toBe("BBG_A1");
    expect(out.duplicateTickers).toEqual(["AAPL"]);
  });

  it("reports emptyMaster when the provider returned nothing and mints all fallbacks", () => {
    const out = buildSecurityMaster(["AAPL", "MSFT"], { securities: [], failures: [] }, "2026-06-30", "polygon", "1.0", "x");
    expect(out.emptyMaster).toBe(true);
    expect(out.securities.map((s) => s.instrumentId).sort()).toEqual(["EH:AAPL", "EH:MSFT"]);
  });

  it("preserves active=false for delisted rows", () => {
    const result: SecurityMasterResult = { securities: [found("DEAD", "BBG_DEAD", false)], failures: [] };
    const out = buildSecurityMaster(["DEAD"], result, "2026-06-30", "polygon", "1.0", "x");
    expect(out.securities[0]!.active).toBe(false);
  });
});

describe("buildTickerMap", () => {
  it("maps ticker -> row", () => {
    const map = buildTickerMap([found("AAPL", "BBG_AAPL")]);
    expect(map.get("AAPL")!.instrumentId).toBe("BBG_AAPL");
  });
});

describe("detectIdentityChanges", () => {
  it("reports a ticker whose instrumentId changed since the prior snapshot", () => {
    const prior = [found("AAPL", "OLD")];
    const today = [found("AAPL", "NEW")];
    expect(detectIdentityChanges(today, prior)).toEqual([{ ticker: "AAPL", from: "OLD", to: "NEW" }]);
  });
  it("is silent when ids are unchanged or the ticker is new", () => {
    expect(detectIdentityChanges([found("AAPL", "X")], [found("AAPL", "X")])).toEqual([]);
    expect(detectIdentityChanges([found("NEWCO", "X")], [])).toEqual([]);
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `npx vitest run tests/securityMaster.test.ts`
Expected: FAIL — cannot find module `../src/securityMaster.js`.

- [ ] **Step 3: Add `SECURITIES_SCHEMA`, `securitiesKey`, `writeSecurities` to `src/storage.ts`**

Add the import:

```typescript
import type { RawBarRow, MetricRow, SecurityMasterRow, SymbolAliasRow } from "./types.js";
```

Add the schema (after `METRIC_SCHEMA`):

```typescript
export const SECURITIES_SCHEMA = new ParquetSchema({
  instrumentId: { type: "UTF8" }, ticker: { type: "UTF8" },
  tickerRoot: { type: "UTF8", optional: true }, tickerSuffix: { type: "UTF8", optional: true },
  name: { type: "UTF8", optional: true }, market: { type: "UTF8", optional: true },
  locale: { type: "UTF8", optional: true }, type: { type: "UTF8", optional: true },
  currencyName: { type: "UTF8", optional: true },
  cik: { type: "UTF8", optional: true }, compositeFigi: { type: "UTF8", optional: true },
  shareClassFigi: { type: "UTF8", optional: true }, primaryExchange: { type: "UTF8", optional: true },
  active: { type: "BOOLEAN" },
  listDate: { type: "UTF8", optional: true }, delistedUtc: { type: "UTF8", optional: true },
  lastUpdatedUtc: { type: "UTF8", optional: true },
  identitySource: { type: "UTF8" }, identityConfidence: { type: "UTF8" }, referenceStatus: { type: "UTF8" },
  source: { type: "UTF8" }, sourceVersion: { type: "UTF8" }, asOfDate: { type: "UTF8" }, ingestedAt: { type: "UTF8" },
});
```

Add the key builder + writer (near the other key builders / writers):

```typescript
export function securitiesKey(asOfDate: string): string {
  return `reference/securities/asOf=${asOfDate}/part.parquet`;
}

export async function writeSecurities(s3: S3Client, bucket: string, asOfDate: string, rows: SecurityMasterRow[]): Promise<string> {
  const key = securitiesKey(asOfDate);
  const body = await toParquet(SECURITIES_SCHEMA, rows as unknown as Record<string, unknown>[]);
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
  return key;
}
```

- [ ] **Step 4: Implement `src/securityMaster.ts`**

```typescript
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import type { SecurityMasterRow, SecurityMasterResult } from "./types.js";
import { makeInstrumentId, splitTicker } from "./identity.js";

export { securitiesKey, writeSecurities } from "./storage.js";

export function securitiesStateKey(): string {
  return "reference/state/securities.json";
}

/** §4 invariant: exactly one row per universe ticker. Provider rows win; missing tickers are minted EH:<ticker>. */
export function buildSecurityMaster(
  universeTickers: string[], result: SecurityMasterResult,
  asOfDate: string, source: string, sourceVersion: string, ingestedAt: string,
): { securities: SecurityMasterRow[]; missingTickers: string[]; duplicateTickers: string[]; emptyMaster: boolean } {
  const byTicker = new Map<string, SecurityMasterRow>();
  const duplicateTickers: string[] = [];
  for (const s of result.securities) {
    if (byTicker.has(s.ticker)) { duplicateTickers.push(s.ticker); continue; } // keep first
    byTicker.set(s.ticker, { ...s, asOfDate, source, sourceVersion, ingestedAt });
  }
  const missingTickers: string[] = [];
  for (const ticker of universeTickers) {
    if (byTicker.has(ticker)) continue;
    missingTickers.push(ticker);
    const { tickerRoot, tickerSuffix } = splitTicker(ticker);
    const id = makeInstrumentId({ ticker }); // bare-ticker fallback -> EH:<ticker>
    byTicker.set(ticker, {
      instrumentId: id.instrumentId, ticker, tickerRoot, tickerSuffix, active: true,
      identitySource: id.identitySource, identityConfidence: id.identityConfidence, referenceStatus: "MISSING_FALLBACK",
      source, sourceVersion, asOfDate, ingestedAt,
    });
  }
  return {
    securities: [...byTicker.values()],
    missingTickers, duplicateTickers,
    emptyMaster: result.securities.length === 0,
  };
}

export function buildTickerMap(rows: SecurityMasterRow[]): Map<string, SecurityMasterRow> {
  return new Map(rows.map((r) => [r.ticker, r]));
}

/** Non-blocking guard: a universe ticker whose instrumentId differs from the prior snapshot. */
export function detectIdentityChanges(
  today: SecurityMasterRow[], prior: SecurityMasterRow[],
): { ticker: string; from: string; to: string }[] {
  const priorByTicker = new Map(prior.map((r) => [r.ticker, r.instrumentId]));
  const changes: { ticker: string; from: string; to: string }[] = [];
  for (const r of today) {
    const was = priorByTicker.get(r.ticker);
    if (was && was !== r.instrumentId) changes.push({ ticker: r.ticker, from: was, to: r.instrumentId });
  }
  return changes;
}

export async function writeSecuritiesState(s3: S3Client, bucket: string, rows: SecurityMasterRow[]): Promise<void> {
  await s3.send(new PutObjectCommand({
    Bucket: bucket, Key: securitiesStateKey(), Body: JSON.stringify(rows), ContentType: "application/json",
  }));
}

export async function readSecuritiesState(s3: S3Client, bucket: string): Promise<SecurityMasterRow[]> {
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: securitiesStateKey() }));
    const parsed = JSON.parse(await res.Body!.transformToString());
    return Array.isArray(parsed) ? (parsed as SecurityMasterRow[]) : [];
  } catch { return []; }
}
```

- [ ] **Step 5: Run the test + full suite**

Run: `npx vitest run tests/securityMaster.test.ts && npm run typecheck && npm test`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/securityMaster.ts src/storage.ts tests/securityMaster.test.ts
git commit -m "feat(security-master): build/write/read master, ticker map, identity-change guard

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Symbol aliases (forward-only windows)

**Files:**
- Create: `src/symbolAliases.ts`
- Modify: `src/storage.ts` (add `SYMBOL_ALIASES_SCHEMA`, `symbolAliasesKey`, `writeSymbolAliases`)
- Test: `tests/symbolAliases.test.ts`

**Interfaces:**
- Consumes: `SecurityMasterRow`, `SymbolAliasRow` from `../types.js`; `SYMBOL_ALIASES_SCHEMA`, `toParquet` from `./storage.js`; `@aws-sdk/client-s3`.
- Produces:
  - `symbolAliasesKey(asOfDate: string): string` → `reference/symbol_aliases/asOf=<asOfDate>/part.parquet`
  - `aliasesStateKey(): string` → `reference/state/symbol_aliases.json`
  - `buildSymbolAliases(securities: SecurityMasterRow[], priorAliases: SymbolAliasRow[], asOfDate: string, previousTradingDay: string, source: string, sourceVersion: string, createdAt: string): { aliases: SymbolAliasRow[]; conflicts: string[] }`
  - `writeSymbolAliases(s3, bucket, asOfDate, rows): Promise<string>` (in `storage.ts`)
  - `writeAliasesState(s3, bucket, rows): Promise<void>`, `readAliasesState(s3, bucket): Promise<SymbolAliasRow[]>`

- [ ] **Step 1: Write the failing test**

Create `tests/symbolAliases.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { buildSymbolAliases, symbolAliasesKey } from "../src/symbolAliases.js";
import type { SecurityMasterRow, SymbolAliasRow } from "../src/types.js";

const sec = (instrumentId: string, ticker: string): SecurityMasterRow => ({
  instrumentId, ticker, tickerRoot: ticker, active: true,
  identitySource: "SHARE_CLASS_FIGI", identityConfidence: "HIGH", referenceStatus: "FOUND",
  source: "polygon", sourceVersion: "1.0", asOfDate: "2026-06-30", ingestedAt: "x",
});

describe("symbolAliasesKey", () => {
  it("builds the asOf-partitioned key", () => {
    expect(symbolAliasesKey("2026-06-30")).toBe("reference/symbol_aliases/asOf=2026-06-30/part.parquet");
  });
});

describe("buildSymbolAliases", () => {
  it("opens a new alias for a security with no prior alias", () => {
    const { aliases } = buildSymbolAliases([sec("ID1", "AAPL")], [], "2026-06-30", "2026-06-29", "polygon", "1.0", "t");
    expect(aliases).toHaveLength(1);
    expect(aliases[0]).toMatchObject({ instrumentId: "ID1", ticker: "AAPL", validFrom: "2026-06-30", validTo: null, confidence: "MEDIUM" });
  });

  it("carries an unchanged open alias forward without duplicating", () => {
    const prior: SymbolAliasRow[] = [{ instrumentId: "ID1", ticker: "AAPL", validFrom: "2026-06-01", validTo: null, source: "polygon", sourceVersion: "1.0", asOfDate: "2026-06-29", confidence: "MEDIUM", createdAt: "t0" }];
    const { aliases } = buildSymbolAliases([sec("ID1", "AAPL")], prior, "2026-06-30", "2026-06-29", "polygon", "1.0", "t");
    expect(aliases).toHaveLength(1);
    expect(aliases[0]).toMatchObject({ ticker: "AAPL", validFrom: "2026-06-01", validTo: null });
  });

  it("on a forward rename, closes the old ticker and opens the new one", () => {
    const prior: SymbolAliasRow[] = [{ instrumentId: "ID1", ticker: "FB", validFrom: "2012-05-18", validTo: null, source: "polygon", sourceVersion: "1.0", asOfDate: "2026-06-29", confidence: "MEDIUM", createdAt: "t0" }];
    const { aliases } = buildSymbolAliases([sec("ID1", "META")], prior, "2026-06-30", "2026-06-29", "polygon", "1.0", "t");
    const fb = aliases.find((a) => a.ticker === "FB")!;
    const meta = aliases.find((a) => a.ticker === "META")!;
    expect(fb.validTo).toBe("2026-06-29");
    expect(meta).toMatchObject({ validFrom: "2026-06-30", validTo: null });
    expect(aliases).toHaveLength(2);
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `npx vitest run tests/symbolAliases.test.ts`
Expected: FAIL — cannot find module `../src/symbolAliases.js`.

- [ ] **Step 3: Add `SYMBOL_ALIASES_SCHEMA`, `symbolAliasesKey`, `writeSymbolAliases` to `src/storage.ts`**

Add the schema (after `SECURITIES_SCHEMA`):

```typescript
export const SYMBOL_ALIASES_SCHEMA = new ParquetSchema({
  instrumentId: { type: "UTF8" }, ticker: { type: "UTF8" },
  tickerRoot: { type: "UTF8", optional: true }, tickerSuffix: { type: "UTF8", optional: true },
  primaryExchange: { type: "UTF8", optional: true },
  validFrom: { type: "UTF8" }, validTo: { type: "UTF8", optional: true },
  source: { type: "UTF8" }, sourceVersion: { type: "UTF8" }, asOfDate: { type: "UTF8" },
  confidence: { type: "UTF8" }, createdAt: { type: "UTF8" },
});
```

Add the key builder + writer (the writer converts `validTo: null` → omitted, which the optional column stores as null):

```typescript
export function symbolAliasesKey(asOfDate: string): string {
  return `reference/symbol_aliases/asOf=${asOfDate}/part.parquet`;
}

export async function writeSymbolAliases(s3: S3Client, bucket: string, asOfDate: string, rows: SymbolAliasRow[]): Promise<string> {
  const key = symbolAliasesKey(asOfDate);
  const flat = rows.map((r) => ({ ...r, validTo: r.validTo ?? undefined }));
  const body = await toParquet(SYMBOL_ALIASES_SCHEMA, flat as unknown as Record<string, unknown>[]);
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
  return key;
}
```

- [ ] **Step 4: Implement `src/symbolAliases.ts`**

```typescript
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import type { SecurityMasterRow, SymbolAliasRow } from "./types.js";

export { symbolAliasesKey, writeSymbolAliases } from "./storage.js";

export function aliasesStateKey(): string {
  return "reference/state/symbol_aliases.json";
}

/** Forward-only alias windows (spec §8.2). No historical reconstruction. */
export function buildSymbolAliases(
  securities: SecurityMasterRow[], priorAliases: SymbolAliasRow[],
  asOfDate: string, previousTradingDay: string, source: string, sourceVersion: string, createdAt: string,
): { aliases: SymbolAliasRow[]; conflicts: string[] } {
  const conflicts: string[] = [];
  // Today's open ticker per instrumentId (one security per instrumentId expected).
  const currentTickerById = new Map<string, SecurityMasterRow>();
  for (const s of securities) {
    if (currentTickerById.has(s.instrumentId) && currentTickerById.get(s.instrumentId)!.ticker !== s.ticker) {
      conflicts.push(s.ticker); continue;
    }
    currentTickerById.set(s.instrumentId, s);
  }

  // Clone prior rows so we can close renamed ones; key open rows by instrumentId.
  const out: SymbolAliasRow[] = priorAliases.map((a) => ({ ...a }));
  const priorOpenById = new Map<string, SymbolAliasRow>();
  for (const a of out) if (a.validTo === null) priorOpenById.set(a.instrumentId, a);

  for (const [instrumentId, sec] of currentTickerById) {
    const open = priorOpenById.get(instrumentId);
    if (!open) {
      out.push(makeAlias(sec, asOfDate, source, sourceVersion, createdAt));
    } else if (open.ticker !== sec.ticker) {
      open.validTo = previousTradingDay;                              // close the old ticker
      out.push(makeAlias(sec, asOfDate, source, sourceVersion, createdAt)); // open the new ticker
    } // else unchanged -> carry forward (already in out)
  }

  // De-dupe identical rows by (instrumentId, ticker, validFrom, validTo).
  const seen = new Set<string>();
  const aliases = out.filter((a) => {
    const k = `${a.instrumentId}|${a.ticker}|${a.validFrom}|${a.validTo ?? ""}`;
    if (seen.has(k)) return false; seen.add(k); return true;
  });
  return { aliases, conflicts };
}

function makeAlias(s: SecurityMasterRow, asOfDate: string, source: string, sourceVersion: string, createdAt: string): SymbolAliasRow {
  return {
    instrumentId: s.instrumentId, ticker: s.ticker, tickerRoot: s.tickerRoot, tickerSuffix: s.tickerSuffix,
    primaryExchange: s.primaryExchange, validFrom: asOfDate, validTo: null,
    source, sourceVersion, asOfDate, confidence: "MEDIUM", createdAt,
  };
}

export async function writeAliasesState(s3: S3Client, bucket: string, rows: SymbolAliasRow[]): Promise<void> {
  await s3.send(new PutObjectCommand({
    Bucket: bucket, Key: aliasesStateKey(), Body: JSON.stringify(rows), ContentType: "application/json",
  }));
}

export async function readAliasesState(s3: S3Client, bucket: string): Promise<SymbolAliasRow[]> {
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: aliasesStateKey() }));
    const parsed = JSON.parse(await res.Body!.transformToString());
    return Array.isArray(parsed) ? (parsed as SymbolAliasRow[]) : [];
  } catch { return []; }
}
```

- [ ] **Step 5: Run the test + full suite**

Run: `npx vitest run tests/symbolAliases.test.ts && npm run typecheck && npm test`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/symbolAliases.ts src/storage.ts tests/symbolAliases.test.ts
git commit -m "feat(aliases): forward-only symbol alias windows + state read/write

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Resolver

**Files:**
- Create: `src/resolver.ts`
- Test: `tests/resolver.test.ts`

**Interfaces:**
- Consumes: `VendorBar`, `SecurityMasterRow`, `ResolvedVendorBar`, `ErrorRecord` from `./types.js`; `buildTickerMap` from `./securityMaster.js`.
- Produces: `resolveBarsToInstruments(bars: VendorBar[], securities: SecurityMasterRow[], tradingDay: string, runId: string, source: string, universeVersion: string): { resolved: ResolvedVendorBar[]; errors: ErrorRecord[] }`

- [ ] **Step 1: Write the failing test**

Create `tests/resolver.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { resolveBarsToInstruments } from "../src/resolver.js";
import type { VendorBar, SecurityMasterRow } from "../src/types.js";

const bar = (ticker: string): VendorBar => ({ ticker, date: "2026-06-30", open: 1, high: 1, low: 1, close: 1, adjustedClose: null, isAdjusted: false, volume: 1, source: "polygon", sourceVersion: "1.0", ingestedAt: "x" });
const sec = (instrumentId: string, ticker: string): SecurityMasterRow => ({ instrumentId, ticker, tickerRoot: ticker, active: true, identitySource: "SHARE_CLASS_FIGI", identityConfidence: "HIGH", referenceStatus: "FOUND", source: "polygon", sourceVersion: "1.0", asOfDate: "2026-06-30", ingestedAt: "x" });

describe("resolveBarsToInstruments", () => {
  it("resolves a bar to its instrumentId and keeps the original ticker", () => {
    const { resolved, errors } = resolveBarsToInstruments([bar("AAPL")], [sec("BBG_AAPL", "AAPL")], "2026-06-30", "R", "polygon", "2026-06-30");
    expect(errors).toEqual([]);
    expect(resolved).toHaveLength(1);
    expect(resolved[0]!.instrumentId).toBe("BBG_AAPL");
    expect(resolved[0]!.ticker).toBe("AAPL");
  });

  it("records unresolved_instrument and excludes a bar with no security row", () => {
    const { resolved, errors } = resolveBarsToInstruments([bar("ZZZZ")], [sec("BBG_AAPL", "AAPL")], "2026-06-30", "R", "polygon", "2026-06-30");
    expect(resolved).toEqual([]);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ ticker: "ZZZZ", reason: "unresolved_instrument", runId: "R" });
  });

  it("never mints an id (unknown ticker is excluded, not given EH:)", () => {
    const { resolved } = resolveBarsToInstruments([bar("ZZZZ")], [], "2026-06-30", "R", "polygon", "2026-06-30");
    expect(resolved).toEqual([]);
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `npx vitest run tests/resolver.test.ts`
Expected: FAIL — cannot find module `../src/resolver.js`.

- [ ] **Step 3: Implement `src/resolver.ts`**

```typescript
import type { VendorBar, SecurityMasterRow, ResolvedVendorBar, ErrorRecord } from "./types.js";
import { buildTickerMap } from "./securityMaster.js";

/** Resolve vendor bars to instrument identity by exact ticker. Never mints — minting is the master's job. */
export function resolveBarsToInstruments(
  bars: VendorBar[], securities: SecurityMasterRow[],
  tradingDay: string, runId: string, source: string, universeVersion: string,
): { resolved: ResolvedVendorBar[]; errors: ErrorRecord[] } {
  const map = buildTickerMap(securities);
  const resolved: ResolvedVendorBar[] = [];
  const errors: ErrorRecord[] = [];
  for (const bar of bars) {
    const sec = map.get(bar.ticker);
    if (!sec) {
      errors.push({
        runId, tradingDay, source, universeVersion, ticker: bar.ticker,
        reason: "unresolved_instrument",
        message: `No security master row found for ticker ${bar.ticker} on ${tradingDay}`,
        createdAt: new Date().toISOString(),
      });
      continue;
    }
    resolved.push({ ...bar, instrumentId: sec.instrumentId });
  }
  return { resolved, errors };
}
```

- [ ] **Step 4: Run the test + full suite**

Run: `npx vitest run tests/resolver.test.ts && npm run typecheck && npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/resolver.ts tests/resolver.test.ts
git commit -m "feat(resolver): resolveBarsToInstruments (exact-ticker, never mints)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: History cache re-keyed by instrumentId

**Files:**
- Modify: `src/historyCache.ts`
- Test: `tests/historyCache.test.ts`

**Interfaces:**
- Consumes: `VendorBar` from `./types.js`.
- Produces: `historyCacheKey(source: string, instrumentId: string): string` → `history/<source>/instrumentId=<instrumentId>/current.json`; `readHistoryCache(s3, bucket, source, instrumentId)`, `writeHistoryCache(s3, bucket, source, instrumentId, bars, maxBars?)` — same shapes, the third positional value is now an `instrumentId`.

- [ ] **Step 1: Update the failing test**

Replace the `historyCacheKey` test and the read/write argument labels in `tests/historyCache.test.ts`:

```typescript
describe("historyCacheKey", () => {
  it("builds the per-instrument key", () => {
    expect(historyCacheKey("polygon", "BBG000B9XRY4")).toBe("history/polygon/instrumentId=BBG000B9XRY4/current.json");
  });
});
```

(Leave the `readHistoryCache`/`writeHistoryCache` tests as-is; their third argument is now interpreted as an instrumentId but the call shape is unchanged — they still pass.)

- [ ] **Step 2: Run it to verify it fails**

Run: `npx vitest run tests/historyCache.test.ts`
Expected: FAIL — `expected 'history/polygon/ticker=BBG000B9XRY4/current.json' to be 'history/polygon/instrumentId=BBG000B9XRY4/current.json'`.

- [ ] **Step 3: Re-key `src/historyCache.ts`**

Rename the third parameter and the key segment in all three functions:

```typescript
export function historyCacheKey(source: string, instrumentId: string): string {
  return `history/${source}/instrumentId=${instrumentId}/current.json`;
}

export async function readHistoryCache(s3: S3Client, bucket: string, source: string, instrumentId: string): Promise<VendorBar[]> {
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: historyCacheKey(source, instrumentId) }));
    const text = await res.Body!.transformToString();
    const parsed = JSON.parse(text);
    return Array.isArray(parsed) ? (parsed as VendorBar[]) : [];
  } catch {
    return [];
  }
}

export async function writeHistoryCache(s3: S3Client, bucket: string, source: string, instrumentId: string, bars: VendorBar[], maxBars = 400): Promise<void> {
  const trimmed = bars.slice(-maxBars);
  await s3.send(new PutObjectCommand({
    Bucket: bucket, Key: historyCacheKey(source, instrumentId),
    Body: JSON.stringify(trimmed), ContentType: "application/json",
  }));
}
```

- [ ] **Step 4: Run the test + full suite**

Run: `npx vitest run tests/historyCache.test.ts && npm run typecheck && npm test`
Expected: PASS. (`handler.ts` passes a string through unchanged, so it still compiles; its value becomes an instrumentId in Task 9.)

- [ ] **Step 5: Commit**

```bash
git add src/historyCache.ts tests/historyCache.test.ts
git commit -m "feat(history-cache): key by instrumentId instead of ticker

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Glue / SAM — instrumentId column + securities & symbol_aliases tables + asOf partition

**Files:**
- Modify: `src/glue.ts`, `template.yaml`
- Test: `tests/glue.test.ts`

**Interfaces:**
- Consumes: `@aws-sdk/client-glue`.
- Produces: `addAsOfPartition(glue, database, table, bucket, prefix, asOf): Promise<void>` (registers a single `asOf=<asOf>` partition).

- [ ] **Step 1: Write the failing test**

Read the existing `tests/glue.test.ts` first to match its mocking idiom, then append:

```typescript
import { addAsOfPartition } from "../src/glue.js";

describe("addAsOfPartition", () => {
  it("registers a single asOf partition with the asOf location", async () => {
    let created: { Values: string[]; StorageDescriptor: { Location: string } } | undefined;
    const glue = {
      send: async (c: any) => {
        if (c.constructor.name === "GetTableCommand") return { Table: { StorageDescriptor: { Columns: [], SerdeInfo: {} } } };
        if (c.constructor.name === "BatchCreatePartitionCommand") created = c.input.PartitionInputList[0];
        return {};
      },
    } as never;
    await addAsOfPartition(glue, "edgehub", "securities", "bkt", "reference/securities", "2026-06-30");
    expect(created!.Values).toEqual(["2026-06-30"]);
    expect(created!.StorageDescriptor.Location).toBe("s3://bkt/reference/securities/asOf=2026-06-30/");
  });
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `npx vitest run tests/glue.test.ts`
Expected: FAIL — `addAsOfPartition` is not exported.

- [ ] **Step 3: Implement `addAsOfPartition` in `src/glue.ts`**

```typescript
export async function addAsOfPartition(glue: GlueClient, database: string, table: string, bucket: string, prefix: string, asOf: string): Promise<void> {
  const location = `s3://${bucket}/${prefix}/asOf=${asOf}/`;
  const tbl = await glue.send(new GetTableCommand({ DatabaseName: database, Name: table }));
  const storageDescriptor = { ...(tbl.Table?.StorageDescriptor ?? {}), Location: location };
  try {
    await glue.send(new BatchCreatePartitionCommand({
      DatabaseName: database, TableName: table,
      PartitionInputList: [{ Values: [asOf], StorageDescriptor: storageDescriptor }],
    }));
  } catch (err) {
    if (((err as { name?: string }).name ?? "").includes("AlreadyExists")) return;
    throw err;
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run tests/glue.test.ts`
Expected: PASS.

- [ ] **Step 5: Add `instrumentId` to the existing Glue tables in `template.yaml`**

In `DailyBarsTable` → `Columns`, add as the **first** entry:

```yaml
            - { Name: instrumentId, Type: string }
            - { Name: ticker, Type: string }
```

In `DailyMetricsTable` → `Columns`, add as the **first** entry:

```yaml
            - { Name: instrumentId, Type: string }
            - { Name: ticker, Type: string }
```

- [ ] **Step 6: Add the two new Glue tables to `template.yaml`**

After `DailyMetricsTable`, add:

```yaml
  SecuritiesTable:
    Type: AWS::Glue::Table
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseName: !Ref GlueDatabase
      TableInput:
        Name: securities
        TableType: EXTERNAL_TABLE
        PartitionKeys: [{ Name: asOf, Type: string }]
        Parameters: { classification: parquet }
        StorageDescriptor:
          Location: !Sub "s3://${DataBucket}/reference/securities/"
          InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
          OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
          SerdeInfo: { SerializationLibrary: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe }
          Columns:
            - { Name: instrumentId, Type: string }
            - { Name: ticker, Type: string }
            - { Name: tickerRoot, Type: string }
            - { Name: tickerSuffix, Type: string }
            - { Name: name, Type: string }
            - { Name: market, Type: string }
            - { Name: locale, Type: string }
            - { Name: type, Type: string }
            - { Name: currencyName, Type: string }
            - { Name: cik, Type: string }
            - { Name: compositeFigi, Type: string }
            - { Name: shareClassFigi, Type: string }
            - { Name: primaryExchange, Type: string }
            - { Name: active, Type: boolean }
            - { Name: listDate, Type: string }
            - { Name: delistedUtc, Type: string }
            - { Name: lastUpdatedUtc, Type: string }
            - { Name: identitySource, Type: string }
            - { Name: identityConfidence, Type: string }
            - { Name: referenceStatus, Type: string }
            - { Name: source, Type: string }
            - { Name: sourceVersion, Type: string }
            - { Name: asOfDate, Type: string }
            - { Name: ingestedAt, Type: string }

  SymbolAliasesTable:
    Type: AWS::Glue::Table
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseName: !Ref GlueDatabase
      TableInput:
        Name: symbol_aliases
        TableType: EXTERNAL_TABLE
        PartitionKeys: [{ Name: asOf, Type: string }]
        Parameters: { classification: parquet }
        StorageDescriptor:
          Location: !Sub "s3://${DataBucket}/reference/symbol_aliases/"
          InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
          OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
          SerdeInfo: { SerializationLibrary: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe }
          Columns:
            - { Name: instrumentId, Type: string }
            - { Name: ticker, Type: string }
            - { Name: tickerRoot, Type: string }
            - { Name: tickerSuffix, Type: string }
            - { Name: primaryExchange, Type: string }
            - { Name: validFrom, Type: string }
            - { Name: validTo, Type: string }
            - { Name: source, Type: string }
            - { Name: sourceVersion, Type: string }
            - { Name: asOfDate, Type: string }
            - { Name: confidence, Type: string }
            - { Name: createdAt, Type: string }
```

- [ ] **Step 7: Validate the template + full suite**

Run: `npm run typecheck && npm test`
Expected: PASS. If `sam` is installed locally, also run `sam validate --lint` and expect `template.yaml is a valid SAM Template`. (If `sam` is not installed, skip — CI runs `sam validate`.)

- [ ] **Step 8: Commit**

```bash
git add src/glue.ts template.yaml tests/glue.test.ts
git commit -m "feat(glue): instrumentId column + securities/symbol_aliases tables + addAsOfPartition

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Pipeline integration

This is the integration task. It removes the Task-1 temporaries and wires the identity steps end to end.

**Files:**
- Modify: `src/storage.ts` (instrumentId in `RAW_SCHEMA`/`METRIC_SCHEMA`), `src/pipeline.ts` (`enrichRaw`, wiring, manifest), `src/metrics.ts` (`computeMetrics` signature), `src/metadata.ts` (`snapshotUniverse`), `src/report.ts` (stats)
- Test: `tests/storage.test.ts`, `tests/metrics.test.ts`, `tests/pipeline.test.ts`, `tests/report.test.ts`

**Interfaces:**
- Consumes: everything produced in Tasks 1–8.
- Produces (final signatures):
  - `enrichRaw(bar: ResolvedVendorBar, ctx: { runId: string; universeVersion: string }): RawBarRow`
  - `computeMetrics(bars: VendorBar[], prov: Provenance, quality: { status; issues }, instrumentId: string): MetricRow`
  - `snapshotUniverse(s3, bucket, date, universe: { universeVersion: string; tickers: string[]; securities: { instrumentId: string; ticker: string; source: string; active: boolean }[] }): Promise<void>`
  - `Deps.readHistory(instrumentId: string)`, `Deps.writeHistory(instrumentId: string, bars)`
  - `RunManifest` fully populated with the five identity stats.

- [ ] **Step 1: Put `instrumentId` into the Parquet RAW/METRIC schemas (test first)**

Add to `tests/storage.test.ts`:

```typescript
import { RAW_SCHEMA, METRIC_SCHEMA } from "../src/storage.js";

describe("parquet schemas carry instrumentId", () => {
  it("RAW_SCHEMA and METRIC_SCHEMA both define instrumentId", () => {
    expect(RAW_SCHEMA.schema.instrumentId).toBeDefined();
    expect(METRIC_SCHEMA.schema.instrumentId).toBeDefined();
  });
});
```

Run: `npx vitest run tests/storage.test.ts` → FAIL (`instrumentId` undefined).

In `src/storage.ts`, add `instrumentId: { type: "UTF8" },` as the first field of **both** `RAW_SCHEMA` and `METRIC_SCHEMA`.

Run: `npx vitest run tests/storage.test.ts` → PASS.

- [ ] **Step 2: Finalize `computeMetrics` (test first)**

Add to `tests/metrics.test.ts` (top of file, after imports):

```typescript
import { computeMetrics } from "../src/metrics.js";
import type { VendorBar, Provenance } from "../src/types.js";

describe("computeMetrics instrumentId", () => {
  it("stamps the passed instrumentId onto the row", () => {
    const bars: VendorBar[] = [{ ticker: "AAPL", date: "2026-06-30", open: 1, high: 1, low: 1, close: 1, adjustedClose: null, isAdjusted: false, volume: 1, source: "x", sourceVersion: "1.0", ingestedAt: "x" }];
    const prov: Provenance = { runId: "R", ingestedAt: "x", source: "x", sourceVersion: "1.0", schemaVersion: "metrics_v2", metricVersion: "1.0", universeVersion: "2026-06-30" };
    const row = computeMetrics(bars, prov, { status: "OK", issues: [] }, "BBG_AAPL");
    expect(row.instrumentId).toBe("BBG_AAPL");
    expect(row.ticker).toBe("AAPL");
  });
});
```

Run: `npx vitest run tests/metrics.test.ts` → FAIL (signature mismatch / wrong instrumentId).

In `src/metrics.ts`, change the signature and replace the temporary fill:

```typescript
export function computeMetrics(bars: VendorBar[], prov: Provenance, quality: { status: QualityStatus; issues: string[] }, instrumentId: string): MetricRow {
```

and in the returned object replace `instrumentId: last.ticker,` with `instrumentId,`.

Run: `npx vitest run tests/metrics.test.ts` → PASS.

- [ ] **Step 3: Finalize `enrichRaw`**

In `src/pipeline.ts`, change `enrichRaw` to take a `ResolvedVendorBar` and use its real `instrumentId`:

```typescript
import type { VendorBar, ResolvedVendorBar, RawBarRow, MetricRow, RunManifest, RunMode, Provenance, ErrorRecord, SecurityMasterRow } from "./types.js";

export function enrichRaw(bar: ResolvedVendorBar, ctx: { runId: string; universeVersion: string }): RawBarRow {
  return { ...bar, runId: ctx.runId, schemaVersion: RAW_SCHEMA_VERSION, metricVersion: METRIC_VERSION, universeVersion: ctx.universeVersion };
}
```

(`ResolvedVendorBar` already has `instrumentId`, so the spread carries it; remove the temporary `instrumentId: bar.ticker`.)

- [ ] **Step 4: Update `snapshotUniverse` in `src/metadata.ts`**

```typescript
export async function snapshotUniverse(
  s3: S3Client, bucket: string, date: string,
  universe: { universeVersion: string; tickers: string[]; securities: { instrumentId: string; ticker: string; source: string; active: boolean }[] },
): Promise<void> {
  await putJson(s3, bucket, universeKey(date), universe);
}
```

- [ ] **Step 5: Add the resolution stats to `buildManifest` and `BuildManifestArgs`**

In `src/pipeline.ts`, extend `BuildManifestArgs` and `buildManifest`:

```typescript
export interface BuildManifestArgs {
  mode: RunMode; runId: string; tradingDay: string; provider: string; universeVersion: string;
  requested: number; succeeded: number; warnings: number; rejected: number; missingBars: number; runtimeSec: number;
  securitiesMastered: number; securitiesResolved: number; unresolvedTickers: number; missingReferenceData: number; aliasRows: number;
}
```

In the `buildManifest` return object replace the Task-1 zero temporaries with the real args:

```typescript
    securitiesMastered: a.securitiesMastered, securitiesResolved: a.securitiesResolved,
    unresolvedTickers: a.unresolvedTickers, missingReferenceData: a.missingReferenceData, aliasRows: a.aliasRows,
```

(Leave the five fields as `0` in the `earlyExit` literal — SKIPPED/FAILURE early-exits have no identity stats.)

- [ ] **Step 6: Wire the identity steps into `runPipeline`**

Add imports near the top of `src/pipeline.ts`:

```typescript
import { writeSecurities } from "./storage.js";
import { buildSecurityMaster, buildTickerMap, detectIdentityChanges, writeSecuritiesState, readSecuritiesState } from "./securityMaster.js";
import { buildSymbolAliases, writeSymbolAliases, writeAliasesState, readAliasesState } from "./symbolAliases.js";
import { resolveBarsToInstruments } from "./resolver.js";
import { addAsOfPartition } from "./glue.js";
```

Inside `runPipeline`, **after** `effectiveDate` is resolved and **before** `snapshotUniverse(...)`, insert the identity block, then move the universe snapshot to after it. Replace the existing:

```typescript
  await snapshotUniverse(deps.s3, deps.bucket, effectiveDate, { universeVersion, tickers });
```

with:

```typescript
  // ── Identity layer (Part 1.5a) ───────────────────────────────────────────
  const ingestedAt = deps.now().toISOString();
  const secResult = await deps.provider.listSecurities(effectiveDate, tickers);
  const built = buildSecurityMaster(tickers, secResult, effectiveDate, deps.provider.name, deps.provider.version, ingestedAt);
  const securities = built.securities;
  await writeSecurities(deps.s3, deps.bucket, effectiveDate, securities);

  // record reference-data shortfalls (non-fatal)
  if (built.emptyMaster) recordError("*", "security_master_empty", "listSecurities returned no rows; all universe tickers minted as EH: fallback");
  for (const t of built.missingTickers) recordError(t, "missing_reference_data", `No reference row for ${t} on ${effectiveDate}; minted EH:${t}`);
  for (const t of built.duplicateTickers) recordError(t, "duplicate_instrument_ticker", `Multiple reference rows for ${t}; kept the first`);

  // identity-change guard vs the last snapshot (non-blocking)
  const priorSecurities = await readSecuritiesState(deps.s3, deps.bucket);
  for (const c of detectIdentityChanges(securities, priorSecurities))
    recordError(c.ticker, "identity_changed", `instrumentId changed ${c.from} -> ${c.to}`);

  // forward symbol aliases
  const priorAliases = await readAliasesState(deps.s3, deps.bucket);
  const aliasBuild = buildSymbolAliases(securities, priorAliases, effectiveDate, deps.previousTradingDay(effectiveDate), deps.provider.name, deps.provider.version, ingestedAt);
  for (const t of aliasBuild.conflicts) recordError(t, "alias_conflict", `Ticker ${t} maps to multiple instrumentIds on ${effectiveDate}`);
  await writeSymbolAliases(deps.s3, deps.bucket, effectiveDate, aliasBuild.aliases);

  // persist read-model state for tomorrow's carry-forward
  await writeSecuritiesState(deps.s3, deps.bucket, securities);
  await writeAliasesState(deps.s3, deps.bucket, aliasBuild.aliases);

  // ticker -> instrumentId for stamping (invariant guarantees one row per universe ticker)
  const idByTicker = new Map(securities.map((s) => [s.ticker, s.instrumentId]));
  let unresolvedTickers = 0;

  // universe snapshot now carries securities
  await snapshotUniverse(deps.s3, deps.bucket, effectiveDate, {
    universeVersion, tickers,
    securities: securities.map((s) => ({ instrumentId: s.instrumentId, ticker: s.ticker, source: s.source, active: s.active })),
  });
```

Note: `recordError` is already defined just below this point in the current code; **move the `recordError` definition (and the `errors`/`seen`/counters declarations it uses) above this identity block** so it is in scope. Concretely, relocate these existing lines to just before the identity block:

```typescript
  const rawToStore: RawBarRow[] = [];
  const metricRows: MetricRow[] = [];
  const errors: ErrorRecord[] = [];
  const seen = new Set<string>();
  let warnings = 0, rejected = 0, missingBars = 0;
  const recordError = (ticker: string, reason: string, message?: string) =>
    errors.push({ runId, tradingDay: effectiveDate, source: deps.provider.name, universeVersion, ticker, reason, message, createdAt: deps.now().toISOString() });
```

(The `prov` constant stays where it is.)

- [ ] **Step 7: Use `instrumentId` in the bar loop (daily resolver + cache key + stamping)**

In the daily-batch fetch block, after populating `latestByTicker`, run the resolver to surface defensive unresolved errors and count resolutions:

```typescript
  const latestByTicker = new Map<string, VendorBar>();
  if (mode === "daily") {
    const result = await deps.provider.getLatestBars(effectiveDate, tickers);
    for (const b of result.bars) latestByTicker.set(b.ticker, b);
    for (const f of result.failures) recordError(f.ticker, f.reason, f.message);
    const { errors: resolveErrors } = resolveBarsToInstruments([...latestByTicker.values()], securities, effectiveDate, runId, deps.provider.name, universeVersion);
    for (const e of resolveErrors) { errors.push(e); unresolvedTickers++; }
  }
```

In the per-ticker loop, derive `instrumentId` and use it for the cache and stamping. Replace the cache reads/writes and the `enrichRaw`/`computeMetrics` calls:

```typescript
  for (const ticker of tickers) {
    try {
      const instrumentId = idByTicker.get(ticker);
      if (!instrumentId) { missingBars++; continue; } // unresolved (defensive; already errored)

      let bars: VendorBar[];
      let cacheBars: VendorBar[] | null = null;
      if (mode === "backfill") {
        const r = await deps.provider.getHistory(ticker, LOOKBACK_DAYS, effectiveDate);
        for (const f of r.failures) recordError(f.ticker || ticker, f.reason, f.message);
        bars = r.bars.filter((b) => b.date <= effectiveDate).sort((a, b) => a.date.localeCompare(b.date));
        if (bars.length === 0) { missingBars++; continue; }
        await deps.writeHistory(instrumentId, bars);
      } else {
        const latest = latestByTicker.get(ticker);
        if (!latest) { missingBars++; continue; }
        const stored = await deps.readHistory(instrumentId);
        let fullBars = mergeHistory(stored, latest);
        if (!hasEnoughHistory(fullBars, MIN_SESSIONS)) {
          const r = await deps.provider.getHistory(ticker, LOOKBACK_DAYS, effectiveDate);
          for (const f of r.failures) recordError(f.ticker || ticker, f.reason, f.message);
          fullBars = mergeHistory(r.bars, latest);
        }
        cacheBars = fullBars;
        bars = fullBars.filter((b) => b.date <= effectiveDate);
      }

      const currentBar = bars.find((b) => b.date === effectiveDate);
      if (!currentBar) { missingBars++; recordError(ticker, "missing_bar_for_date"); continue; }

      const grade = gradeBar(currentBar, seen);
      rawToStore.push(enrichRaw({ ...currentBar, instrumentId }, { runId, universeVersion }));
      if (grade.status === "REJECTED") { rejected++; recordError(ticker, "rejected", grade.issues.join(",")); continue; }
      if (grade.status === "WARN") { warnings++; recordError(ticker, "warn", grade.issues.join(",")); }

      metricRows.push(computeMetrics(bars, prov, grade, instrumentId));
      if (mode === "daily" && cacheBars) await deps.writeHistory(instrumentId, cacheBars);
    } catch (err) {
      recordError(ticker, "pipeline_error", (err as Error).message);
      continue;
    }
  }
```

After the writeRaw/writeMetrics/addPartition block, register the reference partitions:

```typescript
  await addAsOfPartition(deps.glue, deps.database, "securities", deps.bucket, "reference/securities", effectiveDate);
  await addAsOfPartition(deps.glue, deps.database, "symbol_aliases", deps.bucket, "reference/symbol_aliases", effectiveDate);
```

Finally, pass the stats into `buildManifest`:

```typescript
  const manifest = buildManifest({
    mode, runId, tradingDay: effectiveDate, provider: deps.provider.name, universeVersion,
    requested: tickers.length, succeeded: metricRows.length, warnings, rejected, missingBars, runtimeSec,
    securitiesMastered: securities.length, securitiesResolved: metricRows.length,
    unresolvedTickers, missingReferenceData: built.missingTickers.length, aliasRows: aliasBuild.aliases.length,
  });
```

- [ ] **Step 8: Add resolution stats to the Telegram report (test first)**

Add to `tests/report.test.ts` — extend the fixture `m` with the five new fields, then assert:

```typescript
const m: RunManifest = { /* existing fields */, securitiesMastered: 9, securitiesResolved: 9, unresolvedTickers: 0, missingReferenceData: 1, aliasRows: 9, /* ...rest... */ };

it("includes security resolution stats", () => {
  const t = renderReport(m);
  expect(t).toContain("Securities mastered: 9");
  expect(t).toContain("Securities resolved: 9");
  expect(t).toContain("Missing reference (EH fallback): 1");
  expect(t).toContain("Alias rows: 9");
});
```

Run: `npx vitest run tests/report.test.ts` → FAIL (TS error: missing fields / strings absent).

In `src/report.ts`, add the lines before the `Status:` line:

```typescript
    `Securities mastered: ${m.securitiesMastered}`,
    `Securities resolved: ${m.securitiesResolved}`,
    `Missing reference (EH fallback): ${m.missingReferenceData}`,
    `Unresolved tickers: ${m.unresolvedTickers}`,
    `Alias rows: ${m.aliasRows}`,
```

Run: `npx vitest run tests/report.test.ts` → PASS.

- [ ] **Step 9: Fix the pipeline tests for instrumentId-keyed history and the new manifest fields**

In `tests/pipeline.test.ts`:

1. `buildManifest` tests — add the five fields to the `common` object: `securitiesMastered: 0, securitiesResolved: 0, unresolvedTickers: 0, missingReferenceData: 0, aliasRows: 0,`.
2. The default `FakeProvider(hist)` (no securities) now mints `EH:<ticker>` for every universe ticker, so history is keyed by `EH:<ticker>`. In the **"old-date replay does NOT truncate cache"** test, change `readHistory: async (t) => (t === "AAPL" ? aaplStoredCache : [])` to `t === "EH:AAPL"`, and `cacheWrites.get("AAPL")` to `cacheWrites.get("EH:AAPL")`.
3. Add a new test proving the §4 invariant end-to-end:

```typescript
describe("runPipeline (daily, identity layer)", () => {
  it("masters securities, resolves all bars, and mints EH: fallback for missing reference data", async () => {
    const day = "2025-01-06";
    const tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "TSLA", "AVGO", "SPY", "QQQ"];
    const bar = (t: string): VendorBar => ({ ticker: t, date: day, open: 10, high: 11, low: 9, close: 10, adjustedClose: null, isAdjusted: false, volume: 1000, source: "fake", sourceVersion: "1.0", ingestedAt: "x" });
    const hist = new Map(tickers.map((t) => [t, [bar(t)]]));
    // provider returns reference data for everything EXCEPT AAPL -> AAPL must be minted EH:AAPL
    const securities = tickers.filter((t) => t !== "AAPL").map((t) => ({
      instrumentId: `BBG_${t}`, ticker: t, tickerRoot: t, active: true,
      identitySource: "SHARE_CLASS_FIGI" as const, identityConfidence: "HIGH" as const, referenceStatus: "FOUND" as const,
      source: "fake", sourceVersion: "1.0", asOfDate: day, ingestedAt: "x",
    }));
    const putKeys: string[] = [];
    const s3 = { send: async (c: any) => { if (c.input?.Key) putKeys.push(c.input.Key); return {}; } } as never;
    const glue = { send: async (c: any) => (c.constructor.name === "GetTableCommand" ? { Table: { StorageDescriptor: { Columns: [], SerdeInfo: {} } } } : {}) } as never;
    const m = await runPipeline("daily", {
      provider: new FakeProvider(hist, securities), s3, glue, bucket: "b", database: "edgehub", tradingDay: day,
      now: () => new Date(`${day}T22:30:00Z`),
      readHistory: async () => [], writeHistory: async () => {},
      isTradingDay: () => true, previousTradingDay: () => "2025-01-05", calendarCovers: () => true,
    });
    expect(m.securitiesMastered).toBe(9);
    expect(m.missingReferenceData).toBe(1);   // AAPL
    expect(m.unresolvedTickers).toBe(0);       // EH:AAPL still resolves
    expect(m.securitiesResolved).toBe(9);
    expect(putKeys.some((k) => k.startsWith("reference/securities/asOf="))).toBe(true);
    expect(putKeys.some((k) => k.startsWith("reference/symbol_aliases/asOf="))).toBe(true);
  });
});
```

Note: the glue mock must answer `GetTableCommand` (used by `addAsOfPartition`) — the snippet above does. Update the other daily pipeline tests' glue mocks similarly **only if** they start failing on `GetTableCommand` (the backfill SUCCESS test already uses a permissive `glue.send` returning `{}`, which makes `addAsOfPartition`'s `GetTableCommand` return `{}` → `Table` undefined → `StorageDescriptor` `{}`; that is fine and does not throw).

- [ ] **Step 10: Run the full suite**

Run: `npm run typecheck && npm test`
Expected: PASS — all suites green, including the new identity pipeline test.

- [ ] **Step 11: Commit**

```bash
git add src/storage.ts src/metrics.ts src/pipeline.ts src/metadata.ts src/report.ts tests/storage.test.ts tests/metrics.test.ts tests/report.test.ts tests/pipeline.test.ts
git commit -m "feat(pipeline): wire security master, aliases, resolver; stamp instrumentId; report stats

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Verification, docs, and acceptance

**Files:**
- Modify: `docs/DATA_DICTIONARY.md`
- No new tests (this task verifies and documents).

- [ ] **Step 1: Full green gate**

Run: `npm run typecheck && npm test`
Expected: PASS. Confirm the new suites ran: `identity`, `securityMaster`, `symbolAliases`, `resolver`.

- [ ] **Step 2: Grep for leftover temporaries / v1 stragglers**

Run: `grep -rn "instrumentId: bar.ticker\|instrumentId: last.ticker" src/`
Expected: **no matches** (Task-1 temporaries removed in Task 9).
Run: `grep -rn "ticker=\${ticker}\|ticker=" src/historyCache.ts`
Expected: **no matches** (history cache re-keyed).

- [ ] **Step 3: Update `docs/DATA_DICTIONARY.md`**

Add `instrumentId` as the first identity column in both the `daily_bars` and `daily_metrics` sections, and add two short sections for the new dimension tables. Insert after the `daily_bars (raw)` table's identity note:

```markdown
> **Identity (Part 1.5a):** every raw bar and metric row now carries `instrumentId` — the stable
> security id (FIGI-based, or an `EH:` fallback). `ticker` is the as-traded symbol for `date`.
> Query continuous history by `instrumentId`; filter `schemaVersion = 'metrics_v2'` (or
> `instrumentId IS NOT NULL`) to exclude pre-1.5a v1 rows.

## securities (reference dimension, asOf-partitioned)

One row per universe security per `asOf` day. `referenceStatus = FOUND` for real Polygon reference
rows; `MISSING_FALLBACK` for rows EdgeHub minted (`EH:<ticker>`) because reference data was missing.
Columns: instrumentId, ticker, tickerRoot/Suffix, name, market, locale, type, currencyName, cik,
compositeFigi, shareClassFigi, primaryExchange, active, listDate, delistedUtc, lastUpdatedUtc,
identitySource, identityConfidence, referenceStatus, source, sourceVersion, asOfDate, ingestedAt.

## symbol_aliases (reference dimension, asOf-partitioned)

Forward-only ticker↔instrument validity windows. One open row (`validTo = null`) per instrument;
a rename closes the prior ticker (`validTo = previous trading day`) and opens the new one. Columns:
instrumentId, ticker, tickerRoot/Suffix, primaryExchange, validFrom, validTo, source, sourceVersion,
asOfDate, confidence, createdAt.
```

- [ ] **Step 4: Walk the spec §17 acceptance checklist**

Confirm each item against the code (read, don't run AWS): security master created daily; aliases created daily; `instrumentId` on raw + metrics; history cache keyed by `instrumentId`; universe snapshot stores securities + `tickers` fallback; missing-reference ticker kept via `EH:` + logged; same-day rerun still new `runId`; `metadata/current` unchanged; Telegram has stats. Note any gap as a follow-up rather than silently passing.

- [ ] **Step 5: Commit the docs**

```bash
git add docs/DATA_DICTIONARY.md
git commit -m "docs: document instrumentId, securities, and symbol_aliases (Part 1.5a)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: (Deploy-time, manual — not part of the code gate) AWS acceptance**

After this branch merges and deploys, run one daily collection and verify in Athena:

```sql
SELECT instrumentId, ticker, date, close, ma200 FROM edgehub.daily_metrics
WHERE schemaVersion = 'metrics_v2' LIMIT 20;

SELECT m.date, m.ticker, s.name, m.close FROM edgehub.daily_metrics m
JOIN edgehub.securities s ON m.instrumentId = s.instrumentId LIMIT 20;

SELECT ticker, instrumentId, referenceStatus FROM edgehub.securities
WHERE referenceStatus = 'MISSING_FALLBACK';
```

New `asOf` / `year-month-day` partitions may need `MSCK REPAIR TABLE` or are registered by the pipeline's `addAsOfPartition` / `addPartition` calls. Expected: rows carry real `instrumentId`s; the join returns names; the audit query lists any minted fallbacks.

---

## Self-Review

**Spec coverage** (spec §→task):
- §2 D1 scoped universe → Tasks 3, 9 (per-ticker `listSecurities`, universe stays JSON). ✓
- §2 D2 never-drop invariant → Task 4 `buildSecurityMaster` + Task 9 wiring + pipeline test. ✓
- §2 D3 pure id + `identity_changed` → Tasks 2, 4 (`detectIdentityChanges`), 9 (recordError). ✓
- §2 D4 in-place v2 → Tasks 1 (constants/schemas), 8 (Glue column), 9 (Parquet columns). ✓
- §4 EH: fallback + `referenceStatus` + `security_master_empty` → Task 4 + Task 9. ✓
- §5 types → Task 1. §6 identity → Task 2. §7 provider → Task 3. §8 storage/master/aliases → Tasks 4, 5. §9 resolver → Task 6. §10 pipeline → Task 9. §11 universe snapshot → Task 9 (snapshot after master). §12 history cache → Task 7. §13 Glue/SAM → Task 8. §14 queries → Task 10. §15 report → Task 9. §16 tests → every task. §17 acceptance → Task 10. ✓
- §11 ordering (snapshot AFTER master, once) → Task 9 Step 6 places `snapshotUniverse` inside the identity block after `writeSecurities`. ✓

**Placeholder scan:** No "TBD"/"handle edge cases"/"similar to". Every code step shows full code. ✓

**Type consistency:** `makeInstrumentId` returns `{ instrumentId, identitySource, identityConfidence }` (Tasks 2, 3, 4 agree). `buildSecurityMaster` returns `{ securities, missingTickers, duplicateTickers, emptyMaster }` (Tasks 4, 9 agree). `buildSymbolAliases` returns `{ aliases, conflicts }` (Tasks 5, 9 agree). `resolveBarsToInstruments` returns `{ resolved, errors }` (Tasks 6, 9 agree). `computeMetrics(bars, prov, quality, instrumentId)` (Tasks 1 temp, 9 final agree). `enrichRaw(ResolvedVendorBar, ctx)` (Tasks 1 temp, 9 final agree). `historyCacheKey(source, instrumentId)` (Task 7) used as `readHistory(instrumentId)`/`writeHistory(instrumentId, bars)` in Task 9. ✓

**Share-class safety (Fix applied):** the `EH:` fallbacks use the full `ticker`, not `tickerRoot`, so `BRK.A`/`BRK.B` (and `GOOG`/`GOOGL`) get distinct ids even when FIGI is absent. `tickerRoot`/`tickerSuffix` remain only as descriptive metadata columns. Task 2's tests assert this distinctness via both the `EH:<cik>:<ticker>` and `EH:<ticker>:<exchange>` fallbacks.
