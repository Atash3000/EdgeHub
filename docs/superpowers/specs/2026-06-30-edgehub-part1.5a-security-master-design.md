# EdgeHub Part 1.5a — Security Master + Instrument Identity

**Date:** 2026-06-30
**Status:** Approved — build
**Scope:** Additive identity layer on top of Part 1 (Market Data Lake). Adds `instrumentId`, a
scoped security master, a symbol-alias table, and ticker→instrument resolution. **Does not redesign
Part 1.** The Part 1 data design (`project-plan.md`) and deployment design
(`docs/superpowers/specs/2026-06-29-edgehub-deployment-design.md`) remain in force; this document
specifies only the delta.

> Companion to the original Part 1.5a brief. Where the brief and this document differ, **this
> document wins** — it incorporates the design decisions taken during brainstorming (recorded in
> §2).

---

## 1. Mission

Part 1 is **ticker-keyed**. A ticker is a symbol valid for a period of time, not an identity:
`FB → META`, `GOOG`/`GOOGL` are one company but different securities, `BRK.A`/`BRK.B` differ, and
tickers can be recycled after delisting. Part 1.5a adds the identity layer so the lake is safe
against these before it scales.

Core rule:

> **Ticker is not identity. `instrumentId` is identity.** `ticker` remains the as-traded symbol for
> a given date.

The win we are buying: a continuous per-security history that survives renames, queryable by
`instrumentId`, joinable to a rich securities dimension — without polluting the skinny fact rows.

---

## 2. Locked design decisions (from brainstorming)

These four decisions shape everything downstream. They are settled; do not relitigate them during
implementation.

| # | Decision | Choice | Rationale |
|---|----------|--------|-----------|
| **D1** | Security-master / universe scope | **Scoped to universe.** The committed JSON universe stays the ingest list. The security master is built only for the universe tickers. Grouped-daily Polygon bars stay filtered to the JSON universe exactly as Part 1 does. **No full-catalog paging in 1.5a.** | Additive identity upgrade, not a universe-expansion project. Dynamic/auto-IPO universe is deferred to a later phase. Avoids ~6 min of daily reference pagination for data we don't ingest. |
| **D2** | Unresolved-bar behavior | **Never drop a universe stock.** Guarantee one security-master row per universe ticker; mint an `EH:<ticker>` fallback row when reference data is missing (see §4). The resolver only excludes genuinely unexpected non-universe bars. | Preserves Part 1's "never silently lose a held stock" ethos. Makes the resolver trivial. |
| **D3** | `instrumentId` stability | **Pure deterministic function**, no cross-day pinning. Add `identitySource` + `identityConfidence`. Add a **non-blocking `identity_changed` guard** that logs when a universe ticker maps to a different `instrumentId` than the prior snapshot. **No restamping/migration.** | For the liquid S&P/Nasdaq-100 universe Polygon's FIGIs are populated and stable, so flips are negligible. True historical no-flip pinning is Part 1.5b. |
| **D4** | Schema migration | **In-place + version bump.** Add `instrumentId` to the existing `daily_bars`/`daily_metrics` Glue tables and Parquet schemas; bump `dailyBars_v1→v2`, `metrics_v1→v2`. Old test partitions read `instrumentId = NULL`. **No parallel `*_v2` tables.** | Lake is still 9 seed tickers; migration is cheap. One table is easier to query and partition. |

**Future-ready provider signature (D1 refinement):** `listSecurities(asOfDate, tickers?)` — 1.5a always
passes `tickers`; omitting it (full-catalog) is wired for a later phase but unused now.

---

## 3. Design principle — facts skinny, dimensions rich

Unchanged from Part 1, extended:

- **Fact rows** (`raw/`, `metrics/`) carry `instrumentId`, `ticker`, `date`, prices/volume/metrics,
  provenance, quality. They gain **only** `instrumentId`. They never carry name/sector/FIGI/exchange.
- **Dimension rows** (`reference/securities/`, `reference/symbol_aliases/`) carry the mutable
  attributes (name, CIK, FIGI, exchange, active status, type, locale, validity windows). Athena joins
  facts to dimensions by `instrumentId`.

---

## 4. The Section-1 invariant (D2 in detail)

> **The security master always contains exactly one row per universe ticker for the trading day.**

Two cases, decided when the master is built (§7):

1. **Reference data found** — real `SecurityMasterRow` from Polygon, `instrumentId` from the
   FIGI-based fallback chain, `referenceStatus = "FOUND"`.
2. **Reference data missing** (Polygon omits/errors a universe ticker that day) — mint a minimal row:

   ```text
   instrumentId       = EH:<ticker>
   identitySource     = EH_TICKER
   identityConfidence = LOW
   referenceStatus    = MISSING_FALLBACK
   active             = true            // we hold it in the universe; treat as active absent info
   ticker             = <ticker>
   ```

   and record a `missing_reference_data` **warning** (errors/, non-fatal). The stock is **not** dropped.

Consequence: every fetched (universe) bar resolves. `unresolved_instrument` survives only as a
**defensive guard** for a bar whose ticker is not in the universe — impossible today since we fetch
only universe tickers, but kept so the resolver fails safe if that ever changes.

**`security_master_empty` fallback:** if `listSecurities` returns nothing for the whole batch
(reference outage), the pipeline still mints `EH:<ticker>` rows for the full universe (so the run
proceeds identity-safely) and logs one `security_master_empty` warning. The run is never hard-failed
on a reference outage alone.

---

## 5. New / changed types (`src/types.ts`)

### 5.1 Version constants (D4)

```ts
export const SCHEMA_VERSION = "metrics_v2";       // was "metrics_v1"
export const RAW_SCHEMA_VERSION = "dailyBars_v2"; // was "dailyBars_v1"
export const METRIC_VERSION = "1.0";              // unchanged (metric formulas unchanged)
export const SOURCE_VERSION = "1.0";              // unchanged
```

### 5.2 `SecurityMasterRow`

```ts
export type IdentitySource =
  | "SHARE_CLASS_FIGI" | "COMPOSITE_FIGI"
  | "EH_CIK_TICKERROOT" | "EH_TICKERROOT_EXCHANGE" | "EH_TICKER";
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

  // identity provenance
  identitySource: IdentitySource;
  identityConfidence: IdentityConfidence;
  referenceStatus: ReferenceStatus;   // FOUND = real Polygon row; MISSING_FALLBACK = minted EH:<ticker>

  source: string;
  sourceVersion: string;
  asOfDate: string;
  ingestedAt: string;
}
```

### 5.3 `SymbolAliasRow`

```ts
export interface SymbolAliasRow {
  instrumentId: string;
  ticker: string;
  tickerRoot?: string;
  tickerSuffix?: string;
  primaryExchange?: string;
  validFrom: string;          // YYYY-MM-DD
  validTo: string | null;     // null = currently open
  source: string;
  sourceVersion: string;
  asOfDate: string;
  confidence: "HIGH" | "MEDIUM" | "LOW";
  createdAt: string;
}
```

### 5.4 Bar/metric/result types

```ts
export interface ResolvedVendorBar extends VendorBar { instrumentId: string; }

export interface RawBarRow extends VendorBar {
  instrumentId: string;       // NEW (required)
  runId: string; schemaVersion: string; metricVersion: string; universeVersion: string;
}

export interface MetricRow extends Provenance {
  instrumentId: string;       // NEW (required)
  ticker: string; date: string;
  // ...all existing metric fields unchanged...
}

export interface SecurityMasterResult {
  securities: SecurityMasterRow[];
  failures: ProviderFailure[];
}
```

### 5.5 `ErrorRecord.reason` — new values

`unresolved_instrument`, `duplicate_instrument_ticker`, `missing_reference_data`,
`security_master_empty`, `alias_conflict`, `identity_changed` (in addition to the Part 1 set).

### 5.6 `RunManifest` — new fields

```ts
securitiesMastered: number;   // rows written to the security master
securitiesResolved: number;   // bars resolved to an instrumentId
unresolvedTickers: number;    // bars excluded as unresolved_instrument (defensive; ~0)
missingReferenceData: number; // universe tickers minted as EH: fallback
aliasRows: number;            // rows in the alias snapshot
```

---

## 6. Identity (`src/identity.ts`)

```ts
export function makeInstrumentId(input: {
  shareClassFigi?: string; compositeFigi?: string;
  cik?: string; tickerRoot?: string; ticker?: string; primaryExchange?: string;
}): { instrumentId: string; identitySource: IdentitySource; identityConfidence: IdentityConfidence };

export function splitTicker(ticker: string): { tickerRoot: string; tickerSuffix?: string };
```

**Fallback chain (in order):**

| Condition | `instrumentId` | `identitySource` | `identityConfidence` |
|-----------|----------------|------------------|----------------------|
| `shareClassFigi` present | `shareClassFigi` | `SHARE_CLASS_FIGI` | `HIGH` |
| else `compositeFigi` present | `compositeFigi` | `COMPOSITE_FIGI` | `HIGH` |
| else `cik` && `tickerRoot` | `EH:<cik>:<tickerRoot>` | `EH_CIK_TICKERROOT` | `MEDIUM` |
| else `tickerRoot` && `primaryExchange` | `EH:<tickerRoot>:<primaryExchange>` | `EH_TICKERROOT_EXCHANGE` | `LOW` |
| else | `EH:<ticker>` | `EH_TICKER` | `LOW` |

`splitTicker` handles share-class suffixes: `BRK.A → {root: "BRK", suffix: "A"}` (split on `.`). Used
so `BRK.A`/`BRK.B` produce distinct `EH:` ids when FIGI is absent, and to populate `tickerRoot`/
`tickerSuffix` on master/alias rows.

**Tests (`identity.test.ts`):** the spec's five cases — share_class wins; composite second;
`cik+tickerRoot` fallback; `tickerRoot+exchange` fallback; bare-ticker final fallback; plus
`GOOG`/`GOOGL` and `BRK.A`/`BRK.B` distinctness when share-class FIGIs differ; plus confidence-mapping
assertions.

---

## 7. Provider changes

### 7.1 Interface (`src/providers/provider.ts`)

```ts
export interface MarketDataProvider {
  readonly name: string;
  readonly version: string;
  getLatestBars(date: string, tickers: string[]): Promise<ProviderResult>;
  getHistory(ticker: string, lookbackDays: number, endDate?: string): Promise<ProviderResult>;
  listSecurities(asOfDate: string, tickers?: string[]): Promise<SecurityMasterResult>; // NEW
}
```

### 7.2 Polygon (`src/providers/polygon.ts`)

`listSecurities(asOfDate, tickers)` — for each universe ticker, call:

```
GET /v3/reference/tickers?ticker=<T>&date=<asOfDate>&limit=1
   (Authorization: Bearer <key> header, same as existing calls; rate-limited 5/min)
```

Map the result fields → `SecurityMasterRow`:

| Polygon field | `SecurityMasterRow` |
|---------------|---------------------|
| `ticker` | `ticker` (+ `splitTicker` → `tickerRoot`/`tickerSuffix`) |
| `name` | `name` |
| `market` / `locale` / `type` | `market` / `locale` / `type` |
| `currency_name` | `currencyName` |
| `cik` | `cik` |
| `composite_figi` / `share_class_figi` | `compositeFigi` / `shareClassFigi` |
| `primary_exchange` | `primaryExchange` |
| `active` | `active` |
| `list_date` / `delisted_utc` / `last_updated_utc` | `listDate` / `delistedUtc` / `lastUpdatedUtc` |

Then `makeInstrumentId(...)` sets `instrumentId`/`identitySource`/`identityConfidence`;
`referenceStatus = "FOUND"`. A ticker with no result or an error → a `missing_reference_data`
`ProviderFailure` (the **pipeline**, not the provider, mints the EH: fallback row — see §8 — so
minting lives in one place). Rate-limit cost: ~9 calls ≈ ~2 min added to the daily run today.

~6 minutes of cost at full-catalog scale is explicitly **not** paid in 1.5a (D1).

> **⚠️ Reference-call scaling warning.** This per-ticker reference method is a 1.5a-only strategy and
> **must not be scaled to large universes.** At Polygon's 5 req/min limit: ~9 tickers ≈ 2 min (fine),
> ~50 tickers ≈ 10 min (slow), ~500 tickers ≈ 100 min (**exceeds the 900s Lambda timeout — broken**).
> The Lambda timeout is already 900s (`template.yaml`), comfortably enough for today's 9 tickers. When
> the universe grows toward full S&P 500 / Nasdaq 100, **switch to the full-catalog `listSecurities`
> path** (one paginated `?date=` sweep, ~30 pages, fixed cost regardless of N) — that is the
> deferred Part 1.5b/dynamic-universe work, not a tweak to this per-ticker method.

### 7.3 Fake (`src/providers/fake.ts`)

`FakeProvider` gains `listSecurities` returning canned `SecurityMasterRow[]` (constructor takes an
optional `Map<ticker, SecurityMasterRow>` or array), plus the ability to simulate a missing ticker
(→ `missing_reference_data` failure) for the fallback-path test.

---

## 8. Security master & aliases storage

### 8.1 `src/securityMaster.ts`

- `securitiesKey(asOfDate): string` → `reference/securities/asOf=<asOfDate>/part.parquet`.
- `buildSecurityMaster(universeTickers, result, asOfDate, source, sourceVersion, ingestedAt): SecurityMasterRow[]`
  — **owns the §4 invariant**: start from `result.securities`; for every universe ticker without a
  row, mint the `EH:<ticker>` / `MISSING_FALLBACK` row. Guarantees exactly one row per universe
  ticker. De-dupes on `ticker` (a `duplicate_instrument_ticker` warning if Polygon returns two rows
  for one ticker — keep the first, log the rest).
- `writeSecurities(s3, bucket, asOfDate, rows): Promise<string>` → Parquet via a new `SECURITIES_SCHEMA`.
- `buildTickerMap(rows): Map<string, SecurityMasterRow>` — point-in-time ticker→row for the resolver.
- `readLatestSecurities(s3, bucket, beforeAsOf?): Promise<SecurityMasterRow[]>` — list
  `reference/securities/`, read the newest snapshot strictly before today (for the alias logic and the
  `identity_changed` guard). Empty array on miss.

**`identity_changed` guard:** after building today's master, for each ticker present in both today and
the latest prior snapshot, if `instrumentId` differs → log `identity_changed` (errors/, non-blocking).
No restamping.

### 8.2 `src/symbolAliases.ts`

- `symbolAliasesKey(asOfDate): string` → `reference/symbol_aliases/asOf=<asOfDate>/part.parquet`.
- `buildSymbolAliases(today, priorAliases, asOfDate, previousTradingDay): SymbolAliasRow[]` —
  forward-only logic:
  - Carry forward every still-open prior alias whose `(instrumentId, ticker)` still holds today.
  - **New security or new open ticker:** open a row `validFrom = asOfDate`, `validTo = null`,
    `confidence = MEDIUM`.
  - **Forward rename** (an `instrumentId` whose open ticker today differs from its open prior ticker):
    close the prior (`validTo = previousTradingDay`) and open the new (`validFrom = asOfDate`).
  - De-dupe identical open rows; never write a duplicate open `(instrumentId, ticker)`.
  - On an irreconcilable conflict (same ticker → two instrumentIds in one day), keep the
    higher-confidence identity and log `alias_conflict`.
- `writeSymbolAliases(s3, bucket, asOfDate, rows): Promise<string>` → Parquet via `SYMBOL_ALIASES_SCHEMA`.

Each `asOf` snapshot holds the **full** current alias table (carried-forward + today's changes), so the
latest `asOf` partition is the complete alias truth. Full historical reconstruction is **out of scope**
(Part 1.5b).

### 8.3 Parquet schemas (`src/storage.ts`)

Add `SECURITIES_SCHEMA` and `SYMBOL_ALIASES_SCHEMA` (all string/boolean columns; optional where the
type is `?`). Extend `RAW_SCHEMA` and `METRIC_SCHEMA` with `instrumentId: { type: "UTF8" }`.

---

## 9. Resolver (`src/resolver.ts`)

```ts
export function resolveBarsToInstruments(
  bars: VendorBar[],
  securities: SecurityMasterRow[],
  tradingDay: string, runId: string, source: string, universeVersion: string,
): { resolved: ResolvedVendorBar[]; errors: ErrorRecord[] };
```

- Build `buildTickerMap(securities)`; match each bar by **exact ticker**.
- Match → `{ ...bar, instrumentId }`.
- No match (defensive; non-universe) → `unresolved_instrument` error, bar excluded.
- **Never mints** an id and never writes a security row — minting is owned by `buildSecurityMaster`.

**Tests (`resolver.test.ts`):** `AAPL` resolves to its instrumentId; resolved bar keeps original
`ticker`; an unknown ticker (absent from a deliberately-incomplete master) → `unresolved_instrument`
and is excluded.

---

## 10. Pipeline wiring (`src/pipeline.ts`) — additive, order preserved

The Part 1 sequence is unchanged except for the inserted identity steps and three field changes. New
steps run **after** the calendar gate + universe load and **before** the per-ticker bar loop.

```
calendar gate (unchanged)
universe load (unchanged)
── NEW ──────────────────────────────────────────────
listSecurities(effectiveDate, tickers)
buildSecurityMaster(...)  → mint EH: fallbacks (§4) → writeSecurities
readLatestSecurities(...) → identity_changed guard
buildSymbolAliases(...)   → writeSymbolAliases
buildTickerMap(...)       → resolveBarsToInstruments(fetched bars)
snapshotUniverse(...)     ← AFTER the master, so it includes instrumentId (§11); written once
── END NEW ──────────────────────────────────────────
per-ticker loop (unchanged EXCEPT):
  • history cache keyed by instrumentId   (§12)
  • instrumentId stamped on RawBarRow      (enrichRaw gains instrumentId)
  • instrumentId stamped on MetricRow      (computeMetrics receives instrumentId)
writeRaw / writeMetrics / addPartition (unchanged; +instrumentId column)
add securities + symbol_aliases asOf partitions (new)
writeErrors (unchanged; +new reasons)
buildManifest (+ resolution stats) → writeManifest → markCurrent (unchanged)
report (+ resolution stats)
```

- `enrichRaw(bar, ctx)` takes a `ResolvedVendorBar` (or `instrumentId` in `ctx`) and stamps
  `instrumentId`.
- `computeMetrics(bars, prov, grade, instrumentId)` stamps `instrumentId` onto the `MetricRow`
  (smallest signature change; the bars array already carries `ticker`).
- Both **daily** and **backfill** modes resolve via the master built for `effectiveDate`. Backfill
  builds the master for `effectiveDate` the same way.
- `markCurrent` / `metadata/current` behavior is **unchanged** (advances only on SUCCESS/PARTIAL).
- Same-day rerun still mints a **new `runId`** (unchanged).

---

## 11. Universe snapshot upgrade (`src/metadata.ts`, `src/universe.ts`)

`snapshotUniverse` now writes securities, keeping a flat `tickers` list as an emergency fallback:

```json
{
  "universeVersion": "2026-06-30",
  "securities": [
    { "instrumentId": "BBG000B9XRY4", "ticker": "AAPL", "source": "polygon", "active": true }
  ],
  "tickers": ["AAPL", "MSFT", "..."]
}
```

The committed `config/universe/*.json` files are **unchanged** and remain the source of truth for
which tickers we ingest (D1). **`snapshotUniverse` runs AFTER `buildSecurityMaster`** (see §10), so
the snapshot can include `instrumentId` and is written exactly **once** — do not snapshot first and
rewrite.

---

## 12. History cache migration (`src/historyCache.ts`)

```ts
// before: history/<source>/ticker=<ticker>/current.json
// after:  history/<source>/instrumentId=<instrumentId>/current.json
export function historyCacheKey(source: string, instrumentId: string): string;
```

Cached bars are unchanged in content (each still has `ticker`, `date`, OHLCV…), so a cache survives a
rename. Migration is **free**: the new key is a cache miss → the existing auto-backfill path re-seeds
it; the immutable raw partitions remain the rebuildable source of truth. Pipeline `readHistory`/
`writeHistory` deps now take `instrumentId` instead of `ticker`.

**Tests (`historyCache.test.ts`):** key uses `instrumentId`, not `ticker`.

---

## 13. Glue / SAM (`template.yaml`, `src/glue.ts`)

- **Existing tables:** add `{ Name: instrumentId, Type: string }` to `daily_bars` and `daily_metrics`
  column lists. Old Parquet partitions lack the column → Athena returns `NULL` for them.
- **New tables** `securities` and `symbol_aliases`:
  - `EXTERNAL_TABLE`, Parquet SerDe, `Location` `s3://<bucket>/reference/securities/` and
    `.../reference/symbol_aliases/`.
  - Partitioned by a single `asOf` (string) key.
  - Columns mirror `SecurityMasterRow` / `SymbolAliasRow` (string/boolean).
- `src/glue.ts` gains `addAsOfPartition(glue, database, table, bucket, prefix, asOf)` — the current
  `addPartition` assumes `year/month/day`; the new variant registers a single `asOf=<date>` partition
  (same clone-StorageDescriptor + idempotent-AlreadyExists pattern).

---

## 14. Athena / query contract

```sql
-- continuous history for a security across renames (FB → META)
SELECT date, ticker, close, ma200, atr14
FROM edgehub.daily_metrics
WHERE instrumentId = '<share_class_figi>' AND schemaVersion = 'metrics_v2'
ORDER BY date;

-- join facts to the securities dimension (latest asOf = current truth)
SELECT m.date, m.ticker, s.name, s.primaryExchange, m.close, m.ma200
FROM edgehub.daily_metrics m
JOIN edgehub.securities s ON m.instrumentId = s.instrumentId
WHERE m.date = '2026-06-30' AND m.runId = '<runId from metadata/current>';

-- audit: which securities were minted because reference data was missing
SELECT ticker, instrumentId, identitySource, identityConfidence, referenceStatus
FROM edgehub.securities
WHERE referenceStatus = 'MISSING_FALLBACK';
```

Production queries that require identity should filter `schemaVersion = 'metrics_v2'` (or
`instrumentId IS NOT NULL`) to exclude pre-1.5a v1 rows.

---

## 15. Telegram report (`src/report.ts`)

Append to the daily message:

```
Securities mastered: <securitiesMastered>
Securities resolved: <securitiesResolved>
Missing reference (EH fallback): <missingReferenceData>
Unresolved tickers: <unresolvedTickers>
Alias rows: <aliasRows>
```

Rendered from the manifest, so the message and stored record never disagree.

---

## 16. Tests

| File | New / Update | Covers |
|------|--------------|--------|
| `identity.test.ts` | new | fallback order; confidence mapping; GOOG/GOOGL & BRK.A/BRK.B distinctness; `splitTicker` |
| `securityMaster.test.ts` | new | field mapping; one-row-per-universe-ticker invariant; EH: fallback minting + `MISSING_FALLBACK`; `buildTickerMap`; inactive/delisted preserve `active=false`; `identity_changed` detection |
| `symbolAliases.test.ts` | new | open new alias; carry-forward; forward rename closes prior + opens new; de-dupe; `alias_conflict` |
| `resolver.test.ts` | new | exact-ticker resolve; keeps original ticker; unknown → `unresolved_instrument` excluded |
| `historyCache.test.ts` | update | key uses `instrumentId` |
| `metrics.test.ts` | update | `MetricRow` includes `instrumentId` |
| `storage.test.ts` | update | RAW/METRIC schemas include `instrumentId`; securities/alias schemas round-trip |
| `pipeline.test.ts` | update | `listSecurities` called before resolve; minted-fallback keeps the stock; unresolved excluded; universe snapshot stores securities; manifest resolution stats |
| `polygon.test.ts` | update | `listSecurities` maps reference fields; missing ticker → `missing_reference_data` |
| `provider-factory.test.ts` / `fake.ts` | update | fake `listSecurities` |
| `types.test.ts` | update | v2 version constants |
| `report.test.ts` | update | resolution stats lines |

---

## 17. Acceptance criteria

Part 1.5a is complete when:

- [ ] Security master is created daily, one row per universe ticker (§4 invariant holds).
- [ ] `symbol_aliases` snapshot is created daily.
- [ ] Raw bars include `instrumentId`; metric rows include `instrumentId`.
- [ ] History cache is keyed by `instrumentId`.
- [ ] Universe snapshot stores securities (not bare tickers), with a `tickers` fallback.
- [ ] A universe ticker with missing reference data is kept via an `EH:<ticker>` /
      `MISSING_FALLBACK` row and logged `missing_reference_data` — **not dropped**.
- [ ] Athena can query `daily_metrics` by `instrumentId` and join to `securities`.
- [ ] `referenceStatus = 'MISSING_FALLBACK'` audit query works.
- [ ] Same-day rerun still creates a new `runId`; `metadata/current` behavior unchanged.
- [ ] Part 1 pipeline still works end-to-end (daily + backfill).
- [ ] Telegram report includes resolution stats.
- [ ] `npm run typecheck` and `npm test` pass.

---

## 18. Explicitly out of scope (Part 1.5b / later)

Full-catalog dynamic universe & auto-IPO/delisting; historical ticker-event reconstruction; full
20-year alias windows; cross-day `instrumentId` pinning / restamping; corporate-action / split /
dividend adjustment; forward-return labels; backtester; strategy logic; broker/execution; AI/ML.

---

## 19. Implementation order

1. **Types + schema versions** — `types.ts` (new interfaces, `instrumentId` on raw/metric,
   v2 constants, manifest fields, new error reasons); `schemas/dailyBars_v2.json`,
   `schemas/metrics_v2.json`.
2. **Identity helper** — `identity.ts` + `identity.test.ts`.
3. **Provider reference method** — interface + Polygon `listSecurities` + fake; `polygon.test.ts`.
4. **Security master storage** — `securityMaster.ts` (incl. invariant + `identity_changed`),
   `SECURITIES_SCHEMA`, `securitiesKey`, `securityMaster.test.ts`.
5. **Symbol aliases** — `symbolAliases.ts`, `SYMBOL_ALIASES_SCHEMA`, `symbolAliases.test.ts`.
6. **Resolver** — `resolver.ts` + `resolver.test.ts`.
7. **History cache key migration** — `historyCache.ts` + test; thread `instrumentId` through pipeline
   deps.
8. **Raw/metrics schema update** — `instrumentId` in `RAW_SCHEMA`/`METRIC_SCHEMA`, `enrichRaw`,
   `computeMetrics`; `storage.test.ts`, `metrics.test.ts`.
9. **Universe snapshot upgrade** — `metadata.ts` snapshot shape.
10. **Pipeline integration** — wire steps; manifest + report stats; `pipeline.test.ts`, `report.test.ts`.
11. **Glue/SAM** — `template.yaml` column add + two new tables; `glue.ts` `addAsOfPartition`; `glue.test.ts`.
12. **Verify** — `npm run typecheck && npm test`; then a real AWS daily run; confirm the §14 queries.

---

## 20. Final note

This is an **additive** upgrade. Do not redesign Part 1. The shape to keep in mind:

```
Polygon reference data → security master (scoped to universe, EH: fallback)
  → ticker→instrument resolver → instrumentId on raw bars + metrics
  → S3 Parquet → Athena (query by instrumentId, join to securities)
```

The goal is to make all future data identity-safe before EdgeHub scales — not to solve every
historical edge case today.
