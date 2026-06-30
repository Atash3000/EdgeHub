# EdgeHub Part 1 — Deployment & Implementation Design

**Date:** 2026-06-29
**Status:** Approved — build (incorporates review round 3)
**Scope:** Implementation and deployment design for EdgeHub Part 1 — Market Data Lake.
The *data* design (S3 layout, schemas, Glue/Athena, Telegram) is defined in `project-plan.md`
and is treated as fixed. This document specifies **how it is built and deployed**.

> Naming note: this design uses **"metrics"** (MA, ATR, volume, returns) rather than "features".
> "Feature store" carries machine-learning connotations; what Part 1 stores are deterministic market
> metrics. The term "features" is reserved for any future ML layer.

---

## 1. Decisions (locked)

| Area | Decision |
|------|----------|
| IaC / deploy framework | **AWS SAM** (`template.yaml` + `samconfig.toml`) |
| Compute shape | **Option A** — single Lambda monolith, internally modular |
| Language / runtime | **TypeScript** on **Node.js 24** Lambda (`nodejs24.x`, latest stable LTS) |
| Market data vendor | **Finnhub** as the first impl; active vendor chosen by config (`DATA_PROVIDER` env var), swappable behind a `MarketDataProvider` interface — nothing vendor-specific is hardcoded |
| GitHub → AWS auth | **OIDC** (no long-lived keys), deploys only from `main` |
| AWS region | **us-east-1** |
| Ticker universe | **Committed, versioned JSON files** in `config/universe/` |
| Derived data naming | **metrics** (not features); table `daily_metrics`, dir `metrics/`, `metricVersion` |
| Parquet writer | `@dsnp/parquetjs` |
| AWS access | AWS SDK v3 (`@aws-sdk/client-s3`, `-glue`, `-secrets-manager`) |

---

## 2. Guiding constraints (from `project-plan.md`)

- Source of truth is S3; every trading day produces one immutable snapshot.
- Never overwrite historical data; raw data preserved forever.
- Data must be reproducible; computed metrics are versioned.
- Keep Part 1 intentionally simple.
- Failures are recorded, never silently ignored.

---

## 3. Repository layout

```
EdgeHub/
├── template.yaml              # SAM: Lambda, EventBridge Scheduler, S3, Glue DB+tables, IAM
├── samconfig.toml             # SAM deploy config (stack name, region, params)
├── package.json / tsconfig.json
├── config/
│   ├── universe/
│   │   ├── sp500.json
│   │   ├── nasdaq100.json
│   │   └── watchlist.json
│   └── metrics.ts             # METRIC REGISTRY: name, description, dependsOn, window, version
├── schemas/
│   ├── dailyBars_v1.json      # JSON Schema for raw bars (schema registry)
│   └── metrics_v1.json        # JSON Schema for metric rows (schema registry)
├── docs/
│   ├── DATA_DICTIONARY.md     # every column/metric explained (derived from config/metrics.ts)
│   └── superpowers/specs/...  # this design doc
├── src/
│   ├── handler.ts             # Lambda entrypoint; parses mode (daily|backfill), runs pipeline
│   ├── pipeline.ts            # orchestrates the steps, tracks RunStats, owns runId
│   ├── universe.ts            # Step 1: load + merge/dedupe versioned ticker lists
│   ├── providers/
│   │   ├── provider.ts        # MarketDataProvider interface (the generic contract)
│   │   ├── factory.ts         # getProvider(config) -> active impl, selected by DATA_PROVIDER
│   │   └── finnhub.ts         # one concrete impl (rate-limited <=60/min); polygon.ts / tiingo.ts later
│   ├── history.ts             # load trailing bars from S3 for metric computation
│   ├── validate.ts            # data-quality rules -> qualityStatus
│   ├── metrics.ts             # compute ma/atr/returns/52w/flags from config/metrics.ts registry
│   ├── storage.ts             # write Parquet -> S3 (raw + metrics) under runId
│   ├── glue.ts                # add partitions to Glue
│   ├── report.ts              # Telegram message
│   ├── metadata.ts            # write run manifest + current pointer + ingestion logs
│   ├── secrets.ts             # fetch provider + Telegram tokens from Secrets Manager
│   └── types.ts               # RawBar, MetricRow, RunManifest, version constants
├── tests/                     # unit tests (metrics, validate, universe, history)
└── .github/workflows/
    ├── ci.yml                 # on PR: install, typecheck, test, sam validate, sam build
    └── deploy.yml             # on push to main: OIDC -> sam build -> sam deploy
```

---

## 4. Run modes (backfill vs daily)

The same Lambda supports two modes, selected by event payload (`{"mode": "daily"}` default, or
`{"mode": "backfill", "lookbackDays": 400}`). This avoids re-fetching long histories every day.

- **Backfill (manual / one-time / new ticker):** fetch **300–400 trading days** per ticker.
  Used on first setup and automatically for any ticker that has insufficient stored history.
- **Daily (scheduled):** fetch **only the latest daily bar** per ticker, then read trailing history from
  the **per-ticker history cache** to compute MA200, ATR14, 52-week stats, and multi-month returns.
- **Auto-backfill fallback:** during a daily run, if a ticker lacks enough trailing bars (new constituent,
  or gap), the pipeline fetches a lookback window for that ticker only.

**History cache:** instead of scanning 200+ date partitions per ticker, each ticker's trailing ~400 bars
live at `history/<source>/ticker=<T>/current.json`. Each daily run reads the cache, appends today's bar,
computes metrics, and rewrites the cache. The cache is **rebuildable from the immutable raw partitions**,
which remain the source of truth — so this is an index/cache, not a second source of truth.

**Calendar gate:** a minimal market calendar (`calendar.ts` + committed `config/calendar/holidays.json`
with a `coveredYears` list) provides `isTradingDay` / `previousTradingDay` / `calendarCoversYear`. The
scheduled daily run **skips non-trading days** (manifest `SKIPPED`, `current` not advanced). If the
trading day's year isn't in `coveredYears`, the run **fails safely** (`FAILURE` / `calendar_year_missing`,
`current` not advanced) rather than mis-treating real holidays — forcing a calendar-update PR each new
year. (A full exchange calendar with half-days is Part 2.)

This keeps daily runs fast and within provider rate limits, while still producing full metrics.

---

## 4b. Provider abstraction (vendor-agnostic)

No code outside `providers/` knows which vendor is in use. The rest of the pipeline depends only on the
`MarketDataProvider` interface.

```ts
// providers/provider.ts — providers return VendorBar (vendor data only, no provenance)
export interface MarketDataProvider {
  readonly name: string;                 // e.g. "finnhub" — drives the raw/<source>/ path
  readonly version: string;              // reported sourceVersion
  getLatestBars(date: string, tickers: string[]): Promise<ProviderResult>; // exact-date only, no stale fallback
  getHistory(ticker: string, lookbackDays: number): Promise<ProviderResult>;
}
// ProviderResult = { bars: VendorBar[]; failures: ProviderFailure[] }
```

**Per-ticker resilience:** providers return a `ProviderResult` — the bars they fetched **plus** a
`ProviderFailure` for each ticker that errored or had no exact-date bar. A single failing symbol never
throws and never aborts the batch. The pipeline records every failure (see Error handling).

**Provider output vs stored row:** a provider returns `VendorBar` (vendor fields only). The pipeline
enriches each into a `RawBarRow` (= `VendorBar` + full provenance: `runId, schemaVersion, metricVersion,
universeVersion`) before writing. Providers never know `runId`/`universeVersion`.

**Adjustment honesty:** `VendorBar` carries `adjustedClose: number | null` + `isAdjusted: boolean`.
Finnhub free candles are unadjusted, so we store `adjustedClose=null, isAdjusted=false` — we never
fabricate an adjusted price. (True adjustment + corporate actions are Part 2.)

**No stale substitution:** if the exact `tradingDay` bar is absent, the pipeline records a per-ticker
`missing_bar_for_date` failure and writes no row for that ticker — yesterday's bar is never relabeled as today.

- `providers/factory.ts` returns the active impl based on the `DATA_PROVIDER` config value
  (default `finnhub`); the concrete file (`finnhub.ts`, later `polygon.ts` / `tiingo.ts`) is an
  implementation detail behind it.
- **Adding a vendor = drop one new file implementing the interface + set `DATA_PROVIDER`.** No changes
  to the pipeline, storage, metrics, or paths.
- The `source` / `sourceVersion` provenance fields and the `raw/<source>/` prefix are all taken from
  `provider.name` / `provider.version` — never a hardcoded string.

---

## 4c. Metric registry, schema registry & data dictionary

These three artifacts share a single source of truth so they can never drift:

- **Metric registry** — `config/metrics.ts` declares every metric as data:

  ```ts
  { name: "ma200", description: "200-day simple moving average",
    dependsOn: ["close"], window: 200, version: "1.0" }
  ```

  `metrics.ts` (compute) iterates this registry; nothing computes a metric that isn't declared here.
- **Schema registry** — `schemas/dailyBars_v1.json` and `schemas/metrics_v1.json` are versioned JSON
  Schemas. `schemaVersion` on each row references these files (e.g. `metrics_v1`), not a bare number.
- **Data dictionary** — `docs/DATA_DICTIONARY.md` explains every column/metric for human consumers
  (future strategy authors). Generated from the metric registry so it stays in sync.

This is what makes EdgeHub research-grade: a consumer can resolve exactly what `ma200` means, what it
depends on, which version produced it, and which schema validated it.

---

## 5. AWS resources (`template.yaml`)

- **S3 bucket** `edgehub-data-<accountId>-<region>` (suffixed for global uniqueness) — **versioning ON**, top-level prefixes:
  `raw/  metrics/  labels/  corporate_actions/  metadata/  reports/  errors/`
  (`labels/` and `corporate_actions/` are **reserved for Part 2 — created but unused in Part 1**, so
  later first-class data has a home without a migration).
- **Lambda** `edgehub-daily-collector` — Node.js 24, **timeout 900s**, memory ~1024 MB,
  **`ReservedConcurrentExecutions: 1`** (one run at a time — prevents overlapping runs racing on the
  per-ticker history cache).
  Env vars: bucket name, region, `DATA_PROVIDER` (e.g. `finnhub`), `SCHEMA_VERSION`, `METRIC_VERSION`,
  `SOURCE_VERSION`, secret name. Switching vendors is a config/env change, not a code change.
- **EventBridge Scheduler** (`AWS::Scheduler::Schedule`) — **`ScheduleExpressionTimezone: America/New_York`**,
  fires **6:30 PM ET on weekdays** (`cron(30 18 ? * MON-FRI *)` in that timezone). Using Scheduler (not a
  plain EventBridge rule with UTC cron) eliminates DST drift.
- **Glue** — database `edgehub`; external tables `daily_bars` and `daily_metrics` over the partitioned
  S3 Parquet, partitioned by `year/month/day`.
- **Secrets Manager** secret `edgehub/secrets` — `{ <provider>Token, telegramBotToken, telegramChatId }`.
  The **secret value is created manually once** in AWS; the template only declares the secret resource.
- **IAM** — (a) Lambda execution role: S3 RW on the bucket, Glue partition APIs, Secrets read, CloudWatch
  Logs; (b) **deploy role** trusted by GitHub OIDC (see §9).

---

## 6. Storage layout, runId & immutability

Plain S3 versioning is **not** the sole immutability guarantee. Every write goes under a unique `runId`
(UTC timestamp, e.g. `20260629T223000Z`), and a `metadata/current/` pointer records the authoritative run.
(`current`, not `latest` — "latest by time" and "the run consumers should use" are not the same thing;
a newer failed run must not become authoritative.)

The `<source>` path segment is **derived from the active provider's `name`** (not hardcoded), so a
Polygon/Tiingo run lands under its own prefix automatically:

```
raw/<source>/daily/year=2026/month=06/day=29/runId=20260629T223000Z/part.parquet
   e.g. raw/finnhub/daily/...  or  raw/polygon/daily/...
metrics/daily/year=2026/month=06/day=29/runId=20260629T223000Z/part.parquet
metadata/current/daily_metrics/year=2026/month=06/day=29.json  -> { runId, status, rowCount }
metadata/runs/year=2026/month=06/day=29/runId=.../manifest.json
metadata/universe/2026-06-29.json                              -> resolved universe snapshot
reports/year=2026/month=06/day=29/runId=.../report.json
errors/year=2026/month=06/day=29/runId=.../errors.json
history/<source>/ticker=<T>/current.json   (per-ticker trailing-bar cache, rebuildable)
labels/              (reserved, Part 2)
corporate_actions/   (reserved, Part 2)
```

- A re-run on the same day produces a **new `runId`**, never overwriting the prior run's data.
- `metadata/current` advances **only on `SUCCESS` or an accepted `PARTIAL`** (success rate ≥ floor) —
  never on `FAILURE` or `SKIPPED`, so a newer bad/holiday run can't become authoritative.
- Glue partitions point at the date partition; consumers resolve the authoritative run via the
  `metadata/current` pointer. (Part 1 keeps partitioning **date-only**; a ticker partition or compacted
  history is a future optimization if Athena ticker-scans get slow.)
- **Athena rerun rule:** because a date partition can hold multiple `runId`s, a naive date query returns
  duplicate rows. Consumers MUST filter `WHERE runId = '<runId from metadata/current>'`. A consumer-friendly
  `metrics_current/` (accepted run only) is a possible Part 2 convenience; immutable history stays in `metrics/`.

### Provenance fields (on every raw row, metric row, report, error file)

```
runId
ingestedAt
source            # from provider.name, e.g. "finnhub"
sourceVersion
schemaVersion     # e.g. "metrics_v1"
metricVersion
universeVersion   # which universe snapshot this row was produced under
```

---

## 6b. Run manifest

Every run writes `metadata/runs/.../manifest.json` — the run-level record of truth:

```json
{
  "runId": "20260629T223000Z",
  "mode": "daily",
  "tradingDay": "2026-06-29",
  "provider": "finnhub",
  "universeVersion": "2026-06-29",
  "symbolsRequested": 612,
  "symbolsSucceeded": 610,
  "rowsWritten": 610,
  "warnings": 4,
  "rejected": 2,
  "runtimeSec": 302,
  "metricVersion": "1.0",
  "schemaVersion": "metrics_v1",
  "status": "SUCCESS"
}
```

The Telegram report is rendered from this manifest, so the message and the stored record never disagree.

---

## 6c. Universe versioning (reproducibility)

The universe changes over time (index reconstitutions, watchlist edits). For a backtest in 2028 of a
2026 day to be correct, we must know **which tickers were in the universe on that day**.

- The committed `config/universe/*.json` files carry a `universeVersion` (the date/commit they represent).
- Each run snapshots the **resolved** universe it actually used to `metadata/universe/<tradingDay>.json`,
  and stamps `universeVersion` onto every row and the manifest.
- A future backtester reconstructs the exact constituent set for any historical day from these snapshots —
  never guessing today's membership for a past date.

---

## 7. Data flow (the steps, mapped)

`handler` → `pipeline.run(mode)` (generates `runId`):

1. `universe.load()` → merged, deduped ticker list + `universeVersion`; snapshot to `metadata/universe/`.
2. `provider.getLatestBars(...)` / `getHistory(...)` → daily: latest bar; backfill: lookback window.
   Throttled to provider limits. Per-ticker failures are **collected, not thrown**.
3. `validate.checkRaw()` → assigns `qualityStatus` (OK/WARN/REJECTED) + `qualityIssues[]`.
4. `storage.writeRaw()` → `raw/<source>/...runId.../part.parquet`.
5. `history.load()` + `metrics.compute()` (driven by `config/metrics.ts`) → one `MetricRow` per ticker.
6. `storage.writeMetrics()` → `metrics/...runId.../part.parquet`.
7. `glue.addPartition()` for both tables (idempotent).
8. `validate.checkMetrics()` → final dataset validation.
9. `metadata.writeManifest()` + `metadata.markCurrent()` → then `report.sendTelegram(manifest)`.

---

## 8. Data quality (flag, don't only reject)

`validate.ts` assigns each row a `qualityStatus`:

- **REJECTED** — missing close, missing volume, duplicate ticker/date, negative price, negative volume.
  Excluded from `daily_metrics`; recorded in `errors/`.
- **WARN** — present but suspect (e.g. zero volume on a trading day, extreme single-day move, stale repeat).
  **Stored** with `qualityStatus=WARN` and populated `qualityIssues[]` so consumers can filter.
- **OK** — passes all checks.

A dedicated `errors.ts` writes structured `ErrorRecord`s (`runId, tradingDay, source, universeVersion,
ticker, reason, message, createdAt`) to `errors/...runId.../errors.json` — covering provider failures,
`missing_bar_for_date`, REJECTED rows, WARN rows, pipeline errors, and `calendar_year_missing`. Counts
roll up into the manifest. `qualityIssues` is stored as a **JSON-encoded string array** (not comma-joined),
so issue codes parse cleanly downstream. Nothing is silently dropped.

---

## 9. CI/CD (GitHub → AWS)

- **`ci.yml`** (on pull request): `npm ci` → `tsc --noEmit` → `npm test` → `sam validate` → `sam build`.
  No AWS deploy — the merge-safety gate.
- **`deploy.yml`** (on push to `main`): OIDC assume deploy role → `sam build` →
  `sam deploy --no-confirm-changeset`. **Deploys happen only here**, never from a laptop —
  satisfying "every deploy to AWS is from GitHub."
- **One-time bootstrap (manual, documented in the plan):** create the GitHub OIDC provider and the
  deploy IAM role trusting `repo:Atash3000/EdgeHub:ref:refs/heads/main`, and create the Secrets Manager
  secret value. GitHub cannot create the role it needs to authenticate, so this is done once by hand
  (or via a tiny separate bootstrap template).

---

## 10. Error handling

- Per-ticker failures are collected and non-fatal; one bad symbol cannot kill the run.
- Quality rejections excluded from output but logged to `errors/` and counted in the manifest.
- Partial-failure run still writes successful rows + sends a Telegram report showing `rejected`/`warnings`.
- Catastrophic failure (e.g. auth) → FAILURE Telegram + non-zero CloudWatch metric; `metadata/current`
  is **not** advanced, so consumers keep using the prior good run.
- S3 versioning + per-run `runId` paths guarantee no historical overwrite even on same-day re-runs.

---

## 11. Testing

- **Unit (no AWS):** `metrics.ts` (golden values for MA/ATR/returns vs hand-computed fixtures, driven by
  the registry), `validate.ts` (each OK/WARN/REJECTED rule), `universe.ts` (merge/dedupe + versioning),
  `history.ts` (trailing-window assembly, insufficient-history detection). Run in `ci.yml`.
- **Schema conformance:** sample raw/metric rows validated against `schemas/*.json` in tests.
- **Local integration:** `npm run invoke:local` via `sam local invoke` with a **fake `MarketDataProvider`**,
  exercising the full pipeline without hitting the vendor or AWS.

---

## 12. Reserved for Part 2 (designed-for, not built now)

Created/structured now so later work needs no migration, but **explicitly out of Part 1 scope**:

- `labels/` — forward-return labels (30dReturn, 90dReturn, 2R, etc.).
- `corporate_actions/` — splits, dividends, symbol changes.
- **Full exchange calendar** — Part 1 ships a *minimal* static calendar (`config/calendar/holidays.json`)
  that gates the daily run; half-days and a real exchange-calendar source are Part 2.
- **Scale-out to Step Functions** — when the universe grows toward Russell 1000/3000, the modular step
  structure lifts into Step Functions (Map-state fan-out, per-step retry) without rewriting business logic.
- **Vision (not scope): "historical memory" / similarity search** — with versioned metrics across history,
  a future service could answer "find the 20 historical situations most similar to AAPL today" and return
  outcome distributions. Captured here so it informs naming/structure; no Part 1 work.

---

## 13. Success criteria (unchanged from `project-plan.md`)

Daily Lambda runs automatically; data downloads correctly; raw bars in S3; metrics computed and stored
in Parquet; Glue catalog updated; Athena queries work; Telegram report sent; historical data never
overwritten.
