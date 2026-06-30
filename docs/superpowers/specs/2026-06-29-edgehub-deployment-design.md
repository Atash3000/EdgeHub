# EdgeHub Part 1 — Deployment & Implementation Design

**Date:** 2026-06-29
**Status:** Approved (pending final spec review)
**Scope:** Implementation and deployment design for EdgeHub Part 1 — Market Data Lake.
The *data* design (S3 layout, schemas, Glue/Athena, Telegram) is defined in `project-plan.md`
and is treated as fixed. This document specifies **how it is built and deployed**.

---

## 1. Decisions (locked)

| Area | Decision |
|------|----------|
| IaC / deploy framework | **AWS SAM** (`template.yaml` + `samconfig.toml`) |
| Compute shape | **Option A** — single Lambda monolith, internally modular |
| Language / runtime | **TypeScript** on **Node.js 20** Lambda |
| Market data vendor | **Finnhub** as the first impl; active vendor chosen by config (`DATA_PROVIDER` env var), swappable behind a `MarketDataProvider` interface — nothing vendor-specific is hardcoded |
| GitHub → AWS auth | **OIDC** (no long-lived keys), deploys only from `main` |
| AWS region | **us-east-1** |
| Ticker universe | **Committed JSON files** in `config/universe/` |
| Parquet writer | `@dsnp/parquetjs` |
| AWS access | AWS SDK v3 (`@aws-sdk/client-s3`, `-glue`, `-secrets-manager`) |

---

## 2. Guiding constraints (from `project-plan.md`)

- Source of truth is S3; every trading day produces one immutable snapshot.
- Never overwrite historical data; raw data preserved forever.
- Data must be reproducible; computed features are versioned.
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
│   └── universe/
│       ├── sp500.json
│       ├── nasdaq100.json
│       └── watchlist.json
├── src/
│   ├── handler.ts             # Lambda entrypoint; parses mode (daily|backfill), runs pipeline
│   ├── pipeline.ts            # orchestrates the 9 steps, tracks RunStats, owns runId
│   ├── universe.ts            # Step 1: load + merge/dedupe ticker lists
│   ├── providers/
│   │   ├── provider.ts        # MarketDataProvider interface (the generic contract)
│   │   ├── factory.ts         # getProvider(config) -> active impl, selected by DATA_PROVIDER
│   │   └── finnhub.ts         # one concrete impl (rate-limited <=60/min); polygon.ts / tiingo.ts later
│   ├── history.ts             # load trailing bars from S3 for feature computation
│   ├── validate.ts            # Steps 3 & 8: data-quality rules -> qualityStatus
│   ├── features.ts            # Step 5: ma/atr/returns/52w/flags
│   ├── storage.ts             # Steps 4 & 6: write Parquet -> S3 (raw + features) under runId
│   ├── glue.ts                # Step 7: add partitions to Glue
│   ├── report.ts              # Step 9: Telegram message
│   ├── metadata.ts            # write/read metadata/latest pointer + ingestion logs
│   ├── secrets.ts             # fetch Finnhub + Telegram tokens from Secrets Manager
│   └── types.ts               # RawBar, FeatureRow, RunStats, version constants
├── tests/                     # unit tests (features, validate, universe, history)
└── .github/workflows/
    ├── ci.yml                 # on PR: install, typecheck, test, sam validate, sam build
    └── deploy.yml             # on push to main: OIDC -> sam build -> sam deploy
```

---

## 4. Run modes (backfill vs daily)

The same Lambda supports two modes, selected by event payload (`{"mode": "daily"}` default, or
`{"mode": "backfill", "lookbackDays": 400}`). This avoids re-fetching long histories every day.

- **Backfill (manual / one-time / new ticker):** fetch **300–400 trading days** per ticker from Finnhub.
  Used on first setup and automatically for any ticker that has insufficient stored history.
- **Daily (scheduled):** fetch **only the latest daily bar** per ticker, then read trailing history
  from S3 (`history.ts`) to compute MA200, ATR14, 52-week stats, and multi-month returns.
- **Auto-backfill fallback:** during a daily run, if `history.ts` finds a ticker lacks enough
  trailing bars (new constituent, or gap), the pipeline fetches a lookback window for that ticker only.

This keeps daily runs fast and within Finnhub rate limits, while still producing full features.

---

## 4b. Provider abstraction (vendor-agnostic)

No code outside `providers/` knows which vendor is in use. The rest of the pipeline depends only on the
`MarketDataProvider` interface.

```ts
// providers/provider.ts
export interface MarketDataProvider {
  readonly name: string;                 // e.g. "finnhub" — drives the raw/<source>/ path
  getLatestBars(date: string, tickers: string[]): Promise<RawBar[]>;
  getHistory(ticker: string, lookbackDays: number): Promise<RawBar[]>;
}
```

- `providers/factory.ts` returns the active impl based on the `DATA_PROVIDER` config value
  (default `finnhub`); the concrete file (`finnhub.ts`, later `polygon.ts` / `tiingo.ts`) is an
  implementation detail behind it.
- **Adding a vendor = drop one new file implementing the interface + set `DATA_PROVIDER`.** No changes
  to the pipeline, storage, features, or paths.
- The `source` / `sourceVersion` provenance fields and the `raw/<source>/` prefix are all taken from
  `provider.name` / the provider's reported version — never a hardcoded string.

---

## 5. AWS resources (`template.yaml`)

- **S3 bucket** `edgehub-data` — **versioning ON**, folders `raw/ features/ metadata/ reports/ errors/`.
- **Lambda** `edgehub-daily-collector` — Node.js 20, **timeout 900s**, memory ~1024 MB.
  Env vars: bucket name, region, `DATA_PROVIDER` (e.g. `finnhub`), `SCHEMA_VERSION`, `FEATURE_VERSION`,
  `SOURCE_VERSION`, secret name. Switching vendors is a config/env change, not a code change.
- **EventBridge Scheduler** (`AWS::Scheduler::Schedule`) — **`ScheduleExpressionTimezone: America/New_York`**,
  fires **6:30 PM ET on weekdays** (`cron(30 18 ? * MON-FRI *)` in that timezone). Using Scheduler (not a
  plain EventBridge rule with UTC cron) eliminates DST drift.
- **Glue** — database `edgehub`; external tables `daily_bars` and `daily_features` over the partitioned
  S3 Parquet, partitioned by `year/month/day`.
- **Secrets Manager** secret `edgehub/secrets` — `{ finnhubToken, telegramBotToken, telegramChatId }`.
  The **secret value is created manually once** in AWS; the template only declares the secret resource.
- **IAM** — (a) Lambda execution role: S3 RW on the bucket, Glue partition APIs, Secrets read, CloudWatch
  Logs; (b) **deploy role** trusted by GitHub OIDC (see §9).

---

## 6. Storage layout, runId & immutability

Plain S3 versioning is **not** the sole immutability guarantee. Every write goes under a unique `runId`
(UTC timestamp, e.g. `20260629T223000Z`), and a `metadata/latest/` pointer records the last successful run.

The `<source>` path segment is **derived from the active provider's `name`** (not hardcoded), so a
Polygon/Tiingo run lands under its own prefix automatically:

```
raw/<source>/daily/year=2026/month=06/day=29/runId=20260629T223000Z/part.parquet
   e.g. raw/finnhub/daily/...  or  raw/polygon/daily/...
features/daily/year=2026/month=06/day=29/runId=20260629T223000Z/part.parquet
metadata/latest/daily_features/year=2026/month=06/day=29.json   -> { runId, status, rowCount }
reports/year=2026/month=06/day=29/runId=.../report.json
errors/year=2026/month=06/day=29/runId=.../errors.json
```

- A re-run on the same day produces a **new `runId`**, never overwriting the prior run's data.
- Glue partitions point at the date partition; consumers resolve the authoritative run via the
  `metadata/latest` pointer. (Part 1 keeps partitioning **date-only**; a ticker partition or compacted
  history is a future optimization if Athena ticker-scans get slow.)

### Provenance fields (on every raw row, feature row, report, error file)

```
runId
ingestedAt
source            # e.g. "finnhub"
sourceVersion
schemaVersion
featureVersion
```

---

## 7. Data flow (the 9 steps, mapped)

`handler` → `pipeline.run(mode)` (generates `runId`):

1. `universe.load()` → merged, deduped ticker list.
2. `provider.getDailyBars(...)` → daily mode: latest bar; backfill: lookback window. Throttled to ≤60/min.
   Per-ticker failures are **collected, not thrown**.
3. `validate.checkRaw()` → assigns `qualityStatus` (OK/WARN/REJECTED) + `qualityIssues[]`.
4. `storage.writeRaw()` → `raw/...runId.../part.parquet`.
5. `history.load()` + `features.compute()` → one `FeatureRow` per ticker.
6. `storage.writeFeatures()` → `features/...runId.../part.parquet`.
7. `glue.addPartition()` for both tables (idempotent).
8. `validate.checkFeatures()` → final dataset validation.
9. `metadata.markLatest()` then `report.sendTelegram(stats)`.

---

## 8. Data quality (flag, don't only reject)

`validate.ts` assigns each row a `qualityStatus`:

- **REJECTED** — missing close, missing volume, duplicate ticker/date, negative price, negative volume.
  Excluded from `daily_features`; recorded in `errors/`.
- **WARN** — present but suspect (e.g. zero volume on a trading day, extreme single-day move, stale repeat).
  **Stored** with `qualityStatus=WARN` and populated `qualityIssues[]` so consumers can filter.
- **OK** — passes all checks.

Every failure/warning is recorded in `errors/...runId.../errors.json`. Nothing is silently dropped.

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
- Quality rejections excluded from output but logged to `errors/`.
- Partial-failure run still writes successful rows + sends a Telegram report showing `Failed: N`.
- Catastrophic failure (e.g. auth) → FAILURE Telegram + non-zero CloudWatch metric; `metadata/latest`
  is **not** advanced, so consumers keep using the prior good run.
- S3 versioning + per-run `runId` paths guarantee no historical overwrite even on same-day re-runs.

---

## 11. Testing

- **Unit (no AWS):** `features.ts` (golden values for MA/ATR/returns vs hand-computed fixtures),
  `validate.ts` (each OK/WARN/REJECTED rule), `universe.ts` (merge/dedupe), `history.ts` (trailing-window
  assembly, insufficient-history detection). Run in `ci.yml`.
- **Local integration:** `npm run invoke:local` via `sam local invoke` with a **fake `MarketDataProvider`**,
  exercising the full pipeline without hitting Finnhub or AWS.

---

## 12. Scale-out path (out of scope for Part 1, noted)

When the universe grows toward Russell 1000/3000, the single-Lambda 15-min ceiling and Finnhub limits
will not fit. The modular step structure lets each step be lifted into **Step Functions** (Map-state
fan-out, per-step retry) without rewriting business logic. Ticker-level partitioning / history compaction
can be added then.

---

## 13. Success criteria (unchanged from `project-plan.md`)

Daily Lambda runs automatically; data downloads correctly; raw bars in S3; features computed and stored
in Parquet; Glue catalog updated; Athena queries work; Telegram report sent; historical data never
overwritten.
