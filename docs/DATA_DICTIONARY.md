# EdgeHub Data Dictionary

Generated from `config/metrics.ts` (the metric registry). Add a metric there → add its row here.

## daily_bars (raw)

| Column | Type | Meaning |
|--------|------|---------|
| instrumentId | string | Stable security id (FIGI-based, or `EH:` fallback) |
| ticker | string | Symbol (as-traded on `date`) |
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

> **Identity (Part 1.5a):** every raw bar and metric row now carries `instrumentId` — the stable
> security id (FIGI-based, or an `EH:` fallback). `ticker` is the as-traded symbol for `date`.
> Query continuous history by `instrumentId`; filter `schemaVersion = 'metrics_v2'` (or
> `instrumentId IS NOT NULL`) to exclude pre-1.5a v1 rows.
> The history cache was re-keyed from `ticker=` to `instrumentId=`, so the first daily run after
> deploy re-backfills each ticker once (a one-time cache-miss burst) and leaves orphaned `ticker=`
> cache objects behind — self-healing, no data loss.

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

## daily_metrics

Identity & provenance: instrumentId, ticker, date, runId, source, sourceVersion, schemaVersion, metricVersion, universeVersion.

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
