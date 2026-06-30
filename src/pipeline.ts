import { S3Client } from "@aws-sdk/client-s3";
import { GlueClient } from "@aws-sdk/client-glue";
import type { MarketDataProvider } from "./providers/provider.js";
import type { VendorBar, ResolvedVendorBar, RawBarRow, MetricRow, RunManifest, RunMode, Provenance, ErrorRecord, SecurityMasterRow } from "./types.js";
import { SCHEMA_VERSION, RAW_SCHEMA_VERSION, METRIC_VERSION } from "./types.js";
import { loadUniverse } from "./universe.js";
import { gradeBar } from "./validate.js";
import { computeMetrics } from "./metrics.js";
import { mergeHistory, hasEnoughHistory } from "./history.js";
import { writeRaw, writeMetrics, writeSecurities } from "./storage.js";
import { buildSecurityMaster, detectIdentityChanges, writeSecuritiesState, readSecuritiesState } from "./securityMaster.js";
import { buildSymbolAliases, writeSymbolAliases, writeAliasesState, readAliasesState } from "./symbolAliases.js";
import { resolveBarsToInstruments } from "./resolver.js";
import { addPartition, addAsOfPartition } from "./glue.js";
import { writeManifest, markCurrent, snapshotUniverse } from "./metadata.js";
import { writeErrors } from "./errors.js";

export function makeRunId(now: Date): string {
  return now.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
}

export function enrichRaw(bar: ResolvedVendorBar, ctx: { runId: string; universeVersion: string }): RawBarRow {
  return { ...bar, runId: ctx.runId, schemaVersion: RAW_SCHEMA_VERSION, metricVersion: METRIC_VERSION, universeVersion: ctx.universeVersion };
}

const MIN_SUCCESS_RATE = 0.9;

export interface BuildManifestArgs {
  mode: RunMode; runId: string; tradingDay: string; provider: string; universeVersion: string;
  requested: number; succeeded: number; warnings: number; rejected: number; missingBars: number; runtimeSec: number;
  securitiesMastered: number; securitiesResolved: number; unresolvedTickers: number; missingReferenceData: number; aliasRows: number;
}
export function buildManifest(a: BuildManifestArgs): RunManifest {
  const rate = a.requested === 0 ? 0 : a.succeeded / a.requested;
  const status: RunManifest["status"] =
    a.succeeded === 0 || rate < MIN_SUCCESS_RATE ? "FAILURE" : a.succeeded < a.requested ? "PARTIAL" : "SUCCESS";
  return {
    runId: a.runId, mode: a.mode, tradingDay: a.tradingDay, provider: a.provider, universeVersion: a.universeVersion,
    symbolsRequested: a.requested, symbolsSucceeded: a.succeeded, rowsWritten: a.succeeded,
    warnings: a.warnings, rejected: a.rejected, missingBars: a.missingBars,
    securitiesMastered: a.securitiesMastered, securitiesResolved: a.securitiesResolved,
    unresolvedTickers: a.unresolvedTickers, missingReferenceData: a.missingReferenceData, aliasRows: a.aliasRows,
    runtimeSec: a.runtimeSec,
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
  readHistory: (instrumentId: string) => Promise<VendorBar[]>;
  writeHistory: (instrumentId: string, bars: VendorBar[]) => Promise<void>;
  isTradingDay: (date: string) => boolean;
  previousTradingDay: (date: string) => string;
  calendarCovers: (date: string) => boolean;
}

const MIN_SESSIONS = 253; // max metric window: return252d needs 253 closes; 52w needs 252; ma200Rising 201
const LOOKBACK_DAYS = 400;

export async function runPipeline(mode: RunMode, deps: Deps): Promise<RunManifest> {
  const start = deps.now().getTime();
  const runId = makeRunId(deps.now());
  const { tickers, universeVersion } = loadUniverse();

  const earlyExit = (status: RunManifest["status"], note: string): RunManifest => ({
    runId, mode, tradingDay: deps.tradingDay, provider: deps.provider.name, universeVersion,
    symbolsRequested: tickers.length, symbolsSucceeded: 0, rowsWritten: 0, warnings: 0, rejected: 0,
    missingBars: 0, securitiesMastered: 0, securitiesResolved: 0, unresolvedTickers: 0, missingReferenceData: 0, aliasRows: 0,
    runtimeSec: 0, metricVersion: METRIC_VERSION, schemaVersion: SCHEMA_VERSION, status, note,
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

  // Resolve the effective trading day: the market day this run's data actually belongs to.
  // Daily is already gated to a trading day above. Backfill defaults tradingDay to "today",
  // which may be a weekend/holiday — resolve back to the latest trading day so every stored
  // row's `date` matches the partition path it lands in (and the metadata/current pointer).
  const effectiveDate = mode === "backfill" && !deps.isTradingDay(deps.tradingDay)
    ? deps.previousTradingDay(deps.tradingDay)
    : deps.tradingDay;

  const rawToStore: RawBarRow[] = [];
  const metricRows: MetricRow[] = [];
  const errors: ErrorRecord[] = [];
  const seen = new Set<string>();
  let warnings = 0, rejected = 0, missingBars = 0;

  const recordError = (ticker: string, reason: string, message?: string) =>
    errors.push({ runId, tradingDay: effectiveDate, source: deps.provider.name, universeVersion, ticker, reason, message, createdAt: deps.now().toISOString() });

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
  for (const t of aliasBuild.conflicts) recordError(t, "alias_conflict", `Ticker ${t} collides onto an instrumentId already claimed by another ticker on ${effectiveDate}`);
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

  const prov: Provenance = {
    runId, ingestedAt: deps.now().toISOString(), source: deps.provider.name, sourceVersion: deps.provider.version,
    schemaVersion: SCHEMA_VERSION, metricVersion: METRIC_VERSION, universeVersion,
  };

  // Daily mode fetches all latest bars in one resilient batch call (exact effectiveDate).
  const latestByTicker = new Map<string, VendorBar>();
  if (mode === "daily") {
    const result = await deps.provider.getLatestBars(effectiveDate, tickers);
    for (const b of result.bars) latestByTicker.set(b.ticker, b);
    for (const f of result.failures) recordError(f.ticker, f.reason, f.message);
    const { errors: resolveErrors } = resolveBarsToInstruments([...latestByTicker.values()], securities, effectiveDate, runId, deps.provider.name, universeVersion);
    for (const e of resolveErrors) { errors.push(e); unresolvedTickers++; }
  }

  for (const ticker of tickers) {
    try {
      const instrumentId = idByTicker.get(ticker);
      if (!instrumentId) { missingBars++; continue; } // unresolved (defensive; already errored)

      let bars: VendorBar[];
      let cacheBars: VendorBar[] | null = null; // full untruncated history to refresh the cache with (daily only)
      if (mode === "backfill") {
        const r = await deps.provider.getHistory(ticker, LOOKBACK_DAYS, effectiveDate);
        for (const f of r.failures) recordError(f.ticker || ticker, f.reason, f.message);
        bars = r.bars.filter((b) => b.date <= effectiveDate).sort((a, b) => a.date.localeCompare(b.date));
        if (bars.length === 0) { missingBars++; continue; }
        await deps.writeHistory(instrumentId, bars); // seed the cache (backfill's primary job) even if today's bar isn't out yet
      } else {
        const latest = latestByTicker.get(ticker);
        if (!latest) { missingBars++; continue; } // failure already recorded from the batch result
        const stored = await deps.readHistory(instrumentId);
        let fullBars = mergeHistory(stored, latest);
        if (!hasEnoughHistory(fullBars, MIN_SESSIONS)) {
          const r = await deps.provider.getHistory(ticker, LOOKBACK_DAYS, effectiveDate);
          for (const f of r.failures) recordError(f.ticker || ticker, f.reason, f.message);
          fullBars = mergeHistory(r.bars, latest);
        }
        cacheBars = fullBars; // refresh cache with the FULL history (never discard bars newer than effectiveDate on replay)
        bars = fullBars.filter((b) => b.date <= effectiveDate); // computation/selection slice ends at effectiveDate
      }

      // Select the current session strictly by date — no array-position guessing, no stale/wrong-date row.
      const currentBar = bars.find((b) => b.date === effectiveDate);
      if (!currentBar) { missingBars++; recordError(ticker, "missing_bar_for_date"); continue; }

      const grade = gradeBar(currentBar, seen); // single batch-level grading (shared seen set)
      rawToStore.push(enrichRaw({ ...currentBar, instrumentId }, { runId, universeVersion }));
      if (grade.status === "REJECTED") { rejected++; recordError(ticker, "rejected", grade.issues.join(",")); continue; }
      if (grade.status === "WARN") { warnings++; recordError(ticker, "warn", grade.issues.join(",")); }

      metricRows.push(computeMetrics(bars, prov, grade, instrumentId)); // bars end at effectiveDate
      if (mode === "daily" && cacheBars) await deps.writeHistory(instrumentId, cacheBars); // refresh cache (full history) for accepted bars only
    } catch (err) {
      recordError(ticker, "pipeline_error", (err as Error).message); // per-ticker failure is non-fatal
      continue;
    }
  }

  if (rawToStore.length > 0) {
    await writeRaw(deps.s3, deps.bucket, rawToStore, deps.provider.name, effectiveDate, runId);
    await addPartition(deps.glue, deps.database, "daily_bars", deps.bucket, `raw/${deps.provider.name}/daily`, effectiveDate);
  }
  if (metricRows.length > 0) {
    await writeMetrics(deps.s3, deps.bucket, metricRows, effectiveDate, runId);
    await addPartition(deps.glue, deps.database, "daily_metrics", deps.bucket, "metrics/daily", effectiveDate);
  }
  await addAsOfPartition(deps.glue, deps.database, "securities", deps.bucket, "reference/securities", effectiveDate);
  await addAsOfPartition(deps.glue, deps.database, "symbol_aliases", deps.bucket, "reference/symbol_aliases", effectiveDate);
  await writeErrors(deps.s3, deps.bucket, effectiveDate, runId, errors); // no-op if empty

  const runtimeSec = Math.round((deps.now().getTime() - start) / 1000);
  const manifest = buildManifest({
    mode, runId, tradingDay: effectiveDate, provider: deps.provider.name, universeVersion,
    requested: tickers.length, succeeded: metricRows.length, warnings, rejected, missingBars, runtimeSec,
    securitiesMastered: securities.length, securitiesResolved: metricRows.length,
    unresolvedTickers, missingReferenceData: built.missingTickers.length, aliasRows: aliasBuild.aliases.length,
  });

  await writeManifest(deps.s3, deps.bucket, manifest);
  if (manifest.status === "SUCCESS" || manifest.status === "PARTIAL") {
    await markCurrent(deps.s3, deps.bucket, manifest); // accepted runs only; never on FAILURE/SKIPPED
  }
  return manifest;
}
