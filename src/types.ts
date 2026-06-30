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
