export const SCHEMA_VERSION = "metrics_v2";
export const RAW_SCHEMA_VERSION = "dailyBars_v2";
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
  reason: string;   // provider_error | missing_bar_for_date | rejected | warn | pipeline_error | calendar_year_missing | unresolved_instrument | duplicate_instrument_ticker | missing_reference_data | security_master_empty | alias_conflict | identity_changed
  message?: string;
  createdAt: string;
}

/** Stored raw row = vendor bar enriched with full provenance by the pipeline. */
export interface RawBarRow extends VendorBar {
  instrumentId: string;
  runId: string;
  schemaVersion: string; // RAW_SCHEMA_VERSION
  metricVersion: string;
  universeVersion: string;
}

export interface MetricRow extends Provenance {
  instrumentId: string;
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
  securitiesMastered: number;
  securitiesResolved: number;
  unresolvedTickers: number;
  missingReferenceData: number;
  aliasRows: number;
  runtimeSec: number;
  metricVersion: string;
  schemaVersion: string;
  status: "SUCCESS" | "PARTIAL" | "FAILURE" | "SKIPPED";
  note?: string; // reason for SKIPPED/FAILURE early-exit (e.g. "calendar_year_missing")
}
