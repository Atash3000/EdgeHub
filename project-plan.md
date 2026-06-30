# EdgeHub

## Part 1 — Market Data Lake (v1.0)

Status: Design Approved

---

# Mission

EdgeHub is the foundation of the entire trading ecosystem.

It is **NOT** a trading bot.

It does **NOT** decide when to buy or sell.

Its only responsibility is to build a clean, reliable, research-grade market database that every future strategy can trust.

Examples of future consumers:

- MinerVeni
- Gerchik
- Clenow
- CANSLIM
- Future AI research
- Backtester
- Analytics
- Portfolio simulator

Think of EdgeHub as Bloomberg Terminal's backend.

Strategies simply consume its data.

---

# Guiding Principles

1. Source of truth is S3.

2. Every trading day produces one immutable snapshot.

3. Data must be reproducible.

4. Never overwrite historical data.

5. Raw data is preserved forever.

6. Computed features are versioned.

7. Keep Part 1 intentionally simple.

---

# Scope

Part 1 ONLY builds the Market Data Lake.

NOT included:

- trading
- broker APIs
- positions
- portfolio
- AI
- backtester
- strategies
- execution
- alerts except health notification

---

# Architecture

                EventBridge
                     │
                     ▼
         edgehub-daily-collector
                     │
                     ▼
           Download Market Data
                     │
                     ▼
             Compute Features
                     │
                     ▼
                  S3 Data Lake
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
      Raw Bars             Daily Features
                     │
                     ▼
             AWS Glue Catalog
                     │
                     ▼
                 Athena SQL
                     │
                     ▼
             Future Strategy Bots

---

# AWS Stack

Compute

- AWS Lambda

Scheduling

- EventBridge

Storage

- Amazon S3

Catalog

- AWS Glue

SQL

- Athena

Monitoring

- CloudWatch

Secrets

- AWS Secrets Manager

Notifications

- Telegram Bot

No DynamoDB in Part 1.

---

# Storage Format

File Format

Parquet

Compression

Snappy

Partitioning

year=
month=
day=

Example

s3://edgehub-data/features/year=2026/month=06/day=29/part-000.parquet

Reason

Parquet

- compressed
- columnar
- fast Athena scans
- inexpensive
- scalable

---

# Buckets

edgehub-data

Folders

raw/

features/

metadata/

reports/

errors/

---

# raw/

Stores vendor data exactly as received.

Never modified.

Purpose

Historical archive.

Example

raw/finnhub/daily/year=2026/month=06/day=29/

Contains

OHLCV only.

---

# features/

Stores computed indicators.

One row

per

ticker

per

day.

---

# metadata/

Stores

- schema version
- feature version
- ingestion logs

---

# reports/

Stores

Daily summaries.

---

# errors/

Stores

API failures

Missing symbols

Retry logs

---

# Daily Schedule

Run

Every trading day

6:30 PM

America/New_York

Reason

Daily candle complete.

Corporate actions mostly available.

No partial candles.

---

# Daily Pipeline

Step 1

Load ticker universe.

↓

Step 2

Download daily OHLCV.

↓

Step 3

Validate data.

↓

Step 4

Save raw bars.

↓

Step 5

Compute features.

↓

Step 6

Save feature dataset.

↓

Step 7

Update Glue partition.

↓

Step 8

Run data validation.

↓

Step 9

Telegram success report.

---

# Initial Universe

Phase 1

S&P500

-

Nasdaq100

-

Custom Watchlist

Later

Russell1000

Russell3000

All Liquid US Stocks

---

# Raw Daily Bars Schema

ticker

date

open

high

low

close

adjustedClose

volume

source

ingestedAt

---

# Daily Features Schema

Identity

ticker

date

Price

close

Dollar Volume

dollarVolume

Moving Averages

ma20

ma50

ma150

ma200

Average Volume

avgVolume20

avgVolume50

Volatility

atr14

52 Week

high52w

low52w

distanceTo52wHighPct

distanceFrom52wLowPct

Returns

return21d

return63d

return126d

return252d

Trend Flags

above20ma

above50ma

above150ma

above200ma

ma150Above200

ma200Rising

Data Quality

qualityStatus

featureVersion

---

# Data Quality Rules

Reject if

missing close

missing volume

duplicate ticker/date

negative prices

negative volume

Record every failure.

Never silently ignore.

---

# Versioning

Every dataset stores

featureVersion

schemaVersion

sourceVersion

This guarantees reproducibility.

---

# Glue Tables

daily_bars

daily_features

Only two tables.

Keep it simple.

---

# Athena

Support queries like

Top 100 stocks by 6-month return.

Stocks above 200 MA.

Highest ATR.

Near 52-week highs.

High volume expansion.

Nothing strategy-specific.

---

# Telegram Report

Every evening

Example

EdgeHub Daily Update

Date

2026-06-29

Universe

612 stocks

Downloaded

612

Failed

0

Rows Written

612

Feature Version

1.0

Runtime

4m 32s

Status

SUCCESS

---

# Success Criteria

Part 1 is complete when

✓ Daily Lambda runs automatically.

✓ Market data downloads correctly.

✓ Raw bars stored in S3.

✓ Features computed.

✓ Features stored in Parquet.

✓ Glue catalog updated.

✓ Athena queries work.

✓ Telegram report sent.

✓ Historical data never overwritten.

---

# Deliverables

Working Data Lake

Working Daily Collector

Working Feature Engine

Working Athena Queries

Working Health Reports

Nothing else.

---

# Out of Scope

Strategies

Broker APIs

Backtesting

Pattern Detection

Risk Engine

Portfolio

Execution

AI

Machine Learning

Paper Trading

Live Trading

These begin in Part 2.

---

# End State

After Part 1,

EdgeHub becomes the single source of truth for market data.

Every future trading bot will consume data from EdgeHub rather than downloading its own data.

This guarantees consistency, reproducibility, and allows multiple independent strategies to share the same research-grade foundation.
