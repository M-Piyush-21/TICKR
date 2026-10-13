# TICKR

Stock-market data engineering prototype for NSE cash equities. The path you can run without Kafka, Spark, or Snowflake fetches daily candles, checks completeness, and writes an idempotent gold file plus a run manifest. `--fixture` keeps an offline candle generator for tests.

TICKR is not a trading system and not a machine-learning project. The alert rules are deterministic thresholds.

## Live NSE path

Prerequisites: Python 3.10 or newer, and outbound HTTPS. No API key. The source is the Yahoo Finance chart endpoint for `SYMBOL.NS`. It is an unofficial daily-candle URL, not an exchange feed and not a trade tick.

```bash
python3 run_pipeline_local.py
python3 run_pipeline_local.py --execution-date 2026-10-13
python3 -m unittest discover tests -v
```

With no date, the session is the latest date present for every symbol. A date you pass is not replaced when that session is missing. A weekend or a listed 2026 NSE holiday exits 0 and does not call the source. Dussehra `2026-10-20` is one of those skips.

The run writes:

1. Daily candles for RELIANCE, TCS, HDFCBANK, INFY, ICICIBANK, SBIN, BHARTIARTL, ITC, LT, and HINDUNILVR.
2. Bronze and silver JSON under `data/lake/`.
3. A MERGE into `data/warehouse/DAILY_STOCK_METRICS.json` on `(symbol, date)`.
4. `artifacts/latest_run_manifest.json` and `artifacts/nse_market.json` for the dashboard.

`--fixture` keeps the old offline candles. Those are not market prices. The unit tests use that path and a saved chart payload, so they do not need the network.

To use your own market API instead of Yahoo, set `TICKR_DATA_PROVIDER` to something other than `yahoo` and provide `TICKR_MARKET_API_BASE`, `TICKR_MARKET_API_KEY`, and the symbol format. That name has no client yet, so the runner exits before calling anything.

Open the dashboard from the repo root:

```bash
python3 -m http.server 3000
```

Then open `http://localhost:3000`. The chart reads `artifacts/nse_market.json` and shows rupees. If that file is missing, the page uses a labeled browser fallback. The Data Quality tab reads the manifest. The DAG button animates a walkthrough; it does not start Airflow.

`data/` is gitignored. The manifest under `artifacts/` is the reviewed output of the command above.

## What is actually running

| Stage | Status | Where |
|---|---|---|
| NSE daily batch via Yahoo chart, completeness gate, JSON gold MERGE, run manifest | Live when the chart endpoint responds | `run_pipeline_local.py` |
| Offline fixture batch | Verified local, not market prices | `run_pipeline_local.py --fixture` |
| Row checks, completeness, alert rules, candle rules | Implemented and unit-tested | `src/data_quality/`, `src/transformations/` |
| Browser screener and charts | Reads `artifacts/nse_market.json`, otherwise a labeled fallback | `js/app.js` |
| Alpha Vantage daily and quote clients, request ledger | Code only. Needs `ALPHA_VANTAGE_API_KEY` and Kafka | `src/kafka/producer/` |
| Kafka, MinIO, Spark, Airflow, Postgres | Configured in Compose. Not started by the demo command | `docker-compose.yaml` |
| Spark daily-candle job | Code only. Needs the Spark service | `src/spark/jobs/spark_batch_processor.py` |
| Snowflake MERGE | Cloud dependent | `src/snowflake/scripts/load_to_snowflake.py` |
| Airflow DAG | Code only. Needs Compose | `src/airflow/dags/stock_market_batch_dag.py` |

The contract for grain, keys, sessions, and quota is in [docs/DATA_CONTRACT.md](docs/DATA_CONTRACT.md).

## Two paths

```
Path A  daily batch                         status
------------------------------------------  ---------------------------
TIME_SERIES_DAILY  (one candle / symbol)    code only, needs API + Kafka
        |  12s between calls, 10 calls/run
Kafka topic stock-market-batch              configured in Compose
        |  commit only after the sink write
MinIO bronze  raw/historical/...            configured in Compose
        |  exact replays collapse; conflicts fail
Spark or the local runner                   Spark: code only
        |                                    local runner: verified fixture
Gold  (symbol, date)                        local JSON: verified
                                            Snowflake: cloud dependent

Path B  quote snapshot                      status
------------------------------------------  ---------------------------
GLOBAL_QUOTE  5 symbols, 1 cycle / day      code only
        |
Kafka topic stock-market-realtime           configured in Compose
        |
Key (symbol, polled_at)                     not loaded into the daily table
```

Path B is not a tick feed. A quote is a snapshot at poll time.

## Quota

Free-tier limits used here: 5 requests per minute and 25 per day.

| Planned use | Requests |
|---|---|
| Batch: 10 symbols × one `TIME_SERIES_DAILY` | 10 |
| Quotes: 5 symbols × one cycle | 5 |
| Retry reserve | 5 |
| Unused headroom | 5 |

The ledger is `data/quota/alphavantage_ledger.json`. A retry skips symbols that already succeeded and stops when the budget or a quota payload says to stop. That producer still uses the US symbol list and the NYSE calendar. The NSE live command does not spend this budget. A missing session is not replaced with the previous candle.

## Delivery and reruns

The consumer commits the Kafka offset after the bronze write. That is at-least-once. A crash before the write leaves the offset uncommitted. A crash after the write and before the commit can replay the same candle. The silver step and the gold MERGE keep one row per `(symbol, date)`. Identical replays do not sum volume. Conflicting values for that key fail the load instead of being guessed.

Unit tests cover a failed write, a failed commit followed by replay, a repeated local run, a missing symbol, an empty or error-shaped API body, and a holiday skip. They do not start Kafka or Snowflake. A Snowflake run still needs credentials and is not claimed as verified here.

## OHLCV

`TIME_SERIES_DAILY` already returns the day's open, high, low, close, and volume. The batch job keeps that bar. It orders nothing inside the bar because there is nothing inside it.

Interval bars are a different input, used only when several timestamps exist. They are sorted by `event_time` before first open and last close are taken. Volume is summed only in that case. `F.first` / `F.last` without an order column is not used.

`daily_change` is the percent change from open to close. Moving averages are trailing means of daily closes. Fewer than 15 or 30 closes leaves the average null.

## Data quality

Row checks in `src/data_quality/validator.py`: symbol, ISO date, unique `(symbol, date)`, positive close, non-negative volume, high ≥ low, high covering open and close, and rejection of Alpha Vantage error documents.

The completeness gate is separate. It fails the run when an expected symbol or session is missing, when the body is empty, or when the body is a quota or error payload. An empty batch is `FAILED_EMPTY`, not a clean success.

Each local run records run id, session, symbols requested and received, counts, rejected rows, duration, status, and target in the manifest. The dashboard KPI strip shows that file. It does not invent freshness.

## Alerts

`src/data_quality/alerts.py` flags three configurable rules: absolute daily percent move, volume versus a trailing baseline, and distance from MA15. They are heuristics for monitoring. They are not a forecast, not a trading signal, and not investment advice.

## Compose path

This is not part of the verified demo. It needs Docker and the variables in `.env.example`.

```bash
cp .env.example .env
docker-compose up -d
```

- Airflow: `http://localhost:8081` (`airflow` / `airflow`)
- MinIO console: `http://localhost:9001`
- Spark master UI: `http://localhost:8080`

The DAG is `stock_market_batch_pipeline`. It runs `fetch >> consume >> process >> load >> complete`. `catchup` is off. Non-trading sessions skip the tasks. Spark fails the task when the raw prefix is missing or when `(symbol, date)` values conflict. The Snowflake task fails when there is nothing unique to load, so a trading day with no files is not reported as a successful sync.

Snowflake still needs `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and `SNOWFLAKE_PASSWORD`.

## Tests

```bash
python3 -m unittest discover tests -v
```

The suite covers quality rules, the screener, alert metadata, daily-versus-interval OHLCV, moving averages, API error bodies, the quota ledger, completeness, MERGE key conflicts, commit/write replay, and the local batch smoke path. It does not prove a live Kafka, Spark, or Snowflake run.

## How to describe the project

TICKR is a stock-market data engineering prototype with a batch path and a separate quote-snapshot path. Kafka decouples ingestion from storage. MinIO holds raw and processed files when Compose is up. Spark canonicalizes daily candles. The warehouse load is an idempotent MERGE on `(symbol, date)`. The work in this repo is about data quality and replay safety, and about saying which path was actually run.

Analysts need complete, fresh, consistent inputs. The latency and quality bar here is a daily candle for a 10-symbol list, not a tick stream. The local demo proves the contract on a fixture. Live API, Spark, and Snowflake runs are separate and require their own credentials.

## Layout

```
run_pipeline_local.py                 verified local entrypoint
src/contracts/data_contract.py        symbols, quota, holidays, grain
src/ingestion/                        payload parser and request ledger
src/transformations/ohlcv.py          daily collapse and interval aggregation
src/data_quality/                     row checks, completeness, alert rules
src/warehouse/idempotent_load.py      local gold MERGE
src/reliability/replay.py             at-least-once simulation
src/pipeline/local_batch.py           fixture batch used by the demo
src/kafka/                            Alpha Vantage producers and consumers
src/spark/jobs/                       Spark batch and stream jobs
src/snowflake/scripts/                cloud load
src/airflow/dags/                     daily DAG
artifacts/latest_run_manifest.json    last local run
```
