# TICKR data contract

The consumer of this pipeline is an analyst or a dashboard that needs a complete, fresh, deduplicated daily metric for each symbol. Reliable rows reduce manual reconciliation. This repository does not measure financial savings, trading returns, or investment performance.

## Paths

| Path | Source | Grain | Canonical key | What a record is |
|---|---|---|---|---|
| A. Daily batch | Yahoo Finance chart for `SYMBOL.NS` by default. Alpha Vantage `TIME_SERIES_DAILY` remains the optional US compose client. | One OHLCV candle per symbol and trading date | `(symbol, date)` | A daily bar. Not a trade tick and not an intraday bar. |
| B. Quote snapshot | Alpha Vantage `GLOBAL_QUOTE` | One snapshot per poll | `(symbol, polled_at)` | The latest quote at poll time. Not a continuous exchange feed. |

Path B is not folded into `DAILY_STOCK_METRICS`. A quote snapshot does not become a daily candle.

## Layers on path A

| Layer | Where it lands in the local demo | Key | Rule |
|---|---|---|---|
| Raw response | Alpha Vantage JSON body | Request, not a warehouse key | HTTP 200 with `Information`, `Note`, or `Error Message` is a failure. An empty body is a failure. |
| Bronze | `data/lake/raw/historical/year=YYYY/month=MM/day=DD/{symbol}.json` | File per symbol and batch date | The local demo overwrites that file. A Kafka replay can still deliver the same candle again. |
| Silver | `data/lake/processed/historical/date=YYYY-MM-DD/symbol={symbol}/part-00000.json` | `(symbol, date)` | Exact replays collapse to one row. Conflicting values are quarantined. Volume is not summed. |
| Gold | `data/warehouse/DAILY_STOCK_METRICS.json` locally. Snowflake `DAILY_STOCK_METRICS` only with cloud credentials. | `(symbol, date)` | The stage must contain one row per key before MERGE. A second load updates that row. |

`daily_change` is the percent move from open to close, `(close - open) / open * 100`. It is not a dollar change.

`ma_15` and `ma_30` are simple averages of `daily_close` ordered by trading date. They stay null until 15 or 30 sessions exist. The Spark job reads one execution date, so it does not compute those averages. The local runner does, from the fixture history.

## OHLCV

- One daily candle: keep open, high, low, close, and volume. Do not run a tick aggregation over it.
- Several copies of that same candle: keep one. Do not add the volumes together.
- Two different candles for the same `(symbol, date)`: quarantine the key and fail the load.
- Several interval bars that do not overlap: sort by `event_time`, then open is the first open, close is the last close, high is the max, low is the min, and volume is the sum.

## Sessions

The project calendar is NSE cash equities. A row is expected only on a weekday that is not a full 2026 NSE closure. That list is in `src/markets/nse.py`, taken from the NSE equities holiday page and circular CMTR71775. `2026-01-15` is on the exchange page and is included even though the circular excerpt does not list it. `2026-11-08` is Muhurat and a Sunday, so it is not a regular session.

The optional Alpha Vantage producer still uses the NYSE list in `src/contracts/data_contract.py`. The Compose DAG gate is `--market nyse` so it matches that producer.

If the requested NSE session is missing, the live runner records `missing_bar` and does not write a different day's candle under that date. A weekend or a listed NSE holiday is a skip: the command exits 0 and does not call the source.

## Quota

Alpha Vantage's free tier, as used by this project, is 5 requests per minute and 25 requests per day. The planned day is:

| Use | Requests |
|---|---|
| Daily batch, 10 symbols, one `TIME_SERIES_DAILY` each | 10 |
| Quote snapshots, 5 symbols, one cycle | 5 |
| Retry reserve for calls that were sent and did not succeed | 5 |
| Left unused | 5 |
| Planned total | 20 |

Calls are spaced 12 seconds apart. The ledger in `data/quota/alphavantage_ledger.json` counts every request that was sent. A symbol that already succeeded today is not requested again. A quota payload stops the loop. The quote producer runs one cycle and exits.

## Delivery

Manual commit after a successful sink write is at-least-once. If the write succeeds and the offset commit fails, a replay can happen. Deduplication on `(symbol, date)` is what keeps a single gold row. That is not exactly-once delivery, and it is not a claim of zero data loss.

## Runtime labels

| Stage | Label |
|---|---|
| `python3 run_pipeline_local.py` | Live NSE daily candles from the Yahoo chart endpoint when it responds. JSON gold file. |
| `python3 run_pipeline_local.py --fixture` | Verified local. Fixture data. Not market prices. |
| Alpha Vantage producers | Code only, until an API key and Kafka are available. |
| Kafka and MinIO in `docker-compose.yaml` | Configured. Not started by the local demo. |
| `src/spark/jobs/spark_batch_processor.py` | Code only, until Spark is running. |
| `src/snowflake/scripts/load_to_snowflake.py` | Cloud dependent. |
| `src/airflow/dags/stock_market_batch_dag.py` | Code only, until Compose is running. |
| Browser charts and screener | `artifacts/nse_market.json` when present. Otherwise a labeled fallback. |
| Data Quality KPI strip | Reads `artifacts/latest_run_manifest.json`. |
| Alert engine | Implemented. Rule-based. Not a model and not advice. |

## Run manifest

Each local execution writes `artifacts/latest_run_manifest.json` and a copy under `data/warehouse/runs/`. The record includes the run id, session, symbols requested and received, row counts, rejected rows, duration, status, quota note, and target path.
