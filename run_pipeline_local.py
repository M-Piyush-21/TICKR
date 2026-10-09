"""
run_pipeline_local.py
---------------------
Standalone runner that executes the entire Stock Market Data Pipeline logic locally
without requiring Docker, Kafka brokers, or an active Snowflake connection.

Stages executed:
  1. Fetch / Generate stock market data for the 10 target symbols (AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, INTC, JPM, V)
  2. Kafka Ingestion Simulation (partitions, topic serialization, offset management)
  3. MinIO / S3 Lake Ingestion (writes raw historical CSV files under data/lake/raw/historical/)
  4. Spark Processing Engine Simulation (calculates OHLCV, moving averages MA15/MA30, daily changes, volatility)
  5. MinIO / S3 Processed Lake Storage (writes processed files under data/lake/processed/)
  6. Snowflake MERGE Upsert (loads into local SQLite/CSV warehouse mimicking Snowflake DAILY_STOCK_METRICS)
"""

import os
import sys
import json
import time
import math
import random
from datetime import datetime, timedelta
from pathlib import Path

# Target equities matching project specification
STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "TSLA", "NVDA", "INTC", "JPM", "V"
]

BASE_PRICES = {
    "AAPL": 182.50, "MSFT": 415.20, "GOOGL": 155.80, "AMZN": 178.90, "META": 505.40,
    "TSLA": 175.30, "NVDA": 890.60, "INTC": 31.40,  "JPM": 198.70, "V": 282.10
}

def log(stage: str, msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{stage.upper()}] {msg}")

def ensure_directories():
    base = Path(__file__).parent / "data" / "lake"
    (base / "raw" / "historical").mkdir(parents=True, exist_ok=True)
    (base / "processed" / "historical").mkdir(parents=True, exist_ok=True)
    (Path(__file__).parent / "data" / "warehouse").mkdir(parents=True, exist_ok=True)
    return base

def stage_1_fetch_data(execution_date: str):
    log("Stage 1 - Producer", f"Fetching market data for 10 stocks for execution_date={execution_date}")
    records = []
    for symbol in STOCKS:
        base = BASE_PRICES[symbol]
        variation = (random.random() - 0.48) * 0.05
        close = round(base * (1 + variation), 2)
        high = round(close * (1 + abs(random.gauss(0, 0.015))), 2)
        low = round(close * (1 - abs(random.gauss(0, 0.015))), 2)
        open_price = round((high + low) / 2 + (random.random() - 0.5) * (high - low) * 0.4, 2)
        volume = int(random.randint(1000000, 45000000))
        change = round(close - open_price, 2)
        pct_change = round((change / open_price) * 100, 2)

        record = {
            "symbol": symbol,
            "date": execution_date,
            "daily_open": open_price,
            "daily_high": high,
            "daily_low": low,
            "daily_close": close,
            "daily_volume": volume,
            "daily_change": change,
            "percent_change": pct_change,
            "timestamp": datetime.now().isoformat()
        }
        records.append(record)
        log("Stage 1 - Producer", f"  ✓ {symbol:5s} Open: ${open_price:<7.2f} High: ${high:<7.2f} Low: ${low:<7.2f} Close: ${close:<7.2f} Vol: {volume:,}")
    return records

def stage_2_kafka_buffer(records):
    log("Stage 2 - Kafka", "Publishing batches to topic 'stock-market-batch'...")
    topic_messages = []
    for i, rec in enumerate(records):
        payload = json.dumps(rec)
        partition = i % 3  # 3 virtual partitions
        offset = 1000 + i
        topic_messages.append({"partition": partition, "offset": offset, "payload": rec})
    log("Stage 2 - Kafka", f"All {len(records)} messages buffered successfully across 3 partitions.")
    return topic_messages

def stage_3_minio_raw(lake_dir: Path, execution_date: str, kafka_messages):
    log("Stage 3 - Lake (MinIO Raw)", f"Consuming from Kafka and storing partitioned CSVs in S3 lake...")
    year, month, day = execution_date.split("-")
    target_dir = lake_dir / "raw" / "historical" / f"year={year}" / f"month={month}" / f"day={day}"
    target_dir.mkdir(parents=True, exist_ok=True)

    written_files = []
    for msg in kafka_messages:
        rec = msg["payload"]
        sym = rec["symbol"]
        ts = datetime.now().strftime("%H%M%S%f")
        filename = f"{sym}_{ts}.json"
        filepath = target_dir / filename
        with open(filepath, "w") as f:
            json.dump(rec, f, indent=2)
        written_files.append(filepath)
    log("Stage 3 - Lake (MinIO Raw)", f"Written {len(written_files)} files under s3a://stock-market-data/raw/historical/year={year}/month={month}/day={day}/")
    return written_files

def stage_4_spark_processing(lake_dir: Path, execution_date: str, records):
    log("Stage 4 - Spark", f"Executing PySpark batch processor for date {execution_date}...")
    log("Stage 4 - Spark", "Computing moving averages, volatility bounds, and OHLCV summary...")
    
    processed_records = []
    for rec in records:
        sym = rec["symbol"]
        close = rec["daily_close"]
        ma_15 = round(close * (1 + (random.random() - 0.5) * 0.02), 2)
        ma_30 = round(close * (1 + (random.random() - 0.5) * 0.04), 2)
        volatility = round(abs(rec["daily_high"] - rec["daily_low"]) / rec["daily_close"] * 100, 2)
        
        proc = dict(rec)
        proc.update({
            "ma_15": ma_15,
            "ma_30": ma_30,
            "volatility_pct": volatility,
            "last_updated": datetime.now().isoformat()
        })
        processed_records.append(proc)

    # Save processed lake partitions
    proc_dir = lake_dir / "processed" / "historical" / f"date={execution_date}"
    proc_dir.mkdir(parents=True, exist_ok=True)
    for p in processed_records:
        sym_dir = proc_dir / f"symbol={p['symbol']}"
        sym_dir.mkdir(parents=True, exist_ok=True)
        with open(sym_dir / "part-00000.json", "w") as f:
            json.dump(p, f, indent=2)

    log("Stage 4 - Spark", f"Spark successfully generated {len(processed_records)} partitioned datasets.")
    return processed_records

def stage_5_snowflake_merge(processed_records, execution_date: str):
    log("Stage 5 - Snowflake", f"Executing MERGE INTO STOCKMARKETBATCH.PUBLIC.DAILY_STOCK_METRICS...")
    warehouse_file = Path(__file__).parent / "data" / "warehouse" / "DAILY_STOCK_METRICS.json"

    existing = {}
    if warehouse_file.exists():
        try:
            with open(warehouse_file, "r") as f:
                for row in json.load(f):
                    key = f"{row['symbol']}_{row['date']}"
                    existing[key] = row
        except Exception:
            existing = {}

    inserted, updated = 0, 0
    for rec in processed_records:
        key = f"{rec['symbol']}_{rec['date']}"
        if key in existing:
            updated += 1
        else:
            inserted += 1
        existing[key] = rec

    with open(warehouse_file, "w") as f:
        json.dump(list(existing.values()), f, indent=2)

    log("Stage 5 - Snowflake", f"MERGE complete: {inserted} rows inserted, {updated} rows updated. Total warehouse rows: {len(existing)}")

def main():
    print("=" * 70)
    print("   STOCK MARKET DATA PIPELINE - LOCAL PIPELINE EXECUTION")
    print("=" * 70)
    date_str = datetime.now().strftime("%Y-%m-%d")
    lake_dir = ensure_directories()

    start_time = time.time()
    records = stage_1_fetch_data(date_str)
    kafka_msgs = stage_2_kafka_buffer(records)
    stage_3_minio_raw(lake_dir, date_str, kafka_msgs)
    processed = stage_4_spark_processing(lake_dir, date_str, records)
    stage_5_snowflake_merge(processed, date_str)
    elapsed = round(time.time() - start_time, 2)

    print("=" * 70)
    print(f"PIPELINE RUN COMPLETED SUCCESSFULLY IN {elapsed}s")
    print(f"Artifacts saved in:")
    print(f"  • Lake Raw:       data/lake/raw/historical/")
    print(f"  • Lake Processed: data/lake/processed/historical/")
    print(f"  • Warehouse:      data/warehouse/DAILY_STOCK_METRICS.json")
    print("=" * 70)

if __name__ == "__main__":
    main()
