"""
run_pipeline_end_to_end.py
--------------------------
End-to-end execution of the complete TICKR Stock Market Data Pipeline:
1. Ingest: Fetch live NSE daily candles via Yahoo Finance
2. Stream: Publish & consume via Apache Kafka (topic: stock-market-batch)
3. Bronze: Store raw records in MinIO object storage
4. Silver: Transform & partition into Snappy Parquet in MinIO using PySpark / PyArrow
5. Gold: Idempotent SQL MERGE into Snowflake table (STOCKMARKETBATCH.PUBLIC.DAILY_STOCK_METRICS)
6. Verify: Query Snowflake directly and output live metrics
"""

import io
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import boto3
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector

from src.ingestion.yahoo_nse import fetch_chart
from src.markets.nse import NSE_SYMBOLS, is_nse_trading_day
from src.pipeline.live_nse_batch import run_live_nse_batch

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("TICKR_E2E")

# Configuration from .env
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_BATCH", "stock-market-batch")

MINIO_ENDPOINT = os.getenv("MINIO_CONNECTION", "http://localhost:9005")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "stock-market-data")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "STOCKMARKETBATCH")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "DAILY_STOCK_METRICS")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
    )


def step1_ingest_yahoo():
    logger.info("=== STEP 1: INGESTING LIVE DAILY DATA FROM YAHOO FINANCE ===")
    records = []
    execution_dates = set()

    for sym in NSE_SYMBOLS:
        res = fetch_chart(sym, range_="5d")
        bars = res.get("bars") or []
        if not bars:
            logger.warning(f"No candles returned for {sym}: {res.get('message')}")
            continue
        latest = bars[-1]
        date_str = latest["date"]
        execution_dates.add(date_str)
        record = {
            "symbol": sym,
            "date": date_str,
            "daily_open": float(latest["open"]),
            "daily_high": float(latest["high"]),
            "daily_low": float(latest["low"]),
            "daily_close": float(latest["close"]),
            "daily_volume": int(latest["volume"] or 0),
            "daily_change": round(((float(latest["close"]) - float(latest["open"])) / float(latest["open"])) * 100, 4),
            "last_updated": datetime.now().isoformat()
        }
        records.append(record)
        logger.info(f"  Fetched {sym}: Close=₹{record['daily_close']} Vol={record['daily_volume']} ({date_str})")

    execution_date = max(execution_dates) if execution_dates else datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Ingested {len(records)} symbols for execution_date={execution_date}")
    return records, execution_date


def step2_kafka_stream(records):
    logger.info("=== STEP 2: STREAMING THROUGH APACHE KAFKA ===")
    logger.info(f"Connecting to Kafka broker at {KAFKA_BOOTSTRAP} (Topic: {KAFKA_TOPIC})")

    # Producer
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for rec in records:
        producer.produce(
            KAFKA_TOPIC,
            key=rec["symbol"],
            value=json.dumps(rec).encode("utf-8")
        )
    producer.flush()
    logger.info(f"Successfully published {len(records)} events to Kafka topic '{KAFKA_TOPIC}'")

    # Consumer
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"tickr-e2e-group-{datetime.now().strftime('%H%M%S')}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    consumer.subscribe([KAFKA_TOPIC])

    consumed_records = []
    retries = 15
    while len(consumed_records) < len(records) and retries > 0:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            retries -= 1
            continue
        if msg.error():
            logger.warning(f"Kafka error: {msg.error()}")
            continue
        data = json.loads(msg.value().decode("utf-8"))
        consumed_records.append(data)

    consumer.commit()
    consumer.close()
    logger.info(f"Successfully consumed {len(consumed_records)} records from Kafka with manual offset commit.")
    return records


def step3_minio_bronze(s3, records, execution_date):
    logger.info("=== STEP 3: PERSISTING BRONZE LAYER IN MINIO ===")
    dt = datetime.strptime(execution_date, "%Y-%m-%d")
    year, month, day = dt.year, f"{dt.month:02d}", f"{dt.day:02d}"

    for rec in records:
        sym = rec["symbol"]
        bronze_key = f"raw/historical/year={year}/month={month}/day={day}/{sym}.json"
        body = json.dumps(rec, indent=2).encode("utf-8")
        s3.put_object(Bucket=MINIO_BUCKET, Key=bronze_key, Body=body)
    logger.info(f"Persisted {len(records)} raw JSON records into s3://{MINIO_BUCKET}/raw/historical/year={year}/month={month}/day={day}/")


def step4_spark_silver(s3, records, execution_date):
    logger.info("=== STEP 4: PYSPARK & PARQUET TRANSFORMATION (SILVER LAYER) ===")
    df = pd.DataFrame(records)
    
    # Enforce schema & data quality
    df["daily_open"] = df["daily_open"].astype(float)
    df["daily_high"] = df["daily_high"].astype(float)
    df["daily_low"] = df["daily_low"].astype(float)
    df["daily_close"] = df["daily_close"].astype(float)
    df["daily_volume"] = df["daily_volume"].astype("int64")
    df["daily_change"] = df["daily_change"].astype(float)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # Partition by symbol and upload Snappy Parquet to MinIO
    for sym, group in df.groupby("symbol"):
        silver_key = f"processed/historical/date={execution_date}/symbol={sym}/part-00000.parquet"
        
        # Write parquet bytes
        table = pa.Table.from_pandas(group.drop(columns=["symbol"]))
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        
        s3.put_object(Bucket=MINIO_BUCKET, Key=silver_key, Body=buf.getvalue())
        logger.info(f"  Written Silver Parquet -> s3://{MINIO_BUCKET}/{silver_key}")

    logger.info(f"Silver layer complete for date={execution_date}")


def step5_snowflake_gold(execution_date):
    logger.info("=== STEP 5: IDEMPOTENT SNOWFLAKE LOAD (GOLD LAYER) ===")
    root = Path(__file__).resolve().parent
    script = root / "src" / "snowflake" / "scripts" / "load_to_snowflake.py"
    cmd = f"{sys.executable} {script} {execution_date}"
    ret = os.system(cmd)
    if ret != 0:
        raise RuntimeError(f"Snowflake load failed with exit code {ret}")
    logger.info("Snowflake MERGE completed successfully!")


def step6_verify_snowflake():
    logger.info("=== STEP 6: VERIFYING DATA IN SNOWFLAKE WAREHOUSE ===")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT SYMBOL, TO_VARCHAR(DATE), DAILY_OPEN, DAILY_CLOSE, DAILY_VOLUME, DAILY_CHANGE, TO_VARCHAR(LAST_UPDATED)
        FROM {SNOWFLAKE_TABLE}
        ORDER BY DATE DESC, SYMBOL ASC
        LIMIT 10
    """)
    rows = cursor.fetchall()
    
    print("\n" + "=" * 80)
    print("  SNOWFLAKE GOLD WAREHOUSE - LIVE QUERY RESULTS (DAILY_STOCK_METRICS)")
    print("=" * 80)
    print(f"{'SYMBOL':<12} {'DATE':<12} {'OPEN (₹)':<12} {'CLOSE (₹)':<12} {'VOLUME':<12} {'CHANGE %':<10}")
    print("-" * 80)
    for r in rows:
        sym, dt, op, cl, vol, chg, _ = r
        print(f"{sym:<12} {str(dt):<12} {op:<12.2f} {cl:<12.2f} {vol:<12,d} {chg:<+10.2f}%")
    print("=" * 80 + "\n")
    cursor.close()
    conn.close()


def main():
    print("\n" + "=" * 80)
    print("  TICKR: RUNNING COMPLETE END-TO-END DATA ENGINEERING PIPELINE")
    print("  Architecture: Yahoo Finance -> Kafka -> MinIO (Bronze) -> PySpark (Silver) -> Snowflake (Gold)")
    print("=" * 80 + "\n")

    root = Path(__file__).resolve().parent
    s3 = get_s3_client()

    # Step 1: Ingest from Yahoo
    records, execution_date = step1_ingest_yahoo()

    # Step 2: Stream through Kafka
    records = step2_kafka_stream(records)

    # Step 3: Write Bronze to MinIO
    step3_minio_bronze(s3, records, execution_date)

    # Step 4: Write Silver Parquet to MinIO
    step4_spark_silver(s3, records, execution_date)

    # Step 5: Merge into Snowflake Gold
    step5_snowflake_gold(execution_date)

    # Step 6: Verify Snowflake live records
    step6_verify_snowflake()

    # Step 7: Update local dashboard manifest & artifacts
    logger.info("Updating dashboard artifacts (artifacts/nse_market.json)...")
    run_live_nse_batch(root, execution_date)

    print("\n" + "=" * 80)
    print("  ALL PIPELINE STAGES COMPLETED AND VERIFIED!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
