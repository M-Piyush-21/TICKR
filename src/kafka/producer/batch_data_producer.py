import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv


def _bootstrap() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "src" / "contracts" / "data_contract.py").exists():
            entry = str(parent)
            if entry not in sys.path:
                sys.path.insert(0, entry)
            return parent
    raise RuntimeError("Could not locate the TICKR project root.")


PROJECT_ROOT = _bootstrap()

from src.contracts.data_contract import (  # noqa: E402
    API_CALL_DELAY_SECONDS,
    US_BATCH_SYMBOLS,
    is_nyse_trading_day,
)
from src.data_quality.completeness import evaluate_completeness  # noqa: E402
from src.ingestion.alphavantage import parse_daily_payload  # noqa: E402
from src.ingestion.quota import QuotaExceeded, QuotaLedger  # noqa: E402

# Load Env Variables
load_dotenv()

# Configure Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_BATCH = os.getenv('KAFKA_TOPIC_BATCH')

# AlphaVantage Config
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# Free-tier budget lives in src/contracts/data_contract.py.
# This producer spends at most one TIME_SERIES_DAILY call per symbol per day,
# and it will not retry a symbol that already succeeded or a quota error.
# US listings only. NSE symbols are fetched by src/pipeline/live_nse_batch.py.
STOCKS = list(US_BATCH_SYMBOLS)
LEDGER_PATH = PROJECT_ROOT / "data" / "quota" / "alphavantage_ledger.json"


class HistoricalDataCollector:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC_BATCH):
        self.logger = logger
        self.topic = topic

        try:
            self.producer = Producer({
                "bootstrap.servers": bootstrap_servers,
                "client.id": "historical-data-collector-0",
            })
            self.logger.info(f"Producer initialized. Sending to: {bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka Producer: {e}")
            raise

    def fetch_historical_data(self, symbol: str, execution_date: str) -> dict:
        """
        Fetch the TIME_SERIES_DAILY candle for execution_date.

        The response is one daily candle, not a tick. A missing session is
        reported as missing_bar. Another date is never written in its place.
        """
        self.logger.info(f"Fetching daily candle for {symbol} (execution_date={execution_date})")
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",
            "datatype": "json",
            "apikey": ALPHA_VANTAGE_API_KEY,
        }
        try:
            response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            parsed = parse_daily_payload(response.json(), symbol, execution_date)
        except requests.exceptions.RequestException as exc:
            self.logger.error(f"HTTP error fetching data for {symbol}: {exc}")
            return {
                "symbol": symbol,
                "status": "transport_error",
                "message": str(exc),
                "bars": [],
                "retryable": True,
            }
        if parsed["status"] != "ok":
            self.logger.error(f"{symbol}: {parsed['status']} — {parsed['message']}")
        else:
            self.logger.info(f"Fetched 1 daily candle for {symbol} on {execution_date}")
        return parsed

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed for message: {msg.key()}: {err}")
        else:
            self.logger.info(
                f"Message delivered to topic '{msg.topic()}' "
                f"partition [{msg.partition()}] offset {msg.offset()}"
            )

    def produce_to_kafka(self, bar: dict, symbol: str, execution_date: str):
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        record = dict(bar)
        record["batch_id"] = batch_id
        record["batch_date"] = execution_date
        records = [record]
        successful_records = 0
        failed_records = 0

        for record in records:
            try:
                data = json.dumps(record, default=str)
                self.producer.produce(
                    topic=self.topic,
                    key=symbol,
                    value=data,
                    callback=self.delivery_report,
                )
                self.producer.poll(0)
                successful_records += 1
            except Exception as e:
                self.logger.error(f"Failed to produce message for {symbol}: {e}")
                failed_records += 1

        self.producer.flush()
        self.logger.info(
            f"Produced for {symbol}: {successful_records} OK, {failed_records} FAILED"
        )

    def collect_historical_data(self, execution_date: str) -> dict:
        """
        Request one daily candle per symbol.

        Symbols already recorded as successful today are not requested again,
        so an Airflow retry does not spend a second call on them. A quota
        payload stops the loop. The per-minute gap is applied only after a
        request that was actually sent.
        """
        if not is_nyse_trading_day(execution_date):
            self.logger.info(
                f"{execution_date} is not an expected trading session. "
                "No Alpha Vantage calls will be made."
            )
            return {
                "exit_code": 0,
                "results": [],
                "completeness": {
                    "status": "SKIPPED_NON_TRADING_DAY",
                    "passed": True,
                    "reason": (
                        f"{execution_date} is not an NYSE session. "
                        "No Alpha Vantage call was made."
                    ),
                },
            }

        ledger = QuotaLedger(LEDGER_PATH)
        results = []
        self.logger.info(
            f"Starting daily-candle collection for {len(STOCKS)} symbols | "
            f"execution_date={execution_date}"
        )

        for index, symbol in enumerate(STOCKS):
            if ledger.succeeded("TIME_SERIES_DAILY", symbol):
                self.logger.info(f"{symbol} already succeeded today. Skipping the API call.")
                results.append({
                    "symbol": symbol,
                    "status": "already_succeeded",
                    "message": "Ledger has a successful TIME_SERIES_DAILY call for today.",
                    "bars": [],
                })
                continue

            try:
                ledger.authorize("TIME_SERIES_DAILY", symbol)
            except QuotaExceeded as exc:
                self.logger.error(str(exc))
                results.append({
                    "symbol": symbol,
                    "status": "quota_exhausted",
                    "message": str(exc),
                    "bars": [],
                })
                break

            parsed = self.fetch_historical_data(symbol, execution_date)
            outcome = "success" if parsed["status"] == "ok" else parsed["status"]
            ledger.record("TIME_SERIES_DAILY", symbol, outcome)
            results.append(parsed)

            if parsed["status"] == "ok":
                self.produce_to_kafka(parsed["bars"][0], symbol, execution_date)
            elif parsed["status"] == "quota_exhausted":
                self.logger.error("Quota response received. Stopping before further API calls.")
                break

            calls_remaining = index < len(STOCKS) - 1 and parsed["status"] != "quota_exhausted"
            if calls_remaining:
                self.logger.info(
                    f"Rate-limit pause: waiting {API_CALL_DELAY_SECONDS}s before next API call."
                )
                time.sleep(API_CALL_DELAY_SECONDS)

        published = []
        for result in results:
            if result.get("status") == "ok" and result.get("bars"):
                published.append(result["bars"][0])
            elif result.get("status") == "already_succeeded":
                # The candle was published on the earlier successful call.
                published.append({
                    "symbol": result["symbol"],
                    "date": execution_date,
                    "grain": "daily_candle",
                })
        completeness = evaluate_completeness(
            published, execution_date, source_results=results, expected_symbols=STOCKS
        )
        self.logger.info(f"Completeness: {completeness['status']} — {completeness['reason']}")
        exit_code = 0 if completeness["passed"] else 1
        return {"exit_code": exit_code, "results": results, "completeness": completeness}


def main():
    # The Airflow DAG passes {{ ds }} (YYYY-MM-DD) as the first argument
    if len(sys.argv) > 1:
        execution_date = sys.argv[1]
        try:
            datetime.strptime(execution_date, "%Y-%m-%d")  # validate format
        except ValueError:
            logger.error(f"Invalid date format '{execution_date}'. Expected YYYY-MM-DD.")
            sys.exit(1)
    else:
        execution_date = datetime.now().strftime("%Y-%m-%d")
        logger.warning(f"No execution date provided — defaulting to today: {execution_date}")

    logger.info(f"Starting Historical Stock Data Collector | execution_date={execution_date}")

    if not is_nyse_trading_day(execution_date):
        logger.info(
            f"{execution_date} is not an expected trading session. "
            "Exiting before any API or Kafka connection."
        )
        sys.exit(0)

    if not ALPHA_VANTAGE_API_KEY:
        logger.error("ALPHA_VANTAGE_API_KEY is not set in environment variables!")
        sys.exit(1)

    try:
        collector = HistoricalDataCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC_BATCH,
        )
        summary = collector.collect_historical_data(execution_date=execution_date)
        sys.exit(summary["exit_code"])
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
