import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict

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
    QUOTE_CYCLES_PER_DAY,
    QUOTE_SNAPSHOT_SYMBOLS,
)
from src.ingestion.alphavantage import parse_quote_payload  # noqa: E402
from src.ingestion.quota import QuotaExceeded, QuotaLedger  # noqa: E402

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_REALTIME')

# AlphaVantage Config
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# GLOBAL_QUOTE snapshots, not exchange ticks.
# The free-tier plan is one cycle of QUOTE_SNAPSHOT_SYMBOLS per UTC day.
# That leaves room for the 10-symbol daily batch and a retry reserve.
# See src/contracts/data_contract.py. A paid plan would be a config change
# there, not a tighter loop in this file.

STOCKS = list(QUOTE_SNAPSHOT_SYMBOLS)
LEDGER_PATH = PROJECT_ROOT / "data" / "quota" / "alphavantage_ledger.json"


class StreamDataCollector:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC):
        self.logger = logger
        self.topic = topic
        self.ledger = QuotaLedger(LEDGER_PATH)

        # Track previous prices to calculate change/percent_change
        self._prev_prices: Dict[str, Optional[float]] = {s: None for s in STOCKS}

        try:
            self.producer = Producer({
                'bootstrap.servers': bootstrap_servers,
                'client.id': 'alphavantage-stream-producer',
            })
            self.logger.info(f"Kafka producer ready → {bootstrap_servers}, topic={topic}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka Producer: {e}")
            raise

    # ------------------------------------------------------------------
    # AlphaVantage helpers
    # ------------------------------------------------------------------

    def fetch_quote(self, symbol: str) -> Optional[Dict]:
        """
        Call AlphaVantage GLOBAL_QUOTE to get the latest price for a symbol.
        Returns a normalised dict or None on failure.
        """
        try:
            params = {
                "function": "GLOBAL_QUOTE",
                "symbol": symbol,
                "apikey": ALPHA_VANTAGE_API_KEY,
            }
            response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            polled_at = datetime.now(timezone.utc).isoformat()
            parsed = parse_quote_payload(response.json(), symbol, polled_at)
            if parsed["status"] != "ok":
                self.logger.error(f"{symbol}: {parsed['status']} — {parsed['message']}")
                self._last_status = parsed["status"]
                return None

            quote_row = parsed["quote"]
            price = quote_row["price"]
            prev = self._prev_prices[symbol]
            av_change = quote_row["change"]
            av_change_pct = quote_row["percent_change"]
            self._last_status = "ok"

            # If we have a prior price from this session, override with live delta
            if prev is not None:
                session_change = round(price - prev, 4)
                session_change_pct = round((session_change / prev) * 100, 4) if prev else 0.0
            else:
                session_change = av_change
                session_change_pct = av_change_pct

            self._prev_prices[symbol] = price

            return {
                "grain": "quote_snapshot",
                "symbol": symbol,
                "price": price,
                "change": session_change,
                "percent_change": session_change_pct,
                "volume": quote_row["volume"],
                "latest_trading_day": quote_row["latest_trading_day"],
                "previous_close": quote_row["previous_close"],
                "polled_at": polled_at,
                "timestamp": polled_at,
            }

        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP error fetching quote for {symbol}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching quote for {symbol}: {e}")
            return None

    # ------------------------------------------------------------------
    # Kafka helpers
    # ------------------------------------------------------------------

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed [{msg.key()}]: {err}")
        else:
            self.logger.debug(
                f"Delivered → topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
            )

    def publish(self, stock_data: Dict):
        try:
            self.producer.produce(
                self.topic,
                key=stock_data["symbol"],
                value=json.dumps(stock_data),
                callback=self.delivery_report,
            )
            self.producer.poll(0)
        except Exception as e:
            self.logger.error(f"Failed to publish {stock_data['symbol']}: {e}")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def produce_stock_data(self):
        """
        Publish one GLOBAL_QUOTE snapshot per configured symbol, then exit.

        This is not a continuous tick feed. The daily quote budget is one
        cycle. A quota response or an exhausted ledger stops the loop without
        sending more requests.
        """
        self.logger.info("Starting Alpha Vantage quote-snapshot producer")
        self.logger.info(
            f"Config → {len(STOCKS)} symbols | "
            f"{API_CALL_DELAY_SECONDS}s between calls | "
            f"{QUOTE_CYCLES_PER_DAY} cycle/day. "
            "These are GLOBAL_QUOTE snapshots, not exchange ticks."
        )

        cycle_number = 0
        try:
            while cycle_number < QUOTE_CYCLES_PER_DAY:
                cycle_number += 1
                self.logger.info(f"=== Starting quote cycle {cycle_number} ===")
                ok_count = 0
                fail_count = 0
                stop_for_quota = False

                for index, symbol in enumerate(STOCKS):
                    try:
                        self.ledger.authorize("GLOBAL_QUOTE", symbol)
                    except QuotaExceeded as exc:
                        self.logger.error(str(exc))
                        stop_for_quota = True
                        break

                    self._last_status = "transport_error"
                    stock_data = self.fetch_quote(symbol)
                    self.ledger.record(
                        "GLOBAL_QUOTE",
                        symbol,
                        "success" if stock_data else self._last_status,
                    )
                    if self._last_status == "quota_exhausted":
                        self.logger.error("Quota response received. No further quote calls will be sent.")
                        stop_for_quota = True
                        break
                    if stock_data:
                        self.publish(stock_data)
                        ok_count += 1
                        self.logger.info(
                            f"[{symbol}] snapshot price={stock_data['price']} "
                            f"change={stock_data['change']:+.4f} "
                            f"({stock_data['percent_change']:+.4f}%)"
                        )
                    else:
                        fail_count += 1

                    if index < len(STOCKS) - 1:
                        self.logger.debug(
                            f"Waiting {API_CALL_DELAY_SECONDS}s before next API call."
                        )
                        time.sleep(API_CALL_DELAY_SECONDS)

                self.producer.flush()
                self.logger.info(
                    f"Quote cycle {cycle_number} complete — OK: {ok_count}, Failed: {fail_count}"
                )
                if stop_for_quota:
                    break

            self.logger.info(
                "Quote budget for this process is spent. "
                "Exiting instead of polling again. Restart on the next UTC day "
                "if another snapshot cycle is required."
            )

        except KeyboardInterrupt:
            self.logger.info("Producer stopped by user (KeyboardInterrupt)")
        except Exception as e:
            self.logger.error(f"Unexpected error in produce loop: {e}")
        finally:
            self.logger.info("Flushing producer and shutting down…")
            self.producer.flush()


def main():
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("ALPHA_VANTAGE_API_KEY is not set in environment variables!")
        raise EnvironmentError("Missing ALPHA_VANTAGE_API_KEY")

    if not KAFKA_TOPIC:
        logger.error("KAFKA_TOPIC_REALTIME is not set in environment variables!")
        raise EnvironmentError("Missing KAFKA_TOPIC_REALTIME")

    producer = StreamDataCollector(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
    )
    producer.produce_stock_data()


if __name__ == "__main__":
    main()
