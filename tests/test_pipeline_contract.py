"""
Contract tests for grain, quota, completeness, and replay.

These tests do not start Kafka, Spark, MinIO, or Snowflake.
"""

import json
import tempfile
import unittest
from pathlib import Path

from src.contracts.data_contract import (
    ALPHA_VANTAGE_DAILY_LIMIT,
    PLANNED_DAILY_REQUESTS,
    QUOTE_REQUESTS_PER_DAY,
    is_expected_trading_day,
)
from src.data_quality.completeness import evaluate_completeness
from src.data_quality.validator import DataQualityValidator
from src.ingestion.alphavantage import parse_daily_payload, parse_quote_payload
from src.ingestion.quota import QuotaExceeded, QuotaLedger
from src.pipeline.local_batch import run_local_batch, synthetic_daily_candle
from src.reliability.replay import AtLeastOnceConsumer, BufferedMessage, OffsetCommitError, SinkWriteError
from src.transformations.ohlcv import (
    aggregate_interval_bars,
    attach_moving_averages,
    canonicalize_daily_candles,
    simple_moving_average,
)
from src.warehouse.idempotent_load import ConflictingMergeKey, GoldWarehouse


EXECUTION_DATE = "2026-10-13"


def _daily(symbol, close, volume=100):
    return {
        "grain": "daily_candle",
        "symbol": symbol,
        "date": EXECUTION_DATE,
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": volume,
    }


class TestGrainAndOhlcv(unittest.TestCase):
    def test_daily_replays_do_not_sum_volume(self):
        candle = _daily("AAPL", 180.0, volume=1500)
        result = canonicalize_daily_candles([candle, dict(candle), dict(candle)])
        self.assertEqual(len(result["records"]), 1)
        self.assertEqual(result["records"][0]["daily_volume"], 1500)
        self.assertEqual(result["replays_collapsed"], 2)
        self.assertEqual(result["quarantined"], [])

    def test_conflicting_daily_candles_are_quarantined(self):
        first = _daily("AAPL", 180.0, volume=1500)
        second = _daily("AAPL", 181.0, volume=1500)
        result = canonicalize_daily_candles([first, second])
        self.assertEqual(result["records"], [])
        self.assertEqual(result["quarantined"][0]["reason"], "conflicting_daily_candle")

    def test_interval_bars_follow_event_time_not_input_order(self):
        bars = [
            {"symbol": "AAPL", "date": EXECUTION_DATE, "event_time": "2026-10-13T15:00:00",
             "open": 182.0, "high": 182.4, "low": 181.0, "close": 181.2, "volume": 300},
            {"symbol": "AAPL", "date": EXECUTION_DATE, "event_time": "2026-10-13T09:30:00",
             "open": 180.0, "high": 180.5, "low": 179.5, "close": 180.2, "volume": 100},
            {"symbol": "AAPL", "date": EXECUTION_DATE, "event_time": "2026-10-13T12:00:00",
             "open": 180.2, "high": 183.0, "low": 180.0, "close": 182.0, "volume": 200},
        ]
        candle = aggregate_interval_bars(bars)["records"][0]
        self.assertEqual(candle["daily_open"], 180.0)
        self.assertEqual(candle["daily_close"], 181.2)
        self.assertEqual(candle["daily_high"], 183.0)
        self.assertEqual(candle["daily_low"], 179.5)
        self.assertEqual(candle["daily_volume"], 600)

    def test_moving_average_is_null_until_the_window_is_full(self):
        self.assertIsNone(simple_moving_average([1, 2, 3], 15))
        self.assertEqual(simple_moving_average([10.0] * 15, 15), 10.0)
        rows = [
            {"symbol": "AAPL", "date": f"2026-10-{day:02d}", "daily_close": float(day)}
            for day in range(1, 16)
        ]
        enriched = attach_moving_averages(rows, windows=(15,))
        self.assertIsNone(enriched[13]["ma_15"])
        self.assertIsNotNone(enriched[14]["ma_15"])


class TestSourcePayloads(unittest.TestCase):
    def test_quota_body_is_not_a_candle(self):
        parsed = parse_daily_payload(
            {"Information": "Thank you for using Alpha Vantage! frequency is 5 calls per minute"},
            "AAPL",
            EXECUTION_DATE,
        )
        self.assertEqual(parsed["status"], "quota_exhausted")
        self.assertEqual(parsed["bars"], [])
        self.assertFalse(parsed["retryable"])

    def test_empty_body_and_missing_session_are_not_substituted(self):
        empty = parse_daily_payload({}, "AAPL", EXECUTION_DATE)
        self.assertEqual(empty["status"], "empty")
        missing = parse_daily_payload(
            {"Time Series (Daily)": {"2026-10-12": {
                "1. open": "1", "2. high": "1", "3. low": "1", "4. close": "1", "5. volume": "1"
            }}},
            "AAPL",
            EXECUTION_DATE,
        )
        self.assertEqual(missing["status"], "missing_bar")
        self.assertEqual(missing["bars"], [])

    def test_quote_snapshot_is_labeled(self):
        parsed = parse_quote_payload(
            {"Global Quote": {
                "05. price": "182.10",
                "06. volume": "1000",
                "07. latest trading day": EXECUTION_DATE,
                "08. previous close": "181.00",
                "09. change": "1.10",
                "10. change percent": "0.61%",
            }},
            "AAPL",
            "2026-10-13T15:00:00+00:00",
        )
        self.assertEqual(parsed["quote"]["grain"], "quote_snapshot")
        self.assertEqual(parsed["quote"]["polled_at"], "2026-10-13T15:00:00+00:00")

    def test_validator_rejects_error_payload_and_empty_batch(self):
        validator = DataQualityValidator()
        error = validator.validate_dataset([{"Information": "quota", "symbol": "AAPL", "date": EXECUTION_DATE}])
        self.assertEqual(error["processing_status"], "FAILED")
        self.assertIn("Source Error Payload", [item["rule"] for item in error["failed_checks"]])
        empty = validator.validate_dataset([])
        self.assertEqual(empty["processing_status"], "FAILED_EMPTY")


class TestCompletenessAndQuota(unittest.TestCase):
    def test_missing_symbol_fails_the_gate(self):
        rows = [_daily(symbol, 100.0) for symbol in ("AAPL", "MSFT")]
        # canonicalize shape isn't required; completeness reads symbol/date
        result = evaluate_completeness(
            [{"symbol": row["symbol"], "date": row["date"]} for row in rows],
            EXECUTION_DATE,
            expected_symbols=["AAPL", "MSFT", "NVDA"],
        )
        self.assertFalse(result["passed"])
        self.assertEqual(result["missing_symbols"], ["NVDA"])
        self.assertEqual(result["status"], "INCOMPLETE")

    def test_holiday_is_a_skip_not_a_fake_success(self):
        self.assertTrue(is_expected_trading_day("2026-10-13"))
        self.assertTrue(is_expected_trading_day("2026-11-26"))
        self.assertFalse(is_expected_trading_day("2026-10-20"))
        result = evaluate_completeness([], "2026-10-20", expected_symbols=["RELIANCE"])
        self.assertTrue(result["passed"])
        self.assertEqual(result["status"], "SKIPPED_NON_TRADING_DAY")
        self.assertEqual(result["expected_combinations"], 0)

    def test_quota_schedule_fits_the_free_tier(self):
        self.assertLessEqual(PLANNED_DAILY_REQUESTS, ALPHA_VANTAGE_DAILY_LIMIT)
        self.assertEqual(QUOTE_REQUESTS_PER_DAY, 5)
        self.assertLess(10 + QUOTE_REQUESTS_PER_DAY, ALPHA_VANTAGE_DAILY_LIMIT)

    def test_ledger_stops_before_another_quote_cycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = QuotaLedger(Path(tmp) / "ledger.json")
            for symbol in ("AAPL", "MSFT", "GOOGL", "AMZN", "META"):
                ledger.authorize("GLOBAL_QUOTE", symbol)
                ledger.record("GLOBAL_QUOTE", symbol, "success")
            with self.assertRaises(QuotaExceeded):
                ledger.authorize("GLOBAL_QUOTE", "TSLA")
            # The batch path still has its own budget on the same day.
            ledger.authorize("TIME_SERIES_DAILY", "AAPL")


class TestReplayAndMerge(unittest.TestCase):
    def test_commit_failure_replays_without_a_duplicate_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            warehouse = GoldWarehouse(Path(tmp) / "gold.json")
            messages = [
                BufferedMessage(0, synthetic_daily_candle("AAPL", EXECUTION_DATE)),
                BufferedMessage(1, synthetic_daily_candle("MSFT", EXECUTION_DATE)),
            ]
            consumer = AtLeastOnceConsumer(warehouse, fail_commit_offsets=[0])
            with self.assertRaises(OffsetCommitError):
                consumer.run_once(messages)
            self.assertEqual(len(warehouse.rows), 1)
            consumer.run_once(messages)
            self.assertEqual(len(warehouse.rows), 2)
            consumer.run_once(messages)
            self.assertEqual(len(warehouse.rows), 2)
            keys = list(warehouse.rows)
            self.assertEqual(len(keys), len(set(keys)))

    def test_write_failure_does_not_commit_or_store_the_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            warehouse = GoldWarehouse(Path(tmp) / "gold.json")
            message = BufferedMessage(7, synthetic_daily_candle("NVDA", EXECUTION_DATE))
            consumer = AtLeastOnceConsumer(warehouse, fail_write_offsets=[7])
            with self.assertRaises(SinkWriteError):
                consumer.run_once([message])
            self.assertEqual(warehouse.rows, {})
            self.assertEqual(consumer.committed_through, -1)
            consumer.run_once([message])
            self.assertEqual(len(warehouse.rows), 1)

    def test_conflicting_stage_rows_are_rejected_before_merge(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gold.json"
            warehouse = GoldWarehouse(path)
            candle = synthetic_daily_candle("AAPL", EXECUTION_DATE)
            warehouse.merge([candle])
            conflict = dict(candle)
            conflict["close"] = candle["close"] + 5
            with self.assertRaises(ConflictingMergeKey):
                warehouse.merge([candle, conflict])
            reloaded = GoldWarehouse(path)
            self.assertEqual(len(reloaded.rows), 1)
            self.assertEqual(reloaded.metric_rows()[0]["daily_close"], candle["close"])


class TestLocalBatchSmoke(unittest.TestCase):
    def test_rerun_keeps_one_row_per_key_and_the_same_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = run_local_batch(root, EXECUTION_DATE, history_sessions=20)
            second = run_local_batch(root, EXECUTION_DATE, history_sessions=20)
            self.assertEqual(first["status"], "SUCCESS")
            self.assertEqual(second["status"], "SUCCESS")
            self.assertEqual(first["metrics_fingerprint"], second["metrics_fingerprint"])
            self.assertEqual(first["row_counts"]["gold_total"], second["row_counts"]["gold_total"])
            rows = json.loads((root / "data" / "warehouse" / "DAILY_STOCK_METRICS.json").read_text())
            keys = [(row["symbol"], row["date"]) for row in rows]
            self.assertEqual(len(keys), len(set(keys)))
            self.assertEqual(sum(1 for row in rows if row["date"] == EXECUTION_DATE), 10)
            self.assertTrue((root / "artifacts" / "latest_run_manifest.json").exists())

    def test_forced_commit_failure_recovers_to_the_clean_fingerprint(self):
        with tempfile.TemporaryDirectory() as tmp:
            clean_root = Path(tmp) / "clean"
            replay_root = Path(tmp) / "replay"
            clean = run_local_batch(clean_root, EXECUTION_DATE, history_sessions=5)
            recovered = run_local_batch(
                replay_root,
                EXECUTION_DATE,
                history_sessions=5,
                fail_commit_offsets=[0],
                replay=True,
            )
            self.assertEqual(recovered["status"], "SUCCESS")
            self.assertTrue(recovered["recovered"])
            self.assertEqual(clean["metrics_fingerprint"], recovered["metrics_fingerprint"])

    def test_missing_symbol_fails_and_does_not_load_gold(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = run_local_batch(
                root, EXECUTION_DATE, history_sessions=5, omit_symbols=["INFY"]
            )
            self.assertEqual(manifest["exit_code"], 1)
            self.assertEqual(manifest["completeness"]["status"], "INCOMPLETE")
            self.assertIn("INFY", manifest["completeness"]["missing_symbols"])
            self.assertFalse((root / "data" / "warehouse" / "DAILY_STOCK_METRICS.json").exists())

    def test_holiday_writes_a_skip_manifest_and_no_candle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = run_local_batch(root, "2026-10-20", history_sessions=5)
            self.assertEqual(manifest["status"], "SKIPPED_NON_TRADING_DAY")
            self.assertEqual(manifest["exit_code"], 0)
            self.assertFalse((root / "data" / "warehouse" / "DAILY_STOCK_METRICS.json").exists())


if __name__ == "__main__":
    unittest.main()
