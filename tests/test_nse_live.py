"""NSE calendar, Yahoo chart parsing, and the live batch with a fake client."""

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from src.ingestion.yahoo_nse import parse_chart_payload
from src.markets.nse import is_nse_trading_day
from src.pipeline.live_nse_batch import choose_execution_date, run_live_nse_batch

SESSION = "2026-10-13"


def _stamp(session: str) -> int:
    return int(datetime.fromisoformat(f"{session}T09:15:00+05:30").timestamp())


def _chart(symbol: str, bars):
    return {
        "chart": {
            "result": [{
                "meta": {"currency": "INR", "exchangeName": "NSI", "symbol": f"{symbol}.NS"},
                "timestamp": [bar["ts"] for bar in bars],
                "indicators": {
                    "quote": [{
                        "open": [bar["open"] for bar in bars],
                        "high": [bar["high"] for bar in bars],
                        "low": [bar["low"] for bar in bars],
                        "close": [bar["close"] for bar in bars],
                        "volume": [bar["volume"] for bar in bars],
                    }]
                },
            }],
            "error": None,
        }
    }


class TestYahooChartParser(unittest.TestCase):
    def test_ist_session_and_null_bar_are_handled(self):
        # Prices observed from the RELIANCE.NS chart on 2026-10-13.
        payload = _chart("RELIANCE", [
            {"ts": _stamp("2026-10-09"), "open": 1179.0, "high": 1179.0, "low": 1160.199951171875, "close": 1170.300048828125, "volume": 12086708},
            {"ts": _stamp("2026-10-12"), "open": None, "high": None, "low": None, "close": None, "volume": None},
            {"ts": _stamp("2026-10-13"), "open": 1158.9000244140625, "high": 1168.300048828125, "low": 1152.199951171875, "close": 1166.699951171875, "volume": 10983647},
        ])
        parsed = parse_chart_payload(payload, "RELIANCE")
        self.assertEqual(parsed["status"], "ok")
        self.assertEqual(parsed["null_bars_dropped"], 1)
        self.assertEqual([bar["date"] for bar in parsed["bars"]], ["2026-10-09", "2026-10-13"])
        latest = parsed["bars"][-1]
        self.assertEqual(latest["close"], 1166.70)
        self.assertEqual(latest["open"], 1158.90)
        self.assertGreaterEqual(latest["high"], latest["close"])
        self.assertLessEqual(latest["low"], latest["open"])

    def test_non_inr_payload_is_rejected(self):
        payload = _chart("RELIANCE", [
            {"ts": _stamp(SESSION), "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100},
        ])
        payload["chart"]["result"][0]["meta"]["currency"] = "USD"
        parsed = parse_chart_payload(payload, "RELIANCE")
        self.assertEqual(parsed["status"], "source_error")
        self.assertEqual(parsed["bars"], [])


class TestSessionSelection(unittest.TestCase):
    def test_requested_date_is_not_replaced(self):
        choice = choose_execution_date(
            {"RELIANCE": [{"date": "2026-10-09"}]},
            SESSION,
        )
        self.assertEqual(choice["execution_date"], SESSION)
        self.assertEqual(choice["selection"], "requested")

    def test_latest_common_session_is_used_when_no_date_is_requested(self):
        choice = choose_execution_date(
            {
                "RELIANCE": [{"date": "2026-10-09"}, {"date": SESSION}],
                "TCS": [{"date": "2026-10-09"}, {"date": SESSION}, {"date": "2026-10-14"}],
            },
            None,
        )
        self.assertEqual(choice["execution_date"], SESSION)

    def test_dussehra_is_not_a_regular_session(self):
        self.assertFalse(is_nse_trading_day("2026-10-20"))
        self.assertTrue(is_nse_trading_day(SESSION))


class TestLiveBatchWithFakeClient(unittest.TestCase):
    def test_complete_book_is_written_once_per_key(self):
        def fetch(symbol, range_="1y"):
            return parse_chart_payload(_chart(symbol, [
                {"ts": _stamp("2026-10-09"), "open": 100, "high": 102, "low": 99, "close": 101, "volume": 1000},
                {"ts": _stamp(SESSION), "open": 101, "high": 104, "low": 100, "close": 103, "volume": 1100},
            ]), symbol)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = run_live_nse_batch(
                root, SESSION, symbols=["RELIANCE", "TCS"], fetch=fetch, pause_seconds=0
            )
            second = run_live_nse_batch(
                root, SESSION, symbols=["RELIANCE", "TCS"], fetch=fetch, pause_seconds=0
            )
            self.assertEqual(first["status"], "SUCCESS")
            self.assertEqual(first["quota_calls_this_run"], 2)
            self.assertEqual(second["metrics_fingerprint"], first["metrics_fingerprint"])
            self.assertTrue((root / "artifacts" / "nse_market.json").exists())
            rows = __import__("json").loads((root / "data" / "warehouse" / "DAILY_STOCK_METRICS.json").read_text())
            keys = [(row["symbol"], row["date"]) for row in rows]
            self.assertEqual(len(keys), len(set(keys)))
            self.assertEqual(sum(1 for row in rows if row["date"] == SESSION), 2)

    def test_missing_session_does_not_load_gold(self):
        def fetch(symbol, range_="1y"):
            bars = [
                {"ts": _stamp("2026-10-09"), "open": 100, "high": 102, "low": 99, "close": 101, "volume": 1000},
            ]
            if symbol == "RELIANCE":
                bars.append({
                    "ts": _stamp(SESSION), "open": 101, "high": 104, "low": 100, "close": 103, "volume": 1100,
                })
            return parse_chart_payload(_chart(symbol, bars), symbol)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = run_live_nse_batch(
                root, SESSION, symbols=["RELIANCE", "TCS"], fetch=fetch, pause_seconds=0
            )
            self.assertEqual(manifest["exit_code"], 1)
            self.assertIn("TCS", manifest["completeness"]["missing_symbols"])
            self.assertFalse((root / "data" / "warehouse" / "DAILY_STOCK_METRICS.json").exists())

    def test_holiday_does_not_call_the_client(self):
        def fetch(symbol, range_="1y"):
            raise AssertionError("holiday must not fetch")

        with tempfile.TemporaryDirectory() as tmp:
            manifest = run_live_nse_batch(
                Path(tmp), "2026-10-20", symbols=["RELIANCE"], fetch=fetch, pause_seconds=0
            )
            self.assertEqual(manifest["status"], "SKIPPED_NON_TRADING_DAY")
            self.assertEqual(manifest["quota_calls_this_run"], 0)

    def test_unknown_provider_makes_no_call(self):
        def fetch(symbol, range_="1y"):
            raise AssertionError("unknown provider must not fetch")

        with tempfile.TemporaryDirectory() as tmp:
            manifest = run_live_nse_batch(
                Path(tmp),
                SESSION,
                symbols=["RELIANCE"],
                provider="custom",
                fetch=fetch,
                pause_seconds=0,
            )
            self.assertEqual(manifest["exit_code"], 2)
            self.assertIn("TICKR_MARKET_API_BASE", manifest["reason"])


if __name__ == "__main__":
    unittest.main()
