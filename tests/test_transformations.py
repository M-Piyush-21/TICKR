"""
Unit tests for candle construction and moving averages.

Daily candles are not re-summed. Interval bars are ordered by event time.
"""

import unittest

from src.transformations.ohlcv import (
    aggregate_interval_bars,
    canonicalize_daily_candles,
    simple_moving_average,
)


class TestTransformations(unittest.TestCase):
    def test_ohlcv_aggregation(self):
        bars = [
            {"symbol": "AAPL", "date": "2026-10-09", "event_time": "2026-10-09T09:30:00",
             "open": 180.0, "high": 180.0, "low": 180.0, "close": 180.0, "volume": 1000},
            {"symbol": "AAPL", "date": "2026-10-09", "event_time": "2026-10-09T11:00:00",
             "open": 182.5, "high": 182.5, "low": 182.5, "close": 182.5, "volume": 1500},
            {"symbol": "AAPL", "date": "2026-10-09", "event_time": "2026-10-09T14:00:00",
             "open": 179.5, "high": 179.5, "low": 179.5, "close": 179.5, "volume": 2000},
            {"symbol": "AAPL", "date": "2026-10-09", "event_time": "2026-10-09T16:00:00",
             "open": 181.0, "high": 181.0, "low": 181.0, "close": 181.0, "volume": 500},
        ]
        candle = aggregate_interval_bars(bars)["records"][0]
        self.assertEqual(candle["daily_open"], 180.0)
        self.assertEqual(candle["daily_close"], 181.0)
        self.assertEqual(candle["daily_high"], 182.5)
        self.assertEqual(candle["daily_low"], 179.5)
        self.assertEqual(candle["daily_volume"], 5000)
        self.assertEqual(candle["percent_change"], 0.5556)

    def test_daily_candle_is_not_reaggregated(self):
        candle = {
            "symbol": "AAPL", "date": "2026-10-09",
            "open": 181.5, "high": 182.5, "low": 180.8, "close": 181.1, "volume": 5500,
        }
        result = canonicalize_daily_candles([candle, dict(candle)])
        row = result["records"][0]
        self.assertEqual(row["daily_open"], 181.5)
        self.assertEqual(row["daily_close"], 181.1)
        self.assertEqual(row["daily_volume"], 5500)

    def test_moving_average_calculation(self):
        self.assertEqual(simple_moving_average([10.0] * 15, window=15), 10.0)
        self.assertIsNone(simple_moving_average([10.0] * 10, window=15))


if __name__ == "__main__":
    unittest.main()
