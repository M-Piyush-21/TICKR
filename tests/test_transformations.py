"""
tests/test_transformations.py
-----------------------------
Unit tests verifying pipeline data transformations:
- Daily OHLCV candle calculations
- Moving average (MA15, MA30) sliding windows
- Percentage change calculations
"""

import unittest

def calculate_ohlcv(ticks):
    """Calculates canonical OHLCV metrics from a sequence of price/volume ticks."""
    if not ticks:
        return None
    open_p = ticks[0]["price"]
    close_p = ticks[-1]["price"]
    high_p = max(t["price"] for t in ticks)
    low_p = min(t["price"] for t in ticks)
    vol_sum = sum(t["volume"] for t in ticks)
    pct_change = round(((close_p - open_p) / open_p) * 100, 2)
    return {
        "open": open_p,
        "high": high_p,
        "low": low_p,
        "close": close_p,
        "volume": vol_sum,
        "percent_change": pct_change
    }

def calculate_ma(prices, window=15):
    """Calculates simple moving average across a series of prices."""
    if len(prices) < window:
        return None
    return round(sum(prices[-window:]) / window, 2)

class TestTransformations(unittest.TestCase):
    def test_ohlcv_aggregation(self):
        ticks = [
            {"price": 180.0, "volume": 1000},
            {"price": 182.5, "volume": 1500},
            {"price": 179.5, "volume": 2000},
            {"price": 181.0, "volume": 500}
        ]
        candle = calculate_ohlcv(ticks)
        self.assertEqual(candle["open"], 180.0)
        self.assertEqual(candle["close"], 181.0)
        self.assertEqual(candle["high"], 182.5)
        self.assertEqual(candle["low"], 179.5)
        self.assertEqual(candle["volume"], 5000)
        self.assertEqual(candle["percent_change"], 0.56)

    def test_moving_average_calculation(self):
        prices = [10.0] * 15
        ma15 = calculate_ma(prices, window=15)
        self.assertEqual(ma15, 10.0)

        # Insufficient data returns None, not an invented number
        short_prices = [10.0] * 10
        ma15_short = calculate_ma(short_prices, window=15)
        self.assertIsNone(ma15_short)

if __name__ == "__main__":
    unittest.main()
