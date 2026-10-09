"""
tests/test_screener.py
----------------------
Unit tests verifying stock screener rules and filters:
- Positive / Negative daily change filter
- Above / Below 15-day moving average filter
- High volume surge filter
- Graceful missing metric handling
"""

import unittest

def filter_stocks(stocks, change_filter="ALL", ma_filter="ALL", volume_filter="ALL"):
    results = []
    for s in stocks:
        pct = s.get("percentChange")
        price = s.get("price")
        ma15 = s.get("ma15")
        vol = s.get("volume")
        avg_vol = s.get("avgVolume", 25_000_000)

        # Change filter
        if change_filter == "POSITIVE" and (pct is None or pct <= 0):
            continue
        if change_filter == "NEGATIVE" and (pct is None or pct >= 0):
            continue

        # MA filter
        if ma_filter == "ABOVE_MA" and (price is None or ma15 is None or price <= ma15):
            continue
        if ma_filter == "BELOW_MA" and (price is None or ma15 is None or price >= ma15):
            continue

        # Volume filter
        if volume_filter == "SURGE" and (vol is None or vol <= avg_vol * 1.25):
            continue

        results.append(s)
    return results

class TestStockScreener(unittest.TestCase):
    def setUp(self):
        self.sample_stocks = [
            {"symbol": "AAPL", "price": 182.50, "percentChange": 1.25, "ma15": 180.00, "volume": 55_000_000, "avgVolume": 40_000_000},
            {"symbol": "MSFT", "price": 410.00, "percentChange": -0.85, "ma15": 415.00, "volume": 20_000_000, "avgVolume": 25_000_000},
            {"symbol": "NVDA", "price": 910.00, "percentChange": 3.40, "ma15": 880.00, "volume": 60_000_000, "avgVolume": 35_000_000},
            {"symbol": "INTC", "price": 31.00, "percentChange": -2.10, "ma15": None, "volume": 12_000_000, "avgVolume": 20_000_000}
        ]

    def test_positive_gainers_filter(self):
        gainers = filter_stocks(self.sample_stocks, change_filter="POSITIVE")
        symbols = [s["symbol"] for s in gainers]
        self.assertEqual(symbols, ["AAPL", "NVDA"])

    def test_negative_losers_filter(self):
        losers = filter_stocks(self.sample_stocks, change_filter="NEGATIVE")
        symbols = [s["symbol"] for s in losers]
        self.assertEqual(symbols, ["MSFT", "INTC"])

    def test_above_ma15_filter(self):
        above = filter_stocks(self.sample_stocks, ma_filter="ABOVE_MA")
        symbols = [s["symbol"] for s in above]
        self.assertEqual(symbols, ["AAPL", "NVDA"])

    def test_missing_ma15_is_not_fabricated(self):
        # INTC has ma15=None. It should NOT pass ABOVE_MA or BELOW_MA
        above = filter_stocks(self.sample_stocks, ma_filter="ABOVE_MA")
        self.assertNotIn("INTC", [s["symbol"] for s in above])
        below = filter_stocks(self.sample_stocks, ma_filter="BELOW_MA")
        self.assertNotIn("INTC", [s["symbol"] for s in below])

    def test_volume_surge_filter(self):
        surging = filter_stocks(self.sample_stocks, volume_filter="SURGE")
        symbols = [s["symbol"] for s in surging]
        # AAPL (55M > 1.25*40M=50M) and NVDA (60M > 1.25*35M=43.75M)
        self.assertIn("AAPL", symbols)
        self.assertIn("NVDA", symbols)
        self.assertNotIn("MSFT", symbols)

if __name__ == "__main__":
    unittest.main()
