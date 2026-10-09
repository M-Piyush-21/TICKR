"""
tests/test_alerts.py
--------------------
Unit tests for the statistical market alerts engine:
- Price volatility thresholds
- Volume multiplier surge detection
- MA15 divergence indicators
"""

import unittest
from src.data_quality.alerts import MarketActivityAlertEngine

class TestMarketActivityAlertEngine(unittest.TestCase):
    def setUp(self):
        self.engine = MarketActivityAlertEngine(
            price_change_threshold_pct=2.5,
            volume_surge_multiplier=1.5,
            ma_divergence_threshold_pct=3.0
        )

    def test_triggers_volatility_alert_on_large_swing(self):
        records = [
            {"symbol": "TSLA", "percent_change": 4.85, "daily_close": 182.0, "daily_volume": 20000000},
            {"symbol": "AAPL", "percent_change": 0.45, "daily_close": 182.0, "daily_volume": 20000000}
        ]
        alerts = self.engine.evaluate_records(records)
        alert_stocks = [a["stock"] for a in alerts]
        self.assertIn("TSLA", alert_stocks)
        self.assertNotIn("AAPL", alert_stocks)

    def test_triggers_volume_surge_when_exceeding_baseline(self):
        records = [
            {"symbol": "NVDA", "percent_change": 1.2, "daily_close": 890.0, "daily_volume": 55000000}
        ]
        baselines = {"NVDA": {"avg_volume": 25000000}}  # 55M is 2.2x baseline (> 1.5x)
        alerts = self.engine.evaluate_records(records, historical_averages=baselines)
        vol_alerts = [a for a in alerts if a["alert_type"] == "VOLUME_SURGE"]
        self.assertEqual(len(vol_alerts), 1)
        self.assertEqual(vol_alerts[0]["stock"], "NVDA")

    def test_transparent_metadata_included(self):
        records = [{"symbol": "META", "percent_change": -3.2, "daily_close": 500.0, "daily_volume": 10000000}]
        alerts = self.engine.evaluate_records(records)
        self.assertGreater(len(alerts), 0)
        first = alerts[0]
        self.assertIn("observed_value", first)
        self.assertIn("threshold", first)
        self.assertIn("rule_type", first)
        self.assertIn("reason", first)

if __name__ == "__main__":
    unittest.main()
