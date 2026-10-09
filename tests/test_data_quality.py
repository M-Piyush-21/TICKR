"""
tests/test_data_quality.py
--------------------------
Unit tests verifying data quality validation rules:
- Duplicate record detection
- Null / Missing field assertions
- Schema & boundary sanity checks (High >= Low)
- Pass rate calculation
"""

import unittest
from src.data_quality.validator import DataQualityValidator

class TestDataQualityValidator(unittest.TestCase):
    def setUp(self):
        self.validator = DataQualityValidator()

    def test_valid_dataset_passes_100_percent(self):
        clean_records = [
            {
                "symbol": "AAPL",
                "date": "2026-10-09",
                "daily_open": 181.50,
                "daily_high": 182.50,
                "daily_low": 180.50,
                "daily_close": 182.00,
                "daily_volume": 45000000,
                "timestamp": "2026-10-09T14:30:00"
            },
            {
                "symbol": "MSFT",
                "date": "2026-10-09",
                "daily_open": 414.00,
                "daily_high": 418.00,
                "daily_low": 412.00,
                "daily_close": 416.50,
                "daily_volume": 22000000,
                "timestamp": "2026-10-09T14:30:00"
            }
        ]
        result = self.validator.validate_dataset(clean_records, expected_date="2026-10-09")
        self.assertEqual(result["total_records"], 2)
        self.assertEqual(result["failed_checks_count"], 0)
        self.assertEqual(result["pass_rate_pct"], 100.0)
        self.assertEqual(result["processing_status"], "VALIDATED")

    def test_detects_duplicate_records(self):
        duplicate_records = [
            {"symbol": "NVDA", "date": "2026-10-09", "daily_close": 890.0, "daily_volume": 30000000},
            {"symbol": "NVDA", "date": "2026-10-09", "daily_close": 895.0, "daily_volume": 31000000}
        ]
        result = self.validator.validate_dataset(duplicate_records)
        self.assertEqual(result["duplicate_records_count"], 1)
        self.assertGreater(result["failed_checks_count"], 0)
        rule_names = [f["rule"] for f in result["failed_checks"]]
        self.assertIn("Primary Key Deduplication", rule_names)

    def test_detects_missing_and_corrupt_data(self):
        corrupt_records = [
            {"symbol": None, "date": "invalid-date", "daily_close": -50.0, "daily_volume": "bad"}
        ]
        result = self.validator.validate_dataset(corrupt_records)
        self.assertGreater(result["missing_critical_count"], 0)
        self.assertGreater(result["schema_failures_count"], 0)
        self.assertLess(result["pass_rate_pct"], 50.0)

    def test_detects_inverted_high_low_spread(self):
        inverted_record = [
            {
                "symbol": "TSLA",
                "date": "2026-10-09",
                "daily_open": 180.0,
                "daily_high": 170.0,  # Invalid: High < Low!
                "daily_low": 185.0,
                "daily_close": 175.0,
                "daily_volume": 10000000
            }
        ]
        result = self.validator.validate_dataset(inverted_record)
        rules = [f["rule"] for f in result["failed_checks"]]
        self.assertIn("High >= Low Boundary", rules)

if __name__ == "__main__":
    unittest.main()
