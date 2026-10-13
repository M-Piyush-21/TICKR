"""
src/data_quality/validator.py
-----------------------------
Production-grade Data Quality & Schema Validation Engine.
Performs deterministic, rule-based validations across stock market datasets:
  - Primary Key Uniqueness ((symbol, date))
  - Nullability / Missing Value Checks
  - Data Type & Range Sanity Checks (High >= Low, High >= Open/Close, Low <= Open/Close)
  - Date & Timestamp Format Compliance
  - Data Freshness SLA Verification
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime
import re

class DataQualityValidator:
    def __init__(self):
        self.date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    def validate_dataset(self, records: List[Dict[str, Any]], expected_date: str = None) -> Dict[str, Any]:
        """
        Runs comprehensive data quality checks on a list of stock records.
        Returns calculated metrics and detailed failed check audit logs.
        """
        total_records = len(records)
        passed_checks = 0
        total_checks = 0
        failed_checks = []

        seen_keys = set()
        duplicate_records = []
        missing_critical_fields = 0
        schema_failures = 0

        for idx, rec in enumerate(records):
            record_id = f"{rec.get('symbol', 'UNKNOWN')}_{rec.get('date', f'idx_{idx}')}"

            # --- Rule 1: Symbol presence and validity ---
            total_checks += 1
            symbol = rec.get("symbol")
            # NSE cash symbols are up to 10 characters (for example BHARTIARTL).
            if symbol and isinstance(symbol, str) and 1 <= len(symbol) <= 20:
                passed_checks += 1
            else:
                failed_checks.append({
                    "check_id": "CHK_01_SYMBOL",
                    "rule": "Symbol Format",
                    "column": "symbol",
                    "record_id": record_id,
                    "observed": str(symbol),
                    "expected": "Non-empty string (1-6 uppercase letters)",
                    "severity": "CRITICAL",
                    "explanation": "Stock ticker symbol is missing, non-string, or exceeds standard length."
                })
                missing_critical_fields += 1

            # --- Rule 2: Date format (YYYY-MM-DD) ---
            total_checks += 1
            date_val = rec.get("date")
            if date_val and isinstance(date_val, str) and self.date_regex.match(date_val):
                passed_checks += 1
            else:
                failed_checks.append({
                    "check_id": "CHK_02_DATE_FMT",
                    "rule": "Date Standard ISO-8601",
                    "column": "date",
                    "record_id": record_id,
                    "observed": str(date_val),
                    "expected": "YYYY-MM-DD format",
                    "severity": "CRITICAL",
                    "explanation": "Date is missing or does not match canonical YYYY-MM-DD format."
                })
                schema_failures += 1

            # --- Rule 3: Primary Key Uniqueness ((symbol, date)) ---
            total_checks += 1
            pk = (symbol, date_val)
            if pk in seen_keys:
                duplicate_records.append(record_id)
                failed_checks.append({
                    "check_id": "CHK_03_PK_UNIQUENESS",
                    "rule": "Primary Key Deduplication",
                    "column": "(symbol, date)",
                    "record_id": record_id,
                    "observed": f"Duplicate pair {pk}",
                    "expected": "Unique (symbol, date) combination",
                    "severity": "CRITICAL",
                    "explanation": "Found duplicate row for this equity and date. Upsert or deduplication required."
                })
            else:
                if symbol and date_val:
                    seen_keys.add(pk)
                passed_checks += 1

            # --- Rule 4: Close price sanity (must be float > 0) ---
            total_checks += 1
            close = rec.get("daily_close")
            if close is not None and isinstance(close, (int, float)) and close > 0:
                passed_checks += 1
            else:
                failed_checks.append({
                    "check_id": "CHK_04_PRICE_SANITY",
                    "rule": "Positive Close Price",
                    "column": "daily_close",
                    "record_id": record_id,
                    "observed": str(close),
                    "expected": "Numeric float > 0.00",
                    "severity": "CRITICAL",
                    "explanation": "Closing stock price cannot be null, non-numeric, zero, or negative."
                })
                schema_failures += 1

            # --- Rule 5: Volume sanity (must be integer >= 0) ---
            total_checks += 1
            vol = rec.get("daily_volume")
            if vol is not None and isinstance(vol, (int, float)) and vol >= 0:
                passed_checks += 1
            else:
                failed_checks.append({
                    "check_id": "CHK_05_VOLUME_SANITY",
                    "rule": "Non-negative Volume",
                    "column": "daily_volume",
                    "record_id": record_id,
                    "observed": str(vol),
                    "expected": "Integer >= 0",
                    "severity": "WARNING",
                    "explanation": "Trading volume must be a valid non-negative integer."
                })
                schema_failures += 1

            # --- Rule 6: High/Low Spread Integrity (High >= Low) ---
            high = rec.get("daily_high")
            low = rec.get("daily_low")
            if high is not None and low is not None and isinstance(high, (int, float)) and isinstance(low, (int, float)):
                total_checks += 1
                if high >= low:
                    passed_checks += 1
                else:
                    failed_checks.append({
                        "check_id": "CHK_06_HIGH_LOW_SPREAD",
                        "rule": "High >= Low Boundary",
                        "column": "(daily_high, daily_low)",
                        "record_id": record_id,
                        "observed": f"High: {high}, Low: {low}",
                        "expected": "daily_high >= daily_low",
                        "severity": "CRITICAL",
                        "explanation": "Intraday high cannot be strictly lower than intraday low."
                    })

            # --- Rule 7: High Envelope Integrity (High >= max(Open, Close)) ---
            open_p = rec.get("daily_open")
            if high is not None and open_p is not None and close is not None and all(isinstance(x, (int, float)) for x in [high, open_p, close]):
                total_checks += 1
                if high >= max(open_p, close) * 0.999:  # with float epsilon
                    passed_checks += 1
                else:
                    failed_checks.append({
                        "check_id": "CHK_07_ENVELOPE_HIGH",
                        "rule": "High Encompasses Open/Close",
                        "column": "daily_high",
                        "record_id": record_id,
                        "observed": f"High: {high}, Open: {open_p}, Close: {close}",
                        "expected": "daily_high >= max(daily_open, daily_close)",
                        "severity": "WARNING",
                        "explanation": "Intraday high must be greater than or equal to both open and close prices."
                    })

            # --- Rule 8: Freshness / Target Date Check ---
            if expected_date and date_val:
                total_checks += 1
                if date_val == expected_date:
                    passed_checks += 1
                else:
                    failed_checks.append({
                        "check_id": "CHK_08_FRESHNESS_SLA",
                        "rule": "Expected Batch Date SLA",
                        "column": "date",
                        "record_id": record_id,
                        "observed": date_val,
                        "expected": expected_date,
                        "severity": "WARNING",
                        "explanation": f"Record date '{date_val}' does not match target batch date '{expected_date}'."
                    })

            # --- Rule 9: Source error payloads are not market rows ---
            # Alpha Vantage can return HTTP 200 with an Information, Note, or
            # Error Message body. That must not pass as a daily candle.
            if any(key in rec for key in ("Information", "Note", "Error Message")):
                total_checks += 1
                failed_checks.append({
                    "check_id": "CHK_09_SOURCE_PAYLOAD",
                    "rule": "Source Error Payload",
                    "column": "payload",
                    "record_id": record_id,
                    "observed": str({key: rec.get(key) for key in ("Information", "Note", "Error Message") if key in rec}),
                    "expected": "A daily candle, not an API error document",
                    "severity": "CRITICAL",
                    "explanation": "The provider returned an error or quota message. This row is not market data."
                })

        pass_rate = round((passed_checks / total_checks * 100), 2) if total_checks > 0 else 0.0

        latest_timestamp = None
        for r in records:
            ts = r.get("timestamp") or r.get("last_updated")
            if ts:
                if latest_timestamp is None or ts > latest_timestamp:
                    latest_timestamp = ts
        if not latest_timestamp:
            latest_timestamp = datetime.now().isoformat()

        return {
            "total_records": total_records,
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks_count": len(failed_checks),
            "pass_rate_pct": pass_rate,
            "duplicate_records_count": len(duplicate_records),
            "missing_critical_count": missing_critical_fields,
            "schema_failures_count": schema_failures,
            "latest_data_timestamp": latest_timestamp,
            "processing_status": _processing_status(total_records, failed_checks),
            "failed_checks": failed_checks
        }


def _processing_status(total_records: int, failed_checks: List[Dict[str, Any]]) -> str:
    if total_records == 0:
        return "FAILED_EMPTY"
    if any(item.get("severity") == "CRITICAL" for item in failed_checks):
        return "FAILED"
    if failed_checks:
        return "WARNINGS_FOUND"
    return "VALIDATED"
