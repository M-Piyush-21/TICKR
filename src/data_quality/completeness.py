"""
Completeness gate.

Row-level checks can pass while an entire symbol is missing. This gate
compares the batch to the expected symbol set for a trading session.
"""

from typing import Any, Dict, Iterable, List, Optional, Sequence

from src.contracts.data_contract import BATCH_SYMBOLS, is_expected_trading_day


FAILURE_STATUSES = {"quota_exhausted", "source_error", "empty", "malformed", "missing_bar"}


def evaluate_completeness(
    records: Sequence[Dict[str, Any]],
    execution_date: str,
    source_results: Optional[Iterable[Dict[str, Any]]] = None,
    expected_symbols: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    expected = list(expected_symbols or BATCH_SYMBOLS)
    source_results = list(source_results or [])

    if not is_expected_trading_day(execution_date):
        return {
            "status": "SKIPPED_NON_TRADING_DAY",
            "passed": True,
            "execution_date": execution_date,
            "expected_symbols": [],
            "received_symbols": sorted({row.get("symbol") for row in records if row.get("date") == execution_date}),
            "missing_symbols": [],
            "expected_combinations": 0,
            "received_combinations": 0,
            "completeness_ratio": 1.0,
            "source_failures": [],
            "reason": (
                f"{execution_date} is not an expected NSE cash session "
                "(weekend or a listed 2026 full-day holiday). No candle is required."
            ),
        }

    received = sorted({
        row.get("symbol")
        for row in records
        if row.get("date") == execution_date and row.get("symbol")
    })
    missing = [symbol for symbol in expected if symbol not in received]
    source_failures = [
        {
            "symbol": result.get("symbol"),
            "status": result.get("status"),
            "message": result.get("message"),
        }
        for result in source_results
        if result.get("status") in FAILURE_STATUSES
    ]

    expected_count = len(expected)
    received_count = len([symbol for symbol in expected if symbol in received])
    ratio = round(received_count / expected_count, 4) if expected_count else 1.0

    quota_hit = any(item["status"] == "quota_exhausted" for item in source_failures)
    if quota_hit:
        status = "FAILED_QUOTA"
        passed = False
    elif source_failures or missing or not records:
        status = "INCOMPLETE"
        passed = False
    else:
        status = "COMPLETE"
        passed = True

    if not records and not source_failures:
        reason = "The batch produced no rows."
    elif missing:
        reason = f"Missing daily candles for: {', '.join(missing)}."
    elif source_failures:
        reason = "The source returned an empty body or an error payload."
    else:
        reason = "Every expected symbol has one daily candle for this session."

    return {
        "status": status,
        "passed": passed,
        "execution_date": execution_date,
        "expected_symbols": expected,
        "received_symbols": received,
        "missing_symbols": missing,
        "expected_combinations": expected_count,
        "received_combinations": received_count,
        "completeness_ratio": ratio,
        "source_failures": source_failures,
        "reason": reason,
    }
