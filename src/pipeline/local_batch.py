"""
Verified local batch path.

This does not call Alpha Vantage, Kafka, Spark, or Snowflake. It runs the
same grain, completeness, validation, and at-least-once merge rules on a
deterministic daily-candle fixture and writes a local JSON gold file.

The fixture is labeled as a fixture. It is not market data from the API.
"""

import hashlib
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from src.contracts.data_contract import (
    BATCH_SYMBOLS,
    is_expected_trading_day,
    iter_trading_days_ending,
    quota_budget,
)
from src.data_quality.completeness import evaluate_completeness
from src.data_quality.validator import DataQualityValidator
from src.observability.run_manifest import write_manifest
from src.reliability.replay import AtLeastOnceConsumer, BufferedMessage, OffsetCommitError, SinkWriteError
from src.transformations.ohlcv import attach_moving_averages, canonicalize_daily_candles
from src.warehouse.idempotent_load import ConflictingMergeKey, GoldWarehouse

BASE_PRICES = {
    "AAPL": 182.50,
    "MSFT": 415.20,
    "GOOGL": 155.80,
    "AMZN": 178.90,
    "META": 505.40,
    "TSLA": 175.30,
    "NVDA": 890.60,
    "INTC": 31.40,
    "JPM": 198.70,
    "V": 282.10,
}


def _unit(key: str) -> float:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / float(2**64)


def synthetic_daily_candle(symbol: str, session: str) -> Dict[str, Any]:
    """Stable candle for (symbol, session). A second call returns the same bar."""
    # Unknown symbols, including the NSE universe, use a fixed anchor.
    # These numbers are fixture levels. They are not live prices.
    base = BASE_PRICES.get(symbol, 100.0)
    close = round(base * (1 + (_unit(f"{symbol}|{session}|close") - 0.5) * 0.04), 2)
    spread = round(close * (0.005 + _unit(f"{symbol}|{session}|range") * 0.015), 2)
    high = round(close + spread / 2, 2)
    low = round(close - spread / 2, 2)
    open_price = round(low + (high - low) * _unit(f"{symbol}|{session}|open"), 2)
    high = round(max(high, open_price, close), 2)
    low = round(min(low, open_price, close), 2)
    volume = int(1_000_000 + _unit(f"{symbol}|{session}|volume") * 40_000_000)
    return {
        "grain": "daily_candle",
        "source": "deterministic_fixture",
        "symbol": symbol,
        "date": session,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def build_fixture_history(
    execution_date: str,
    symbols: Sequence[str],
    history_sessions: int,
    omit_symbols: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    sessions = iter_trading_days_ending(execution_date, history_sessions)
    omitted = set(omit_symbols or [])
    rows = []
    for session in sessions:
        for symbol in symbols:
            if session == execution_date and symbol in omitted:
                continue
            rows.append(synthetic_daily_candle(symbol, session))
    return rows


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(__import__("json").dumps(payload, indent=2))


def _metrics(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fields = (
        "symbol", "date", "daily_open", "daily_high", "daily_low",
        "daily_close", "daily_volume", "daily_change", "ma_15", "ma_30",
    )
    return [
        {field: row.get(field) for field in fields}
        for row in sorted(rows, key=lambda row: (row["date"], row["symbol"]))
    ]


def run_local_batch(
    root: Path,
    execution_date: str,
    symbols: Optional[Sequence[str]] = None,
    history_sessions: int = 40,
    omit_symbols: Optional[Sequence[str]] = None,
    extra_messages: Optional[Sequence[Dict[str, Any]]] = None,
    fail_write_offsets: Optional[Sequence[int]] = None,
    fail_commit_offsets: Optional[Sequence[int]] = None,
    replay: bool = False,
) -> Dict[str, Any]:
    """
    Run the local batch once.

    fail_write_offsets / fail_commit_offsets inject a single failure each,
    matching the at-least-once tests. `replay=True` runs a second attempt
    after recording the failure so the caller can inspect the recovered state.
    The CLI does not set those flags.
    """
    started = time.perf_counter()
    started_at = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    universe = list(symbols or BATCH_SYMBOLS)
    root = Path(root)
    warehouse_path = root / "data" / "warehouse" / "DAILY_STOCK_METRICS.json"

    def finish(status: str, payload: Dict[str, Any], exit_code: int) -> Dict[str, Any]:
        manifest = {
            "run_id": run_id,
            "pipeline": "local_daily_batch",
            "runtime_status": "verified_local",
            "data_source": "deterministic_fixture",
            "grain": "daily_candle",
            "canonical_key": ["symbol", "date"],
            "execution_date": execution_date,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(time.perf_counter() - started, 4),
            "status": status,
            "exit_code": exit_code,
            "symbols_requested": universe,
            "quota": quota_budget(),
            "quota_calls_this_run": 0,
            "quota_note": "This local path does not call Alpha Vantage.",
            "target": "data/warehouse/DAILY_STOCK_METRICS.json",
            **payload,
        }
        write_manifest(root, manifest)
        manifest["manifest_path"] = str(root / "artifacts" / "latest_run_manifest.json")
        return manifest

    if not is_expected_trading_day(execution_date):
        return finish(
            "SKIPPED_NON_TRADING_DAY",
            {
                "completeness": evaluate_completeness([], execution_date, expected_symbols=universe),
                "symbols_received": [],
                "row_counts": {"batch": 0, "rejected": 0, "gold_total": 0},
                "duplicate_key_count": 0,
                "invalid_record_count": 0,
            },
            0,
        )

    raw_rows = build_fixture_history(
        execution_date, universe, history_sessions, omit_symbols=omit_symbols
    )
    if extra_messages:
        raw_rows.extend(extra_messages)

    try:
        canonical = canonicalize_daily_candles(raw_rows, source="deterministic_fixture")
        if canonical["quarantined"]:
            raise ConflictingMergeKey(canonical["quarantined"])
    except ConflictingMergeKey as exc:
        return finish(
            "FAILED_CONFLICTING_KEYS",
            {
                "completeness": evaluate_completeness([], execution_date, expected_symbols=universe),
                "symbols_received": [],
                "quarantined": exc.quarantined,
                "row_counts": {"batch": 0, "rejected": len(exc.quarantined), "gold_total": 0},
                "duplicate_key_count": len(exc.quarantined),
                "invalid_record_count": len(exc.quarantined),
            },
            1,
        )

    enriched = attach_moving_averages(canonical["records"])
    batch_rows = [row for row in enriched if row["date"] == execution_date]
    completeness = evaluate_completeness(batch_rows, execution_date, expected_symbols=universe)
    quality = DataQualityValidator().validate_dataset(batch_rows, expected_date=execution_date)

    if not completeness["passed"] or quality["processing_status"] != "VALIDATED":
        return finish(
            "FAILED_QUALITY",
            {
                "completeness": completeness,
                "quality": _quality_summary(quality),
                "symbols_received": completeness["received_symbols"],
                "row_counts": {
                    "batch": len(batch_rows),
                    "rejected": quality["failed_checks_count"],
                    "gold_total": 0,
                },
                "duplicate_key_count": quality["duplicate_records_count"],
                "invalid_record_count": quality["failed_checks_count"],
            },
            1,
        )

    messages = [
        BufferedMessage(offset=index, payload=row)
        for index, row in enumerate(sorted(batch_rows, key=lambda item: item["symbol"]))
    ]
    warehouse = GoldWarehouse(warehouse_path)
    # Prior sessions give the moving averages a real ordered history.
    # They are fixture rows, merged idempotently, and are not the batch.
    history_rows = [row for row in enriched if row["date"] != execution_date]
    if history_rows:
        warehouse.merge(history_rows, source="deterministic_fixture")

    consumer = AtLeastOnceConsumer(
        warehouse,
        fail_write_offsets=fail_write_offsets,
        fail_commit_offsets=fail_commit_offsets,
    )
    failure = None
    try:
        consumer.run_once(messages)
    except (SinkWriteError, OffsetCommitError) as exc:
        failure = exc.__class__.__name__
        if not replay:
            _persist_layers(root, execution_date, batch_rows, warehouse)
            return finish(
                "FAILED_SINK",
                {
                    "completeness": completeness,
                    "quality": _quality_summary(quality),
                    "symbols_received": completeness["received_symbols"],
                    "failure": failure,
                    "committed_through": consumer.committed_through,
                    "row_counts": _counts(batch_rows, warehouse, canonical),
                    "duplicate_key_count": 0,
                    "invalid_record_count": 0,
                    "recovered": False,
                },
                1,
            )
        consumer.run_once(messages)

    _persist_layers(root, execution_date, batch_rows, warehouse)
    counts = _counts(batch_rows, warehouse, canonical)
    return finish(
        "SUCCESS",
        {
            "completeness": completeness,
            "quality": _quality_summary(quality),
            "symbols_received": completeness["received_symbols"],
            "failure_injected": failure,
            "recovered": failure is not None,
            "committed_through": consumer.committed_through,
            "row_counts": counts,
            "duplicate_key_count": 0,
            "invalid_record_count": 0,
            "freshness": {
                "source_session": execution_date,
                "gold_available_at": datetime.now(timezone.utc).isoformat(),
                "processing_seconds": round(time.perf_counter() - started, 4),
            },
            "metrics_fingerprint": _fingerprint(warehouse),
        },
        0,
    )


def _quality_summary(quality: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "processing_status": quality["processing_status"],
        "pass_rate_pct": quality["pass_rate_pct"],
        "total_records": quality["total_records"],
        "failed_checks_count": quality["failed_checks_count"],
        "duplicate_records_count": quality["duplicate_records_count"],
    }


def _counts(batch_rows, warehouse: GoldWarehouse, canonical: Dict[str, Any]) -> Dict[str, int]:
    return {
        "batch": len(batch_rows),
        "history": len(canonical["records"]) - len(batch_rows),
        "replays_collapsed": canonical["replays_collapsed"],
        "rejected": len(canonical["quarantined"]),
        "gold_total": len(warehouse.rows),
        "gold_for_execution_date": sum(
            1 for row in warehouse.rows.values() if row["date"] == batch_rows[0]["date"]
        ) if batch_rows else 0,
    }


def _fingerprint(warehouse: GoldWarehouse) -> str:
    import json
    encoded = json.dumps(_metrics(warehouse.metric_rows()), sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _persist_layers(root: Path, execution_date: str, batch_rows: List[Dict], warehouse: GoldWarehouse) -> None:
    year, month, day = execution_date.split("-")
    bronze = root / "data" / "lake" / "raw" / "historical" / f"year={year}" / f"month={month}" / f"day={day}"
    silver = root / "data" / "lake" / "processed" / "historical" / f"date={execution_date}"
    for row in batch_rows:
        _write_json(bronze / f"{row['symbol']}.json", row)
        _write_json(silver / f"symbol={row['symbol']}" / "part-00000.json", row)
    warehouse.save()
