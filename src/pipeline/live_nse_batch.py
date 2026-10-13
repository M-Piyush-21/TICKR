"""
Live NSE daily batch.

Fetches one daily candle history per symbol, checks the NSE session,
canonicalizes replays, and MERGEs into the local gold file. Quote snapshots
are not accepted here.

The default provider is the Yahoo Finance chart endpoint (no key). Set
TICKR_DATA_PROVIDER to anything else and this runner stops before calling
out. The fields for a replacement client are TICKR_MARKET_API_BASE,
TICKR_MARKET_API_KEY, and the symbol format.
"""

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from src.data_quality.completeness import evaluate_completeness
from src.data_quality.validator import DataQualityValidator
from src.ingestion.yahoo_nse import SOURCE_NAME, fetch_chart
from src.markets.nse import NSE_METADATA, NSE_SYMBOLS, is_nse_trading_day
from src.observability.run_manifest import write_manifest
from src.transformations.ohlcv import attach_moving_averages, canonicalize_daily_candles
from src.warehouse.idempotent_load import ConflictingMergeKey, GoldWarehouse

YAHOO_PROVIDERS = {"yahoo", "yahoo_chart"}


def choose_execution_date(
    bars_by_symbol: Dict[str, Sequence[Dict[str, Any]]],
    requested: Optional[str],
) -> Dict[str, Any]:
    """
    A requested session is used as given. It is not replaced when missing.

    With no request, the session is the latest date present for every symbol
    that is also an expected NSE trading day.
    """
    if requested:
        return {
            "execution_date": requested,
            "selection": "non_trading_day" if not is_nse_trading_day(requested) else "requested",
        }

    common = None
    for bars in bars_by_symbol.values():
        sessions = {bar["date"] for bar in bars if is_nse_trading_day(bar["date"])}
        common = sessions if common is None else common & sessions
    if not common:
        return {"execution_date": None, "selection": "no_common_session"}
    return {
        "execution_date": max(common),
        "selection": "latest_common_completed_session",
    }


def run_live_nse_batch(
    root: Path,
    execution_date: Optional[str] = None,
    symbols: Optional[Sequence[str]] = None,
    provider: Optional[str] = None,
    fetch: Optional[Callable[..., Dict[str, Any]]] = None,
    pause_seconds: float = 0.4,
    chart_range: str = "1y",
) -> Dict[str, Any]:
    started = time.perf_counter()
    started_at = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    universe = list(symbols or NSE_SYMBOLS)
    root = Path(root)
    provider_name = (provider if provider is not None else os.environ.get("TICKR_DATA_PROVIDER", "yahoo"))
    provider_name = provider_name.strip().lower()
    calls = 0

    def finish(status: str, payload: Dict[str, Any], exit_code: int, session: Optional[str]) -> Dict[str, Any]:
        manifest = {
            "run_id": run_id,
            "pipeline": "live_nse_daily_batch",
            "runtime_status": "live_nse_yahoo_chart" if provider_name in YAHOO_PROVIDERS else "blocked_unknown_provider",
            "data_source": SOURCE_NAME if provider_name in YAHOO_PROVIDERS else provider_name,
            "market": "NSE",
            "currency": "INR",
            "timezone": "Asia/Kolkata",
            "grain": "daily_candle",
            "canonical_key": ["symbol", "date"],
            "execution_date": session,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(time.perf_counter() - started, 4),
            "status": status,
            "exit_code": exit_code,
            "symbols_requested": universe,
            "quota_calls_this_run": calls,
            "quota_note": (
                "Unofficial Yahoo chart endpoint. Calls are spaced "
                f"{pause_seconds}s apart. This is not the Alpha Vantage ledger."
            ),
            "target": "data/warehouse/DAILY_STOCK_METRICS.json",
            "dashboard_artifact": "artifacts/nse_market.json",
            **payload,
        }
        write_manifest(root, manifest)
        manifest["manifest_path"] = str(root / "artifacts" / "latest_run_manifest.json")
        return manifest

    if provider_name not in YAHOO_PROVIDERS:
        return finish(
            "FAILED_PROVIDER",
            {
                "reason": (
                    f"TICKR_DATA_PROVIDER={provider_name!r} has no client yet. "
                    "The working source is the Yahoo chart endpoint. To swap in "
                    "your API, provide TICKR_MARKET_API_BASE, TICKR_MARKET_API_KEY, "
                    "and the symbol format (for example RELIANCE.NS). Nothing was called."
                ),
                "symbols_received": [],
                "row_counts": {"batch": 0, "rejected": 0, "gold_total": 0},
                "duplicate_key_count": 0,
                "invalid_record_count": 0,
            },
            2,
            execution_date,
        )

    if execution_date and not is_nse_trading_day(execution_date):
        return finish(
            "SKIPPED_NON_TRADING_DAY",
            {
                "session_selection": "non_trading_day",
                "completeness": evaluate_completeness([], execution_date, expected_symbols=universe),
                "symbols_received": [],
                "row_counts": {"batch": 0, "rejected": 0, "gold_total": 0},
                "duplicate_key_count": 0,
                "invalid_record_count": 0,
            },
            0,
            execution_date,
        )

    fetcher = fetch or fetch_chart
    bars_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    source_results = []
    null_bars_dropped = 0
    for index, symbol in enumerate(universe):
        if index and pause_seconds:
            time.sleep(pause_seconds)
        calls += 1
        try:
            parsed = fetcher(symbol, range_=chart_range)
        except Exception as exc:
            parsed = {
                "symbol": symbol,
                "status": "source_error",
                "message": f"{exc.__class__.__name__}: {exc}",
                "bars": [],
                "null_bars_dropped": 0,
            }
        bars_by_symbol[symbol] = list(parsed.get("bars") or [])
        null_bars_dropped += int(parsed.get("null_bars_dropped") or 0)
        source_results.append({
            "symbol": symbol,
            "status": parsed.get("status"),
            "message": parsed.get("message"),
        })

    choice = choose_execution_date(bars_by_symbol, execution_date)
    session = choice["execution_date"]
    if session is None:
        return finish(
            "FAILED_QUALITY",
            {
                "session_selection": choice["selection"],
                "completeness": {
                    "status": "INCOMPLETE",
                    "passed": False,
                    "reason": "The symbols do not share a completed NSE session.",
                    "missing_symbols": universe,
                    "received_symbols": [],
                    "expected_combinations": len(universe),
                    "received_combinations": 0,
                    "completeness_ratio": 0.0,
                },
                "source_results": source_results,
                "null_bars_dropped": null_bars_dropped,
                "symbols_received": [],
                "row_counts": {"batch": 0, "rejected": 0, "gold_total": 0},
                "duplicate_key_count": 0,
                "invalid_record_count": 0,
            },
            1,
            None,
        )

    raw_rows = [
        bar
        for bars in bars_by_symbol.values()
        for bar in bars
        if bar["date"] <= session
    ]
    for item in source_results:
        if item["status"] == "ok" and not any(
            bar["date"] == session for bar in bars_by_symbol[item["symbol"]]
        ):
            item["status"] = "missing_bar"
            item["message"] = f"No daily candle for {session}. A different session was not substituted."

    try:
        canonical = canonicalize_daily_candles(raw_rows, source=SOURCE_NAME)
        if canonical["quarantined"]:
            raise ConflictingMergeKey(canonical["quarantined"])
    except ConflictingMergeKey as exc:
        return finish(
            "FAILED_CONFLICTING_KEYS",
            {
                "session_selection": choice["selection"],
                "completeness": evaluate_completeness([], session, source_results, universe),
                "quarantined": exc.quarantined,
                "null_bars_dropped": null_bars_dropped,
                "symbols_received": [],
                "row_counts": {"batch": 0, "rejected": len(exc.quarantined), "gold_total": 0},
                "duplicate_key_count": len(exc.quarantined),
                "invalid_record_count": len(exc.quarantined),
            },
            1,
            session,
        )

    enriched = attach_moving_averages(canonical["records"])
    batch_rows = [row for row in enriched if row["date"] == session]
    completeness = evaluate_completeness(
        batch_rows, session, source_results, expected_symbols=universe
    )
    quality = DataQualityValidator().validate_dataset(batch_rows, expected_date=session)
    if not completeness["passed"] or quality["processing_status"] != "VALIDATED":
        return finish(
            "FAILED_QUALITY",
            {
                "session_selection": choice["selection"],
                "completeness": completeness,
                "quality": _quality_summary(quality),
                "source_results": source_results,
                "null_bars_dropped": null_bars_dropped,
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
            session,
        )

    warehouse_path = root / "data" / "warehouse" / "DAILY_STOCK_METRICS.json"
    warehouse = GoldWarehouse(warehouse_path)
    removed = [
        key for key, row in list(warehouse.rows.items())
        if row.get("symbol") not in universe
    ]
    for key in removed:
        del warehouse.rows[key]
    warehouse.merge(enriched, source=SOURCE_NAME)
    enriched = attach_moving_averages(list(warehouse.rows.values()))
    for row in enriched:
        warehouse.rows[f"{row['symbol']}|{row['date']}"] = row
    _persist_layers(root, session, batch_rows, warehouse)
    _write_market_artifact(root, session, universe, enriched)

    return finish(
        "SUCCESS",
        {
            "session_selection": choice["selection"],
            "completeness": completeness,
            "quality": _quality_summary(quality),
            "source_results": source_results,
            "null_bars_dropped": null_bars_dropped,
            "removed_non_nse_rows": len(removed),
            "symbols_received": completeness["received_symbols"],
            "row_counts": {
                "batch": len(batch_rows),
                "history": len(enriched) - len(batch_rows),
                "rejected": 0,
                "gold_total": len(warehouse.rows),
                "gold_for_execution_date": len(batch_rows),
            },
            "duplicate_key_count": 0,
            "invalid_record_count": 0,
            "freshness": {
                "source_session": session,
                "gold_available_at": datetime.now(timezone.utc).isoformat(),
                "processing_seconds": round(time.perf_counter() - started, 4),
            },
            "metrics_fingerprint": hashlib.sha256(
                json.dumps(
                    [
                        {
                            "symbol": row["symbol"],
                            "date": row["date"],
                            "daily_close": row["daily_close"],
                            "daily_volume": row["daily_volume"],
                        }
                        for row in sorted(warehouse.rows.values(), key=lambda item: (item["date"], item["symbol"]))
                    ],
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest(),
        },
        0,
        session,
    )


def _quality_summary(quality: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "processing_status": quality["processing_status"],
        "pass_rate_pct": quality["pass_rate_pct"],
        "total_records": quality["total_records"],
        "failed_checks_count": quality["failed_checks_count"],
        "duplicate_records_count": quality["duplicate_records_count"],
    }


def _persist_layers(root: Path, execution_date: str, batch_rows: List[Dict], warehouse: GoldWarehouse) -> None:
    year, month, day = execution_date.split("-")
    bronze = root / "data" / "lake" / "raw" / "historical" / f"year={year}" / f"month={month}" / f"day={day}"
    silver = root / "data" / "lake" / "processed" / "historical" / f"date={execution_date}"
    for row in batch_rows:
        _write_json(bronze / f"{row['symbol']}.json", row)
        _write_json(silver / f"symbol={row['symbol']}" / "part-00000.json", row)
    warehouse.save()


def _write_market_artifact(root: Path, session: str, universe: Sequence[str], rows: Sequence[Dict[str, Any]]) -> None:
    grouped: Dict[str, List[Dict[str, Any]]] = {symbol: [] for symbol in universe}
    for row in rows:
        if row["symbol"] not in grouped:
            continue
        grouped[row["symbol"]].append(row)
    payload = {
        "market": "NSE",
        "currency": "INR",
        "timezone": "Asia/Kolkata",
        "provider": SOURCE_NAME,
        "source": (
            "Yahoo Finance chart API for NSE cash equities (.NS). "
            "Unofficial endpoint, not an exchange feed and not a trade tick."
        ),
        "as_of": session,
        "symbols": [],
    }
    for symbol in universe:
        meta = NSE_METADATA.get(symbol, {"name": symbol, "sector": ""})
        candles = []
        for row in sorted(grouped[symbol], key=lambda item: item["date"]):
            candles.append({
                "date": row["date"],
                "open": row["daily_open"],
                "high": row["daily_high"],
                "low": row["daily_low"],
                "close": row["daily_close"],
                "volume": row["daily_volume"],
                "change": round(row["daily_close"] - row["daily_open"], 2),
                "percentChange": row.get("percent_change"),
                "ma15": row.get("ma_15"),
                "ma30": row.get("ma_30"),
            })
        payload["symbols"].append({
            "symbol": symbol,
            "name": meta["name"],
            "sector": meta["sector"],
            "candles": candles,
        })
    _write_json(root / "artifacts" / "nse_market.json", payload)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2))
    temporary.replace(path)
