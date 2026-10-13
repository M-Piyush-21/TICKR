"""
Local stand-in for the Snowflake MERGE.

The warehouse key is (symbol, date). Loading the same candle twice updates
the existing row. Conflicting candles for that key are rejected and nothing
from the conflicting key is written. Identical replays collapse to one row.
"""

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from src.transformations.ohlcv import canonicalize_daily_candles


class ConflictingMergeKey(Exception):
    def __init__(self, quarantined: List[Dict[str, Any]]):
        self.quarantined = quarantined
        symbols = ", ".join(
            f"{row.get('symbol')}:{row.get('date')}" for row in quarantined
        )
        super().__init__(f"Refusing MERGE because these keys conflict: {symbols}")


class GoldWarehouse:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.rows: Dict[str, Dict[str, Any]] = {}
        if self.path.exists():
            self.rows = {
                _key(row): row
                for row in json.loads(self.path.read_text() or "[]")
            }

    def merge(self, records: Iterable[Dict[str, Any]], source: Optional[str] = None) -> Dict[str, Any]:
        prepared = prepare_merge_records(list(records), source=source)
        inserted = 0
        updated = 0
        for record in prepared["records"]:
            key = _key(record)
            if key in self.rows:
                updated += 1
            else:
                inserted += 1
            self.rows[key] = record
        self.save()
        return {
            "inserted": inserted,
            "updated": updated,
            "rejected": len(prepared["quarantined"]),
            "total": len(self.rows),
            "quarantined": prepared["quarantined"],
            "replays_collapsed": prepared["replays_collapsed"],
        }

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        ordered = sorted(self.rows.values(), key=lambda row: (row["date"], row["symbol"]))
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(ordered, indent=2))
        temporary.replace(self.path)

    def metric_rows(self) -> List[Dict[str, Any]]:
        return sorted(self.rows.values(), key=lambda row: (row["date"], row["symbol"]))


def _key(row: Dict[str, Any]) -> str:
    return f"{row['symbol']}|{row['date']}"


def prepare_merge_records(
    records: List[Dict[str, Any]],
    source: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Return one row per (symbol, date). Raise if any key has conflicting OHLCV.
    """
    prepared = canonicalize_daily_candles(records, source=source)
    if prepared["quarantined"]:
        raise ConflictingMergeKey(prepared["quarantined"])
    return prepared
