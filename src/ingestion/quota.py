"""
Persistent Alpha Vantage request ledger.

The ledger counts every request that was sent, including ones that came back
as a quota error. A retry must not call the provider again after the daily
budget is spent, and a symbol that already succeeded today is not requested
again.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.contracts.data_contract import (
    ALPHA_VANTAGE_DAILY_LIMIT,
    BATCH_REQUESTS_PER_RUN,
    QUOTE_REQUESTS_PER_DAY,
    RETRY_RESERVE,
)


class QuotaExceeded(Exception):
    """Raised before a request that would pass the daily cap."""


def _utc_day(now: Optional[datetime] = None) -> str:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).strftime("%Y-%m-%d")


class QuotaLedger:
    def __init__(self, path: Path, daily_limit: int = ALPHA_VANTAGE_DAILY_LIMIT):
        self.path = Path(path)
        self.daily_limit = daily_limit
        self._state = self._load()

    def _load(self) -> dict:
        if not self.path.exists():
            return {"calls": []}
        try:
            with self.path.open() as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError):
            return {"calls": []}
        if not isinstance(payload, dict) or not isinstance(payload.get("calls"), list):
            return {"calls": []}
        return payload

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        with temporary.open("w") as handle:
            json.dump(self._state, handle, indent=2)
        temporary.replace(self.path)

    def calls_on(self, day: Optional[str] = None) -> list:
        target = day or _utc_day()
        return [row for row in self._state["calls"] if row.get("utc_day") == target]

    def count_on(self, day: Optional[str] = None, endpoint: Optional[str] = None) -> int:
        rows = self.calls_on(day)
        if endpoint is None:
            return len(rows)
        return sum(1 for row in rows if row.get("endpoint") == endpoint)

    def succeeded(self, endpoint: str, symbol: str, day: Optional[str] = None) -> bool:
        for row in self.calls_on(day):
            if (
                row.get("endpoint") == endpoint
                and row.get("symbol") == symbol
                and row.get("outcome") == "success"
            ):
                return True
        return False

    def authorize(self, endpoint: str, symbol: str, now: Optional[datetime] = None) -> None:
        """
        Refuse the call when the daily cap is already spent or this path's
        planned budget is already used. Does not record the attempt.
        """
        day = _utc_day(now)
        if self.count_on(day) >= self.daily_limit:
            raise QuotaExceeded(
                f"Alpha Vantage daily limit of {self.daily_limit} is already recorded "
                f"for {day}. Refusing {endpoint} for {symbol}."
            )

        endpoint_caps = {
            "TIME_SERIES_DAILY": BATCH_REQUESTS_PER_RUN + RETRY_RESERVE,
            "GLOBAL_QUOTE": QUOTE_REQUESTS_PER_DAY,
        }
        cap = endpoint_caps.get(endpoint)
        if cap is not None and self.count_on(day, endpoint=endpoint) >= cap:
            raise QuotaExceeded(
                f"{endpoint} budget of {cap} requests is already recorded for {day}. "
                f"Refusing another call for {symbol}."
            )

    def record(
        self,
        endpoint: str,
        symbol: str,
        outcome: str,
        now: Optional[datetime] = None,
    ) -> None:
        current = now or datetime.now(timezone.utc)
        self._state["calls"].append({
            "utc_day": _utc_day(current),
            "endpoint": endpoint,
            "symbol": symbol,
            "outcome": outcome,
            "recorded_at": current.astimezone(timezone.utc).isoformat(),
        })
        self._save()
