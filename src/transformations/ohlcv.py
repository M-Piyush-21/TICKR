"""
OHLCV rules matched to the input grain.

Daily candles from TIME_SERIES_DAILY are already one bar per (symbol, date).
Replays of that bar are collapsed. Conflicting values are quarantined.
Volume is not summed, because the inputs are not non-overlapping trades.

Interval bars (multiple timestamps inside one session) are a different input.
Those are ordered by event_time, then open is the first open, close is the
last close, high is the max, low is the min, and volume is summed.
"""

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


OHLCV_FIELDS = ("open", "high", "low", "close", "volume")


def _pick(record: Dict[str, Any], *names: str):
    for name in names:
        if name in record and record[name] is not None:
            return record[name]
    return None


def _as_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> Optional[int]:
    number = _as_float(value)
    if number is None:
        return None
    return int(number)


def _date_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    return text[:10]


def normalize_bar(record: Dict[str, Any]) -> Dict[str, Any]:
    close = _as_float(_pick(record, "close", "daily_close", "price"))
    open_price = _as_float(_pick(record, "open", "daily_open"))
    if open_price is None:
        open_price = close
    return {
        "symbol": record.get("symbol"),
        "date": _date_text(_pick(record, "date", "trading_date", "latest_trading_day")),
        "event_time": _pick(record, "event_time", "timestamp", "polled_at", "date"),
        "open": open_price,
        "high": _as_float(_pick(record, "high", "daily_high")),
        "low": _as_float(_pick(record, "low", "daily_low")),
        "close": close,
        "volume": _as_int(_pick(record, "volume", "daily_volume")),
        "grain": record.get("grain") or "daily_candle",
    }


def _signature(bar: Dict[str, Any]) -> Tuple:
    return (
        bar["open"],
        bar["high"],
        bar["low"],
        bar["close"],
        bar["volume"],
    )


def _percent_change(open_price: float, close_price: float) -> Optional[float]:
    if open_price in (None, 0):
        return None
    return round((close_price - open_price) / open_price * 100, 4)


def to_daily_record(bar: Dict[str, Any], source: Optional[str] = None) -> Dict[str, Any]:
    record = {
        "grain": "daily_candle",
        "symbol": bar["symbol"],
        "date": bar["date"],
        "daily_open": bar["open"],
        "daily_high": bar["high"],
        "daily_low": bar["low"],
        "daily_close": bar["close"],
        "daily_volume": bar["volume"],
        "daily_change": _percent_change(bar["open"], bar["close"]),
        "percent_change": _percent_change(bar["open"], bar["close"]),
    }
    if source:
        record["source"] = source
    return record


def canonicalize_daily_candles(
    records: Iterable[Dict[str, Any]],
    source: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Collapse exact replays of a daily candle. Quarantine conflicting keys.
    """
    grouped: Dict[Tuple, List[Dict[str, Any]]] = {}
    for record in records:
        bar = normalize_bar(record)
        grouped.setdefault((bar["symbol"], bar["date"]), []).append(bar)

    canonical = []
    quarantined = []
    replays_collapsed = 0
    for key, bars in grouped.items():
        signatures = {_signature(bar) for bar in bars}
        if len(signatures) > 1:
            quarantined.append({
                "symbol": key[0],
                "date": key[1],
                "reason": "conflicting_daily_candle",
                "version_count": len(signatures),
                "row_count": len(bars),
            })
            continue
        if len(bars) > 1:
            replays_collapsed += len(bars) - 1
        canonical.append(to_daily_record(bars[-1], source=source))

    canonical.sort(key=lambda row: (row["date"] or "", row["symbol"] or ""))
    return {
        "records": canonical,
        "quarantined": quarantined,
        "replays_collapsed": replays_collapsed,
    }


def aggregate_interval_bars(records: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build one daily candle from non-overlapping interval bars.

    event_time is required. Bars are sorted by that field before first/last
    are taken, so input order cannot change the open or the close.
    Volume is summed only in this function, and only across those intervals.
    """
    grouped: Dict[Tuple, List[Dict[str, Any]]] = {}
    quarantined = []
    for record in records:
        bar = normalize_bar(record)
        if bar["event_time"] in (None, ""):
            quarantined.append({
                "symbol": bar["symbol"],
                "date": bar["date"],
                "reason": "missing_event_time",
            })
            continue
        grouped.setdefault((bar["symbol"], bar["date"]), []).append(bar)

    canonical = []
    for key, bars in grouped.items():
        ordered = sorted(bars, key=lambda bar: str(bar["event_time"]))
        if any(bar["open"] is None or bar["close"] is None for bar in ordered):
            quarantined.append({
                "symbol": key[0],
                "date": key[1],
                "reason": "missing_open_or_close",
            })
            continue
        highs = [bar["high"] if bar["high"] is not None else max(bar["open"], bar["close"]) for bar in ordered]
        lows = [bar["low"] if bar["low"] is not None else min(bar["open"], bar["close"]) for bar in ordered]
        volumes = [bar["volume"] for bar in ordered]
        if any(volume is None for volume in volumes):
            quarantined.append({
                "symbol": key[0],
                "date": key[1],
                "reason": "missing_volume",
            })
            continue
        combined = {
            "symbol": key[0],
            "date": key[1],
            "open": ordered[0]["open"],
            "close": ordered[-1]["close"],
            "high": max(highs),
            "low": min(lows),
            "volume": sum(volumes),
        }
        canonical.append(to_daily_record(combined))

    canonical.sort(key=lambda row: (row["date"] or "", row["symbol"] or ""))
    return {"records": canonical, "quarantined": quarantined}


def simple_moving_average(closes: Sequence[float], window: int) -> Optional[float]:
    if window < 1 or len(closes) < window:
        return None
    sample = closes[-window:]
    return round(sum(sample) / window, 4)


def attach_moving_averages(
    records: Sequence[Dict[str, Any]],
    windows: Sequence[int] = (15, 30),
) -> List[Dict[str, Any]]:
    """
    SMA of daily_close ordered by trading date, per symbol.

    Sessions before the window is full stay null. The missing value is not
    filled with the latest close.
    """
    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for record in records:
        by_symbol.setdefault(record["symbol"], []).append(dict(record))

    output = []
    for symbol in sorted(by_symbol):
        ordered = sorted(by_symbol[symbol], key=lambda row: row["date"])
        closes: List[float] = []
        for row in ordered:
            closes.append(row["daily_close"])
            for window in windows:
                row[f"ma_{window}"] = simple_moving_average(closes, window)
            output.append(row)
    return output
