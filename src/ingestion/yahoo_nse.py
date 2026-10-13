"""
Daily candles for NSE cash equities from the Yahoo Finance chart endpoint.

This is an unofficial chart URL. It is not a licensed exchange feed, and it
can throttle or change without notice. One response is one daily OHLCV bar
per session, not a trade tick.

No API key is required. A symbol such as RELIANCE is requested as RELIANCE.NS.
Bars with a null open, high, low, close, or volume are dropped. A missing
session is not replaced with another day's candle.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS"
SOURCE_NAME = "yahoo_finance_chart"


def fetch_chart(symbol: str, range_: str = "1y", timeout: int = 30) -> Dict[str, Any]:
    """GET the daily chart. Raises requests.HTTPError on a non-200 response."""
    import requests

    url = CHART_URL.format(symbol=symbol)
    response = requests.get(
        url,
        params={"interval": "1d", "range": range_},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=timeout,
    )
    if response.status_code == 429:
        return {
            "symbol": symbol,
            "status": "quota_exhausted",
            "message": "Yahoo chart returned HTTP 429.",
            "bars": [],
            "null_bars_dropped": 0,
        }
    response.raise_for_status()
    parsed = parse_chart_payload(response.json(), symbol)
    parsed["http_status"] = response.status_code
    return parsed


def parse_chart_payload(payload: Any, symbol: str) -> Dict[str, Any]:
    """Turn a chart JSON body into daily bars keyed by the IST session date."""
    if not isinstance(payload, dict) or not isinstance(payload.get("chart"), dict):
        return _failure(symbol, "malformed", "Response has no chart object.")

    chart = payload["chart"]
    if chart.get("error"):
        return _failure(symbol, "source_error", str(chart["error"]))

    result = chart.get("result") or []
    if not result or not isinstance(result[0], dict):
        return _failure(symbol, "empty", "Chart result is empty.")

    block = result[0]
    meta = block.get("meta") or {}
    currency = meta.get("currency")
    if currency and currency != "INR":
        return _failure(
            symbol,
            "source_error",
            f"Expected INR for an NSE cash symbol, got {currency}.",
        )

    timestamps = block.get("timestamp") or []
    quote_sets = (block.get("indicators") or {}).get("quote") or []
    if not quote_sets or not isinstance(quote_sets[0], dict):
        return _failure(symbol, "malformed", "Chart quote block is missing.")

    quote = quote_sets[0]
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    bars = []
    dropped = 0
    for index, stamp in enumerate(timestamps):
        raw = (
            _at(opens, index),
            _at(highs, index),
            _at(lows, index),
            _at(closes, index),
            _at(volumes, index),
        )
        if any(value is None for value in raw):
            dropped += 1
            continue
        try:
            session = datetime.fromtimestamp(int(stamp), IST).date().isoformat()
            open_price, high, low, close = (_paise(value) for value in raw[:4])
            volume = int(raw[4])
        except (TypeError, ValueError, OSError):
            dropped += 1
            continue
        high = round(max(high, open_price, close), 2)
        low = round(min(low, open_price, close), 2)
        if volume < 0 or high < low or open_price <= 0 or close <= 0:
            dropped += 1
            continue
        bars.append({
            "grain": "daily_candle",
            "source": SOURCE_NAME,
            "symbol": symbol,
            "date": session,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        })

    if not bars:
        failure = _failure(symbol, "empty", "Every chart bar was null or unusable.")
        failure["null_bars_dropped"] = dropped
        return failure

    return {
        "symbol": symbol,
        "status": "ok",
        "message": None,
        "bars": bars,
        "null_bars_dropped": dropped,
        "currency": currency or "INR",
        "provider_symbol": meta.get("symbol") or f"{symbol}.NS",
    }


def _at(values, index):
    if index >= len(values):
        return None
    return values[index]


def _paise(value: Any) -> float:
    return round(float(value), 2)


def _failure(symbol: str, status: str, message: str) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "status": status,
        "message": message,
        "bars": [],
        "null_bars_dropped": 0,
    }
