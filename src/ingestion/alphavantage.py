"""
Parse Alpha Vantage JSON bodies.

HTTP 200 is not success. The provider returns quota and error messages in
the JSON body with a 200 status. Those payloads are rejected here.
"""

from typing import Any, Dict, List


def _error_kind(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return "malformed"
    if payload.get("Information") or payload.get("Note"):
        return "quota_exhausted"
    if payload.get("Error Message"):
        return "source_error"
    return ""


def parse_daily_payload(payload: Any, symbol: str, execution_date: str) -> Dict[str, Any]:
    """
    Parse one TIME_SERIES_DAILY body for one symbol.

    A missing bar on the requested session is returned as status=missing_bar.
    The most recent other session is not substituted. Doing that would write
    Friday's candle into a Saturday partition and look like a complete run.
    """
    if not isinstance(payload, dict) or not payload:
        return {
            "symbol": symbol,
            "status": "empty",
            "message": "Alpha Vantage returned an empty body.",
            "bars": [],
        }

    kind = _error_kind(payload)
    if kind == "quota_exhausted":
        message = str(payload.get("Information") or payload.get("Note"))
        return {
            "symbol": symbol,
            "status": "quota_exhausted",
            "message": message,
            "bars": [],
            "retryable": False,
        }
    if kind == "source_error":
        return {
            "symbol": symbol,
            "status": "source_error",
            "message": str(payload.get("Error Message")),
            "bars": [],
            "retryable": False,
        }
    if kind == "malformed":
        return {
            "symbol": symbol,
            "status": "malformed",
            "message": "Response was not a JSON object.",
            "bars": [],
            "retryable": False,
        }

    series = payload.get("Time Series (Daily)")
    if not isinstance(series, dict) or not series:
        return {
            "symbol": symbol,
            "status": "empty",
            "message": "Time Series (Daily) was missing or empty.",
            "bars": [],
            "retryable": False,
        }

    if execution_date not in series:
        dates = sorted(series.keys())
        return {
            "symbol": symbol,
            "status": "missing_bar",
            "message": (
                f"No daily candle for {symbol} on {execution_date}. "
                f"Series covers {dates[0]} through {dates[-1]}. "
                "The pipeline does not substitute another session."
            ),
            "bars": [],
            "retryable": False,
        }

    values = series[execution_date]
    try:
        bar = {
            "grain": "daily_candle",
            "symbol": symbol,
            "date": execution_date,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"]),
        }
    except (KeyError, TypeError, ValueError) as exc:
        return {
            "symbol": symbol,
            "status": "malformed",
            "message": f"Daily candle for {symbol} on {execution_date} could not be parsed: {exc}",
            "bars": [],
            "retryable": False,
        }

    return {
        "symbol": symbol,
        "status": "ok",
        "message": "Daily candle parsed.",
        "bars": [bar],
        "retryable": False,
    }


def parse_quote_payload(payload: Any, symbol: str, polled_at: str) -> Dict[str, Any]:
    """Parse one GLOBAL_QUOTE body. The result is a snapshot, not a tick."""
    if not isinstance(payload, dict) or not payload:
        return {
            "symbol": symbol,
            "status": "empty",
            "message": "Alpha Vantage returned an empty quote body.",
            "quote": None,
        }

    kind = _error_kind(payload)
    if kind:
        message = str(
            payload.get("Information")
            or payload.get("Note")
            or payload.get("Error Message")
            or "Unusable quote payload."
        )
        return {
            "symbol": symbol,
            "status": kind,
            "message": message,
            "quote": None,
            "retryable": kind not in {"quota_exhausted", "source_error"},
        }

    quote = payload.get("Global Quote")
    if not isinstance(quote, dict) or not quote.get("05. price"):
        return {
            "symbol": symbol,
            "status": "empty",
            "message": f"Global Quote for {symbol} was empty.",
            "quote": None,
            "retryable": False,
        }

    try:
        change_pct_raw = str(quote.get("10. change percent", "0")).replace("%", "")
        parsed = {
            "grain": "quote_snapshot",
            "symbol": symbol,
            "price": float(quote["05. price"]),
            "change": float(quote.get("09. change", 0) or 0),
            "percent_change": float(change_pct_raw or 0),
            "volume": int(float(quote.get("06. volume", 0) or 0)),
            "latest_trading_day": quote.get("07. latest trading day", ""),
            "previous_close": float(quote.get("08. previous close", 0) or 0),
            "polled_at": polled_at,
        }
    except (TypeError, ValueError) as exc:
        return {
            "symbol": symbol,
            "status": "malformed",
            "message": f"Quote for {symbol} could not be parsed: {exc}",
            "quote": None,
            "retryable": False,
        }

    return {
        "symbol": symbol,
        "status": "ok",
        "message": "Quote snapshot parsed.",
        "quote": parsed,
        "retryable": False,
    }


def bars_from_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    bars: List[Dict[str, Any]] = []
    for result in results:
        bars.extend(result.get("bars") or [])
    return bars
