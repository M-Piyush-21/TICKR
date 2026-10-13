"""
NSE cash equities.

Regular session is about 09:15–15:30 Asia/Kolkata. This module vendors the
2026 weekday closures. It does not invent holidays for other years.

Sources, checked for calendar year 2026:
  https://www.nseindia.com/resources/exchange-communication-holidays
  https://nsearchives.nseindia.com/content/circulars/CMTR71775.pdf

2026-01-15 (Maharashtra municipal elections) is on the NSE equities holiday
page. The circular excerpt does not list it. It is included because the
exchange page lists it as a trading holiday.

2026-11-08 is Diwali Laxmi Pujan / Muhurat. It falls on a Sunday, so the
weekend check already excludes it. It is not a regular session. Muhurat
timings are notified separately and are not treated as a normal candle.
"""

from datetime import datetime, timedelta

NSE_TIMEZONE = "Asia/Kolkata"
NSE_CURRENCY = "INR"

# Provider symbols are these tickers plus ".NS".
NSE_UNIVERSE = (
    {"symbol": "RELIANCE", "name": "Reliance Industries", "sector": "Energy"},
    {"symbol": "TCS", "name": "Tata Consultancy Services", "sector": "Information Technology"},
    {"symbol": "HDFCBANK", "name": "HDFC Bank", "sector": "Banking"},
    {"symbol": "INFY", "name": "Infosys", "sector": "Information Technology"},
    {"symbol": "ICICIBANK", "name": "ICICI Bank", "sector": "Banking"},
    {"symbol": "SBIN", "name": "State Bank of India", "sector": "Banking"},
    {"symbol": "BHARTIARTL", "name": "Bharti Airtel", "sector": "Telecom"},
    {"symbol": "ITC", "name": "ITC", "sector": "FMCG"},
    {"symbol": "LT", "name": "Larsen & Toubro", "sector": "Infrastructure"},
    {"symbol": "HINDUNILVR", "name": "Hindustan Unilever", "sector": "FMCG"},
)

NSE_SYMBOLS = tuple(item["symbol"] for item in NSE_UNIVERSE)
NSE_METADATA = {item["symbol"]: item for item in NSE_UNIVERSE}

NSE_FULL_HOLIDAYS_2026 = frozenset({
    "2026-01-15",  # Maharashtra municipal elections (NSE holiday page)
    "2026-01-26",  # Republic Day
    "2026-03-03",  # Holi
    "2026-03-26",  # Ram Navami
    "2026-03-31",  # Mahavir Jayanti
    "2026-04-03",  # Good Friday
    "2026-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-05-28",  # Bakri Id
    "2026-06-26",  # Muharram
    "2026-09-14",  # Ganesh Chaturthi
    "2026-10-02",  # Mahatma Gandhi Jayanti
    "2026-10-20",  # Dussehra
    "2026-11-10",  # Diwali — Balipratipada
    "2026-11-24",  # Guru Nanak Jayanti
    "2026-12-25",  # Christmas
})


def parse_iso_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def is_nse_trading_day(value: str) -> bool:
    """
    Weekdays that are not on the 2026 NSE full-closure list.

    Dates outside 2026 use weekday-only logic. Holiday coverage for other
    years is not inferred.
    """
    day = parse_iso_date(value)
    if day.weekday() >= 5:
        return False
    if value in NSE_FULL_HOLIDAYS_2026:
        return False
    return True


def iter_nse_trading_days_ending(end_date: str, count: int) -> list:
    """Return `count` expected NSE sessions ending at end_date, ascending."""
    if count < 1 or not is_nse_trading_day(end_date):
        return []
    found = []
    cursor = parse_iso_date(end_date)
    for _ in range(count * 4 + 14):
        key = cursor.strftime("%Y-%m-%d")
        if is_nse_trading_day(key):
            found.append(key)
            if len(found) == count:
                break
        cursor -= timedelta(days=1)
    found.reverse()
    return found
