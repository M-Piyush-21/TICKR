"""
Data contract for TICKR.

The project market is NSE cash equities. A daily candle is one OHLCV bar
per symbol and session. It is not a trade tick.

Path A — daily batch
  Live source: Yahoo Finance chart for SYMBOL.NS (no key, unofficial).
  Optional compose source: Alpha Vantage TIME_SERIES_DAILY for the US list.
  Grain: one daily OHLCV candle per symbol and trading date.
  Canonical key: (symbol, date).

Path B — quote snapshot
  Source: Alpha Vantage GLOBAL_QUOTE, US symbols only.
  Grain: one quote snapshot per poll.
  Canonical key: (symbol, polled_at).
  This is not a continuous exchange tick feed and is not merged into the
  daily table.

Quota (Alpha Vantage free tier, as documented by the provider):
  5 requests / minute, 25 requests / day.
  Planned use stays inside that cap, including a retry reserve.
  The NSE Yahoo path does not spend this budget.
"""

from datetime import datetime, timedelta

from src.markets.nse import NSE_SYMBOLS, is_nse_trading_day

# Default universe for the local fixture, completeness gate, and live NSE run.
BATCH_SYMBOLS = list(NSE_SYMBOLS)

# Alpha Vantage TIME_SERIES_DAILY coverage used by the Kafka producer.
# These are not sent to the NSE client.
US_BATCH_SYMBOLS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "TSLA",
    "NVDA",
    "INTC",
    "JPM",
    "V",
]

# One GLOBAL_QUOTE cycle uses a subset so the batch and the quote path
# can both run on the free-tier daily cap.
QUOTE_SNAPSHOT_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]

ALPHA_VANTAGE_DAILY_LIMIT = 25
ALPHA_VANTAGE_PER_MINUTE_LIMIT = 5
# 60 seconds / 5 calls. Spacing is what enforces the per-minute cap.
API_CALL_DELAY_SECONDS = 12

BATCH_REQUESTS_PER_RUN = len(US_BATCH_SYMBOLS)  # one TIME_SERIES_DAILY call per US symbol
QUOTE_CYCLES_PER_DAY = 1
QUOTE_REQUESTS_PER_DAY = len(QUOTE_SNAPSHOT_SYMBOLS) * QUOTE_CYCLES_PER_DAY
RETRY_RESERVE = 5
# 10 batch + 5 quotes + 5 retries = 20, which is under the 25/day cap.
# The remaining 5 requests are unused headroom, not a second quote cycle.
PLANNED_DAILY_REQUESTS = BATCH_REQUESTS_PER_RUN + QUOTE_REQUESTS_PER_DAY + RETRY_RESERVE

if PLANNED_DAILY_REQUESTS > ALPHA_VANTAGE_DAILY_LIMIT:
    raise RuntimeError(
        f"Planned Alpha Vantage usage {PLANNED_DAILY_REQUESTS} exceeds "
        f"the daily limit {ALPHA_VANTAGE_DAILY_LIMIT}."
    )

DAILY_CANDLE_KEY = ("symbol", "date")
QUOTE_SNAPSHOT_KEY = ("symbol", "polled_at")

# NYSE cash-market full-day closures for 2026.
# Source: NYSE Group holiday calendar (nyse.com/markets/hours-calendars).
# Early closes are still trading sessions and stay in the expected set:
#   2026-11-27 (day after Thanksgiving), 2026-12-24 (Christmas Eve).
NYSE_FULL_HOLIDAYS_2026 = frozenset({
    "2026-01-01",  # New Year's Day
    "2026-01-19",  # Martin Luther King, Jr. Day
    "2026-02-16",  # Washington's Birthday
    "2026-04-03",  # Good Friday
    "2026-05-25",  # Memorial Day
    "2026-06-19",  # Juneteenth
    "2026-07-03",  # Independence Day observed
    "2026-09-07",  # Labor Day
    "2026-11-26",  # Thanksgiving Day
    "2026-12-25",  # Christmas Day
})

NYSE_EARLY_CLOSES_2026 = frozenset({
    "2026-11-27",
    "2026-12-24",
})

# What a reviewer can actually run from this repository.
RUNTIME_STATUS = {
    "local_batch_fixture": "verified_local",
    "live_nse_batch": "yahoo_chart_no_key",
    "alphavantage_client": "code_only_requires_api_key",
    "kafka_minio_compose": "configured_not_exercised_by_local_demo",
    "spark_batch_job": "code_only_requires_spark_cluster",
    "snowflake_merge": "cloud_dependent",
    "airflow_dag": "code_only_requires_compose",
    "quote_snapshot_producer": "code_only_quota_capped",
    "dashboard_market_charts": "reads_nse_market_json_or_labeled_fallback",
    "dashboard_pipeline_kpis": "reads_local_run_manifest",
    "alert_engine": "implemented_rule_based",
}


def quota_budget() -> dict:
    return {
        "daily_limit": ALPHA_VANTAGE_DAILY_LIMIT,
        "per_minute_limit": ALPHA_VANTAGE_PER_MINUTE_LIMIT,
        "call_spacing_seconds": API_CALL_DELAY_SECONDS,
        "batch_requests_per_run": BATCH_REQUESTS_PER_RUN,
        "quote_requests_per_day": QUOTE_REQUESTS_PER_DAY,
        "quote_cycles_per_day": QUOTE_CYCLES_PER_DAY,
        "quote_symbols": list(QUOTE_SNAPSHOT_SYMBOLS),
        "retry_reserve": RETRY_RESERVE,
        "planned_daily_requests": PLANNED_DAILY_REQUESTS,
        "unused_headroom": ALPHA_VANTAGE_DAILY_LIMIT - PLANNED_DAILY_REQUESTS,
        "guarantee": (
            "Manual offset commit is at-least-once. A successful sink write "
            "followed by a failed commit can be replayed. Canonical rows are "
            "deduplicated on (symbol, date) before the gold load. This is not "
            "exactly-once delivery."
        ),
    }


def parse_iso_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def is_nyse_trading_day(value: str) -> bool:
    """Weekdays that are not on the 2026 NYSE full-closure list."""
    day = parse_iso_date(value)
    if day.weekday() >= 5:
        return False
    if value in NYSE_FULL_HOLIDAYS_2026:
        return False
    return True


def is_expected_trading_day(value: str) -> bool:
    """Project calendar: NSE cash equities. See src/markets/nse.py."""
    return is_nse_trading_day(value)


def iter_trading_days_ending(end_date: str, count: int) -> list:
    """Return `count` expected sessions ending at end_date, ascending."""
    if count < 1:
        return []
    if not is_expected_trading_day(end_date):
        return []
    found = []
    cursor = parse_iso_date(end_date)
    # Bound the walk so a bad holiday list cannot loop forever.
    for _ in range(count * 4 + 14):
        key = cursor.strftime("%Y-%m-%d")
        if is_expected_trading_day(key):
            found.append(key)
            if len(found) == count:
                break
        cursor -= timedelta(days=1)
    found.reverse()
    return found
