"""Print RUN or SKIP for an execution date. Used by the Airflow tasks."""

import argparse
import sys
from pathlib import Path


def _bootstrap() -> None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "src" / "contracts" / "data_contract.py").exists():
            entry = str(parent)
            if entry not in sys.path:
                sys.path.insert(0, entry)
            return


_bootstrap()

from src.contracts.data_contract import is_expected_trading_day, is_nyse_trading_day


def decision_for(execution_date: str, market: str = "nse") -> str:
    if market == "nyse":
        return "RUN" if is_nyse_trading_day(execution_date) else "SKIP"
    if market != "nse":
        raise ValueError(f"Unknown market {market!r}.")
    return "RUN" if is_expected_trading_day(execution_date) else "SKIP"


def main() -> int:
    parser = argparse.ArgumentParser(description="Print RUN or SKIP for a session.")
    parser.add_argument("execution_date", help="YYYY-MM-DD")
    parser.add_argument("--market", choices=("nse", "nyse"), default="nse")
    args = parser.parse_args()
    try:
        print(decision_for(args.execution_date, args.market))
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
