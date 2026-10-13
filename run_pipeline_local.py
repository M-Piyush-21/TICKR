"""
NSE daily batch.

Default: fetch live daily candles for the NSE universe and write the gold
file plus artifacts/nse_market.json. No API key.

  python3 run_pipeline_local.py
  python3 run_pipeline_local.py --execution-date 2026-10-13

A requested session that is missing is a failure. It is not replaced with
another day's candle. A weekend or a listed 2026 NSE holiday exits 0 and
does not call the source.

  python3 run_pipeline_local.py --fixture --execution-date 2026-10-13

The fixture path does not call the network. It is the offline demo.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from src.pipeline.live_nse_batch import run_live_nse_batch
from src.pipeline.local_batch import run_local_batch


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the TICKR NSE daily batch.")
    parser.add_argument(
        "--execution-date",
        default=None,
        help="Session as YYYY-MM-DD. Live mode defaults to the latest completed session shared by every symbol.",
    )
    parser.add_argument(
        "--history-sessions",
        type=int,
        default=40,
        help="Fixture only. Prior sessions used so MA15 and MA30 have inputs.",
    )
    parser.add_argument(
        "--fixture",
        action="store_true",
        help="Use deterministic candles instead of the live NSE source.",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parent

    print("=" * 70)
    if args.fixture:
        execution_date = args.execution_date or datetime.now().strftime("%Y-%m-%d")
        print("TICKR FIXTURE BATCH  |  deterministic daily candles  |  not a live API run")
        print("=" * 70)
        manifest = run_local_batch(
            root,
            execution_date,
            history_sessions=args.history_sessions,
        )
    else:
        print("TICKR NSE BATCH  |  Yahoo chart daily candles  |  unofficial, no API key")
        print("=" * 70)
        manifest = run_live_nse_batch(root, args.execution_date)

    counts = manifest.get("row_counts", {})
    print(f"Status:          {manifest['status']}")
    print(f"Execution date:  {manifest['execution_date']}")
    print(f"Session pick:    {manifest.get('session_selection', 'fixture')}")
    print(f"Run id:          {manifest['run_id']}")
    print(f"Batch rows:      {counts.get('batch', 0)}")
    print(f"Gold rows:       {counts.get('gold_total', 0)}")
    print(f"API calls:       {manifest.get('quota_calls_this_run', 0)}")
    print(f"Rejected:        {counts.get('rejected', 0)}")
    print(f"Manifest:        {manifest['manifest_path']}")
    completeness = manifest.get("completeness") or {}
    if completeness.get("reason"):
        print(f"Completeness:    {completeness['status']} — {completeness['reason']}")
    if manifest.get("reason"):
        print(f"Detail:          {manifest['reason']}")
    print("=" * 70)
    return int(manifest["exit_code"])


if __name__ == "__main__":
    sys.exit(main())
