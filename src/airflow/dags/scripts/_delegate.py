"""Run the canonical pipeline script so the Airflow copies cannot drift."""

import runpy
import sys
from pathlib import Path


def delegate(relative_path: str) -> None:
    here = Path(__file__).resolve()
    target = None
    for parent in here.parents:
        candidate = parent / relative_path
        if candidate.exists():
            target = candidate
            break
    if target is None:
        raise SystemExit(f"Canonical script not found: {relative_path}")
    sys.argv[0] = str(target)
    runpy.run_path(str(target), run_name="__main__")
