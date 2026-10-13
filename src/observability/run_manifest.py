"""Write one JSON manifest per pipeline execution."""

import json
from pathlib import Path
from typing import Any, Dict


def write_manifest(root: Path, manifest: Dict[str, Any]) -> Path:
    root = Path(root)
    latest = root / "artifacts" / "latest_run_manifest.json"
    history = root / "data" / "warehouse" / "runs" / f"{manifest['run_id']}.json"
    for path in (latest, history):
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(manifest, indent=2))
        temporary.replace(path)
    return latest
