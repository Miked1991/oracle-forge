"""Append-only JSONL for schema registry build events (`logs/schema_registry.jsonl`)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def log_schema_registry_event(
    entry: Dict[str, Any],
    *,
    repo_root: Optional[Path] = None,
    log_path: Optional[Path] = None,
) -> Path:
    """
    Append one JSON object. Expected keys include timestamp, dataset_id, question, phase,
    status, duration_ms, etc.; unknown keys are preserved.
    """
    root = repo_root or _repo_root()
    path = log_path or (root / "logs" / "schema_registry.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    row = dict(entry)
    row.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    if os.getenv("ORACLE_FORGE_DISABLE_SCHEMA_REGISTRY_LOG", "").lower() in {"1", "true", "yes", "on"}:
        return path
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path
