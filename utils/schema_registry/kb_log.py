"""Append-only JSONL for KB generation events (`logs/kb_generation.jsonl`)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def log_kb_generation_event(
    entry: Dict[str, Any],
    *,
    repo_root: Optional[Path] = None,
    log_path: Optional[Path] = None,
) -> Path:
    root = repo_root or _repo_root()
    path = log_path or (root / "logs" / "kb_generation.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    row = dict(entry)
    row.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    if os.getenv("ORACLE_FORGE_DISABLE_KB_GENERATION_LOG", "").lower() in {"1", "true", "yes", "on"}:
        return path
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path
