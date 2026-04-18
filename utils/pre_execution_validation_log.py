"""Structured JSONL log for deterministic pre-execution validation (Phase 6)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def pre_execution_validation_log_enabled() -> bool:
    return os.getenv("ORACLE_FORGE_PREEXEC_VALIDATION_LOG", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def append_pre_execution_validation_log(
    repo_root: Path,
    entry: Dict[str, Any],
    *,
    path_override: Optional[str] = None,
) -> None:
    """Append one JSON line to ``logs/pre_execution_validator.jsonl``."""
    if not pre_execution_validation_log_enabled():
        return
    raw = (path_override or os.getenv("ORACLE_FORGE_PREEXEC_VALIDATION_LOG_PATH", "").strip()).strip()
    rel = Path(raw) if raw else repo_root / "logs" / "pre_execution_validator.jsonl"
    path = rel if rel.is_absolute() else (repo_root / rel)
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"timestamp_utc": datetime.now(timezone.utc).isoformat(), **entry}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
