"""Phase 7: JSONL log for pre-execution repair attempts (no tool execution)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def preexec_repair_log_enabled() -> bool:
    return os.getenv("ORACLE_FORGE_PREEXEC_REPAIR_LOG", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def append_preexec_repair_log(
    repo_root: Path,
    entry: Dict[str, Any],
    *,
    path_override: Optional[str] = None,
) -> None:
    if not preexec_repair_log_enabled():
        return
    raw = (path_override or os.getenv("ORACLE_FORGE_PREEXEC_REPAIR_LOG_PATH", "").strip()).strip()
    rel = Path(raw) if raw else repo_root / "logs" / "preexec_repair.jsonl"
    path = rel if rel.is_absolute() else (repo_root / rel)
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"timestamp_utc": datetime.now(timezone.utc).isoformat(), **entry}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def preexec_repair_max_attempts() -> int:
    return max(0, int(os.getenv("ORACLE_FORGE_PREEXEC_REPAIR_MAX", "3")))
