"""Phase 8: structured log for tool outputs, merge strategy, and shaped answer."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def execution_merge_log_enabled() -> bool:
    return os.getenv("ORACLE_FORGE_EXECUTION_MERGE_LOG", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def append_execution_merge_log(
    repo_root: Path,
    entry: Dict[str, Any],
    *,
    path_override: Optional[str] = None,
) -> None:
    if not execution_merge_log_enabled():
        return
    raw = (path_override or os.getenv("ORACLE_FORGE_EXECUTION_MERGE_LOG_PATH", "").strip()).strip()
    rel = Path(raw) if raw else repo_root / "logs" / "execution_merge.jsonl"
    path = rel if rel.is_absolute() else (repo_root / rel)
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"timestamp_utc": datetime.now(timezone.utc).isoformat(), **entry}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def truncate_tool_preview(obj: Any, max_chars: int = 12000) -> Any:
    if obj is None:
        return None
    s = json.dumps(obj, ensure_ascii=False) if isinstance(obj, (dict, list)) else str(obj)
    if len(s) <= max_chars:
        return obj if isinstance(obj, (dict, list)) else s
    return s[: max_chars - 24] + "\n... (truncated)"
