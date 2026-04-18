"""Structured JSONL log for Phase 5 query builder (prompts, responses, schema slices)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def query_builder_log_enabled() -> bool:
    return os.getenv("ORACLE_FORGE_QUERY_BUILDER_LOG", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def append_query_builder_log(
    repo_root: Path,
    entry: Dict[str, Any],
    *,
    path_override: Optional[str] = None,
) -> None:
    """Append one JSON line. Disable with ORACLE_FORGE_QUERY_BUILDER_LOG=false."""
    if not query_builder_log_enabled():
        return
    raw = (path_override or os.getenv("ORACLE_FORGE_QUERY_BUILDER_LOG_PATH", "").strip()).strip()
    rel = Path(raw) if raw else repo_root / "logs" / "query_builder.jsonl"
    path = rel if rel.is_absolute() else (repo_root / rel)
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"timestamp_utc": datetime.now(timezone.utc).isoformat(), **entry}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def truncate_for_log(obj: Any, max_chars: int = 8000) -> Any:
    """Keep logs readable; cap serialized length."""
    if obj is None:
        return None
    s = json.dumps(obj, ensure_ascii=False) if isinstance(obj, (dict, list)) else str(obj)
    if len(s) <= max_chars:
        return obj
    return s[: max_chars - 20] + "\n... (truncated)"
