"""
Append-only JSONL of LLM **inputs** (messages + request params) for routing and SQL/query generation.

Default path: ``logs/llm_io.jsonl``. Disable with ``ORACLE_FORGE_LLM_IO_LOG=false``.
Override path with ``ORACLE_FORGE_LLM_IO_LOG_PATH`` (relative to repo root or absolute).

Never logs API keys (only JSON body fields sent after auth headers).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def llm_io_log_enabled() -> bool:
    return os.getenv("ORACLE_FORGE_LLM_IO_LOG", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _max_chars_per_message() -> int:
    raw = os.getenv("ORACLE_FORGE_LLM_IO_MAX_MSG_CHARS", "").strip()
    if not raw:
        return 2_000_000
    try:
        return max(0, int(raw, 10))
    except ValueError:
        return 2_000_000


def truncate_message_contents(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Copy messages; truncate string ``content`` per env to keep JSONL bounded."""
    cap = _max_chars_per_message()
    if cap == 0:
        return list(messages)
    out: List[Dict[str, Any]] = []
    for m in messages:
        if not isinstance(m, dict):
            continue
        role = m.get("role")
        content = m.get("content")
        if isinstance(content, str) and len(content) > cap:
            content = content[: max(0, cap - 30)] + "\n... (truncated)"
        item = {"role": role, "content": content}
        out.append(item)
    return out


def append_llm_io_log(
    repo_root: Path,
    entry: Dict[str, Any],
    *,
    path_override: Optional[str] = None,
) -> None:
    if not llm_io_log_enabled():
        return
    raw = (path_override or os.getenv("ORACLE_FORGE_LLM_IO_LOG_PATH", "").strip()).strip()
    rel = Path(raw) if raw else repo_root / "logs" / "llm_io.jsonl"
    path = rel if rel.is_absolute() else (repo_root / rel)
    path.parent.mkdir(parents=True, exist_ok=True)
    msgs = entry.get("messages")
    if isinstance(msgs, list):
        entry = {**entry, "messages": truncate_message_contents(msgs)}
    row = {"timestamp_utc": datetime.now(timezone.utc).isoformat(), **entry}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
