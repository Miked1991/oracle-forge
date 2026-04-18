"""Tests for ``utils/llm_io_log``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from utils.llm_io_log import append_llm_io_log, llm_io_log_enabled, truncate_message_contents


def test_truncate_message_contents_respects_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_FORGE_LLM_IO_MAX_MSG_CHARS", "20")
    msgs = [{"role": "user", "content": "x" * 100}]
    out = truncate_message_contents(msgs)
    assert len(out[0]["content"]) < 100
    assert "truncated" in out[0]["content"]


def test_append_llm_io_log_writes_jsonl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_FORGE_LLM_IO_LOG", "true")
    log_path = tmp_path / "llm_io.jsonl"
    append_llm_io_log(
        tmp_path,
        {"phase": "routing", "messages": [{"role": "user", "content": "hi"}], "model": "m"},
        path_override=str(log_path),
    )
    assert log_path.is_file()
    row = json.loads(log_path.read_text(encoding="utf-8").strip())
    assert row["phase"] == "routing"
    assert "timestamp_utc" in row


def test_llm_io_log_disabled_no_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_FORGE_LLM_IO_LOG", "false")
    assert not llm_io_log_enabled()
    log_path = tmp_path / "skip.jsonl"
    append_llm_io_log(
        tmp_path,
        {"phase": "routing", "messages": []},
        path_override=str(log_path),
    )
    assert not log_path.exists()
