"""Phase 7: pre-execution repair loop in QueryPlanner (no tool execution)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from agent.planner import QueryPlanner


def test_preexec_repair_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ORACLE_FORGE_PREEXEC_REPAIR_MAX", "3")
    monkeypatch.setenv("ORACLE_FORGE_PREEXEC_REPAIR_LOG", "false")
    calls: List[int] = []

    class FakeGen:
        def __init__(self, _root: Path) -> None:
            pass

        def generate_steps(
            self,
            question: str,
            selected: List[str],
            context: Dict[str, Any],
            replan_notes: Optional[List[str]] = None,
        ) -> Dict[str, Any]:
            calls.append(len(calls))
            if len(calls) == 1:
                return {
                    "schema_gate_failed": True,
                    "gate_detail": "test_gate",
                    "steps": [],
                }
            return {
                "steps": [
                    {
                        "database": "postgresql",
                        "dialect": "sql",
                        "sql": "SELECT 1 AS n",
                    }
                ],
                "model": "test",
            }

    monkeypatch.setattr("agent.planner.LLMQueryGenerator", FakeGen)
    monkeypatch.setattr("agent.planner._llm_sql_enabled", lambda: True)

    ctx = {
        "schema_metadata": {
            "postgresql": {"tables": [{"name": "review", "fields": {"n": "int"}}]},
        },
        "schema_bundle_json": "{}",
        "schema_bundle": {"engines": {"postgresql": {"tables": [{"name": "review"}]}}},
    }
    planner = QueryPlanner(ctx)
    plan = planner.create_plan("q", ["postgresql"])
    assert plan.get("steps")
    assert len(calls) == 2
    trace = plan.get("preexec_repair_trace") or []
    assert len(trace) == 2
    assert trace[0]["resolved"] is False
    assert trace[1]["resolved"] is True


def test_preexec_repair_exhausted_returns_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_FORGE_PREEXEC_REPAIR_MAX", "1")
    monkeypatch.setenv("ORACLE_FORGE_PREEXEC_REPAIR_LOG", "false")

    class AlwaysFail:
        def __init__(self, _root: Path) -> None:
            pass

        def generate_steps(self, *a: Any, **k: Any) -> Dict[str, Any]:
            return {"schema_gate_failed": True, "gate_detail": "always", "steps": []}

    monkeypatch.setattr("agent.planner.LLMQueryGenerator", AlwaysFail)
    monkeypatch.setattr("agent.planner._llm_sql_enabled", lambda: True)

    planner = QueryPlanner({"schema_metadata": {}, "schema_bundle_json": "{}"})
    plan = planner.create_plan("q", ["postgresql"])
    assert plan.get("schema_gate_failed") is True
    assert plan.get("preexec_repair_exhausted") is True
