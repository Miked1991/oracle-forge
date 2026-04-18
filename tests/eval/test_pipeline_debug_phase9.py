"""Phase 9: pipeline debug snapshot + eval wiring."""

from __future__ import annotations

import pytest

from utils.pipeline_debug_snapshot import extract_pipeline_debug


def test_extract_pipeline_debug_minimal_shape() -> None:
    outcome = {
        "status": "success",
        "architecture_disclosure": {"llm_model": "m", "llm_used_for_reasoning": True, "mcp_tools_used": ["t"]},
        "predicted_queries": [{"database": "postgresql"}],
        "validation_status": {"valid": True},
        "semantic_alignment": {"ok": True},
        "merge_info": {"merge_strategy": "single_db"},
        "query_trace": [
            {
                "tool_used": "postgres_sql_query",
                "raw_query": "SELECT 1",
                "duration_ms": 1,
                "success": True,
            }
        ],
        "closed_loop": {"ok": True},
        "plan": {
            "steps": [{"database": "postgresql", "dialect": "sql", "query_payload": {"sql": "SELECT 1"}}],
            "query_pipeline": {
                "metadata": {"four_phase": True},
                "trace": [
                    {"phase": "schema_link", "engine": "postgresql", "scoped": ["a"], "readiness_ok": True},
                    {"phase": "query_build", "engine": "postgresql", "builder_kind": "single_table", "attempts_used": 1},
                ],
            },
            "preexec_repair_trace": [{"attempt": 0, "resolved": True}],
        },
        "context_layers_used": ["schema_metadata"],
    }
    dbg = extract_pipeline_debug(outcome, schema_info={"hint": "x"})
    assert dbg["routing"]["llm_model"] == "m"
    assert "postgresql" in dbg["table_selection"]
    assert dbg["repair_attempts"]["preexec_repair_trace"]
    assert dbg["execution"]["tool_traces"][0]["raw_query"] == "SELECT 1"
    assert dbg["schema_info_snapshot"] and "hint" in dbg["schema_info_snapshot"]


def test_run_agent_contract_pipeline_debug_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_FORGE_PIPELINE_DEBUG", "true")
    from agent.main import run_agent_contract

    def fake_run_agent(**_: object) -> dict:
        return {
            "answer": 1,
            "query_trace": [],
            "confidence": 1.0,
            "status": "success",
            "closed_loop": {},
            "dataset_id": None,
            "merge_info": None,
            "plan": {},
            "validation_status": {},
            "semantic_alignment": {},
            "error": None,
            "error_type": None,
            "metrics": {},
            "predicted_queries": [],
        }

    monkeypatch.setattr("agent.main.run_agent", fake_run_agent)
    out = run_agent_contract({"question": "q", "schema_info": {"k": 1}})
    assert "pipeline_debug" in out
    assert out["pipeline_debug"]["schema_info_snapshot"]
