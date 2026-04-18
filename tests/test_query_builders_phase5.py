"""Phase 5 query builder classification and prompt shape (no live LLM)."""

from __future__ import annotations

from pathlib import Path

import pytest

from agent.llm_query_generator import LLMQueryGenerator
from agent.query_builders import (
    augment_system_for_builder_kind,
    build_per_engine_user_prompt,
    classify_builder_kind,
)


def test_classify_builder_kind() -> None:
    assert classify_builder_kind([], []) == "single_table"
    assert classify_builder_kind(["a"], []) == "single_table"
    assert classify_builder_kind(["a", "b"], []) == "multi_table"
    assert classify_builder_kind(["a"], ["fix"]) == "repair"


def test_augment_system_contains_mode() -> None:
    base = "Return JSON with sql."
    assert "REPAIR" in augment_system_for_builder_kind(base, "repair")
    assert "SINGLE_TABLE" in augment_system_for_builder_kind(base, "single_table")
    assert "MULTI_TABLE" in augment_system_for_builder_kind(base, "multi_table")


def test_user_prompt_shapes_contain_contract_and_schema() -> None:
    schema = '{"tables":[{"name":"t","columns":["x"]}]}'
    for kind in ("single_table", "multi_table", "repair"):
        u = build_per_engine_user_prompt(
            kind=kind,  # type: ignore[arg-type]
            question="q?",
            contract_json='{"summary":"s"}',
            engine="postgresql",
            rationale="because",
            hints={},
            playbook_summary="",
            eng_hints=[],
            schema_json=schema,
            err_block="",
            yelp_parking_extra="",
        )
        assert "CONTRACT:" in u and "SCHEMA:" in u or "LINKED_SCHEMA:" in u
        assert schema in u


def test_generate_steps_per_database_uses_expected_query_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ORACLE_FORGE_SQL_BUILDER_PROMPT_LOG", "false")
    monkeypatch.setenv("ORACLE_FORGE_QUERY_BUILDER_LOG", "false")
    monkeypatch.setenv("ORACLE_FORGE_PREEXEC_VALIDATION_LOG", "false")
    monkeypatch.setenv("ORACLE_FORGE_LLM_PREEXEC_SCHEMA_RETRIES", "3")
    gen = LLMQueryGenerator(repo_root=Path(__file__).resolve().parents[1])
    gen.provider = "openrouter"
    gen.openrouter_api_key = "test-key-not-used"

    captured: dict[str, str] = {}

    def capture_openrouter(system: str, user: str, **_kwargs: object) -> dict:
        captured["system"] = system
        captured["user"] = user
        return {
            "sql": "SELECT stars FROM review",
        }

    monkeypatch.setattr(gen, "_openrouter_json", capture_openrouter)

    schema = {
        "postgresql": {
            "tables": [{"name": "review", "fields": {"stars": "integer", "business_id": "text"}}]
        }
    }
    context = {
        "schema_metadata": schema,
        "context_layers": {},
        "llm_guidance": {"rationale": "r", "query_hints": {}},
        "schema_bundle_json": "{}",
    }

    out = gen.generate_steps("stars?", ["postgresql"], context)
    assert out is not None
    assert out["steps"][0]["sql"].startswith("SELECT")
    assert "SINGLE_TABLE" in captured["system"] or "MULTI_TABLE" in captured["system"]
    assert "CONTRACT:" in captured["user"]
