"""LLM query generator pre-execution schema validation loop (no live LLM)."""

from __future__ import annotations

from pathlib import Path

import pytest

from agent.llm_query_generator import LLMQueryGenerator


def test_generate_steps_retries_on_schema_validation_until_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_FORGE_SQL_BUILDER_PROMPT_LOG", "false")
    monkeypatch.setenv("ORACLE_FORGE_PREEXEC_VALIDATION_LOG", "false")
    monkeypatch.setenv("ORACLE_FORGE_LLM_PREEXEC_SCHEMA_RETRIES", "3")
    gen = LLMQueryGenerator(repo_root=Path(__file__).resolve().parents[2])
    gen.provider = "openrouter"
    gen.openrouter_api_key = "test-key-not-used"

    schema = {
        "postgresql": {
            "tables": [{"name": "review", "fields": {"stars": "integer", "business_id": "text"}}]
        }
    }
    context = {
        "schema_metadata": schema,
        "context_layers": {},
        "llm_guidance": {},
        "schema_bundle_json": "{}",
    }

    calls: list[int] = []

    def fake_openrouter(system: str, user: str, **_kwargs: object) -> dict:
        calls.append(1)
        if len(calls) == 1:
            return {
                "steps": [
                    {
                        "database": "postgresql",
                        "dialect": "sql",
                        "sql": "SELECT AVG(rating) FROM review",
                    }
                ]
            }
        return {
            "steps": [
                {
                    "database": "postgresql",
                    "dialect": "sql",
                    "sql": "SELECT AVG(stars) FROM review",
                }
            ]
        }

    monkeypatch.setattr(gen, "_openrouter_json", fake_openrouter)

    out = gen.generate_steps("avg stars?", ["postgresql"], context)
    assert out is not None
    assert out["steps"][0]["sql"] == "SELECT AVG(stars) FROM review"
    assert out.get("preexec_schema_attempts") == 2
    assert len(calls) == 2


def test_generate_steps_returns_none_after_exhausting_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_FORGE_SQL_BUILDER_PROMPT_LOG", "false")
    monkeypatch.setenv("ORACLE_FORGE_PREEXEC_VALIDATION_LOG", "false")
    monkeypatch.setenv("ORACLE_FORGE_LLM_PREEXEC_SCHEMA_RETRIES", "1")
    gen = LLMQueryGenerator(repo_root=Path(__file__).resolve().parents[2])
    gen.provider = "openrouter"
    gen.openrouter_api_key = "test-key-not-used"

    schema = {
        "postgresql": {
            "tables": [{"name": "review", "fields": {"stars": "integer"}}]
        }
    }
    context = {
        "schema_metadata": schema,
        "context_layers": {},
        "llm_guidance": {},
        "schema_bundle_json": "{}",
    }

    def always_bad(system: str, user: str, **_kwargs: object) -> dict:
        return {
            "steps": [
                {
                    "database": "postgresql",
                    "dialect": "sql",
                    "sql": "SELECT phantom FROM review",
                }
            ]
        }

    monkeypatch.setattr(gen, "_openrouter_json", always_bad)

    out = gen.generate_steps("q", ["postgresql"], context)
    assert isinstance(out, dict)
    assert out.get("generation_failed") is True
    assert out.get("steps") == []
    assert out.get("pipeline_trace")
