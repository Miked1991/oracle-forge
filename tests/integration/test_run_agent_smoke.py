"""Smoke tests for run_agent / run_agent_contract with mock MCP (Phase F)."""

from __future__ import annotations

import pytest

from agent.llm_reasoner import LLMGuidance
from agent.main import run_agent, run_agent_contract


@pytest.fixture
def mock_mcp_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_FORGE_MOCK_MODE", "true")
    monkeypatch.setenv("ORACLE_FORGE_ALLOW_MOCK_FALLBACK", "false")


def _fake_openrouter_plan(self, question, available_databases, context):
    """Avoid live OpenRouter calls in smoke tests."""
    dbs = list(available_databases)[:1] if available_databases else ["mongodb"]
    return LLMGuidance(
        selected_databases=dbs,
        rationale="smoke-test routing stub",
        query_hints={},
        model="openai/gpt-4o-mini",
        used_llm=True,
        selected_tables={dbs[0]: []} if dbs else {},
    )


@pytest.fixture
def mock_openrouter_routing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("agent.main.OpenRouterRoutingReasoner.plan", _fake_openrouter_plan)


def test_run_agent_mock_returns_observability_fields(mock_mcp_env: None, mock_openrouter_routing: None) -> None:
    out = run_agent(
        "List subscriber revenue",
        ["postgresql", "mongodb"],
        {},
        dataset_id="smoke_dataset",
    )
    assert out.get("dataset_id") == "smoke_dataset"
    assert "status" in out
    assert "closed_loop" in out
    if out.get("status") == "success":
        assert "merge_info" in out
        assert out.get("merge_info") is None or isinstance(out.get("merge_info"), dict)


def test_run_agent_contract_includes_extended_fields(mock_mcp_env: None, mock_openrouter_routing: None) -> None:
    payload = {
        "question": "Count support tickets",
        "available_databases": ["mongodb"],
        "schema_info": {},
        "dataset": "contract_ds",
    }
    out = run_agent_contract(payload)
    assert out.get("dataset_id") == "contract_ds"
    assert "plan" in out
    assert "validation_status" in out
    assert "predicted_queries" in out
    assert "database_results" in out
    assert isinstance(out.get("database_results"), list)
