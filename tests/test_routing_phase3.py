"""Phase 3: registry-backed routing summaries and selected_tables validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from agent.llm_reasoner import LLMRoutingFailed, OpenRouterRoutingReasoner
from utils.schema_registry.routing_compact import (
    compact_registry_routing_summary,
    filter_selected_tables_to_registry,
    load_registry_json_optional,
)


def _registry_bookreview_like() -> dict:
    return {
        "dataset_id": "bookreview",
        "schema_registry_version": "1.0",
        "engines": {
            "sqlite": {
                "available": True,
                "tables": [{"name": "review", "columns": [{"name": "rating"}]}],
                "collections": [],
            },
            "postgresql": {
                "available": True,
                "tables": [{"name": "dim_x", "columns": []}],
                "collections": [],
            },
        },
    }


@pytest.mark.parametrize(
    "dataset_marker,expect_substr",
    [
        ("bookreview", "review"),
        ("yelp", "business"),
        ("agnews", "article"),
    ],
)
def test_compact_summary_matches_registry_tables(
    dataset_marker: str,
    expect_substr: str,
    tmp_path: Path,
) -> None:
    """When a real registry artifact exists, compact summary mentions expected tables."""
    repo = Path(__file__).resolve().parents[1]
    reg_path = repo / "artifacts" / "schema_registry" / f"{dataset_marker}.json"
    if not reg_path.is_file():
        pytest.skip(f"missing registry artifact: {reg_path}")
    reg = load_registry_json_optional(repo, dataset_marker)
    assert reg is not None
    text = compact_registry_routing_summary(reg, ["sqlite", "duckdb", "postgresql", "mongodb"])
    assert expect_substr.lower() in text.lower()


def test_filter_selected_tables_drops_unknown_names() -> None:
    reg = _registry_bookreview_like()
    raw = {"sqlite": ["review", "nope_table"], "postgresql": ["dim_x"]}
    out = filter_selected_tables_to_registry(raw, reg, ["sqlite", "postgresql"])
    assert out["sqlite"] == ["review"]
    assert out["postgresql"] == ["dim_x"]


def test_openrouter_plan_parses_selected_tables(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Mock OpenRouter: ensure selected_tables are normalized against registry."""
    reg = _registry_bookreview_like()
    reg_path = tmp_path / "artifacts" / "schema_registry" / "bookreview.json"
    reg_path.parent.mkdir(parents=True)
    import json

    reg_path.write_text(json.dumps(reg), encoding="utf-8")

    def fake_openrouter(self: OpenRouterRoutingReasoner, prompt: str, **_kwargs: object):
        payload = {
            "selected_databases": ["sqlite"],
            "selected_tables": {"sqlite": ["review", "bogus"]},
            "rationale": "test",
            "query_hints": {},
        }
        return payload, json.dumps(payload)

    monkeypatch.setattr(OpenRouterRoutingReasoner, "_plan_with_openrouter", fake_openrouter)
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test-not-real")

    r = OpenRouterRoutingReasoner(repo_root=tmp_path)
    ctx = {
        "schema_metadata": {"sqlite": {"tables": [{"name": "review"}], "collections": []}},
        "user_question": "q",
        "dataset_id": "bookreview",
        "context_layers": {},
        "schema_bundle_json": "{}",
    }
    g = r.plan("How many reviews?", ["sqlite"], ctx)
    assert g.selected_databases == ["sqlite"]
    assert g.selected_tables.get("sqlite") == ["review"]


def test_openrouter_fail_fast_logs_and_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def boom(self: OpenRouterRoutingReasoner, prompt: str, **_kwargs: object):
        raise LLMRoutingFailed("simulated")

    monkeypatch.setattr(OpenRouterRoutingReasoner, "_plan_with_openrouter", boom)
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test-not-real")
    log_calls: list = []
    monkeypatch.setattr(
        "agent.llm_reasoner.log_routing_event",
        lambda entry, repo_root=None, log_path=None: log_calls.append(entry) or tmp_path / "x.jsonl",
    )

    r = OpenRouterRoutingReasoner(repo_root=tmp_path)
    with pytest.raises(LLMRoutingFailed):
        r.plan("q?", ["sqlite"], {"schema_metadata": {}, "user_question": "q", "context_layers": {}, "dataset_id": None})
    assert any(c.get("status") == "error" for c in log_calls)
