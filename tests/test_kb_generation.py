"""Phase 2: KB generated from schema registry + context builder trust tiers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agent.context_builder import ContextBuilder
from utils.schema_registry.kb_generator import (
    authoritative_kb_file_path,
    render_authoritative_markdown,
    write_authoritative_kb,
)


def _minimal_registry() -> dict:
    return {
        "schema_registry_version": "1.0",
        "dataset_id": "test_ds",
        "built_at_utc": "2026-01-01T00:00:00+00:00",
        "sources": {"datasets_config": "eval/datasets.json"},
        "verified_joins": [],
        "engines": {
            "sqlite": {
                "available": True,
                "engine": "sqlite",
                "tables": [
                    {
                        "name": "widgets",
                        "kind": "table",
                        "columns": [
                            {
                                "name": "id",
                                "data_type": "INTEGER",
                                "nullable": False,
                                "is_primary_key": True,
                            },
                            {"name": "sku", "data_type": "TEXT", "nullable": True, "is_primary_key": False},
                        ],
                        "primary_key": ["id"],
                        "foreign_keys": [],
                    }
                ],
                "collections": [],
            }
        },
        "dataset_intent_summary": "Test dataset.",
    }


def test_render_authoritative_markdown_contains_registry_tables_and_pk() -> None:
    md = render_authoritative_markdown(_minimal_registry())
    assert "AUTHORITATIVE" in md
    assert "`widgets`" in md
    assert "`id`" in md
    assert "primary_key" in md


def test_write_authoritative_kb_matches_registry_roundtrip(tmp_path: Path) -> None:
    reg_path = tmp_path / "artifacts" / "schema_registry" / "roundtrip.json"
    reg_path.parent.mkdir(parents=True)
    reg = _minimal_registry()
    reg["dataset_id"] = "roundtrip"
    reg_path.write_text(json.dumps(reg), encoding="utf-8")

    out, summary = write_authoritative_kb(
        "roundtrip",
        tmp_path,
        registry=reg,
        log=False,
    )
    assert summary["status"] == "ok"
    text = out.read_text(encoding="utf-8")
    assert "widgets" in text
    # Column line must reflect registry
    assert "sku" in text


def test_context_builder_loads_authoritative_when_file_exists(tmp_path: Path) -> None:
    did = "ctx_test"
    kb_path = authoritative_kb_file_path(did, tmp_path)
    kb_path.parent.mkdir(parents=True, exist_ok=True)
    reg = _minimal_registry()
    reg["dataset_id"] = did
    kb_path.write_text(render_authoritative_markdown(reg), encoding="utf-8")

    cb = ContextBuilder(repo_root=tmp_path)
    ctx = cb.build(
        "q?",
        ["sqlite"],
        {},
        {},
        dataset_id=did,
    )
    layers = ctx["context_layers"]
    assert "authoritative_registry" in layers
    assert any("widgets" in v for v in layers["authoritative_registry"].values())
    assert ctx["kb_generation"]["authoritative_registry_loaded"] is True
    assert "authoritative_registry" in (ctx.get("kb_trust_tiers") or {}).get("authoritative", [])


def test_context_builder_hint_when_authoritative_missing(tmp_path: Path) -> None:
    cb = ContextBuilder(repo_root=tmp_path)
    ctx = cb.build("q?", ["sqlite"], {}, {}, dataset_id="missing_ds")
    assert ctx["kb_generation"]["authoritative_registry_loaded"] is False
    assert ctx["kb_generation"]["hint_if_missing"]
