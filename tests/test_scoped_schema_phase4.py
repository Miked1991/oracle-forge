"""Phase 4: scoped schema packages — only selected tables + registry intent summaries."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from utils.scoped_schema_pack import (
    build_scoped_schema_bundle,
    names_to_include_for_engine,
    rebuild_with_scoped_pack,
    schema_metadata_stub_from_registry,
    should_apply_table_scope,
)


def _registry_min() -> dict:
    return {
        "dataset_id": "t",
        "engines": {
            "sqlite": {
                "available": True,
                "tables": [
                    {
                        "name": "keep_me",
                        "columns": [{"name": "a", "data_type": "INT"}],
                        "intent_summary": "KEEP INTENT",
                        "row_count_estimate": 5,
                    },
                    {
                        "name": "drop_me",
                        "columns": [{"name": "b", "data_type": "TEXT"}],
                        "intent_summary": "DROP",
                    },
                ],
                "collections": [],
            }
        },
    }


def _meta_min() -> dict:
    return {
        "sqlite": {
            "tables": [
                {"name": "keep_me", "fields": {"a": "int"}},
                {"name": "drop_me", "fields": {"b": "text"}},
            ],
            "collections": [],
        }
    }


def test_schema_metadata_stub_from_registry_shape() -> None:
    meta = schema_metadata_stub_from_registry(_registry_min())
    assert "sqlite" in meta
    assert any(t["name"] == "keep_me" for t in meta["sqlite"]["tables"])


def test_should_apply_table_scope_requires_nonempty_list() -> None:
    assert should_apply_table_scope({}, ["sqlite"]) is False
    assert should_apply_table_scope({"sqlite": []}, ["sqlite"]) is False
    assert should_apply_table_scope({"sqlite": ["keep_me"]}, ["sqlite"]) is True


def test_names_to_include_intersection() -> None:
    meta = _meta_min()["sqlite"]
    t, c = names_to_include_for_engine(
        "sqlite",
        {"sqlite": ["keep_me"]},
        meta,
        scope_active=True,
    )
    assert t == {"keep_me"}
    assert c == set()


def test_build_scoped_only_includes_listed_tables_and_intent() -> None:
    reg = _registry_min()
    meta = _meta_min()
    b = build_scoped_schema_bundle(
        meta,
        reg,
        ["sqlite"],
        {"sqlite": ["keep_me"]},
        "t",
        playbook=None,
    )
    assert b.get("scoped") is True
    names = [x["name"] for x in b["engines"]["sqlite"]["tables"]]
    assert names == ["keep_me"]
    assert b["engines"]["sqlite"]["tables"][0].get("intent_summary") == "KEEP INTENT"


def test_rebuild_context_sets_scoped_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_FORGE_DISABLE_SCOPED_SCHEMA_LOG", "1")
    reg_path = tmp_path / "artifacts" / "schema_registry" / "t.json"
    reg_path.parent.mkdir(parents=True)
    reg_path.write_text(json.dumps(_registry_min()), encoding="utf-8")

    ctx = {
        "schema_metadata": _meta_min(),
        "llm_guidance": {
            "selected_databases": ["sqlite"],
            "selected_tables": {"sqlite": ["keep_me"]},
        },
        "dataset_playbook": None,
        "context_layers": {"schema_metadata": {"runtime/schema_metadata.json": "{}"}},
    }
    rebuild_with_scoped_pack(ctx, ["sqlite"], "t", repo_root=tmp_path)
    assert ctx.get("schema_bundle_mode") == "scoped_tables"
    bundle = ctx["schema_bundle"]
    names = [x["name"] for x in bundle["engines"]["sqlite"]["tables"]]
    assert names == ["keep_me"]
    assert "drop_me" not in json.dumps(bundle)


def test_unknown_table_names_fall_back_to_full_engine() -> None:
    """If routing lists names that match nothing, include all tables (graceful)."""
    b = build_scoped_schema_bundle(
        _meta_min(),
        _registry_min(),
        ["sqlite"],
        {"sqlite": ["nope"]},
        "t",
        playbook=None,
    )
    names = sorted([x["name"] for x in b["engines"]["sqlite"]["tables"]])
    assert names == ["drop_me", "keep_me"]
