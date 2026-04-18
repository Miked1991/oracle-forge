"""Phase 6: deterministic pre-execution validation (registry joins, Mongo $lookup, logging)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agent.query_safety import validate_llm_generated_steps, validate_mongo_pipeline, validate_sql


@pytest.fixture(autouse=True)
def _strict_sql_columns_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_FORGE_STRICT_SQL_COLUMNS", "true")


def test_verified_join_rejects_unlisted_pair() -> None:
    schema = {
        "_validation_registry": {
            "verified_joins": [{"left": "business", "right": "review"}],
            "dataset_id": "t",
        },
        "postgresql": {
            "tables": [
                {"name": "business", "fields": {"business_id": "text", "name": "text"}},
                {"name": "review", "fields": {"business_id": "text", "stars": "int"}},
                {"name": "user", "fields": {"user_id": "text"}},
            ]
        },
    }
    ok, msg = validate_sql(
        "postgresql",
        "SELECT * FROM business b JOIN user u ON b.name = u.user_id",
        schema,
    )
    assert not ok
    assert "disallowed_join" in msg


def test_verified_join_allows_listed_pair() -> None:
    schema = {
        "_validation_registry": {
            "verified_joins": [{"left": "business", "right": "review"}],
        },
        "postgresql": {
            "tables": [
                {"name": "business", "fields": {"business_id": "text"}},
                {"name": "review", "fields": {"business_id": "text", "stars": "int"}},
            ]
        },
    }
    ok, msg = validate_sql(
        "postgresql",
        "SELECT * FROM business b JOIN review r ON r.business_id = b.business_id",
        schema,
    )
    assert ok, msg


def test_fk_metadata_defines_allowed_edge() -> None:
    schema = {
        "postgresql": {
            "tables": [
                {
                    "name": "business_category",
                    "fields": {"business_id": "text", "category": "text"},
                    "foreign_keys": [
                        {
                            "columns": ["business_id"],
                            "referenced_table": "business",
                            "referenced_columns": ["business_id"],
                        }
                    ],
                },
                {"name": "business", "fields": {"business_id": "text"}},
                {"name": "review", "fields": {"business_id": "text"}},
            ]
        }
    }
    bad = "SELECT * FROM business_category bc JOIN review r ON bc.business_id = r.business_id"
    ok, msg = validate_sql("postgresql", bad, schema)
    assert not ok
    assert "disallowed_join" in msg


def test_mongo_lookup_collection_allowlist() -> None:
    schema = {
        "mongodb": {
            "collections": [
                {"name": "business", "fields": {"x": "int"}},
                {"name": "review", "fields": {"y": "int"}},
            ]
        }
    }
    pipe = [{"$lookup": {"from": "phantom", "localField": "id", "foreignField": "id", "as": "z"}}]
    ok, msg = validate_mongo_pipeline("mongodb", "business", pipe, schema)
    assert not ok
    assert "unknown_lookup_collection" in msg


def test_validate_llm_generated_steps_writes_log(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_FORGE_PREEXEC_VALIDATION_LOG", "true")
    log_path = tmp_path / "logs" / "pre_execution_validator.jsonl"
    monkeypatch.setenv("ORACLE_FORGE_PREEXEC_VALIDATION_LOG_PATH", str(log_path))
    schema = {
        "postgresql": {"tables": [{"name": "review", "fields": {"stars": "integer"}}]}
    }
    steps = [{"database": "postgresql", "dialect": "sql", "sql": "SELECT AVG(stars) FROM review"}]
    ok, errs = validate_llm_generated_steps(
        steps,
        schema,
        validation_log_repo_root=tmp_path,
        validation_log_question="q",
        validation_log_dataset_id="yelp",
    )
    assert ok and not errs
    assert log_path.is_file()
    line = log_path.read_text(encoding="utf-8").strip().splitlines()[-1]
    row = json.loads(line)
    assert row["all_ok"] is True
    assert row["dataset_id"] == "yelp"


def test_cte_still_validates_under_join_rules() -> None:
    """CTE names must not force a false disallowed_join (physical edges only)."""
    schema = {
        "_validation_registry": {"verified_joins": [{"left": "business", "right": "review"}]},
        "postgresql": {
            "tables": [
                {"name": "business", "fields": {"business_id": "text"}},
                {"name": "review", "fields": {"business_id": "text", "stars": "int"}},
            ]
        },
    }
    sql = """
    WITH x AS (SELECT business_id FROM business LIMIT 1)
    SELECT * FROM x JOIN review r ON r.business_id = x.business_id
    """
    ok, msg = validate_sql("postgresql", sql, schema)
    assert ok, msg
