"""Phase 8: merge strategy labeling and empty fallback."""

from __future__ import annotations

from agent.main import _merge_outputs


def test_merge_single_db_labels_trace() -> None:
    trace: list = []
    out = _merge_outputs(
        [{"ok": True, "database": "postgresql", "data": [{"a": 1}]}],
        trace,
    )
    assert out == [{"a": 1}]
    assert any(e.get("merge_strategy") == "single_db" for e in trace if isinstance(e, dict))


def test_merge_multi_step_uses_join_trace() -> None:
    trace: list = []
    out = _merge_outputs(
        [
            {"ok": True, "database": "postgresql", "data": [{"id": 1, "x": 10}]},
            {"ok": True, "database": "mongodb", "data": [{"id": 1, "y": 20}]},
        ],
        trace,
    )
    assert isinstance(out, list)
    assert any("merge_strategy" in e for e in trace if isinstance(e, dict))


def test_merge_empty_single_step() -> None:
    trace: list = []
    out = _merge_outputs([{"ok": True, "database": "postgresql", "data": []}], trace)
    assert out == []
    assert any(
        e.get("merge_failure_reason") == "single_successful_step_returned_zero_rows"
        for e in trace
        if isinstance(e, dict)
    )
