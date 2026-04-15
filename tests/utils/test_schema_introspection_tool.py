"""Tests for utils.schema_introspection_tool.SchemaIntrospectionTool."""

from pathlib import Path

from utils.schema_introspection_tool import SchemaIntrospectionTool


def test_collect_fallback_without_mcp_uses_stub_when_no_dab(tmp_path: Path) -> None:
    tool = SchemaIntrospectionTool(repo_root=tmp_path)
    meta = tool.collect(mcp_schema_metadata=None)
    assert "postgresql" in meta
    assert meta["postgresql"]["tables"]
