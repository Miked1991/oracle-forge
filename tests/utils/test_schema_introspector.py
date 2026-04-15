"""Tests for utils.schema_introspector.SchemaIntrospector."""

from utils.schema_introspector import ColumnInfo, SchemaIntrospector, TableInfo


def test_get_all_schemas_as_text_empty() -> None:
    intro = SchemaIntrospector(db_executor=None)
    assert intro.get_all_schemas_as_text() == ""


def test_column_info_fields() -> None:
    c = ColumnInfo(name="id", data_type="integer", is_primary_key=True)
    assert c.name == "id"
