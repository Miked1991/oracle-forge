"""Canonical schema registry: live introspection → JSON artifacts + structured logs."""

from __future__ import annotations

from utils.schema_registry.builder import (
    SCHEMA_REGISTRY_VERSION,
    build_schema_registry,
    default_registry_path,
)
from utils.schema_registry.kb_generator import (
    authoritative_kb_file_path,
    render_authoritative_markdown,
    write_authoritative_kb,
)
from utils.schema_registry.kb_log import log_kb_generation_event
from utils.schema_registry.schema_log import log_schema_registry_event

__all__ = [
    "SCHEMA_REGISTRY_VERSION",
    "authoritative_kb_file_path",
    "build_schema_registry",
    "default_registry_path",
    "log_kb_generation_event",
    "log_schema_registry_event",
    "render_authoritative_markdown",
    "write_authoritative_kb",
]
