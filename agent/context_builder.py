from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from .utils import canonical_db_name


class ContextBuilder:
    def __init__(self, repo_root: Optional[Path] = None) -> None:
        self.repo_root = repo_root or Path(__file__).resolve().parents[1]
        self.kb_root = self.repo_root / "kb"

    def build(
        self,
        question: str,
        available_databases: List[str],
        schema_info: Optional[Dict[str, Any]],
        discovered_schema_metadata: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        architecture_docs = [
            "architecture/memory.md",
            "architecture/conductor_worker_pattern.md",
            "architecture/openai_layers.md",
            "architecture/tool_scoping_philosophy.md",
        ]
        domain_docs = [
            "domain/joins/join_key_mappings.md",
            "domain/joins/cross_db_join_patterns.md",
            "domain/domain_terms/business_glossary.md",
            "domain/domain_terms/authoritative_tables.md",
            "domain/domain_terms/fiscal_calendar.md",
            "domain/unstructured/text_extraction_patterns.md",
            "domain/unstructured/sentiment_mapping.md",
            "domain/unstructured/null_guards.md",
        ]
        correction_docs = [
            "corrections/failure_log.md",
            "corrections/failure_by_category.md",
            "corrections/resolved_patterns.md",
            "corrections/regression_prevention.md",
        ]

        db_docs = []
        for db in available_databases:
            normalized = canonical_db_name(db)
            if normalized:
                db_docs.append(f"domain/databases/{normalized}_schemas.md")
        domain_docs.extend(db_docs)

        layers = {
            "v1_architecture": self._load_documents(architecture_docs),
            "v2_domain": self._load_documents(domain_docs),
            "v3_corrections": self._load_documents(correction_docs),
        }
        schema_metadata = self._merge_schema_info(schema_info, discovered_schema_metadata)
        schema_layer = {"runtime/schema_metadata.json": json.dumps(schema_metadata, ensure_ascii=False)}
        domain_layer = layers["v2_domain"]
        interaction_layer = {
            **layers["v3_corrections"],
            "runtime/runtime_corrections.json": json.dumps(self._load_runtime_corrections(), ensure_ascii=False),
        }
        join_mappings = layers["v2_domain"].get("domain/joins/join_key_mappings.md", "")
        failure_log = layers["v3_corrections"].get("corrections/failure_log.md", "")
        resolved = layers["v3_corrections"].get("corrections/resolved_patterns.md", "")
        return {
            "question": question,
            "context_layers": {
                **layers,
                "schema_metadata": schema_layer,
                "domain_institutional": domain_layer,
                "interaction_memory": interaction_layer,
            },
            "schema_metadata": schema_metadata,
            "schema_patterns": self._extract_schema_patterns(layers["v2_domain"]),
            "join_key_rules": self._extract_join_key_rules(join_mappings),
            "known_failures": self._extract_known_failures(failure_log),
            "resolved_patterns": self._extract_resolved_patterns(resolved),
            "runtime_corrections": self._load_runtime_corrections(),
            "layer_usage_contract": {
                "schema_metadata": "database schema introspection and metadata constraints",
                "domain_institutional": "business definitions, institutional conventions, join guidance",
                "interaction_memory": "corrections, known failures, and successful strategies",
                "v1_architecture": "tool selection and routing constraints",
                "v2_domain": "schema understanding, terms, id formatting",
                "v3_corrections": "failure-driven replanning and retries",
            },
        }

    def _load_documents(self, rel_paths: List[str]) -> Dict[str, str]:
        loaded: Dict[str, str] = {}
        for rel_path in rel_paths:
            file_path = self.kb_root / rel_path
            if file_path.exists():
                loaded[rel_path] = file_path.read_text(encoding="utf-8")
        return loaded

    def _merge_schema_info(
        self,
        schema_info: Optional[Dict[str, Any]],
        discovered_schema_metadata: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        for source in [schema_info or {}, discovered_schema_metadata or {}]:
            for db_name, payload in source.items():
                normalized = canonical_db_name(db_name)
                merged.setdefault(normalized, {"tables": [], "collections": []})
                if not isinstance(payload, dict):
                    continue
                for key in ["tables", "collections"]:
                    values = payload.get(key, [])
                    if isinstance(values, list):
                        for item in values:
                            if item not in merged[normalized][key]:
                                merged[normalized][key].append(item)
        return merged

    def _extract_schema_patterns(self, docs: Dict[str, str]) -> List[Dict[str, str]]:
        patterns: List[Dict[str, str]] = []
        field_re = re.compile(r"-\s*([A-Za-z0-9_]+)\s*\(([^)]+)\)")
        for rel_path, content in docs.items():
            if "domain/databases/" not in rel_path:
                continue
            for match in field_re.finditer(content):
                patterns.append(
                    {
                        "source": rel_path,
                        "field_name": match.group(1),
                        "field_type": match.group(2).strip(),
                    }
                )
        return patterns

    def _extract_join_key_rules(self, content: str) -> List[Dict[str, str]]:
        rules: List[Dict[str, str]] = []
        table_row = re.compile(r"\|\s*([A-Za-z0-9_]+)\s*\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|")
        for match in table_row.finditer(content):
            rules.append(
                {
                    "entity": match.group(1).strip(),
                    "left_format": match.group(2).strip(),
                    "right_format": match.group(3).strip(),
                    "transformation": match.group(4).strip(),
                }
            )
        return rules

    def _extract_known_failures(self, content: str) -> List[Dict[str, str]]:
        failures: List[Dict[str, str]] = []
        pattern = re.compile(r"\*\*\[(Q\d+)\]\*\*\s*→\s*(.+?)\n\*\*Correct:\*\*\s*(.+?)(?:\n|$)")
        for match in pattern.finditer(content):
            failures.append(
                {
                    "query_id": match.group(1).strip(),
                    "failure": match.group(2).strip(),
                    "correction": match.group(3).strip(),
                }
            )
        return failures

    def _extract_resolved_patterns(self, content: str) -> List[Dict[str, str]]:
        patterns: List[Dict[str, str]] = []
        title_re = re.compile(r"## Pattern (.+)")
        confidence_re = re.compile(r"\*\*Confidence:\*\*\s*(.+)")
        active_title = ""
        for line in content.splitlines():
            title_match = title_re.search(line)
            if title_match:
                active_title = title_match.group(1).strip()
            conf_match = confidence_re.search(line)
            if conf_match:
                patterns.append({"pattern": active_title or "Unnamed", "confidence": conf_match.group(1).strip()})
        return patterns

    def _load_runtime_corrections(self) -> List[Dict[str, Any]]:
        file_path = self.repo_root / "docs" / "driver_notes" / "runtime_corrections.jsonl"
        if not file_path.exists():
            return []
        entries: List[Dict[str, Any]] = []
        for line in file_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
                if isinstance(payload, dict):
                    entries.append(payload)
            except json.JSONDecodeError:
                continue
        return entries[-300:]
