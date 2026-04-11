from __future__ import annotations

import re
import json
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
        base_docs = [
            "architecture/memory.md",
            "architecture/conductor_worker_pattern.md",
            "architecture/openai_layers.md",
            "correction/failure_log.md",
            "correction/resolved_patterns.md",
            "domain/joins/join_key_mappings.md",
            "domain/joins/cross_db_join_patterns.md",
            "domain/domain_terms/business_glossary.md",
        ]
        db_docs = []
        for db in available_databases:
            normalized = canonical_db_name(db)
            if normalized:
                db_docs.append(f"domain/databases/{normalized}_schemas.md")
        docs = self._load_documents(base_docs + db_docs)
        failure_log = docs.get("correction/failure_log.md", "")
        join_mappings = docs.get("domain/joins/join_key_mappings.md", "")
        schema_metadata = self._merge_schema_info(schema_info, discovered_schema_metadata)
        return {
            "question": question,
            "documents": docs,
            "schema_metadata": schema_metadata,
            "schema_patterns": self._extract_schema_patterns(docs),
            "join_key_rules": self._extract_join_key_rules(join_mappings),
            "known_failures": self._extract_known_failures(failure_log),
            "runtime_corrections": self._load_runtime_corrections(),
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
        table_row = re.compile(
            r"\|\s*([A-Za-z0-9_]+)\s*\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|"
        )
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
        return entries[-200:]
