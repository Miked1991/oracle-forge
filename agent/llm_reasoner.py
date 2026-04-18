from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
import httpx

from utils.llm_io_log import append_llm_io_log
from utils.routing_log import log_routing_event
from utils.routing_policy import (
    build_schema_routing_summary,
    first_instruction_line,
    normalize_routing_selection,
)
from utils.schema_registry.routing_compact import (
    compact_registry_routing_summary,
    filter_selected_tables_to_registry,
    load_registry_json_optional,
)
from utils.token_limiter import TokenLimiter
from utils.schema_registry.builder import default_registry_path

from agent.utils import canonical_db_name

_logger = logging.getLogger(__name__)


class LLMRoutingFailed(RuntimeError):
    """OpenRouter routing failed or misconfigured; agent must not use heuristic fallback."""


@dataclass
class LLMGuidance:
    selected_databases: List[str]
    rationale: str
    query_hints: Dict[str, Any]
    model: str
    used_llm: bool
    selected_tables: Dict[str, List[str]] = field(default_factory=dict)


class OpenRouterRoutingReasoner:
    """
    Database routing uses **OpenRouter only** (no Groq). Any API or contract failure raises
    :class:`LLMRoutingFailed` — there is no keyword fallback.
    """

    def __init__(self, repo_root: Optional[Path] = None, token_limiter: Optional[TokenLimiter] = None) -> None:
        self.repo_root = repo_root or Path(__file__).resolve().parents[1]
        load_dotenv(self.repo_root / ".env", override=False)
        self.openrouter_api_key = self._clean_env("OPENROUTER_API_KEY")
        self.model_name = self._resolve_model_name()
        self.openrouter_base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").strip().rstrip("/")
        self.openrouter_site_url = os.getenv("OPENROUTER_SITE_URL", "").strip()
        self.openrouter_app_name = os.getenv("OPENROUTER_APP_NAME", "").strip()
        self.token_limiter = token_limiter or TokenLimiter()
        self.http_client = httpx.Client(timeout=40)

    def plan(self, question: str, available_databases: List[str], context: Dict[str, Any]) -> LLMGuidance:
        if not self.openrouter_api_key:
            raise LLMRoutingFailed(
                "OPENROUTER_API_KEY is missing or placeholder; routing requires a valid OpenRouter API key."
            )

        t0 = time.perf_counter()
        raw_content = ""
        schema_metadata = context.get("schema_metadata") or {}
        narrow_q = str(context.get("user_question") or question)
        dataset_id = context.get("dataset_id")
        registry = load_registry_json_optional(
            self.repo_root, str(dataset_id).strip() if isinstance(dataset_id, str) and str(dataset_id).strip() else None
        )
        registry_compact = compact_registry_routing_summary(registry, available_databases) if registry else ""

        context_layers = context.get("context_layers", {})
        trimmed_layers = self.token_limiter.trim_context_layers(context_layers)
        bundle = (context.get("schema_bundle_json") or "")[:6000]
        routing_summary = build_schema_routing_summary(schema_metadata, available_databases)
        instruction_line = first_instruction_line(
            str(context.get("routing_question") or ""),
            narrow_q,
        )
        dp = context.get("dataset_playbook")
        dataset_playbook = dp if isinstance(dp, dict) else None
        prompt = self._build_prompt(
            question,
            available_databases,
            trimmed_layers,
            schema_bundle_snippet=bundle,
            dataset_id=dataset_id if isinstance(dataset_id, str) else None,
            schema_routing_summary=routing_summary,
            registry_compact_summary=registry_compact,
            instruction_line=instruction_line,
            dataset_playbook=dataset_playbook,
        )
        prompt = self.token_limiter.truncate_text(prompt, self.token_limiter.max_prompt_tokens)

        log_base: Dict[str, Any] = {
            "dataset_id": str(dataset_id).strip() if isinstance(dataset_id, str) else None,
            "question": narrow_q[:2000],
            "phase": "routing",
            "registry_loaded": bool(registry),
            "input_artifact_refs": [],
        }
        if registry is not None:
            rid = str(dataset_id).strip() if isinstance(dataset_id, str) else ""
            if rid:
                try:
                    log_base["input_artifact_refs"].append(
                        str(default_registry_path(rid, self.repo_root).relative_to(self.repo_root))
                    )
                except ValueError:
                    log_base["input_artifact_refs"].append(str(default_registry_path(rid, self.repo_root)))

        raw_content = ""
        try:
            payload, raw_content = self._plan_with_openrouter(
                prompt,
                log_context={
                    "dataset_id": str(dataset_id).strip() if isinstance(dataset_id, str) else None,
                    "question_preview": narrow_q[:4000],
                },
            )
        except LLMRoutingFailed as exc:
            log_routing_event(
                {
                    **log_base,
                    "status": "error",
                    "error": str(exc),
                    "llm_response_raw": raw_content[:8000] if raw_content else None,
                    "duration_ms": int((time.perf_counter() - t0) * 1000),
                },
                repo_root=self.repo_root,
            )
            raise
        except Exception as exc:
            if os.getenv("ORACLE_FORGE_DEBUG_LLM_ROUTING", "").lower() in {"1", "true", "yes", "on"}:
                _logger.warning("OpenRouter routing failed: %s: %s", type(exc).__name__, exc)
            log_routing_event(
                {
                    **log_base,
                    "status": "error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "llm_response_raw": raw_content[:8000] if raw_content else None,
                    "duration_ms": int((time.perf_counter() - t0) * 1000),
                },
                repo_root=self.repo_root,
            )
            raise LLMRoutingFailed(f"OpenRouter request failed: {exc}") from exc

        if not isinstance(payload, dict) or not payload:
            log_routing_event(
                {
                    **log_base,
                    "status": "error",
                    "error": "empty_or_invalid_payload",
                    "llm_response_raw": raw_content[:8000] if raw_content else None,
                    "duration_ms": int((time.perf_counter() - t0) * 1000),
                },
                repo_root=self.repo_root,
            )
            raise LLMRoutingFailed("OpenRouter returned a non-object or empty JSON payload.")

        selected = payload.get("selected_databases", [])
        if not isinstance(selected, list):
            selected = []
        selected_norm = [str(item).strip().lower() for item in selected if str(item).strip()]
        avail_l = [d.lower() for d in available_databases]
        filtered = [db for db in selected_norm if db in avail_l]
        if not filtered:
            log_routing_event(
                {
                    **log_base,
                    "status": "error",
                    "error": "no_matching_selected_databases",
                    "llm_response_raw": raw_content[:8000] if raw_content else None,
                    "parsed_payload": payload,
                    "duration_ms": int((time.perf_counter() - t0) * 1000),
                },
                repo_root=self.repo_root,
            )
            raise LLMRoutingFailed(
                "OpenRouter JSON did not list any selected_databases that match available_databases."
            )
        filtered = normalize_routing_selection(narrow_q, filtered, available_databases, schema_metadata)
        if not filtered:
            log_routing_event(
                {
                    **log_base,
                    "status": "error",
                    "error": "normalization_empty",
                    "llm_response_raw": raw_content[:8000] if raw_content else None,
                    "parsed_payload": payload,
                    "duration_ms": int((time.perf_counter() - t0) * 1000),
                },
                repo_root=self.repo_root,
            )
            raise LLMRoutingFailed("Routing normalization yielded no databases after LLM selection.")

        selected_tables_raw = payload.get("selected_tables", {})
        selected_tables: Dict[str, List[str]] = {}
        if registry:
            selected_tables = filter_selected_tables_to_registry(
                selected_tables_raw, registry, available_databases
            )
            for db in filtered:
                selected_tables.setdefault(db, [])
        elif isinstance(selected_tables_raw, dict):
            for k, v in selected_tables_raw.items():
                if isinstance(v, list):
                    db = canonical_db_name(str(k))
                    selected_tables[db] = [str(x).strip() for x in v if str(x).strip()]

        rationale = str(payload.get("rationale", "LLM-guided routing."))[:500]
        duration_ms = int((time.perf_counter() - t0) * 1000)
        log_routing_event(
            {
                **log_base,
                "status": "ok",
                "prompt_preview": prompt[:6000],
                "llm_response_raw": raw_content[:8000] if raw_content else None,
                "parsed_selected_databases": selected_norm,
                "normalized_selected_databases": filtered,
                "parsed_selected_tables": selected_tables_raw if isinstance(selected_tables_raw, dict) else None,
                "normalized_selected_tables": selected_tables,
                "duration_ms": duration_ms,
                "model": self.model_name,
            },
            repo_root=self.repo_root,
        )
        return LLMGuidance(
            selected_databases=filtered,
            rationale=rationale,
            query_hints=payload.get("query_hints", {}) if isinstance(payload.get("query_hints", {}), dict) else {},
            model=self.model_name,
            used_llm=True,
            selected_tables=selected_tables,
        )

    def _plan_with_openrouter(
        self,
        prompt: str,
        *,
        log_context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], str]:
        headers = {
            "Authorization": f"Bearer {self.openrouter_api_key}",
            "Content-Type": "application/json",
        }
        if self.openrouter_site_url:
            headers["HTTP-Referer"] = self.openrouter_site_url
        if self.openrouter_app_name:
            headers["X-Title"] = self.openrouter_app_name

        body = {
            "model": self.model_name,
            "temperature": 0,
            "max_tokens": 320,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a database routing and query planning assistant for a multi-DB data agent. "
                        "Return strict JSON with keys: selected_databases, selected_tables, rationale, query_hints. "
                        "selected_tables must be an object mapping each chosen engine name (postgresql, sqlite, "
                        "duckdb, mongodb) to an array of table or collection NAMES you need — use EXACT names from "
                        "the COMPACT REGISTRY SUMMARY when it is provided. "
                        "Prefer the smallest set of databases: use ONE engine unless the task clearly needs "
                        "joining across systems or both relational SQL and document data."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        }
        append_llm_io_log(
            self.repo_root,
            {
                "phase": "routing",
                "provider": "openrouter",
                "model": self.model_name,
                "messages": body["messages"],
                "request": {
                    "temperature": body.get("temperature"),
                    "max_tokens": body.get("max_tokens"),
                    "response_format": body.get("response_format"),
                },
                **(log_context or {}),
            },
        )
        response = self.http_client.post(f"{self.openrouter_base_url}/chat/completions", headers=headers, json=body)
        response.raise_for_status()
        data = response.json()
        choices = data.get("choices", [])
        if not choices:
            raise LLMRoutingFailed("OpenRouter returned no choices.")
        message = choices[0].get("message", {})
        content = message.get("content", "{}")
        if isinstance(content, list):
            content = "".join(str(part.get("text", "")) for part in content if isinstance(part, dict))
        raw = str(content).strip()
        parsed = self._parse_json_content(raw)
        if not isinstance(parsed, dict):
            raise LLMRoutingFailed("OpenRouter message content was not a JSON object.")
        return parsed, raw

    @staticmethod
    def _parse_json_content(content: str) -> Dict[str, Any]:
        text = content.strip()
        if text.startswith("```"):
            text = text.strip("`")
            if text.lower().startswith("json"):
                text = text[4:].strip()
        try:
            parsed = json.loads(text or "{}")
        except json.JSONDecodeError as exc:
            raise LLMRoutingFailed(f"OpenRouter returned invalid JSON: {exc}") from exc
        return parsed if isinstance(parsed, dict) else {}

    def _resolve_model_name(self) -> str:
        configured = os.getenv("MODEL_NAME", "").strip()
        if configured:
            return configured
        return "openai/gpt-4o-mini"

    @staticmethod
    def _clean_env(name: str) -> str:
        value = os.getenv(name, "").strip()
        if not value:
            return ""
        lowered = value.lower()
        if lowered in {"your_api_key_here", "your_key_here", "changeme"}:
            return ""
        if lowered.startswith("your_") and ("_key_here" in lowered or "_api_key_here" in lowered):
            return ""
        return value

    def _build_prompt(
        self,
        question: str,
        available_databases: List[str],
        context_layers: Dict[str, Any],
        schema_bundle_snippet: str = "",
        dataset_id: Optional[str] = None,
        schema_routing_summary: str = "",
        registry_compact_summary: str = "",
        instruction_line: str = "",
        dataset_playbook: Optional[Dict[str, Any]] = None,
    ) -> str:
        context_json = json.dumps(context_layers, ensure_ascii=False)[:5000]
        playbook_block = ""
        if isinstance(dataset_playbook, dict) and (dataset_playbook.get("summary") or "").strip():
            suggest = dataset_playbook.get("suggest_engines_order") or []
            sug_txt = ", ".join(str(x) for x in suggest[:12]) if suggest else ""
            playbook_block = (
                "BENCHMARK PLAYBOOK (dataset intent — use to choose engines and cross-DB work):\n"
                f"{str(dataset_playbook.get('summary', ''))[:4500]}\n"
                + (f"Suggested engine priority: {sug_txt}\n" if sug_txt else "")
                + "\n"
            )
        primary = ""
        if schema_bundle_snippet.strip():
            primary = (
                "PRIMARY schema bundle (authoritative table/collection names and fields — prefer routing to "
                "engines that have relevant objects listed here):\n"
                f"{schema_bundle_snippet}\n\n"
            )
        registry_block = ""
        if registry_compact_summary.strip():
            registry_block = (
                "COMPACT REGISTRY SUMMARY (authoritative table/collection names per engine — prefer this for "
                "selected_tables):\n"
                f"{registry_compact_summary}\n\n"
            )
        summary_block = ""
        if schema_routing_summary.strip():
            summary_block = (
                "Live MCP schema routing summary (supplementary — non-empty engines):\n"
                f"{schema_routing_summary}\n\n"
            )
        ds_line = f"Dataset id (benchmark scope): {dataset_id}\n" if dataset_id else ""
        task_line = f"Task focus (first line): {instruction_line}\n" if instruction_line else ""
        return (
            f"{playbook_block}{registry_block}{primary}{summary_block}{ds_line}{task_line}"
            f"Question: {question}\n"
            f"Available databases: {available_databases}\n"
            "Use the compact registry summary (when present), schema bundle, and live summary to choose the "
            "minimum set of databases and the specific tables/collections required.\n"
            "Supporting context layers (trimmed):\n"
            f"{context_json}\n"
            "Return JSON only with keys: selected_databases, selected_tables, rationale, query_hints."
        )


# Backward-compatible alias for imports and docs.
GroqLlamaReasoner = OpenRouterRoutingReasoner
