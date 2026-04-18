"""
LLM-driven SQL / MongoDB aggregation generation using KB + schema context.

Used when ORACLE_FORGE_LLM_SQL is enabled (see eval/LLM_QUERY_GENERATION_PLAN.md).

Debug: full chat ``messages`` (routing + query generation) append to ``logs/llm_io.jsonl``
(``ORACLE_FORGE_LLM_IO_LOG=false`` to disable). Legacy: ``docs/driver_notes/sql_builder_llm_prompts.jsonl`` via
``ORACLE_FORGE_SQL_BUILDER_PROMPT_LOG``.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
import httpx

from agent.query_builders import (
    augment_system_for_builder_kind,
    build_per_engine_user_prompt,
    classify_builder_kind,
    schema_slice_summary,
)
from agent.query_pipeline import (
    AnswerContract,
    PlannerBackend,
    answer_contract_from_planner_json,
    build_answer_contract,
    contract_to_prompt_json,
    phase_schema_link,
)
from agent.query_safety import validate_llm_generated_steps
from agent.utils import canonical_db_name
from utils.dataset_playbooks import (
    playbook_engine_generation_hints,
    playbook_generation_hints_markdown,
)
from utils.schema_bundle import narrow_schema_bundle_json
from utils.repair_packet import RepairPacket, split_repair_and_legacy_notes
from utils.sql_builder_scope import (
    build_scoped_engine_schema_dict,
    select_collections_for_mongo_engine,
    select_tables_for_sql_engine,
)
from utils.llm_io_log import append_llm_io_log
from utils.query_builder_log import append_query_builder_log, truncate_for_log
from utils.token_limiter import TokenLimiter
from utils.yelp_benchmark_sql import yelp_attributes_parking_offer_sql, yelp_parking_question_hint_line

try:
    from groq import Groq
except Exception:  # pragma: no cover
    Groq = None  # type: ignore[assignment]


def _clean_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    return value if value and "your_" not in value.lower() else ""


def _schema_brief(schema_metadata: Dict[str, Any], databases: List[str]) -> Dict[str, Any]:
    brief: Dict[str, Any] = {}
    for db in databases:
        norm = canonical_db_name(db)
        meta = schema_metadata.get(norm) or schema_metadata.get(db) or {}
        tables: List[str] = []
        for item in meta.get("tables") or []:
            if isinstance(item, dict) and item.get("name"):
                tables.append(str(item["name"]))
            elif isinstance(item, str):
                tables.append(item)
        cols: List[str] = []
        for item in meta.get("collections") or []:
            if isinstance(item, dict) and item.get("name"):
                cols.append(str(item["name"]))
            elif isinstance(item, str):
                cols.append(item)
        brief[norm] = {"tables": tables[:80], "collections": cols[:40]}
    return brief


def _engine_schema_digest(schema_metadata: Dict[str, Any], databases: List[str]) -> str:
    """
    Per-engine tables with column names for LLM grounding and pre-execution validation.
    Must stay in sync with validate_step_payload / schema_metadata.fields.
    """
    brief: Dict[str, Any] = {}
    for db in databases:
        key = canonical_db_name(db)
        meta = schema_metadata.get(key) or schema_metadata.get(db) or {}
        tables_out: List[Dict[str, Any]] = []
        for item in meta.get("tables") or []:
            if not isinstance(item, dict) or not item.get("name"):
                continue
            fields = item.get("fields") or {}
            col_list: List[str] = []
            if isinstance(fields, dict) and fields:
                col_list = sorted(str(k) for k in fields.keys())
            tables_out.append({"name": str(item["name"]), "columns": col_list[:220]})
        colls_out: List[Dict[str, Any]] = []
        for item in meta.get("collections") or []:
            if not isinstance(item, dict) or not item.get("name"):
                continue
            fields = item.get("fields") or {}
            sample_keys: List[str] = []
            if isinstance(fields, dict) and fields:
                sample_keys = sorted(str(k) for k in fields.keys())[:120]
            colls_out.append({"name": str(item["name"]), "sample_field_keys": sample_keys})
        brief[key] = {"tables": tables_out[:80], "collections": colls_out[:40]}
    raw = json.dumps(brief, ensure_ascii=False, indent=2)
    return raw[:14000]


def _sql_builder_prompt_log_enabled() -> bool:
    return os.getenv("ORACLE_FORGE_SQL_BUILDER_PROMPT_LOG", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _log_sql_builder_llm_prompts(
    repo_root: Path,
    *,
    question: str,
    dataset_id: Optional[str],
    selected_databases: List[str],
    attempt_index: int,
    system: str,
    user_prompt: str,
    provider: str,
    model: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Append one JSON line per LLM call (debugging). Disable with ORACLE_FORGE_SQL_BUILDER_PROMPT_LOG=false."""
    if not _sql_builder_prompt_log_enabled():
        return
    raw_path = os.getenv("ORACLE_FORGE_SQL_BUILDER_PROMPT_LOG_PATH", "").strip()
    rel = Path(raw_path) if raw_path else repo_root / "docs" / "driver_notes" / "sql_builder_llm_prompts.jsonl"
    path = rel if rel.is_absolute() else (repo_root / rel)
    path.parent.mkdir(parents=True, exist_ok=True)
    entry: Dict[str, Any] = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_id": dataset_id,
        "question": question,
        "selected_databases": selected_databases,
        "schema_validation_attempt": attempt_index,
        "provider": provider,
        "model": model,
        "system_prompt": system,
        "user_prompt": user_prompt,
        "system_prompt_chars": len(system),
        "user_prompt_chars": len(user_prompt),
    }
    if extra:
        entry.update(extra)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _intent_coverage_one_liner() -> str:
    return (
        "Satisfy every part of the question: use enough columns/rows (GROUP BY/ORDER BY as needed); "
        "do not emit one scalar if multiple outputs are asked."
    )


def _format_fix_block(fix_notes: List[str]) -> str:
    """Prefer structured ``repair_packet:`` JSON lines first, then legacy prose."""
    if not fix_notes:
        return ""
    repair, legacy = split_repair_and_legacy_notes(list(fix_notes))
    parts: List[str] = []
    if repair:
        parts.append("STRUCTURED_REPAIR (parse JSON after repair_packet: prefix):\n" + "\n".join(repair))
    if legacy:
        parts.append("NOTES:\n" + "\n".join(f"- {n}" for n in legacy[-10:]))
    return "\n\n".join(parts) + "\n\n" if parts else ""


def _dialect_line(engine: str) -> str:
    e = canonical_db_name(engine)
    if e == "postgresql":
        return (
            "PostgreSQL — use only identifiers listed in SCOPED_SCHEMA (no invented columns like category_name). "
            "If a column is typed text/varchar and you compare to dates, cast it (e.g. col::date) first."
        )
    if e == "duckdb":
        return "DuckDB — use listed column names for this engine (may differ from PostgreSQL)."
    if e == "sqlite":
        return "SQLite — use listed tables/columns only."
    if e == "mongodb":
        return "MongoDB — output one aggregation pipeline; use listed collection and field keys."
    return "Use only listed schema objects."


def _system_prompt_per_engine(engine: str) -> str:
    e = canonical_db_name(engine)
    if e == "mongodb":
        return (
            "Return one JSON object: keys \"collection\", \"pipeline\" (read-only aggregation; no $where). "
            f"{_dialect_line(e)} {_intent_coverage_one_liner()}"
        )
    return (
        "Return one JSON object with key \"sql\": a single read-only SELECT (WITH allowed). "
        f"{_dialect_line(e)} {_intent_coverage_one_liner()}"
    )


class LLMQueryGenerator:
    def __init__(self, repo_root: Optional[Path] = None, token_limiter: Optional[TokenLimiter] = None) -> None:
        self.repo_root = repo_root or Path(__file__).resolve().parents[1]
        load_dotenv(self.repo_root / ".env", override=False)
        self.groq_api_key = _clean_env("GROQ_API_KEY")
        self.openrouter_api_key = _clean_env("OPENROUTER_API_KEY")
        self.provider = self._resolve_provider()
        self.model_name = self._resolve_model_name()
        self.openrouter_base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").strip().rstrip("/")
        self.openrouter_site_url = os.getenv("OPENROUTER_SITE_URL", "").strip()
        self.openrouter_app_name = os.getenv("OPENROUTER_APP_NAME", "").strip()
        self.token_limiter = token_limiter or TokenLimiter()
        self.client = Groq(api_key=self.groq_api_key) if self.provider == "groq" and self.groq_api_key and Groq is not None else None
        self.http_client = httpx.Client(timeout=60)
        self.max_gen_tokens = int(os.getenv("ORACLE_FORGE_LLM_GEN_MAX_TOKENS", "2048"))

    def _resolve_provider(self) -> str:
        configured = os.getenv("LLM_PROVIDER", "").strip().lower()
        if configured in {"groq", "openrouter"}:
            return configured
        if self.openrouter_api_key:
            return "openrouter"
        return "groq"

    def _resolve_model_name(self) -> str:
        configured = os.getenv("MODEL_NAME", "").strip()
        if configured:
            return configured
        if self.provider == "openrouter":
            return "openai/gpt-4o-mini"
        return "llama-3.3-70b-versatile"

    def _llm_planner_enabled(self) -> bool:
        return os.getenv("ORACLE_FORGE_LLM_PLANNER", "").strip().lower() in {"1", "true", "yes", "on"}

    def _llm_answer_contract(self, question: str, context: Dict[str, Any]) -> Optional[AnswerContract]:
        """Phase 1 (optional): LLM produces JSON intent; no SQL."""
        system = (
            "You are phase 1 (planner) only — output intent as JSON, not SQL. "
            "Keys: summary (string), output_grain (string), metrics (string array), "
            "filters (string array), dimensions (string array), time_bounds (string array), "
            "requires_join_or_group (boolean)."
        )
        user = (
            f"QUESTION:\n{question}\nDATASET_ID:{context.get('dataset_id') or ''}\n"
            "Describe what the answer must contain (grain, metrics, dimensions, filters)."
        )
        ds_for_log: Optional[str] = None
        if isinstance(context.get("dataset_id"), str) and context.get("dataset_id"):
            ds_for_log = str(context["dataset_id"]).strip() or None
        io_extra: Dict[str, Any] = {
            "subphase": "planner_contract",
            "dataset_id": ds_for_log,
            "question_preview": (question or "")[:4000],
            "generation_mode": "per_database",
            "pipeline_phase": "planner",
        }
        try:
            if self.provider == "openrouter":
                raw = self._openrouter_json(system, user, io_extra=io_extra)
            else:
                raw = self._groq_json(system, user, io_extra=io_extra)
        except Exception:
            return None
        if not isinstance(raw, dict) or not raw:
            return None
        contract = answer_contract_from_planner_json(raw, fallback_summary="llm_planner")
        _log_sql_builder_llm_prompts(
            self.repo_root,
            question=question,
            dataset_id=ds_for_log,
            selected_databases=[],
            attempt_index=0,
            system=system,
            user_prompt=user,
            provider=self.provider,
            model=self.model_name,
            extra={"generation_mode": "per_database", "pipeline_phase": "planner", "planner_backend": "llm"},
        )
        return contract

    def _per_db_generation_failed(
        self,
        *,
        gate_detail: str,
        planner_backend: PlannerBackend,
        pipeline_trace: List[Dict[str, Any]],
        engines_linked: List[str],
        total_attempts: int,
    ) -> Dict[str, Any]:
        """Structured failure so ``query_pipeline`` trace survives (vs bare ``None``)."""
        return {
            "steps": [],
            "generation_failed": True,
            "gate_detail": gate_detail,
            "model": self.model_name,
            "preexec_schema_attempts": total_attempts,
            "generation_mode": "per_database",
            "pipeline_metadata": {
                "four_phase": True,
                "planner_backend": planner_backend,
                "engines_linked": engines_linked,
            },
            "pipeline_trace": pipeline_trace,
        }

    def _log_query_builder_phase(
        self,
        *,
        ds_for_log: Optional[str],
        question: str,
        db: str,
        builder_kind: str,
        scoped_tables: List[str],
        schema_json: str,
        attempt_index: int,
        system: str,
        user_prompt: str,
        status: str,
        payload: Any = None,
        extra_detail: Optional[str] = None,
    ) -> None:
        append_query_builder_log(
            self.repo_root,
            {
                "dataset_id": ds_for_log,
                "question": (question or "")[:4000],
                "engine": db,
                "builder_kind": builder_kind,
                "scoped_tables": scoped_tables,
                "schema_slice_chars": len(schema_json),
                "schema_slice_summary": schema_slice_summary(schema_json),
                "attempt_index": attempt_index,
                "system_prompt": system,
                "user_prompt": user_prompt,
                "system_prompt_chars": len(system),
                "user_prompt_chars": len(user_prompt),
                "status": status,
                "llm_response": truncate_for_log(payload),
                "detail": extra_detail,
            },
        )

    def _phase_run_planner(
        self,
        question: str,
        ds_for_log: Optional[str],
        context: Dict[str, Any],
    ) -> Tuple[AnswerContract, PlannerBackend, List[Dict[str, Any]]]:
        trace: List[Dict[str, Any]] = [{"phase": "planner"}]
        if self._llm_planner_enabled():
            c = self._llm_answer_contract(question, context)
            if c is not None:
                trace[0].update({"backend": "llm", "contract_json": contract_to_prompt_json(c)})
                return c, "llm", trace
        c = build_answer_contract(question, ds_for_log)
        trace[0].update({"backend": "heuristic", "contract_json": contract_to_prompt_json(c)})
        return c, "heuristic", trace

    def generate_steps(
        self,
        question: str,
        selected_databases: List[str],
        context: Dict[str, Any],
        replan_notes: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Returns dict with key "steps": [ { "database", "dialect", "sql" | ("collection","pipeline") } ]
        or None on failure.

        Default: one LLM call per selected database with scoped tables/collections only
        (``ORACLE_FORGE_LLM_SQL_MONOLITHIC=true`` restores the previous single-call mode).
        """
        if self.provider == "groq" and self.client is None:
            return None

        monolithic = os.getenv("ORACLE_FORGE_LLM_SQL_MONOLITHIC", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if monolithic:
            return self._generate_steps_monolithic(question, selected_databases, context, replan_notes)
        return self._generate_steps_per_database(question, selected_databases, context, replan_notes)

    @staticmethod
    def _normalize_single_engine_response(engine: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Turn per-engine LLM JSON into the flat step shape expected by the planner."""
        db = canonical_db_name(engine)
        if db == "mongodb":
            if isinstance(payload.get("steps"), list) and payload["steps"]:
                first = payload["steps"][0]
                if isinstance(first, dict):
                    payload = first
            col = str(payload.get("collection") or "").strip()
            pipe = payload.get("pipeline")
            if not col or not isinstance(pipe, list):
                return None
            return {
                "database": db,
                "dialect": "mongodb_aggregation",
                "collection": col,
                "pipeline": pipe,
            }
        if isinstance(payload.get("steps"), list) and payload["steps"]:
            first = payload["steps"][0]
            if isinstance(first, dict) and first.get("sql"):
                payload = first
        sql = str(payload.get("sql") or "").strip()
        if not sql:
            return None
        return {"database": db, "dialect": "sql", "sql": sql}

    def _generate_steps_per_database(
        self,
        question: str,
        selected_databases: List[str],
        context: Dict[str, Any],
        replan_notes: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Option B: one scoped LLM call per engine in routing order."""
        schema_metadata = context.get("schema_metadata") or {}
        llm_g = context.get("llm_guidance") or {}
        hints = llm_g.get("query_hints") if isinstance(llm_g, dict) else {}
        rationale = (llm_g.get("rationale") or "")[:500] if isinstance(llm_g, dict) else ""
        dp = context.get("dataset_playbook") if isinstance(context.get("dataset_playbook"), dict) else {}
        playbook_summary = str(dp.get("summary", ""))[:2000] if dp else ""

        max_schema_retries = max(0, int(os.getenv("ORACLE_FORGE_LLM_PREEXEC_SCHEMA_RETRIES", "3")))
        ds_for_log: Optional[str] = None
        if isinstance(context.get("dataset_id"), str) and context.get("dataset_id"):
            ds_for_log = str(context["dataset_id"]).strip() or None

        steps_out: List[Dict[str, Any]] = []
        total_attempts = 0
        shared_fix: List[str] = list(replan_notes or [])

        # Phase 1 — planner (heuristic or optional LLM)
        contract, planner_backend, pipeline_trace = self._phase_run_planner(
            question, ds_for_log, context
        )
        engines_linked: List[str] = []

        for db_raw in selected_databases:
            db = canonical_db_name(str(db_raw))
            fix_notes: List[str] = list(shared_fix)
            scoped_tables: List[str] = []
            if db == "mongodb":
                scoped_tables = select_collections_for_mongo_engine(question, schema_metadata, dp)
                scoped = build_scoped_engine_schema_dict(
                    schema_metadata, db, collection_names=scoped_tables
                )
            else:
                scoped_tables = select_tables_for_sql_engine(question, db, schema_metadata, dp)
                scoped = build_scoped_engine_schema_dict(schema_metadata, db, table_names=scoped_tables)

            # Phase 2 — schema link (readiness gate + compact JSON)
            linked_payload, gate_err = phase_schema_link(
                db, scoped_tables, scoped, schema_metadata, max_json_chars=10000
            )
            if linked_payload is None:
                shared_fix.append(
                    RepairPacket(
                        error_type="need_schema_refresh",
                        engine=db,
                        hint=gate_err,
                        allowed_tables=scoped_tables,
                        known_columns=None,
                    ).to_prompt_line()
                )
                pipeline_trace.append(
                    {
                        "phase": "schema_link",
                        "engine": db,
                        "scoped": scoped_tables,
                        "readiness_ok": False,
                        "error": gate_err,
                    }
                )
                continue

            engines_linked.append(db)
            pipeline_trace.append(
                {
                    "phase": "schema_link",
                    "engine": db,
                    "scoped": scoped_tables,
                    "readiness_ok": True,
                }
            )

            schema_json = linked_payload.linked_schema_json
            eng_hints = playbook_engine_generation_hints(dp, db) if dp else []
            yelp_parking_extra = ""
            if (
                (ds_for_log or "").lower() == "yelp"
                and "parking" in question.lower()
                and db == "postgresql"
            ):
                yelp_parking_extra = (
                    "DETERMINISTIC_PREDICATE (prefer this for Yelp parking on attributes TEXT):\n"
                    f"  {yelp_attributes_parking_offer_sql('b.attributes')}\n"
                    f"  ({yelp_parking_question_hint_line()})\n"
                )

            engine_ok = False
            for attempt in range(max_schema_retries + 1):
                err_block = _format_fix_block(fix_notes)
                kind = classify_builder_kind(scoped_tables, fix_notes)
                base_sys = _system_prompt_per_engine(db)
                system = augment_system_for_builder_kind(base_sys, kind)
                user_prompt = build_per_engine_user_prompt(
                    kind=kind,
                    question=question,
                    contract_json=contract_to_prompt_json(contract),
                    engine=db,
                    rationale=rationale,
                    hints=hints,
                    playbook_summary=playbook_summary,
                    eng_hints=eng_hints,
                    schema_json=schema_json,
                    err_block=err_block,
                    yelp_parking_extra=yelp_parking_extra,
                )
                user_prompt = self.token_limiter.truncate_text(user_prompt, self.token_limiter.max_prompt_tokens)

                _log_sql_builder_llm_prompts(
                    self.repo_root,
                    question=question,
                    dataset_id=ds_for_log,
                    selected_databases=list(selected_databases),
                    attempt_index=attempt,
                    system=system,
                    user_prompt=user_prompt,
                    provider=self.provider,
                    model=self.model_name,
                    extra={
                        "generation_mode": "per_database",
                        "pipeline_phase": "generator",
                        "planner_backend": planner_backend,
                        "engine": db,
                        "scoped_tables": scoped_tables,
                        "builder_kind": kind,
                    },
                )

                io_extra = {
                    "subphase": "per_database",
                    "generation_mode": "per_database",
                    "dataset_id": ds_for_log,
                    "engine": db,
                    "schema_attempt": attempt,
                    "builder_kind": kind,
                    "question_preview": (question or "")[:4000],
                }
                try:
                    if self.provider == "openrouter":
                        payload = self._openrouter_json(system, user_prompt, io_extra=io_extra)
                    else:
                        payload = self._groq_json(system, user_prompt, io_extra=io_extra)
                except Exception as exc:
                    self._log_query_builder_phase(
                        ds_for_log=ds_for_log,
                        question=question,
                        db=db,
                        builder_kind=kind,
                        scoped_tables=scoped_tables,
                        schema_json=schema_json,
                        attempt_index=attempt,
                        system=system,
                        user_prompt=user_prompt,
                        status="llm_error",
                        extra_detail=str(exc)[:2000],
                    )
                    if attempt >= max_schema_retries:
                        return self._per_db_generation_failed(
                            gate_detail=f"{db}: llm_request_failed after retries",
                            planner_backend=planner_backend,
                            pipeline_trace=pipeline_trace,
                            engines_linked=engines_linked,
                            total_attempts=total_attempts,
                        )
                    fix_notes.append(f"{db}: llm_request_failed attempt {attempt + 1}")
                    continue

                if not isinstance(payload, dict):
                    self._log_query_builder_phase(
                        ds_for_log=ds_for_log,
                        question=question,
                        db=db,
                        builder_kind=kind,
                        scoped_tables=scoped_tables,
                        schema_json=schema_json,
                        attempt_index=attempt,
                        system=system,
                        user_prompt=user_prompt,
                        status="bad_payload_type",
                        payload=payload,
                    )
                    if attempt >= max_schema_retries:
                        return self._per_db_generation_failed(
                            gate_detail=f"{db}: non_object_response after retries",
                            planner_backend=planner_backend,
                            pipeline_trace=pipeline_trace,
                            engines_linked=engines_linked,
                            total_attempts=total_attempts,
                        )
                    fix_notes.append(f"{db}: non_object_response")
                    continue

                raw_step = self._normalize_single_engine_response(db, payload)
                if raw_step is None:
                    self._log_query_builder_phase(
                        ds_for_log=ds_for_log,
                        question=question,
                        db=db,
                        builder_kind=kind,
                        scoped_tables=scoped_tables,
                        schema_json=schema_json,
                        attempt_index=attempt,
                        system=system,
                        user_prompt=user_prompt,
                        status="parse_error",
                        payload=payload,
                    )
                    if attempt >= max_schema_retries:
                        return self._per_db_generation_failed(
                            gate_detail=f"{db}: missing sql or collection/pipeline after retries",
                            planner_backend=planner_backend,
                            pipeline_trace=pipeline_trace,
                            engines_linked=engines_linked,
                            total_attempts=total_attempts,
                        )
                    fix_notes.append(f"{db}: missing sql or collection/pipeline in JSON")
                    continue

                ok, errs = validate_llm_generated_steps(
                    [raw_step],
                    schema_metadata,
                    validation_log_repo_root=self.repo_root,
                    validation_log_question=question,
                    validation_log_dataset_id=ds_for_log,
                )
                if ok:
                    self._log_query_builder_phase(
                        ds_for_log=ds_for_log,
                        question=question,
                        db=db,
                        builder_kind=kind,
                        scoped_tables=scoped_tables,
                        schema_json=schema_json,
                        attempt_index=attempt,
                        system=system,
                        user_prompt=user_prompt,
                        status="ok",
                        payload=payload,
                    )
                    steps_out.append(raw_step)
                    total_attempts += attempt + 1
                    pipeline_trace.append(
                        {
                            "phase": "query_build",
                            "engine": db,
                            "builder_kind": kind,
                            "attempts_used": attempt + 1,
                        }
                    )
                    engine_ok = True
                    break
                self._log_query_builder_phase(
                    ds_for_log=ds_for_log,
                    question=question,
                    db=db,
                    builder_kind=kind,
                    scoped_tables=scoped_tables,
                    schema_json=schema_json,
                    attempt_index=attempt,
                    system=system,
                    user_prompt=user_prompt,
                    status="schema_validation_failed",
                    payload=payload,
                    extra_detail=" | ".join(errs[:8]),
                )
                fix_notes.append(f"{db} schema: " + " | ".join(errs[:8]))
                if attempt >= max_schema_retries:
                    return self._per_db_generation_failed(
                        gate_detail=f"{db}: schema_validation_failed after retries: " + " | ".join(errs[:8]),
                        planner_backend=planner_backend,
                        pipeline_trace=pipeline_trace,
                        engines_linked=engines_linked,
                        total_attempts=total_attempts,
                    )
            if not engine_ok:
                return self._per_db_generation_failed(
                    gate_detail=f"{db}: engine_not_ok_without_retries_exhausted",
                    planner_backend=planner_backend,
                    pipeline_trace=pipeline_trace,
                    engines_linked=engines_linked,
                    total_attempts=total_attempts,
                )

        if not steps_out:
            return {
                "steps": [],
                "schema_gate_failed": True,
                "gate_detail": "need_schema_refresh:no_engine_passed_schema_gate",
                "model": self.model_name,
                "preexec_schema_attempts": total_attempts,
                "generation_mode": "per_database",
                "pipeline_metadata": {
                    "four_phase": True,
                    "planner_backend": planner_backend,
                    "engines_linked": engines_linked,
                },
                "pipeline_trace": pipeline_trace,
            }

        return {
            "steps": steps_out,
            "model": self.model_name,
            "preexec_schema_attempts": total_attempts,
            "generation_mode": "per_database",
            "pipeline_metadata": {
                "four_phase": True,
                "planner_backend": planner_backend,
                "engines_linked": engines_linked,
            },
            "pipeline_trace": pipeline_trace,
        }

    def _generate_steps_monolithic(
        self,
        question: str,
        selected_databases: List[str],
        context: Dict[str, Any],
        replan_notes: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Previous behavior: one LLM call with full multi-engine schema (large prompt)."""
        schema_metadata = context.get("schema_metadata") or {}
        layers = context.get("context_layers") or {}
        llm_g = context.get("llm_guidance") or {}
        bundle_json = (context.get("schema_bundle_json") or "").strip() or json.dumps(
            _schema_brief(schema_metadata, selected_databases), ensure_ascii=False
        )
        bundle_json = narrow_schema_bundle_json(bundle_json, selected_databases)
        bundle_json = bundle_json[:12000]
        hints = llm_g.get("query_hints") if isinstance(llm_g, dict) else {}
        rationale = (llm_g.get("rationale") or "")[:800] if isinstance(llm_g, dict) else ""
        dp = context.get("dataset_playbook")
        playbook_block = ""
        if isinstance(dp, dict):
            summ = str(dp.get("summary", "") or "").strip()
            if summ:
                playbook_block = (
                    "\nDATASET PLAYBOOK (mandatory benchmark semantics; choose tables/collections and fields accordingly):\n"
                    f"{summ[:6000]}\n"
                )
            gh_md = playbook_generation_hints_markdown(dp, selected_databases)
            if gh_md:
                playbook_block += (
                    "\nGENERATION_HINTS_BY_ENGINE (follow hints for each step's database):\n"
                    f"{gh_md}\n"
                )

        layer_json = json.dumps(layers, ensure_ascii=False)[:4000]
        schema_digest = _engine_schema_digest(schema_metadata, selected_databases)
        max_schema_retries = max(
            0,
            int(os.getenv("ORACLE_FORGE_LLM_PREEXEC_SCHEMA_RETRIES", "3")),
        )
        fix_notes: List[str] = list(replan_notes or [])

        system = (
            "You are a database query generator. Output a single JSON object with key 'steps' only. "
            "Each step must include: 'database' (one of the selected names), "
            "'dialect' ('sql' for PostgreSQL/SQLite/DuckDB or 'mongodb_aggregation' for MongoDB). "
            "For SQL databases include 'sql' string. For MongoDB include 'collection' string and 'pipeline' array. "
            f"{_intent_coverage_one_liner()} "
            "JSON key 'steps' only. Use ENGINE SCHEMAS column names exactly; CTEs allowed. "
            "Match dialect per database (e.g. review.stars vs review.rating per schema). "
            "Follow DATASET PLAYBOOK when it conflicts with generic names."
        )

        for attempt in range(max_schema_retries + 1):
            replan_block = ""
            if fix_notes:
                replan_block = (
                    "\nPrevious errors to fix before returning JSON (required):\n"
                    + "\n".join(f"- {n}" for n in fix_notes[-16:])
                )

            user_prompt = (
                f"Question: {question}\n"
                "OUTPUT REQUIREMENT: Each step must fully cover every part of the question that step is "
                "responsible for (all requested identifiers and metrics in that engine); use multiple "
                "columns or CTEs as needed—do not return only one scalar if the question asks for several "
                "distinct answers.\n"
                f"Selected databases (in order): {selected_databases}\n"
                f"Routing rationale: {rationale}\n"
                f"Query hints (JSON): {json.dumps(hints, ensure_ascii=False) if hints else '{}'}\n"
                f"{playbook_block}"
                "ENGINE SCHEMAS (authoritative columns per table/collection — SQL must match this exactly):\n"
                f"{schema_digest}\n"
                "PRIMARY schema bundle (narrowed; use with ENGINE SCHEMAS above):\n"
                f"{bundle_json}\n"
                f"{replan_block}\n"
                "Supporting KB context (trimmed JSON):\n"
                f"{layer_json}\n"
                "Produce ONE step per database in selected_databases order. "
                "For postgresql/sqlite/duckdb use key 'sql' with a single read-only SELECT (no DDL/DML). "
                "For mongodb use 'collection' and 'pipeline' (JSON array of aggregation stages). "
                "Return strict JSON: {\"steps\": [...]}"
            )
            user_prompt = self.token_limiter.truncate_text(user_prompt, self.token_limiter.max_prompt_tokens)

            ds_for_log: Optional[str] = None
            if isinstance(context.get("dataset_id"), str) and context.get("dataset_id"):
                ds_for_log = str(context["dataset_id"]).strip() or None
            _log_sql_builder_llm_prompts(
                self.repo_root,
                question=question,
                dataset_id=ds_for_log,
                selected_databases=list(selected_databases),
                attempt_index=attempt,
                system=system,
                user_prompt=user_prompt,
                provider=self.provider,
                model=self.model_name,
                extra={"generation_mode": "monolithic"},
            )

            io_extra = {
                "subphase": "monolithic_sql",
                "generation_mode": "monolithic",
                "dataset_id": ds_for_log,
                "schema_attempt": attempt,
                "question_preview": (question or "")[:4000],
            }
            try:
                if self.provider == "openrouter":
                    payload = self._openrouter_json(system, user_prompt, io_extra=io_extra)
                else:
                    payload = self._groq_json(system, user_prompt, io_extra=io_extra)
            except Exception:
                if attempt >= max_schema_retries:
                    return None
                fix_notes.append(f"llm_request_failed on attempt {attempt + 1}")
                continue

            if not isinstance(payload, dict):
                if attempt >= max_schema_retries:
                    return None
                fix_notes.append(f"attempt_{attempt + 1}: model_returned_non_object")
                continue
            steps = payload.get("steps")
            if not isinstance(steps, list) or not steps:
                if attempt >= max_schema_retries:
                    return None
                fix_notes.append(f"attempt_{attempt + 1}: missing_or_empty_steps")
                continue

            ok, errs = validate_llm_generated_steps(
                steps,
                schema_metadata,
                validation_log_repo_root=self.repo_root,
                validation_log_question=question,
                validation_log_dataset_id=ds_for_log,
            )
            if ok:
                return {
                    "steps": steps,
                    "model": self.model_name,
                    "preexec_schema_attempts": attempt + 1,
                    "generation_mode": "monolithic",
                }
            fix_notes.append(
                f"schema_validation_failed (attempt {attempt + 1}): " + " | ".join(errs[:10])
            )
            if attempt >= max_schema_retries:
                return None

        return None

    def _groq_json(
        self,
        system: str,
        user: str,
        *,
        io_extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if self.client is None:
            raise RuntimeError("Groq client unavailable")
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        append_llm_io_log(
            self.repo_root,
            {
                "phase": "query_generation",
                "provider": "groq",
                "model": self.model_name,
                "messages": messages,
                "request": {
                    "temperature": 0,
                    "max_tokens": self.max_gen_tokens,
                    "response_format": {"type": "json_object"},
                },
                **(io_extra or {}),
            },
        )
        response = self.client.chat.completions.create(
            model=self.model_name,
            temperature=0,
            max_tokens=self.max_gen_tokens,
            response_format={"type": "json_object"},
            messages=messages,
        )
        content = (response.choices[0].message.content or "{}").strip()
        return self._parse_json(content)

    def _openrouter_json(
        self,
        system: str,
        user: str,
        *,
        io_extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self.openrouter_api_key:
            raise RuntimeError("OPENROUTER_API_KEY missing")
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
            "max_tokens": self.max_gen_tokens,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        append_llm_io_log(
            self.repo_root,
            {
                "phase": "query_generation",
                "provider": "openrouter",
                "model": self.model_name,
                "messages": body["messages"],
                "request": {
                    "temperature": body.get("temperature"),
                    "max_tokens": body.get("max_tokens"),
                    "response_format": body.get("response_format"),
                },
                **(io_extra or {}),
            },
        )
        r = self.http_client.post(f"{self.openrouter_base_url}/chat/completions", headers=headers, json=body)
        r.raise_for_status()
        data = r.json()
        choices = data.get("choices") or []
        if not choices:
            return {}
        msg = choices[0].get("message") or {}
        content = msg.get("content") or "{}"
        if isinstance(content, list):
            content = "".join(str(part.get("text", "")) for part in content if isinstance(part, dict))
        return self._parse_json(str(content).strip())

    @staticmethod
    def _parse_json(content: str) -> Dict[str, Any]:
        text = content.strip()
        if text.startswith("```"):
            text = text.strip("`")
            if text.lower().startswith("json"):
                text = text[4:].strip()
        parsed = json.loads(text or "{}")
        return parsed if isinstance(parsed, dict) else {}
