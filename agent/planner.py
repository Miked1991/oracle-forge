from __future__ import annotations

import asyncio
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from .dab_yelp_postgres import is_yelp_template_question, postgres_sql_for_yelp_question
from .llm_query_generator import LLMQueryGenerator
from .utils import canonical_db_name
from utils.dataset_playbooks import playbook_engine_table_preferences, playbook_mongo_primary_collection
from utils.execution_hints import enrich_replan_notes
from utils.preexec_repair_log import append_preexec_repair_log, preexec_repair_max_attempts
from utils.preexec_repair_notes import build_preexec_failure_notes
from utils.query_router import QueryRouter
from utils.routing_policy import normalize_routing_selection


def _llm_sql_enabled() -> bool:
    """When true: LLM generates SQL/pipelines; Yelp string→SQL oracle is skipped."""
    return os.getenv("ORACLE_FORGE_LLM_SQL", "false").strip().lower() in {"1", "true", "yes", "on"}


def _query_pipeline_from_generator(gen_out: Any) -> Optional[Dict[str, Any]]:
    """Attach four-phase pipeline metadata/trace from ``LLMQueryGenerator.generate_steps``."""
    if not isinstance(gen_out, dict):
        return None
    out: Dict[str, Any] = {}
    md = gen_out.get("pipeline_metadata")
    tr = gen_out.get("pipeline_trace")
    if isinstance(md, dict):
        out["metadata"] = md
    if isinstance(tr, list):
        out["trace"] = tr
    return out if out else None


@dataclass
class PlanStep:
    step_id: int
    database: str
    objective: str
    selection_reason: str
    dialect: str
    query_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "step_id": self.step_id,
            "database": self.database,
            "objective": self.objective,
            "selection_reason": self.selection_reason,
            "dialect": self.dialect,
            "query_payload": self.query_payload,
        }


class QueryPlanner:
    def __init__(self, context: Dict[str, Any]) -> None:
        self.context = context
        self._repo_root = Path(__file__).resolve().parents[1]

    def create_plan(
        self,
        question: str,
        available_databases: List[str],
        routing_question: str | None = None,
        *,
        replan_notes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        # `question` = latest user utterance (exact string for Yelp SQL templates + tool payloads).
        # `routing_question` = optional transcript + current question for LLM/router/heuristics.
        route = (routing_question or question).strip()
        route_l = route.lower()
        available = [canonical_db_name(item) for item in available_databases]
        # Default: LLM (then router/heuristics) picks DBs. Optional oracle mode forces Postgres-only
        # for Yelp template questions so curated SQL in dab_yelp_postgres.py always runs.
        force_pg = (
            not _llm_sql_enabled()
            and os.getenv("ORACLE_FORGE_YELP_FORCE_POSTGRESQL", "").lower() in {"1", "true", "yes"}
            and is_yelp_template_question(question)
            and "postgresql" in available
        )
        if force_pg:
            selected = ["postgresql"]
        else:
            selected = self._select_databases(route_l, available)
        steps: List[PlanStep] = []
        gen_snapshot: Optional[Dict[str, Any]] = None
        preexec_repair_trace: List[Dict[str, Any]] = []
        if _llm_sql_enabled():
            gen = LLMQueryGenerator(self._repo_root)
            repair_notes: List[str] = list(replan_notes or [])
            max_repairs = preexec_repair_max_attempts()
            gen_out: Optional[Dict[str, Any]] = None
            for attempt in range(max_repairs + 1):
                gen_out = gen.generate_steps(
                    question, selected, self.context, replan_notes=repair_notes or None
                )
                if isinstance(gen_out, dict):
                    gen_snapshot = gen_out
                resolved = False
                if isinstance(gen_out, dict) and not gen_out.get("schema_gate_failed") and not gen_out.get(
                    "generation_failed"
                ):
                    raw_steps = gen_out.get("steps")
                    if isinstance(raw_steps, list) and raw_steps:
                        llm_steps = self._plan_steps_from_llm(question, route_l, selected, raw_steps)
                        if llm_steps:
                            steps = llm_steps
                            resolved = True
                append_preexec_repair_log(
                    self._repo_root,
                    {
                        "phase": "preexec_repair",
                        "attempt_index": attempt,
                        "max_attempts": max_repairs + 1,
                        "resolved": resolved,
                        "had_schema_gate_failed": bool(isinstance(gen_out, dict) and gen_out.get("schema_gate_failed")),
                        "had_generation_failed": bool(isinstance(gen_out, dict) and gen_out.get("generation_failed")),
                        "gate_detail": (
                            str(gen_out.get("gate_detail", ""))[:1500] if isinstance(gen_out, dict) else None
                        ),
                        "repair_notes_count": len(repair_notes),
                        "question": question[:400],
                    },
                )
                preexec_repair_trace.append(
                    {
                        "attempt": attempt,
                        "resolved": resolved,
                        "schema_gate_failed": bool(isinstance(gen_out, dict) and gen_out.get("schema_gate_failed")),
                        "generation_failed": bool(isinstance(gen_out, dict) and gen_out.get("generation_failed")),
                    }
                )
                if resolved:
                    break
                if attempt < max_repairs:
                    repair_notes.extend(build_preexec_failure_notes(gen_out, self.context))
                    if not isinstance(gen_out, dict) or (
                        gen_out.get("steps") and not resolved
                    ):
                        repair_notes.append(
                            "preexec:steps_present_but_plan_mapping_failed: cover every selected database with a valid step."
                        )
                    continue
                if isinstance(gen_out, dict) and (
                    gen_out.get("schema_gate_failed") or gen_out.get("generation_failed")
                ):
                    blocked: Dict[str, Any] = {
                        "question": question,
                        "plan_type": "schema_blocked",
                        "requires_join": False,
                        "kb_layers_used": ["v1_architecture", "v2_domain", "v3_corrections"],
                        "routing_constraints": self._routing_constraints(),
                        "steps": [],
                        "schema_gate_failed": True,
                        "generation_failed": bool(gen_out.get("generation_failed")),
                        "gate_detail": str(gen_out.get("gate_detail") or "need_schema_refresh"),
                        "preexec_repair_exhausted": True,
                        "preexec_repair_trace": preexec_repair_trace,
                    }
                    qp = _query_pipeline_from_generator(gen_out)
                    if qp:
                        blocked["query_pipeline"] = qp
                    return blocked
        if not steps:
            for index, db in enumerate(selected, start=1):
                dialect = "mongodb_aggregation" if db == "mongodb" else "sql"
                payload = self._build_query_payload(question, db, dialect)
                steps.append(
                    PlanStep(
                        step_id=index,
                        database=db,
                        objective=f"Fetch relevant evidence from {db}",
                        selection_reason=self._selection_reason(route_l, db),
                        dialect=dialect,
                        query_payload=payload,
                    )
                )
        plan_out: Dict[str, Any] = {
            "question": question,
            "plan_type": "multi_db" if len(steps) > 1 else "single_db",
            "requires_join": len(steps) > 1 or "join" in route_l or "correlate" in route_l,
            "kb_layers_used": ["v1_architecture", "v2_domain", "v3_corrections"],
            "routing_constraints": self._routing_constraints(),
            "steps": [step.to_dict() for step in steps],
        }
        qp = _query_pipeline_from_generator(gen_snapshot)
        if qp:
            plan_out["query_pipeline"] = qp
        if preexec_repair_trace:
            plan_out["preexec_repair_trace"] = preexec_repair_trace
        return plan_out

    def execute_closed_loop(
        self,
        question: str,
        available_databases: List[str],
        step_executor: Callable[[Dict[str, Any]], Dict[str, Any]],
        max_replans: int = 2,
        routing_question: str | None = None,
    ) -> Dict[str, Any]:
        replans = 0
        all_attempts: List[Dict[str, Any]] = []
        plan = self.create_plan(question, available_databases, routing_question=routing_question)
        if not plan.get("steps"):
            return {
                "ok": False,
                "attempts": [{"attempt": 1, "plan": plan, "results": [], "failure": "no_executable_steps"}],
                "final_plan": plan,
                "schema_gate_failed": plan.get("schema_gate_failed"),
            }
        while replans <= max_replans:
            step_results = []
            for step in plan["steps"]:
                outcome = step_executor(step)
                step_results.append(outcome)
            all_attempts.append({"attempt": replans + 1, "plan": plan, "results": step_results})
            if all(item.get("ok") for item in step_results):
                return {"ok": True, "attempts": all_attempts, "final_plan": plan}
            failure_types = [item.get("error_type", "unknown_error") for item in step_results if not item.get("ok")]
            step_errors = [str(item.get("error") or "") for item in step_results if not item.get("ok")]
            corrected = self._replan_with_corrections(
                question,
                available_databases,
                plan,
                failure_types,
                routing_question=routing_question,
                step_errors=step_errors,
            )
            plan = corrected
            replans += 1
        return {"ok": False, "attempts": all_attempts, "final_plan": plan}

    def _select_databases(self, question: str, available: List[str]) -> List[str]:
        schema_md = self.context.get("schema_metadata") or {}
        llm_guidance = self.context.get("llm_guidance", {})
        llm_selected = llm_guidance.get("selected_databases", []) if isinstance(llm_guidance, dict) else []
        if isinstance(llm_selected, list):
            selected_llm = [canonical_db_name(item) for item in llm_selected if canonical_db_name(item) in available]
            if selected_llm:
                return normalize_routing_selection(question, selected_llm, available, schema_md)

        router_picks: List[str] = []
        try:
            router = QueryRouter()
            routes = asyncio.run(router.route(question))
            for route in routes:
                db_value = canonical_db_name(getattr(route, "value", str(route)))
                if db_value in available and db_value not in router_picks:
                    router_picks.append(db_value)
            if router_picks:
                return normalize_routing_selection(question, router_picks, available, schema_md)
        except Exception:
            router_picks = []

        picks: List[str] = []
        rulebook = {
            "postgresql": ["sql", "subscriber", "business", "review", "relational", "table"],
            "mongodb": ["mongo", "document", "ticket", "issue", "sentiment", "aggregation", "pipeline"],
            "sqlite": ["sqlite", "transaction", "inventory", "store"],
            "duckdb": ["duckdb", "analytics", "window", "trend", "cube", "aggregate"],
        }
        for db, keywords in rulebook.items():
            if db in available and any(keyword in question for keyword in keywords):
                picks.append(db)
        if ("join" in question or "correlate" in question or "across" in question) and "mongodb" in available and "postgresql" in available:
            if "postgresql" not in picks:
                picks.append("postgresql")
            if "mongodb" not in picks:
                picks.append("mongodb")
        if not picks and available:
            priority = ["postgresql", "mongodb", "sqlite", "duckdb"]
            for candidate in priority:
                if candidate in available:
                    picks.append(candidate)
                    break
        ordered: List[str] = []
        for candidate in ["postgresql", "mongodb", "sqlite", "duckdb"]:
            if candidate in picks and candidate not in ordered:
                ordered.append(candidate)
        return normalize_routing_selection(question, ordered, available, schema_md)

    def _selection_reason(self, question: str, db: str) -> str:
        if db == "mongodb":
            return "MongoDB selected for document-oriented or aggregation intent and nested fields."
        if db == "postgresql":
            return "PostgreSQL selected as primary SQL source with strongest relational coverage."
        if db == "sqlite":
            return "SQLite selected for lightweight transactional queries."
        if db == "duckdb":
            return "DuckDB selected for analytical aggregate processing."
        return f"{db} selected based on routing heuristics."

    def _build_query_payload(self, question: str, db: str, dialect: str) -> Dict[str, Any]:
        q_lower = question.lower()
        schema = self.context.get("schema_metadata", {}).get(db, {})
        if db == "mongodb":
            dp = self.context.get("dataset_playbook") or {}
            collection = playbook_mongo_primary_collection(dp) or self._first_name(
                schema.get("collections"), "primary_collection"
            )
            pipeline: List[Dict[str, Any]] = [{"$limit": 100}]
            if "count" in q_lower:
                pipeline = [
                    {"$limit": 100},
                    {"$group": {"_id": None, "count": {"$sum": 1}}},
                ]
            if "average rating" in q_lower or "review rating" in q_lower:
                pipeline = [{"$limit": 200}]
            return {
                "database": db,
                "dialect": dialect,
                "collection": collection,
                "pipeline": pipeline,
                "question": question,
            }
        if db == "postgresql":
            if not _llm_sql_enabled():
                yelp_sql = postgres_sql_for_yelp_question(question)
                if yelp_sql:
                    return {
                        "database": db,
                        "dialect": dialect,
                        "sql": yelp_sql,
                        "question": question,
                    }
        table = self._select_sql_table(q_lower, schema.get("tables"), db)
        if not table:
            sql = "SELECT 1 AS health_check"
            return {
                "database": db,
                "dialect": dialect,
                "sql": sql,
                "question": question,
            }
        sql = f"SELECT * FROM {table} LIMIT 100"
        if "count" in q_lower:
            sql = f"SELECT COUNT(*) AS count FROM {table}"
        if "average rating" in q_lower or "review rating" in q_lower:
            avg_col = self._sql_avg_rating_column(db)
            sql = f"SELECT AVG({avg_col}) AS avg_rating FROM {table}"
        return {
            "database": db,
            "dialect": dialect,
            "sql": sql,
            "question": question,
        }

    @staticmethod
    def _sql_avg_rating_column(db: str) -> str:
        """DuckDB Yelp slice uses `rating`; PostgreSQL uses `stars` per kb/domain/databases/postgresql_schemas.md."""
        if db == "postgresql":
            return "stars"
        if db == "duckdb":
            return "rating"
        return "rating"

    def _select_sql_table(self, question: str, tables: Any, db: str) -> str:
        candidates: List[str] = []
        if isinstance(tables, list):
            for item in tables:
                if isinstance(item, dict) and isinstance(item.get("name"), str):
                    candidates.append(item["name"])
                elif isinstance(item, str):
                    candidates.append(item)
        if not candidates:
            return ""

        playbook = self.context.get("dataset_playbook") or {}
        candidates = self._playbook_filter_avoid_tables(question, candidates, playbook, db)
        preferred = self._playbook_preferred_sql_table(candidates, playbook, db)
        if preferred:
            return preferred

        lowered = question.lower()
        if any(token in lowered for token in ["rating", "review"]):
            for preferred in ["review", "reviews"]:
                if preferred in candidates:
                    return preferred
                for table in candidates:
                    if preferred in table.lower():
                        return table
        if "tip" in lowered:
            for preferred in ["tip", "tips"]:
                if preferred in candidates:
                    return preferred
                for table in candidates:
                    if preferred in table.lower():
                        return table
        if "user" in lowered:
            for preferred in ["user", "users"]:
                if preferred in candidates:
                    return preferred
                for table in candidates:
                    if preferred in table.lower():
                        return table
        if "checkin" in lowered:
            for preferred in ["checkin", "checkins"]:
                if preferred in candidates:
                    return preferred
                for table in candidates:
                    if preferred in table.lower():
                        return table
        return candidates[0]

    def _playbook_filter_avoid_tables(
        self, question: str, candidates: List[str], playbook: Dict[str, Any], db: str
    ) -> List[str]:
        prefs = playbook_engine_table_preferences(playbook, db)
        lowered = question.lower()
        out = list(candidates)
        for rule in prefs.get("avoid") or []:
            if not isinstance(rule, dict):
                continue
            kws = [str(k).lower() for k in (rule.get("question_keywords") or []) if k]
            if not kws or not any(k in lowered for k in kws):
                continue
            avoid_names = {str(a).lower() for a in (rule.get("avoid") or []) if a}
            if not avoid_names:
                continue
            filtered = [c for c in out if c.lower() not in avoid_names]
            if filtered:
                out = filtered
        return out

    @staticmethod
    def _playbook_preferred_sql_table(
        candidates: List[str], playbook: Dict[str, Any], db: str
    ) -> str:
        prefs = playbook_engine_table_preferences(playbook, db)
        order = prefs.get("preferred_order") or []
        by_lower = {c.lower(): c for c in candidates}
        for name in order:
            key = name.strip().lower()
            if key in by_lower:
                return by_lower[key]
            for c in candidates:
                if key == c.lower() or key in c.lower():
                    return c
        return ""

    @staticmethod
    def _first_name(collection: Any, fallback: str) -> str:
        if isinstance(collection, list) and collection:
            first = collection[0]
            if isinstance(first, dict) and "name" in first:
                return first["name"]
            if isinstance(first, str):
                return first
        return fallback

    def _plan_steps_from_llm(
        self,
        question: str,
        route_l: str,
        selected: List[str],
        raw_steps: List[Any],
    ) -> List[PlanStep]:
        """Map LLM JSON steps to PlanStep list; must cover every selected database."""
        if not isinstance(raw_steps, list) or not selected:
            return []
        by_db: Dict[str, Any] = {}
        for item in raw_steps:
            if not isinstance(item, dict):
                continue
            db = canonical_db_name(str(item.get("database", "")))
            if db:
                by_db[db] = item
        out: List[PlanStep] = []
        for index, db in enumerate(selected, start=1):
            raw = by_db.get(db)
            if not isinstance(raw, dict):
                return []
            if db == "mongodb":
                dialect = "mongodb_aggregation"
                col = str(raw.get("collection") or "").strip()
                pipe = raw.get("pipeline")
                if not col or not isinstance(pipe, list):
                    return []
                payload = {
                    "database": db,
                    "dialect": dialect,
                    "collection": col,
                    "pipeline": pipe,
                    "question": question,
                }
            else:
                dialect = "sql"
                sql = str(raw.get("sql") or "").strip()
                if not sql:
                    return []
                payload = {"database": db, "dialect": dialect, "sql": sql, "question": question}
            out.append(
                PlanStep(
                    step_id=index,
                    database=db,
                    objective=f"Fetch relevant evidence from {db}",
                    selection_reason=self._selection_reason(route_l, db),
                    dialect=dialect,
                    query_payload=payload,
                )
            )
        return out

    def _replan_with_corrections(
        self,
        question: str,
        available_databases: List[str],
        prior_plan: Dict[str, Any],
        failure_types: List[str],
        routing_question: str | None = None,
        step_errors: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        replan_notes: List[str] = []
        replan_notes.extend(f"failure_type:{ft}" for ft in failure_types)
        raw_errs = [str(err).strip() for err in (step_errors or []) if str(err).strip()]
        replan_notes.extend(enrich_replan_notes(raw_errs, self.context.get("schema_metadata")))
        plan = self.create_plan(
            question,
            available_databases,
            routing_question=routing_question,
            replan_notes=replan_notes[:24] or None,
        )
        known_failures = self.context.get("known_failures", [])
        resolved_patterns = self.context.get("resolved_patterns", [])
        correction_notes = []
        if any(ft == "join_mismatch" for ft in failure_types):
            correction_notes.append("Replan with join-key normalization strategy from v3 corrections.")
        if any(ft == "join_key_mismatch" for ft in failure_types):
            correction_notes.append("Replan with join-key normalization strategy from v3 corrections.")
        if any(ft == "schema_mismatch" for ft in failure_types) or any(ft == "schema_error" for ft in failure_types):
            correction_notes.append("Replan with stricter schema introspection table/field selection.")
        if any(ft == "sql_dialect_error" for ft in failure_types) or any(ft == "dialect_error" for ft in failure_types):
            correction_notes.append("Replan enforcing dialect constraints from v1 architecture layer.")
        if any(ft == "tool_routing_error" for ft in failure_types):
            correction_notes.append("Replan with explicit database-tool compatibility constraints.")
        if any(ft == "unsafe_sql" for ft in failure_types):
            correction_notes.append("Regenerate read-only SQL or Mongo pipeline; respect schema allowlist.")
        if not correction_notes:
            correction_notes.append("Generic replan based on prior failures and resolved patterns.")
        plan["replan_context"] = {
            "failure_types": failure_types,
            "known_failures_loaded": len(known_failures),
            "resolved_patterns_loaded": len(resolved_patterns),
            "correction_notes": correction_notes,
            "previous_plan_type": prior_plan.get("plan_type"),
        }
        return plan

    def _routing_constraints(self) -> List[str]:
        return [
            "Use architecture layer for tool selection and db routing.",
            "Use domain layer for schema terms and id formatting.",
            "Use corrections layer for self-correction replanning.",
        ]
