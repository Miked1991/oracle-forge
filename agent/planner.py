from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List

from .dab_yelp_postgres import is_yelp_template_question, postgres_sql_for_yelp_question
from .utils import canonical_db_name
from utils.query_router import QueryRouter


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

    def create_plan(
        self,
        question: str,
        available_databases: List[str],
        routing_question: str | None = None,
    ) -> Dict[str, Any]:
        # `question` = latest user utterance (exact string for Yelp SQL templates + tool payloads).
        # `routing_question` = optional transcript + current question for LLM/router/heuristics.
        route = (routing_question or question).strip()
        route_l = route.lower()
        available = [canonical_db_name(item) for item in available_databases]
        # Default: LLM (then router/heuristics) picks DBs. Optional oracle mode forces Postgres-only
        # for Yelp template questions so curated SQL in dab_yelp_postgres.py always runs.
        force_pg = (
            os.getenv("ORACLE_FORGE_YELP_FORCE_POSTGRESQL", "").lower() in {"1", "true", "yes"}
            and is_yelp_template_question(question)
            and "postgresql" in available
        )
        if force_pg:
            selected = ["postgresql"]
        else:
            selected = self._select_databases(route_l, available)
        steps: List[PlanStep] = []
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
        return {
            "question": question,
            "plan_type": "multi_db" if len(steps) > 1 else "single_db",
            "requires_join": len(steps) > 1 or "join" in route_l or "correlate" in route_l,
            "kb_layers_used": ["v1_architecture", "v2_domain", "v3_corrections"],
            "routing_constraints": self._routing_constraints(),
            "steps": [step.to_dict() for step in steps],
        }

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
        while replans <= max_replans:
            step_results = []
            for step in plan["steps"]:
                outcome = step_executor(step)
                step_results.append(outcome)
            all_attempts.append({"attempt": replans + 1, "plan": plan, "results": step_results})
            if all(item.get("ok") for item in step_results):
                return {"ok": True, "attempts": all_attempts, "final_plan": plan}
            failure_types = [item.get("error_type", "unknown_error") for item in step_results if not item.get("ok")]
            corrected = self._replan_with_corrections(
                question, available_databases, plan, failure_types, routing_question=routing_question
            )
            plan = corrected
            replans += 1
        return {"ok": False, "attempts": all_attempts, "final_plan": plan}

    def _select_databases(self, question: str, available: List[str]) -> List[str]:
        llm_guidance = self.context.get("llm_guidance", {})
        llm_selected = llm_guidance.get("selected_databases", []) if isinstance(llm_guidance, dict) else []
        if isinstance(llm_selected, list):
            selected_llm = [canonical_db_name(item) for item in llm_selected if canonical_db_name(item) in available]
            if selected_llm:
                return selected_llm

        router_picks: List[str] = []
        try:
            router = QueryRouter()
            routes = asyncio.run(router.route(question))
            for route in routes:
                db_value = canonical_db_name(getattr(route, "value", str(route)))
                if db_value in available and db_value not in router_picks:
                    router_picks.append(db_value)
            if router_picks:
                return router_picks
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
        return ordered

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
            collection = self._first_name(schema.get("collections"), "primary_collection")
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
            yelp_sql = postgres_sql_for_yelp_question(question)
            if yelp_sql:
                return {
                    "database": db,
                    "dialect": dialect,
                    "sql": yelp_sql,
                    "question": question,
                }
        table = self._select_sql_table(q_lower, schema.get("tables"))
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

    def _select_sql_table(self, question: str, tables: Any) -> str:
        candidates: List[str] = []
        if isinstance(tables, list):
            for item in tables:
                if isinstance(item, dict) and isinstance(item.get("name"), str):
                    candidates.append(item["name"])
                elif isinstance(item, str):
                    candidates.append(item)
        if not candidates:
            return ""

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

    @staticmethod
    def _first_name(collection: Any, fallback: str) -> str:
        if isinstance(collection, list) and collection:
            first = collection[0]
            if isinstance(first, dict) and "name" in first:
                return first["name"]
            if isinstance(first, str):
                return first
        return fallback

    def _replan_with_corrections(
        self,
        question: str,
        available_databases: List[str],
        prior_plan: Dict[str, Any],
        failure_types: List[str],
        routing_question: str | None = None,
    ) -> Dict[str, Any]:
        plan = self.create_plan(question, available_databases, routing_question=routing_question)
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
