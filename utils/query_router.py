"""Route natural language queries to appropriate database systems.

Implements the Conductor-Worker pattern described in
kb/architecture/conductor_worker_pattern.md:

  1. route()                — decide which DB engines are needed
  2. split_query_for_cross_db() — produce a SubQuery per engine so that
                               the LLM/agent can generate engine-specific
                               SQL / pipelines without mixing table
                               references across engines
  3. Per-engine helpers     — build concrete query templates from schema
                               introspector results
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

# ---------------------------------------------------------------------------
# Enums & data structures
# ---------------------------------------------------------------------------

class DatabaseType(Enum):
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    MONGODB = "mongodb"
    DUCKDB = "duckdb"


@dataclass
class SubQuery:
    """
    Everything the LLM/agent needs to build one engine's slice of a
    cross-database query.

    Fields
    ------
    db_type         : which engine this sub-query targets
    natural_language: the portion of the original question relevant to this DB
    table_hints     : candidate table/collection names from the schema
    join_key        : (local_field, remote_field) if this engine owns one side
                      of the cross-DB join; None for the "receiving" side
    filter_hints    : concept tokens the WHERE clause should target
                      (e.g. ['Q3 2024', 'open ticket', 'purchase'])
    """
    db_type: DatabaseType
    natural_language: str
    table_hints: list[str]
    join_key: tuple[str, str] | None      # (local_col, remote_col)
    filter_hints: list[str]


# ---------------------------------------------------------------------------
# Routing keyword catalogue
#
# Each dataset / domain is mapped to the engine that owns it.
# Keys are lowercase tokens; values are the corresponding DatabaseType.
# Longer / more specific tokens score higher than short generic ones.
# ---------------------------------------------------------------------------
_ROUTING_CATALOGUE: list[tuple[str, DatabaseType, int]] = [
    # ── dataset names ────────────────────────────────────────────────────────
    ("crmarenapro",         DatabaseType.POSTGRESQL, 5),
    ("yelp",                DatabaseType.DUCKDB,     4),
    ("pancancer",           DatabaseType.DUCKDB,     5),
    ("tcga",                DatabaseType.DUCKDB,     5),
    ("github_repos",        DatabaseType.DUCKDB,     5),
    ("github",              DatabaseType.SQLITE,     3),
    ("stockmarket",         DatabaseType.POSTGRESQL, 4),
    ("bookreview",          DatabaseType.POSTGRESQL, 4),

    # ── table / collection names ─────────────────────────────────────────────
    ("orders",              DatabaseType.POSTGRESQL, 4),
    ("customers",           DatabaseType.POSTGRESQL, 4),
    ("mutations",           DatabaseType.POSTGRESQL, 5),
    ("nps_scores",          DatabaseType.SQLITE,     4),
    ("support_tickets",     DatabaseType.POSTGRESQL, 4),
    ("churn_predictions",   DatabaseType.DUCKDB,     4),
    ("gene_expression",     DatabaseType.DUCKDB,     5),
    ("business",            DatabaseType.DUCKDB,     3),
    ("checkin",             DatabaseType.DUCKDB,     4),
    ("loyalty",             DatabaseType.DUCKDB,     4),
    ("repositories",        DatabaseType.SQLITE,     4),
    ("dependencies",        DatabaseType.SQLITE,     4),
    ("contributors",        DatabaseType.DUCKDB,     4),
    ("finance.fact_revenue",DatabaseType.POSTGRESQL, 5),
    ("sales.order_line",    DatabaseType.POSTGRESQL, 3),
    ("reviews",             DatabaseType.MONGODB,    4),
    # NOTE: "tickets" removed — "support_tickets" (PostgreSQL) is the authoritative
    # ticket table.  "tickets" as a standalone token was incorrectly routing
    # support-ticket queries to MongoDB, conflicting with support_tickets→POSTGRESQL.

    # ── domain / field terms ─────────────────────────────────────────────────
    ("churn risk",          DatabaseType.DUCKDB,     3),
    ("gene expression",     DatabaseType.DUCKDB,     5),
    ("review rating",       DatabaseType.MONGODB,    4),
    ("review text",         DatabaseType.MONGODB,    4),
    ("readme",              DatabaseType.SQLITE,     4),
    ("machine learning",    DatabaseType.SQLITE,     3),
    ("deep learning",       DatabaseType.SQLITE,     3),
    ("daily return",        DatabaseType.POSTGRESQL, 4),
    ("stock",               DatabaseType.POSTGRESQL, 3),
    ("aapl",                DatabaseType.POSTGRESQL, 5),
    ("check-in",            DatabaseType.DUCKDB,     3),
    ("check-ins",           DatabaseType.DUCKDB,     3),
    ("useful",              DatabaseType.MONGODB,    3),
    ("wait time",           DatabaseType.MONGODB,    3),
    ("open support ticket", DatabaseType.DUCKDB,     4),
    ("open ticket",         DatabaseType.DUCKDB,     4),
    ("purchase",            DatabaseType.POSTGRESQL, 3),
    ("categories",          DatabaseType.DUCKDB,     3),
    ("category",            DatabaseType.DUCKDB,     2),

    # ── compound engine+table tokens (weight 6) ──────────────────────────────
    # These fire when a query explicitly qualifies a table with its engine name
    # (e.g. "SQLite `customers` table" or "DuckDB loyalty table").  Weight 6
    # beats the generic single-token entries below (weight 4) so the correct
    # engine wins even when a same-named table exists in another engine.
    # Required fix: M6 probe — "customers" alone scores PostgreSQL +4, but
    # "sqlite customers" scores SQLite +6, correctly overriding the conflict.
    ("sqlite customers",    DatabaseType.SQLITE,     6),
    ("sqlite `customers`",  DatabaseType.SQLITE,     6),
    ("duckdb loyalty",      DatabaseType.DUCKDB,     6),
    ("duckdb `loyalty`",    DatabaseType.DUCKDB,     6),

    # ── NPS routing ───────────────────────────────────────────────────────────
    # nps_scores table lives in SQLite.  "nps score" / "nps" alone are not
    # substrings of "nps_scores", so add explicit tokens so D5/J2 queries
    # route to the correct engine without relying on schema-introspector boost.
    ("nps score",           DatabaseType.SQLITE,     5),
    ("nps",                 DatabaseType.SQLITE,     3),

    # ── explicit engine name keywords ────────────────────────────────────────
    # Queries that explicitly name the engine (e.g. "Join the SQLite customers
    # table with DuckDB loyalty table") must route to the named engines even
    # when no table-level catalogue entry matches.  Weight 4 ensures these
    # beat generic single-table hints but lose to dataset-specific tokens (5).
    ("sqlite",              DatabaseType.SQLITE,     4),
    ("duckdb",              DatabaseType.DUCKDB,     4),
    ("postgresql",          DatabaseType.POSTGRESQL, 4),
    ("postgres",            DatabaseType.POSTGRESQL, 4),
    ("mongodb",             DatabaseType.MONGODB,    4),
    ("mongo",               DatabaseType.MONGODB,    3),

    # ── generic engine hints (lowest priority) ───────────────────────────────
    ("unstructured",        DatabaseType.MONGODB,    2),
    ("free text",           DatabaseType.MONGODB,    2),
    ("description",         DatabaseType.MONGODB,    2),
    ("comment",             DatabaseType.MONGODB,    2),
    ("feedback",            DatabaseType.MONGODB,    2),
    ("analytics",           DatabaseType.DUCKDB,     2),
    ("aggregate",           DatabaseType.DUCKDB,     2),
    ("parquet",             DatabaseType.DUCKDB,     3),
    ("local",               DatabaseType.SQLITE,     1),
    ("embedded",            DatabaseType.SQLITE,     2),
    ("config",              DatabaseType.SQLITE,     1),
]

# Default table name per engine when no catalogue hint is found
_DEFAULT_TABLE: dict[DatabaseType, str] = {
    DatabaseType.POSTGRESQL: "customers",
    DatabaseType.MONGODB:    "reviews",
    DatabaseType.DUCKDB:     "business",
    DatabaseType.SQLITE:     "repositories",
}

# Cross-DB join key registry: maps (left_db, right_db) to the canonical
# join-key pair so SubQuery can populate join_key automatically.
_JOIN_KEY_REGISTRY: dict[
    tuple[DatabaseType, DatabaseType], tuple[str, str]
] = {
    (DatabaseType.POSTGRESQL, DatabaseType.MONGODB):  ("customer_id",   "customer_ref"),
    (DatabaseType.MONGODB,    DatabaseType.POSTGRESQL):("customer_ref",  "customer_id"),
    (DatabaseType.POSTGRESQL, DatabaseType.DUCKDB):   ("customer_id",   "customer_id"),
    (DatabaseType.DUCKDB,     DatabaseType.POSTGRESQL):("customer_id",   "customer_id"),
    (DatabaseType.SQLITE,     DatabaseType.DUCKDB):   ("customer_id",   "cust_id"),
    (DatabaseType.DUCKDB,     DatabaseType.SQLITE):   ("cust_id",       "customer_id"),
    (DatabaseType.DUCKDB,     DatabaseType.MONGODB):  ("business_id",   "business_id"),
    (DatabaseType.MONGODB,    DatabaseType.DUCKDB):   ("business_id",   "business_id"),
}


# ---------------------------------------------------------------------------
# QueryRouter
# ---------------------------------------------------------------------------

class QueryRouter:
    """
    Determine which database(s) to query and how to split the work.

    The router is schema-aware: it calls schema_introspector.get_relevant_tables()
    to boost scores for engines that own tables matching the query.  If no
    schema introspector is available (e.g. in unit tests), keyword-only routing
    is used.

    Usage
    -----
    router = QueryRouter(schema_introspector)
    routes = await router.route(query)

    if router.needs_cross_db_join(query, routes):
        sub_queries = router.split_query_for_cross_db(query, routes)
        # hand each sub_query to the matching DB worker
    """

    def __init__(self, schema_introspector: Any = None) -> None:
        self.schema = schema_introspector

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def route(self, query: str) -> list[DatabaseType]:
        """
        Return the ordered list of DatabaseTypes needed for *query*.

        Scoring:
          - Catalogue keyword match  +weight
          - Schema table match       +3 per relevant table
          - Unstructured text hint   +5 for MongoDB
          - Cross-DB join hint       → return all engines with score > 0
        """
        if self.schema is not None:
            try:
                await self.schema.refresh()  # type: ignore[union-attr]
            except Exception:
                pass  # degrade gracefully if schema refresh fails

        query_lower = query.lower()
        scores: dict[DatabaseType, int] = {db: 0 for db in DatabaseType}

        # Catalogue scoring — longer tokens first to avoid partial shadowing
        for token, db_type, weight in sorted(
            _ROUTING_CATALOGUE, key=lambda t: len(t[0]), reverse=True
        ):
            if token in query_lower:
                scores[db_type] += weight

        # Schema-based boost
        if self.schema is not None:
            try:
                relevant = await self.schema.get_relevant_tables(query, top_k=3)  # type: ignore[union-attr]
                for table in relevant:
                    try:
                        scores[DatabaseType(table.database)] += 3
                    except ValueError:
                        pass
            except Exception:
                pass

        # Named-engine suppression ─────────────────────────────────────────────
        # When a query explicitly names ≥ 2 engines (e.g. "SQLite … DuckDB …"),
        # generic table-token matches must not pull in a third, unnamed engine.
        #
        # Example: M6 — "Join the SQLite `customers` table with DuckDB `loyalty`"
        # "customers" → PostgreSQL +4 fires even though PostgreSQL is not involved.
        # Fix: detect named engines; zero out all non-named engine scores so only
        # the explicitly mentioned engines participate in routing.
        _EXPLICIT_ENGINE_MAP: dict[str, DatabaseType] = {
            "sqlite":     DatabaseType.SQLITE,
            "duckdb":     DatabaseType.DUCKDB,
            "postgresql": DatabaseType.POSTGRESQL,
            "postgres":   DatabaseType.POSTGRESQL,
            "mongodb":    DatabaseType.MONGODB,
            "mongo":      DatabaseType.MONGODB,
        }
        named_engines = {
            db for tok, db in _EXPLICIT_ENGINE_MAP.items() if tok in query_lower
        }
        if len(named_engines) >= 2:
            for db in DatabaseType:
                if db not in named_engines:
                    scores[db] = 0

        # Unstructured text extraction → MongoDB boost
        if any(
            w in query_lower
            for w in ("extract", "free text", "note", "comment", "description",
                       "review text", "readme", "feedback")
        ):
            scores[DatabaseType.MONGODB] += 5

        # Cross-DB join signal → return all engines with score > 0
        if any(
            w in query_lower
            for w in ("join", "combine", "merge", "across", "together", "both")
        ):
            return [db for db in DatabaseType if scores[db] > 0] or [DatabaseType.POSTGRESQL]

        candidates = sorted(
            [(db, s) for db, s in scores.items() if s > 0],
            key=lambda x: x[1],
            reverse=True,
        )
        return [db for db, _ in candidates[:3]] if candidates else [DatabaseType.POSTGRESQL]

    def needs_cross_db_join(self, query: str, routes: list[DatabaseType]) -> bool:
        """Return True when the query requires joining results from > 1 engine."""
        q = query.lower()
        return (
            len(routes) > 1
            or any(w in q for w in ("across", "combine", "both"))
            or bool(re.search(r"join.*different", q))
        )

    def split_query_for_cross_db(
        self, query: str, routes: list[DatabaseType]
    ) -> dict[DatabaseType, SubQuery]:
        """
        Decompose *query* into one SubQuery per engine in *routes*.

        Each SubQuery carries:
          - the natural-language slice of the question relevant to that engine
          - table hints derived from the routing catalogue
          - the join key pair (if this engine participates in a cross-DB join)
          - filter-concept tokens extracted from the query

        The agent / LLM uses these SubQuery objects to construct the actual
        SQL statements or MongoDB pipelines — this method never writes raw
        SQL itself.
        """
        query_lower = query.lower()
        filter_hints = self._extract_filter_hints(query)
        sub_queries: dict[DatabaseType, SubQuery] = {}

        for db_type in routes:
            table_hints = self._table_hints_for(db_type, query_lower)
            nl_slice = self._nl_slice(query, db_type, table_hints)

            # Resolve join key: this engine vs. the first other engine in routes
            join_key: tuple[str, str] | None = None
            for other in routes:
                if other != db_type:
                    join_key = _JOIN_KEY_REGISTRY.get((db_type, other))
                    if join_key:
                        break

            sub_queries[db_type] = SubQuery(
                db_type=db_type,
                natural_language=nl_slice,
                table_hints=table_hints,
                join_key=join_key,
                filter_hints=filter_hints,
            )

        return sub_queries

    # ------------------------------------------------------------------
    # SQL / MongoDB template builders
    #
    # These produce *template* queries — they are placeholders for the
    # LLM to fill in predicates.  They are intentionally not executable
    # as-is; their purpose is to give the LLM the correct table name,
    # engine dialect, and join key so it can generate the real query.
    # ------------------------------------------------------------------

    def build_sql_template(
        self,
        sub_query: SubQuery,
        select_cols: str = "*",
        where_clause: str | None = None,
    ) -> str:
        """
        Return a SQL SELECT skeleton for *sub_query*.

        Uses the first table hint as the FROM target.  The caller (LLM/agent)
        is expected to replace the WHERE clause with a real predicate.

        Example output (PostgreSQL, J1 probe):
            -- PostgreSQL: churn risk score
            SELECT *
            FROM churn_predictions
            WHERE <predicate>   -- replace with: churn_score > 0.7
        """
        engine = sub_query.db_type.value
        table = sub_query.table_hints[0] if sub_query.table_hints else "relevant_table"
        hint_comment = (
            f"-- filter concepts: {', '.join(sub_query.filter_hints)}"
            if sub_query.filter_hints else ""
        )
        join_comment = ""
        if sub_query.join_key:
            local, remote = sub_query.join_key
            join_comment = f"-- join key: {local} → {remote} (remote engine)"

        where = f"WHERE {where_clause}" if where_clause else "WHERE <predicate>"

        parts = [
            f"-- {engine}: {sub_query.natural_language}",
        ]
        if hint_comment:
            parts.append(hint_comment)
        if join_comment:
            parts.append(join_comment)
        parts += [
            f"SELECT {select_cols}",
            f"FROM {table}",
            where + ";",
        ]
        return "\n".join(parts)

    def build_mongodb_template(
        self,
        sub_query: SubQuery,
        match_stage: dict[str, Any] | None = None,
        group_stage: dict[str, Any] | None = None,
    ) -> str:
        """
        Return a MongoDB aggregation pipeline skeleton for *sub_query*.

        Produces a Python-style pipeline string with $match and optional
        $group.  The caller (LLM/agent) fills in the actual field names
        and values.

        Example output (U1 probe — Yelp wait-time reviews):
            # MongoDB: Yelp reviews mentioning wait time in 2024
            # filter concepts: wait time, 2024, negative
            # join key: business_id → business_id (remote engine)
            pipeline = [
                {"$match": {"date": {"$regex": "2024"}, "text": {"$regex": "<wait_pattern>"}}},
                {"$count": "total"}
            ]
            db.reviews.aggregate(pipeline)
        """
        collection = sub_query.table_hints[0] if sub_query.table_hints else "collection"
        hint_comment = (
            f"# filter concepts: {', '.join(sub_query.filter_hints)}"
            if sub_query.filter_hints else ""
        )
        join_comment = ""
        if sub_query.join_key:
            local, remote = sub_query.join_key
            join_comment = f"# join key: {local} → {remote} (remote engine)"

        match = match_stage or {"<field>": "<value>"}
        stages = [f'    {{"$match": {match}}}']
        if group_stage:
            stages.append(f'    {{"$group": {group_stage}}}')
        else:
            stages.append('    {"$count": "total"}')

        lines = [f"# MongoDB: {sub_query.natural_language}"]
        if hint_comment:
            lines.append(hint_comment)
        if join_comment:
            lines.append(join_comment)
        lines += [
            "pipeline = [",
            ",\n".join(stages),
            "]",
            f"db.{collection}.aggregate(pipeline)",
        ]
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _table_hints_for(self, db_type: DatabaseType, query_lower: str) -> list[str]:
        """
        Return catalogue table/collection names that belong to *db_type*
        and whose token appears in *query_lower*, ordered by weight desc.
        """
        matches: list[tuple[int, str]] = []
        for token, engine, weight in _ROUTING_CATALOGUE:
            if engine == db_type and token in query_lower:
                matches.append((weight, token))
        matches.sort(reverse=True)
        hints = [t for _, t in matches]
        # Guarantee at least one hint using the engine's canonical default table
        if not hints:
            hints = [_DEFAULT_TABLE.get(db_type, "relevant_table")]
        return hints

    def _nl_slice(
        self, query: str, db_type: DatabaseType, table_hints: list[str]
    ) -> str:
        """
        Produce a short natural-language description of what this engine
        needs to return, used to populate SubQuery.natural_language.
        """
        tables = ", ".join(table_hints[:2]) if table_hints else db_type.value
        return f"{db_type.value} slice: {tables} — {query[:80]}"

    @staticmethod
    def _extract_filter_hints(query: str) -> list[str]:
        """
        Pull time periods, thresholds, and named entities from the query
        to use as filter-concept hints for the LLM.
        """
        hints: list[str] = []

        # Fiscal / calendar periods
        for m in re.finditer(
            r"\b(Q[1-4]\s+\d{4}|FY\d{4}|\d{4}-\d{2}-\d{2}|\d{4})\b",
            query, re.IGNORECASE
        ):
            hints.append(m.group())

        # Numeric thresholds
        for m in re.finditer(r"\b(?:above|below|over|under|more than|less than)\s+[\d.]+", query, re.IGNORECASE):
            hints.append(m.group())

        # Named tickers / gene names
        for m in re.finditer(r"\b[A-Z]{2,5}\b", query):
            candidate = m.group()
            if candidate not in {"WHERE", "FROM", "JOIN", "AND", "NOT", "IN",
                                  "SELECT", "NULL", "LIKE", "OR"}:
                hints.append(candidate)

        return list(dict.fromkeys(hints))  # deduplicate, preserve order
