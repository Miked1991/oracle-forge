"""Discover and cache schema information from all connected databases."""

import asyncio
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

@dataclass
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool = True
    is_primary_key: bool = False
    sample_values: List[Any] = field(default_factory=list)

@dataclass
class TableInfo:
    name: str
    database: str
    columns: Dict[str, ColumnInfo]
    row_count: int = 0
    description: str = ""

class SchemaIntrospector:
    """
    Introspect and cache schema from PostgreSQL, SQLite, MongoDB, DuckDB.
    
    Features:
    - Parallel schema discovery across databases
    - Column type mapping
    - Sample value collection for context
    - Relationship detection (foreign keys, references)
    - Change detection for schema evolution
    """
    
    def __init__(self, db_executor: Any) -> None:
        self.db: Any = db_executor
        self.schemas: Dict[str, Dict[str, TableInfo]] = defaultdict(dict)  # db_type -> table_name -> TableInfo
        self._cache_ttl = 300  # 5 minutes
        self._last_refresh = 0
    
    async def refresh(self, force: bool = False) -> None:
        """Refresh schema cache from all databases."""
        import time
        now = time.time()
        if not force and (now - self._last_refresh) < self._cache_ttl:
            return
        
        tasks = [
            self._introspect_postgresql(),
            self._introspect_sqlite(),
            self._introspect_mongodb(),
            self._introspect_duckdb()
        ]
        await asyncio.gather(*tasks)
        self._last_refresh = now
    
    async def _introspect_postgresql(self) -> None:
        """Introspect PostgreSQL schema."""
        try:
            async with self.db.postgres_pool.acquire() as conn:
                # Get all tables
                tables = await conn.fetch("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                """)
                
                for table in tables:
                    table_name = table['table_name']
                    
                    # Get columns
                    columns = await conn.fetch("""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_name = $1
                    """, table_name)
                    
                    # Get sample values (first 3 rows)
                    sample = await conn.fetch(f"SELECT * FROM {table_name} LIMIT 3")
                    
                    col_info: Dict[str, ColumnInfo] = {}
                    for col in columns:
                        sample_vals: List[Any] = [row[col['column_name']] for row in sample if row[col['column_name']] is not None]
                        col_info[col['column_name']] = ColumnInfo(
                            name=col['column_name'],
                            data_type=col['data_type'],
                            nullable=col['is_nullable'] == 'YES',
                            sample_values=sample_vals[:2]
                        )
                    
                    row_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                    self.schemas['postgresql'][table_name] = TableInfo(
                        name=table_name,
                        database='postgresql',
                        columns=col_info,
                        row_count=row_count or 0
                    )
        except Exception as e:
            print(f"PostgreSQL introspection failed: {e}")
    
    async def _introspect_sqlite(self) -> None:
        """Introspect SQLite schema (sync DB calls offloaded to executor).

        SQLite's sqlite3 module is synchronous.  Running it directly inside an
        async method blocks the event loop for the entire introspection duration.
        Offloading to run_in_executor keeps the loop responsive while the
        blocking I/O completes in a thread-pool worker.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._introspect_sqlite_sync)

    def _introspect_sqlite_sync(self) -> None:
        """Blocking SQLite introspection — called via run_in_executor."""
        try:
            conn: Any = self.db.sqlite_conn
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = cursor.fetchall()

            for (table_name,) in tables:
                cursor = conn.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()

                cursor = conn.execute(f"SELECT * FROM {table_name} LIMIT 3")
                sample = cursor.fetchall()
                col_names: List[str] = (
                    [desc[0] for desc in cursor.description]
                    if cursor.description else []
                )

                col_info: Dict[str, ColumnInfo] = {}
                for col in columns:
                    col_name: str = col[1]
                    col_type: str = col[2]
                    idx = col_names.index(col_name) if col_name in col_names else -1
                    sample_vals: List[Any] = [
                        row[idx] for row in sample
                        if idx >= 0 and row[idx] is not None
                    ]
                    col_info[col_name] = ColumnInfo(
                        name=col_name,
                        data_type=col_type,
                        nullable=col[3] == 0,
                        sample_values=sample_vals[:2],
                    )

                count_cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count: int = count_cursor.fetchone()[0]
                self.schemas['sqlite'][table_name] = TableInfo(
                    name=table_name,
                    database='sqlite',
                    columns=col_info,
                    row_count=row_count,
                )
        except Exception as e:
            print(f"SQLite introspection failed: {e}")
    
    async def _introspect_mongodb(self) -> None:
        """Introspect MongoDB collections (PyMongo sync calls offloaded to executor)."""
        try:
            loop = asyncio.get_running_loop()
            db = self.db.mongo_client.get_database('dab')

            collections = await loop.run_in_executor(None, db.list_collection_names)

            for coll_name in collections:
                collection = db[coll_name]
                sample = await loop.run_in_executor(
                    None, lambda c=collection: list(c.find().limit(3))
                )
                row_count = await loop.run_in_executor(
                    None, collection.count_documents, dict()
                )

                if sample:
                    # Infer schema from first document
                    col_info: Dict[str, ColumnInfo] = {}
                    for key, value in sample[0].items():
                        if key == '_id':
                            continue
                        col_info[key] = ColumnInfo(
                            name=key,
                            data_type=type(value).__name__,
                            sample_values=[doc.get(key) for doc in sample[:2] if doc.get(key)]
                        )

                    self.schemas['mongodb'][coll_name] = TableInfo(
                        name=coll_name,
                        database='mongodb',
                        columns=col_info,
                        row_count=row_count
                    )
        except Exception as e:
            print(f"MongoDB introspection failed: {e}")
    
    async def _introspect_duckdb(self) -> None:
        """Introspect DuckDB schema."""
        try:
            result = self.db.duckdb_conn.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'main'
            """).fetchall()
            
            for (table_name,) in result:
                # Get columns
                columns = self.db.duckdb_conn.execute(f"DESCRIBE {table_name}").fetchall()
                
                # Get sample — read description before fetchall to avoid exhausted cursor
                sample_rel = self.db.duckdb_conn.execute(f"SELECT * FROM {table_name} LIMIT 3")
                col_names = [desc[0] for desc in sample_rel.description]
                sample = sample_rel.fetchall()
                
                col_info: Dict[str, ColumnInfo] = {}
                for col in columns:
                    col_name: str = col[0]
                    col_type: str = col[1]
                    idx: int = col_names.index(col_name) if col_name in col_names else -1
                    sample_vals: List[Any] = [row[idx] for row in sample if idx >= 0 and row[idx] is not None]
                    
                    col_info[col_name] = ColumnInfo(
                        name=col_name,
                        data_type=col_type,
                        nullable=col[2] == 'YES',
                        sample_values=sample_vals[:2]
                    )
                
                row_count = self.db.duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                self.schemas['duckdb'][table_name] = TableInfo(
                    name=table_name,
                    database='duckdb',
                    columns=col_info,
                    row_count=row_count
                )
        except Exception as e:
            print(f"DuckDB introspection failed: {e}")
    
    async def get_relevant_tables(self, query: str, top_k: int = 5) -> List[TableInfo]:
        """
        Find tables relevant to a natural language query.
        Uses keyword matching on table/column names.
        """
        await self.refresh()
        
        query_lower = query.lower()
        keywords = set(query_lower.split())
        relevance: List[Tuple[int, TableInfo]] = []

        for _, tables in self.schemas.items():
            for table_name, table_info in tables.items():
                score = 0
                table_lower = table_name.lower()

                # Phrase-level match: query "gene expression" → table gene_expression.
                # Underscore-separated table names become space-separated phrases so
                # natural-language queries like "low gene expression for TP53" hit the
                # gene_expression table even though no single word equals "gene_expression".
                table_phrase = table_lower.replace('_', ' ')
                if len(table_phrase) > 3 and table_phrase in query_lower:
                    score += 4  # phrase match outweighs individual word hits

                # Word-level match: individual underscore-split tokens vs query tokens.
                table_words = set(table_lower.split('_'))
                score += len(keywords & table_words) * 2

                # Column name match: individual underscore-split tokens vs query tokens.
                for col_name in table_info.columns.keys():
                    col_words = set(col_name.lower().split('_'))
                    score += len(keywords & col_words)

                if score > 0:
                    relevance.append((score, table_info))

        relevance.sort(key=lambda t: t[0], reverse=True)
        return [info for _, info in relevance[:top_k]]

    def get_all_schemas_as_text(self) -> str:
        """Convert all schemas to text for context injection."""
        lines: List[str] = []
        for db_label, tables in self.schemas.items():
            lines.append(f"\n## Database: {db_label.upper()}")
            for table_name, table_info in tables.items():
                lines.append(f"\n### Table: {table_name}")
                lines.append(f"Columns: {', '.join(table_info.columns.keys())}")
                for col_name, col_info in list(table_info.columns.items())[:5]:
                    samples: List[Any] = col_info.sample_values[:2]
                    sample_str = f" (e.g., {samples})" if samples else ""
                    lines.append(f"  - {col_name}: {col_info.data_type}{sample_str}")

        return "\n".join(lines)