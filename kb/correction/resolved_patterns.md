# Resolved Patterns - Permanent Fixes (autoDream Output)

## Last Consolidated: 2026-04-10

## Category: Join Key Transformation

### Pattern PG-INT to Mongo-String

**Confidence:** 14/14 successes
**Datasets:** Telecom, Healthcare
**Apply when:** source = PostgreSQL table with _id INT column, target = MongoDB with "CUST-{id}" or "PT-{id}"
**Transformation:**

```python
# Tables whose PG integer IDs map to MongoDB "CUST-{id}" string keys.
# Extended from {subscriber, customer} to cover all known entity-owning tables.
_CUST_PREFIX_TABLES = frozenset({
    'subscriber', 'subscribers',
    'customer', 'customers',
    'orders', 'order',           # order-level joins where customer_id is the FK
    'accounts', 'account',
    'contracts', 'contract',
    'clients', 'client',
})

def transform(source_value, source_table, source_db, target_db):
    if source_db == 'postgresql' and target_db == 'mongodb':
        table_lower = source_table.lower()
        if any(t in table_lower for t in _CUST_PREFIX_TABLES):
            return f"CUST-{source_value}"
        if 'patient' in table_lower:
            return f"PT-{source_value}"
    return source_value
```

## Pattern Case-Insensitive Business Match

**Confidence:** 9/10 successes
**Dataset:** Yelp
**Apply when:** comparing business_id from different sources
**Transformation:** .lower() on both sides before comparison

## Pattern Trailing Space Removal

**Confidence:** 7/7 successes
**Apply when:** MongoDB string comparison fails unexpectedly
**Transformation:** .strip() on all MongoDB string keys before matching

## Category: Sentiment Extraction

## Pattern Negative Sentiment Detection

**Confidence:** 23/25 successes
**Lexicon:** frustrated, angry, terrible, awful, worst, broken, not working, failed, error, complaint, unhappy, disappointed, useless, waste
**Negation handling:** "not good" = negative, "not bad" = non-negative
**Implementation:** text.lower(), check for indicator, check for negation prefix

## Category: Date Filtering

## Pattern Fiscal Quarter Detection

**Confidence:** 8/8 successes
**Rule:** If dataset = 'telecom' and query contains 'Q3', use dates '2025-10-01' to '2025-12-31'

If dataset = 'telecom' and query contains 'Q1', use '2025-01-01' to '2025-03-31'
Otherwise use calendar quarters

## Category: Multi-Database Routing

## Pattern Three-Step Join

**Confidence:** 11/12 successes

Steps:

1.Spawn PG worker for primary transaction data

2.Spawn Mongo worker for secondary ticket/comment data

3.Resolve join keys using transformation pattern

4.Merge results on transformed key

5.Spawn DuckDB worker for analytical aggregation on merged results

## Category: Concurrency Control

## Pattern Concurrent Worker Rate Limiting

**Confidence:** 1/1 successes
**Apply when:** batch size > 10 queries OR concurrent workers > 10
**Rule:** max 10 concurrent workers, 30-second timeout per query

Use the `asyncio` pattern — the codebase is fully async (`rate_limiter.py`,
`schema_introspector.py`, `test_probes.py` all use `asyncio.gather`).
`ThreadPoolExecutor` creates a sync/async boundary that raises
`RuntimeError: This event loop is already running` under the existing event loop.

**Implementation:**

```python
import asyncio

MAX_CONCURRENT_WORKERS = 10
QUERY_TIMEOUT_SECONDS = 30

async def run_all_queries(query_batch):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_WORKERS)

    async def _run(query):
        async with semaphore:
            return await asyncio.wait_for(
                run_query(query), timeout=QUERY_TIMEOUT_SECONDS
            )

    return await asyncio.gather(*[_run(q) for q in query_batch])
```

## Instructions for Agent

These patterns have high confidence scores.
Apply them automatically when conditions match.
Do not log to failure_log.md for these patterns (they are resolved).
