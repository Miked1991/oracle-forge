# Conductor-Worker Pattern for Multi-Database Queries

## From Week 4 Brownfield Cartographer

**Conductor Agent (holds KB context, orchestrates):**

- Receives natural language query
- Parses: which databases needed? what join keys? what extraction?
- Spawns workers with database-specific KB subsets
- Merges results, resolves conflicts

**Worker Agents (database-specific):**

- PostgreSQL worker: gets kb/domain/databases/postgresql_schemas.md
- MongoDB worker: gets kb/domain/databases/mongodb_schemas.md
- Each worker returns: {result, query_trace, confidence}

## Implementation for DAB

**Query:** "Which customer segments had declining repeat purchase rates in Q3, correlated with support ticket volume?"

**Conductor decides:**

1. Transaction data → PostgreSQL worker (repeat purchases)
2. Support tickets → MongoDB worker (ticket counts)
3. Join key: customer_id (integer in PG, "CUST-{int}" in Mongo)
4. Time filter: Q3 (fiscal calendar from kb/domain/domain_terms/)

**Execution:**
conductor.spawn(postgres_worker, "repeat_purchases Q3")
conductor.spawn(mongodb_worker, "support_tickets Q3")
conductor.resolve_join(worker1.result, worker2.result, using="customer_id")
conductor.correlate(results)

## Failure Recovery

If worker returns error: conductor logs to kb/corrections/failure_log.md
Then spawns corrected worker with additional context from corrections log

## Injection Test

Q: How does the agent handle multi-database queries?
A: The Conductor spawns database-specific workers (one per DB), then merges results from all workers into a unified response.
