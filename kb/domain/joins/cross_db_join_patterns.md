
---

## File: `kb/v2-domain/joins/cross_db_join_patterns.md`

```markdown
# Cross-Database Join Patterns

## Pattern 1: PostgreSQL to MongoDB (One-to-Many)

**Scenario:** Join customer transactions (PG) with support tickets (Mongo)

**Steps:**
1. Query PostgreSQL for customer_ids
2. Transform each ID: f"CUST-{customer_id}"
3. Query MongoDB with transformed IDs
4. Merge results on transformed key

**MongoDB Aggregation Pipeline:**
```javascript
db.support_tickets.aggregate([
  { $match: { customer_id: { $in: transformed_ids } } },
  { $group: { _id: "$customer_id", ticket_count: { $sum: 1 } } }
])

```

## Pattern 2: MongoDB to PostgreSQL (String to INT)

Scenario: Join MongoDB customer data to PostgreSQL transactions

Steps:

Query MongoDB for customer_ids (strings like "CUST-12345")

Extract INT: int(id.replace('CUST-', ''))

Query PostgreSQL with INTs

Merge results

## Pattern 3: Three-Way Join (PG → Mongo → DuckDB)

Scenario: Transaction data + support tickets + analytics

Steps:

PG worker: get base customer transactions

Mongo worker: get ticket counts using transformed IDs

Conductor merges PG + Mongo on customer_id

DuckDB worker: run analytical aggregation on merged results

## Failure Recovery Pattern

If join returns empty result:

Check format mismatch (INT vs STRING with prefix)

Apply transformation

Retry join

Log to failure_log.md if still failing

## Injection Test

Q: What are the steps for PostgreSQL to MongoDB join?
A: Query PG, transform IDs with f"CUST-{id}", query Mongo with transformed IDs, merge results
