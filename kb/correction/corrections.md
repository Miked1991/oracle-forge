```markdown

# Corrections Log - KB v3

*This log is read by the agent at session start. Each entry documents a failure and its fix.*

## Failure: 2026-04-14T10:30:00
**Query:** "How many customers have both a transaction in PostgreSQL and a support ticket in MongoDB?"
**What went wrong:** Agent attempted direct join between PostgreSQL (customer_id=12345) and MongoDB (customer_id="CUST-12345"). Zero results returned.
**Correct approach:** Add key resolution: CAST(pg_customer_id AS TEXT) on PostgreSQL side, and REPLACE(mongo_customer_id, 'CUST-', '') on MongoDB side before joining.
**Fixed in version:** v1.0.1

## Failure: 2026-04-14T14:20:00
**Query:** "What is the average sentiment of reviews about service?"
**What went wrong:** Agent returned raw review text instead of calculated sentiment score.
**Correct approach:** First extract sentiment using sandbox with NLP, then average the scores.
**Fixed in version:** v1.0.2

## Failure: 2026-04-15T09:15:00
**Query:** "Which customers were active in Q3?"
**What went wrong:** Agent used "has any order ever" as definition of active.
**Correct approach:** Use domain definition: purchased in last 90 days from Q3 end date.
**Fixed in version:** v1.0.3

## Failure: 2026-04-15T16:45:00
**Query:** "Compare revenue from PostgreSQL with ticket count from MongoDB"
**What went wrong:** Agent queried databases sequentially but failed to merge by customer_id.
**Correct approach:** Execute both queries, extract customer_id from both, perform application-side merge.
**Fixed in version:** v1.0.4

## Failure: 2026-04-16T11:00:00
**Query:** "Count negative sentiment mentions in support notes"
**What went wrong:** Agent attempted to COUNT on text field directly.
**Correct approach:** Extract sentiment using keyword matching in sandbox, then COUNT where negative > positive.
**Fixed in version:** v1.0.5
