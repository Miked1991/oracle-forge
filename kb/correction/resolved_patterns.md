# Resolved Patterns - Permanent Fixes (autoDream Output)

## Last Consolidated: 2026-04-10



## Category: Join Key Transformation

### Pattern PG-INT to Mongo-String

**Confidence:** 14/14 successes
**Datasets:** Telecom, Healthcare
**Apply when:** source = PostgreSQL table with _id INT column, target = MongoDB with "CUST-{id}" or "PT-{id}"
**Transformation:**

```python
def transform(source_value, source_db, target_db):
    if source_db == 'postgresql' and target_db == 'mongodb':
        if 'subscriber' in source_table or 'customer' in source_table:
            return f"CUST-{source_value}"
        if 'patient' in source_table:
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

## Instructions for Agent

These patterns have high confidence scores.
Apply them automatically when conditions match.
Do not log to failure_log.md for these patterns (they are resolved).
