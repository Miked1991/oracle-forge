# autoDream Memory Consolidation Pattern

## What autoDream Does

Compresses session transcripts into persistent memory without losing signal. Runs on Fridays (weekly) or when correction log exceeds 50 entries. Output is written to resolved_patterns.md.

## The Algorithm

**Input:** Session transcripts (last 10 sessions, filtered by relevance)
**Output:** Updated resolved_patterns.md (kb/v3-corrections/resolved_patterns.md)

**Process:**

1. Extract all [query] → [wrong] → [correct] triples
2. Cluster by failure category (multi-db, join keys, unstructured, domain)
3. For each cluster:
   - Keep the most specific correct approach
   - Remove duplicates
   - Add confidence score (how many times this fix worked)
4. Write to resolved_patterns.md with format:

```markdown
## Category: Ill-Formatted Join Keys
### Pattern: Integer Customer ID → MongoDB String
Confidence: 12 successes, 0 failures
Apply when: PostgreSQL customer_id column exists, MongoDB collection has "CUST-{id}"
Transformation: f"CUST-{customer_id}"
