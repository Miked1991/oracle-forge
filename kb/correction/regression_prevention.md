
---

## File: `kb/v3-corrections/regression_prevention.md`

```markdown
# Regression Prevention - Queries That Broke and Were Fixed

## Regression Test Set (run after EVERY change)

| Query ID | Failure | Fix | Test Command |
|----------|---------|-----|--------------|
| Q023 | INT→String join failed | Added resolve_join_key | `assert join_result is not None` |
| Q045 | Returned raw text | Added extract_before_count | `assert isinstance(result, int)` |
| Q067 | Wrong quarter | Added fiscal calendar lookup | `assert date_range == fiscal_q3` |
| Q089 | SQL to MongoDB | Added input validation | `assert worker.rejects_sql()` |
| Q102 | Trailing space mismatch | Added .strip() | `assert mongo_key == pg_key.strip()` |
| Q134 | No lowercasing on sentiment | Added .lower() | `assert extract_sentiment('ANGRY') == 'negative'` |
| Q156 | Case-sensitive join | Added .lower() to both sides | `assert join_on_id('ABC123', 'abc123') is not None` |

## Regression Run Script

```bash
python eval/regression_test.py \
  --test-set regression_queries.json \
  --expected regression_expected.json \
  --fail-fast \
  --output regression_report.html
```

## Rules

## If regression test fails on ANY query

2.Revert the change immediately

3.Log failure to failure_log.md

4.Do not deploy until all regression tests pass

5.Update this file with new failure mode

## If regression tests pass:

1.Deploy change

2.Update confidence scores in resolved_patterns.md

## Critical Principle

## Never break what already works

The regression suite is your safety net.
If it fails, you do not pass Go. You do not collect $200.
You revert and fix.

## Injection Test

Q: What happens if regression test fails?
A: Revert change, log failure, do not deploy until all tests pass
