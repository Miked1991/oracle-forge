# Evaluation Harness Schema (from Week 5 Ledger)

## Trace Schema (every tool call)

```json
{
  "query_id": "dab_yelp_003",
  "timestamp": "2026-04-10T14:32:01Z",
  "steps": [
    {
      "step": 1,
      "tool": "query_postgres",
      "input": "SELECT customer_id FROM transactions",
      "output": "[{customer_id: 12345}]",
      "duration_ms": 234,
      "success": true
    }
  ],
  "final_answer": "high-value segment",
  "pass@1": true,
  "confidence": 0.92
}

# Evaluation Harness Schema (from Week 5 Ledger)

## Trace Schema (every tool call)

```json
{
  "query_id": "dab_yelp_003",
  "timestamp": "2026-04-10T14:32:01Z",
  "steps": [
    {
      "step": 1,
      "tool": "query_postgres",
      "input": "SELECT customer_id FROM transactions",
      "output": "[{customer_id: 12345}]",
      "duration_ms": 234,
      "success": true
    }
  ],
  "final_answer": "high-value segment",
  "pass@1": true,
  "confidence": 0.92
}

Scoring

pass@1 = (correct first answers) / (total queries)

    Minimum 50 trials per query

    Confidence interval reported (95% CI)

Regression Detection

Run held-out set after every change
If score drops > 2%: reject change, log to regression_prevention.md
Injection Test

Q: What is pass@1 and how is it calculated?
A: pass@1 = correct first answers / total queries. Minimum 50 trials per query.
