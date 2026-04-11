# DAB Scoring Method

## pass@1 Definition

**pass@1 = (number of correct first answers) / (total queries)**

- "First answer" means the agent's initial response before any correction
- Agent can self-correct internally, but final answer is what's scored
- Minimum 50 trials per query for statistical significance

## Confidence Intervals

Report 95% confidence interval using Wilson score:

```python
ci_lower, ci_upper = wilson_score(correct_count, total_count, 0.95)
```

## Submission Format for GitHub PR

```json
{
  "team_name": "Oracle Forge",
  "submission_date": "2026-04-18",
  "pass@1": 0.587,
  "confidence_interval": [0.542, 0.632],
  "trials_per_query": 50,
  "results_by_query": {
    "dab_yelp_001": {"correct": 48, "total": 50},
    "dab_yelp_002": {"correct": 47, "total": 50}
  }
}
```

## Leaderboard Comparison

Current SOTA: PromptQL + Gemini 3.1 Pro at 54.3% pass@1
Target: >55% to beat SOTA

## Injection Test

Q: What is pass@1?
A: correct first answers divided by total queries
