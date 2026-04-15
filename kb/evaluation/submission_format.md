# DAB Benchmark Submission Format

## PR Requirements

**Title:** "[Team llama] - TRP1 FDE Programme, April 2026"

**Files to submit:**

1. `submission/team_name_results.json` — the results JSON file
2. `AGENT.md` (architecture description)

## AGENT.md Template

```markdown
# Agent Architecture: [Team Name]

## Overview
[2-3 sentences describing approach]

## Key Design Decisions
- Context layering: [describe your 3+ layers]
- Multi-database routing: [conductor-worker pattern]
- Self-correction: [kb/corrections/ failure log + resolved patterns]

## What Worked
- [Pattern 1]
- [Pattern 2]

## What Didn't
- [Failure pattern and fix]

## Score
pass@1: [score] (95% CI: [lower, upper])
Trials: 50 per query
```

## PR Link

Once submitted, add link to results/ directory:
results/dab_pr_link.txt

## Injection Test

Q: What files are required for DAB submission?
A: The results JSON file (team_name_results.json) and AGENT.md
