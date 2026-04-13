# Signal Corps Engagement Log - Oracle Forge

_All external posts, threads, and community interactions. Updated daily._
_Covers: X, Reddit, Medium, LinkedIn, Discord_

---

## Week 8 (Apr 7-11, 2026)

### Project Milestones (context for content grounding)

| Date | Milestone | Owner | Commit/Evidence |
|------|-----------|-------|-----------------|
| 2026-04-07 | KB v0.1 initial structure created | Mikiyas | -- |
| 2026-04-08 | KB v1.0 Architecture layer: 6 docs (memory system, autoDream, tool scoping, OpenAI 6-layer, conductor/worker, eval harness). 6/6 injection tests pass | Mikiyas | `8f6caf9` |
| 2026-04-09 | KB v2.0 Domain layer: 9 docs (4 DB schemas, join key mappings, cross-DB patterns, text extraction, sentiment, business glossary). 9/9 injection tests pass | Mikiyas | `76aa867` - `9cf152f` |
| 2026-04-09 | Repo initialized on GitHub, .gitignore configured | Eyor, Mikiyas | `46d261e`, `6ab02eb` |
| 2026-04-10 | KB v3.0 Corrections layer: 4 docs (failure log, failure by category, resolved patterns, regression prevention). 6/6 injection tests pass. Total: 21/21 | Mikiyas | `4d976ef` |
| 2026-04-10 | Inception document committed to planning/ | Gashaw, Mikiyas | `91634c1` |
| 2026-04-10 | REFERENCEDOC.md added for team onboarding | Mikiyas | `a6fbd59` |
| 2026-04-11 | Signal Corps engagement infrastructure created (this file, community log, resource report) | Kirubel | feat/signal-corps-engagement |
| 2026-04-12 | KB injection test harness rerun: 21/21 pass on llama-3.1-8b-instant after 13 iterations. New INJECTION_TEST_LOG.md committed | Mikias | `2843265` |
| 2026-04-13 | Agent pipeline merged with develop (planner, context_builder, tools_client, sandbox_client, utils with normalize_join_key) | Eyor | `d5cd573` |
| 2026-04-13 | Probes + utilities + tests pushed: 19 probes (probes.md + test_probes.py), 6 utility modules (date_normalizer, join_key_resolver, query_router, rate_limiter, schema_introspector, unstructured_extractor), tests/ infrastructure (join_keys + routing). New KB docs: authoritative_tables, fiscal_calendar, null_guards. 2,421 insertions / 24 files | Mikias, Gashaw | `ad68f9a` |

### X/Twitter

| Date | Author | Type | Content Summary | Link | Likes | Replies |
|------|--------|------|-----------------|------|-------|---------|
| 2026-04-09 | Kirubel | Tweet | PostgreSQL + MongoDB friction: ill-formatted join keys in a single query. Links to DAB paper + repo | [link](https://x.com/kirubeltewodro2/status/2042250450888503584) | 1 | 0 |
| 2026-04-09 | Kirubel | Tweet | DAB 38% pass@1 ceiling = engineering gap signal, not benchmark flaw | [link](https://x.com/kirubeltewodro2/status/2042263948691415485) | 1 | 1 |
| 2026-04-10 | Kirubel | Tweet | Medium article announcement: cross-DB join key format mismatch | [link](https://x.com/kirubeltewodro2/status/2042676161499570186) | 1 | 0 |

### Medium/LinkedIn

| Date | Author | Title | Platform | Word Count | Link | Views |
|------|--------|-------|----------|------------|------|-------|
| 2026-04-10 | Kirubel | Engineering Resilience: Solving the Cross-Database Join Key Format Mismatch in AI Agents | Medium | ~1200 | [link](https://medium.com/@kirutew17654321/engineering-resilience-solving-the-cross-database-join-key-format-mismatch-in-ai-agents-ffb17b9d5a02) | -- |
<<<<<<< HEAD
| 2026-04-11 | Meseret | The Silent Killer of AI Data Agents (And How We're Engineering Around It) | LinkedIn | ~1800 | [link](https://www.linkedin.com/pulse/silent-killer-ai-data-agents-how-were-engineering-around-bolled-rsg8f ) | 52 |
=======
| 2026-04-11 | Meseret | The Silent Killer of AI Data Agents (And How We're Engineering Around It) | LinkedIn | -- | [link](https://www.linkedin.com/posts/meseret-bolled-8b395325b_aiengineering-dataengineering-aiagents-activity-7448667030389497856-bPq4) | -- |

>>>>>>> 4f11ad015e710bd545cf7d2af57d1489fd3a3f2d
### Reddit

| Date | Author | Subreddit | Title | Link | Upvotes | Comments |
|------|--------|-----------|-------|------|---------|----------|
| 2026-04-11 | Kirubel | r/learnmachinelearning | DataAgentBench shows the best frontier model hits 38% on realistic multi-DB data queries - what's actually causing the failures? | [link](https://www.reddit.com/r/learnmachinelearning/comments/1sieo3g/dataagentbench_shows_the_best_frontier_model_hits/) | -- | 1 (u/Far-Comparison-9745 reply Apr 13) |
| 2026-04-11 | Kirubel | r/LocalLLaMA | I kept running into cases where retrieval was the bottleneck -- injection testing with Groq Llama (21/21 pass rate) | [link](https://www.reddit.com/r/LocalLLaMA/comments/1siqbda/i_kept_running_into_cases_where_retrieval_was/) | -- | 1 (u/matt-k-wong) |
| 2026-04-13 | Kirubel | r/learnmachinelearning | Silent cross database join failures: has anyone dealt with int vs prefixed string ID mismatches? | [link](https://www.reddit.com/r/learnmachinelearning/comments/1sknnoa/silent_cross_database_join_failures_has_anyone/) | TBD | TBD |

### Reddit Replies (substantive comments in threads)

| Date | Author | Subreddit | Replying To | Summary | Link |
|------|--------|-----------|-------------|---------|------|
| 2026-04-11 | Kirubel | r/LocalLLaMA | u/matt-k-wong asking about model size | Clarified: Llama 3.3 70B via Groq. Explained structured docs (tables + code) outperform prose even at 70B -- same info as prose failed injection test, as table passed immediately. Invited comparison across param counts. | [thread](https://www.reddit.com/r/LocalLLaMA/comments/1siqbda/i_kept_running_into_cases_where_retrieval_was/) |
| 2026-04-11 | Kirubel | r/LocalLLaMA | OP of "Curated 550 Free LLM Tools" post | Substantive comment: flagged genai-toolbox (MCP Toolbox) as our standard interface layer, asked community about OSS for ill-formatted join key resolution (PG int ↔ MongoDB "CUST-00123"), suggested DAB + promptfoo + langsmith additions, offered to PR. | [comment](https://www.reddit.com/r/LocalLLaMA/comments/1sigg35/curated_550_free_llm_tools_for_builders_apis/) |
| 2026-04-13 | Kirubel | r/LocalLLaMA | Follow-up on own r/LocalLLaMA injection-testing post | Closed the loop on injection testing at sub-8B model scale: 21/21 pass on llama-3.1-8b-instant, confirming structured docs > raw context length is model-size-agnostic. Linked INJECTION_TEST_LOG.md on repo for verification. | [thread](https://www.reddit.com/r/LocalLLaMA/comments/1siqbda/i_kept_running_into_cases_where_retrieval_was/) |

### LinkedIn Replies (comments received on articles)

| Date | Author | Article | From | Comment Summary | Link |
|------|--------|---------|------|-----------------|------|
| 2026-04-12 | Meseret | The Silent Killer of AI Data Agents | The AI Agent Index (19 followers) | "Silent failures are genuinely harder to handle than loud ones. Confident wrong answers erode trust without leaving a clear signal to debug. The No data found problem usually traces back to schema mismatches or query construction issues that only surface at the edges of real-world data." | [link](https://www.linkedin.com/pulse/silent-killer-ai-data-agents-how-were-engineering-around-bolled-rsg8f ) |

### Discord

| Date | Author | Server/Channel | Topic | Link |
|------|--------|----------------|-------|------|
| 2026-04-13 | Kirubel | Cohort class group | Peer asked for DAB Discord link. Confirmed via GitHub + leaderboard site that no official DAB Discord exists (UC Berkeley EPIC + Hasura PromptQL route everything through GitHub issues). Provided 3 verified alternative invites: Hugging Face, EleutherAI, LlamaIndex. First-mover help in cohort. | -- |
| 2026-04-13 | Kirubel | Hugging Face | Joined server (https://discord.gg/JfAtkvEtRb). Substantive engagement scheduled Apr 14-16. | https://discord.gg/JfAtkvEtRb |
| 2026-04-13 | Kirubel | EleutherAI | Joined server (https://discord.gg/zBGx3azzUn). Substantive engagement scheduled Apr 14-16. | https://discord.gg/zBGx3azzUn |
| 2026-04-13 | Kirubel | LlamaIndex | Joined server (https://discord.com/invite/eN6D2HQ4aX). Substantive engagement scheduled Apr 14-16. | https://discord.com/invite/eN6D2HQ4aX |

---
### Engagement Metrics

| Date | Platform | Content | Reactions | Comments | Impressions | Views |
|------|----------|---------|-----------|----------|-------------|-------|
| 2026-04-11 | LinkedIn | The Silent Killer of AI Data Agents (Meseret) | 28 | 1,132 | 52 |

### Internal Slack Daily Posts

| Date | Author | Channel | Content Summary |
|------|--------|---------|-----------------|
| 2026-04-07 to 2026-04-11 | Meseret | #oracle-standup | Daily Signal Corps updates posted every working day covering: what shipped, what is stuck, what is next, community intel gathered. 5 posts total across Week 8. |
### Internal Slack Daily Posts

| Author | Channel | Period | Content Summary |
|--------|---------|--------|-----------------|
| Kirubel | #oracle-standup | 2026-04-07 to 2026-04-11 | Posted daily Slack updates across all 5 days of Week 8 based on standup discussions and GitHub commit status. Updates covered: KB layer completions, injection test results, X thread publications, Medium article launch, Reddit posts, and Signal Corps infrastructure setup. 5 posts total. |

---

### Google Meet Standup Facilitation

| Author | Period | Role | Summary |
|--------|--------|------|---------|
| Meseret | 2026-04-07 to 2026-04-11 | Standup Facilitator | Led  daily Google Meet standups across Week 8. Responsibilities included: opening each session, reminding team members to join, giving each of the 6 members floor time to share progress, asking targeted questions about blockers and next steps, and ensuring alignment between Drivers, Intelligence Officers, and Signal Corps before closing each session. |

## Week 9 (Apr 14-18, 2026)

### X/Twitter

| Date | Author | Type | Content Summary | Link | Likes | Replies |
|------|--------|------|-----------------|------|-------|---------|
| 2026-04-13 | Kirubel | Reply | Domain Knowledge Trap (churn rate definition) — placed under @ashpreetbedi's Dash v2 thread (text-to-SQL agent context kit) | [link](https://x.com/kirubeltewodro2/status/2043614126912500174) | TBD | **1 (@matanzutta)** |
| 2026-04-13 | Kirubel | Reply | Domain Knowledge Trap reply placement | [link](https://x.com/kirubeltewodro2/status/2043614629004243389) | TBD | TBD |
| 2026-04-13 | Kirubel | Reply | Domain Knowledge Trap reply placement | [link](https://x.com/kirubeltewodro2/status/2043615424114200756) | TBD | TBD |
| 2026-04-13 | Kirubel | Reply | Negation Problem reply placement | [link](https://x.com/kirubeltewodro2/status/2043616814421180639) | TBD | TBD |
| 2026-04-13 | Kirubel | Reply | Negation Problem reply placement (under @0xcgn) | [link](https://x.com/kirubeltewodro2/status/2043618142870474802) | TBD | TBD |

### Medium/LinkedIn

| Date | Author | Title | Platform | Word Count | Link | Views |
|------|--------|-------|----------|------------|------|-------|
| 2026-04-11 | Meseret | The Silent Killer of AI Data Agents (And How We're Engineering Around It) | LinkedIn | -- | [link](https://www.linkedin.com/posts/meseret-bolled-8b395325b_aiengineering-dataengineering-aiagents-activity-7448667030389497856-bPq4) | -- |

### Reddit

| Date | Author | Subreddit | Title | Link | Upvotes | Comments |
|------|--------|-----------|-------|------|---------|----------|
| 2026-04-13 | Kirubel | r/learnmachinelearning | Silent cross database join failures: has anyone dealt with int vs prefixed string ID mismatches? | [link](https://www.reddit.com/r/learnmachinelearning/comments/1sknnoa/silent_cross_database_join_failures_has_anyone/) | TBD | TBD |

---

## Community Intelligence

_External responses or findings that changed the team's technical approach._

| Date | Source | Finding | Impact on Build |
|------|--------|---------|-----------------|
| 2026-04-11 | r/MachineLearning | Posting blocked: account too new or karma too low for r/MachineLearning. Pivoted to r/learnmachinelearning | Adjusted community targeting. r/learnmachinelearning has lower barrier, still relevant audience. Will build karma for r/MachineLearning access in Week 9. |
| 2026-04-11 | u/matt-k-wong (r/LocalLLaMA) | Validated "longer docs = lower quality" as universal LLM property; linked our injection test approach to Karpathy's wiki thesis | Reinforced Mikias's table-heavy, Q&A-anchored KB format; 21/21 injection test pass on llama-3.1-8b-instant (Apr 12) is a direct outcome |
| 2026-04-13 | DAB official channels (GitHub + leaderboard site) | No official DAB Discord exists. UC Berkeley EPIC Lab + Hasura PromptQL route community through GitHub issues only | Cohort community strategy redirected to Hugging Face / EleutherAI / LlamaIndex Discords per Practitioner Manual guidance. Three Discords joined. |
| 2026-04-13 | u/Far-Comparison-9745 (r/learnmachinelearning) | Asked the right question: how to capture domain rules never in schema (e.g., "active = last 90 days") | Validates need for `kb/domain/domain_terms/business_glossary.md` + SME capture loop; informs how we frame the domain knowledge story externally |
| 2026-04-13 | @matanzutta on X (replied to Kirubel's Domain Knowledge Trap reply under @ashpreetbedi/Dash v2) | "the gap between what the schema says and what the business actually means is where most agent queries go wrong" — verbatim restatement of our Domain Knowledge thesis from a non-coordinated practitioner | **Strongest external validation in portfolio so far.** Confirms our framing lands with practitioners outside the cohort. Cite in final engagement summary and Week 9 X content. |

---

## Engagement Summary Stats

### Week 8 Totals
- **X posts:** 3
- **Medium articles:** 1 (Kirubel, ~1200 words, published)
- **LinkedIn articles:** 1 (Meseret, "Silent Killer of AI Data Agents", published Apr 11)
- **SC article deliverable:** ✅ 2/2
- **Reddit posts:** 2
- **Reddit comments/replies:** 2 substantive (u/matt-k-wong validation thread + "550 Free LLM Tools" comment)
- **Discord engagement:** Cohort class group (1, first-mover help) + 3 servers joined (HF, EleutherAI, LlamaIndex)

### Week 9 In-Progress Totals (as of 2026-04-13)
- **X reply-placements:** 5 (one received reply from @matanzutta validating thesis)
- **Reddit posts:** 1 (new "Silent cross database join failures" post on r/learnmachinelearning)
- **Reddit replies received:** 1 (u/Far-Comparison-9745 asking about automating domain rule capture)
- **Discord servers joined:** 3 (substantive engagement deploying Apr 14-16)

### Accounts Tracked for Reply-Threading

| Account | Platform | Focus Area | Link |
|---------|----------|------------|------|
| @shipp_ai | X | AI engineering | https://x.com/shipp_ai |
| @_avichawla | X | Data/ML engineering | https://x.com/_avichawla |
| @himanshustwts | X | AI/ML threads | https://x.com/himanshustwts |
| @sh_reya | X | AI engineering | https://x.com/sh_reya |
