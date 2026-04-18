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
| 2026-04-11 | Repo admin: PR #1 (signal corps infra) + PR #2 (develop sync) merged to main; README add/add conflict resolved. Established team's PR review + merge pipeline | Gemechis | `47cc278`, `e753e1a`, `e158dd2` |
| 2026-04-12 | KB injection test harness rerun: 21/21 pass on llama-3.1-8b-instant after 13 iterations. New INJECTION_TEST_LOG.md committed | Mikias | `2843265` |
| 2026-04-13 | Agent pipeline merged with develop (planner, context_builder, tools_client, sandbox_client, utils with normalize_join_key) | Eyor | `d5cd573` |
| 2026-04-13 | Probes + utilities + tests pushed: 19 probes (probes.md + test_probes.py), 6 utility modules (date_normalizer, join_key_resolver, query_router, rate_limiter, schema_introspector, unstructured_extractor), tests/ infrastructure (join_keys + routing). New KB docs: authoritative_tables, fiscal_calendar, null_guards. 2,421 insertions / 24 files | Mikias, Gashaw | `ad68f9a` |
| 2026-04-14 | Adopted fork-first DataAgentBench workflow; wrote setup guide README pointing at forked DAB with OpenRouter + prompt structure changes. Unblocked team access to real DAB dataset via `common_scaffold/DataAgent.py` integration point | Gemechis | `99102de` |
| 2026-04-14 | LLM config switched Groq → OpenRouter; DB environment standardized on Docker (postgres + mongo via `mcp/docker-compose.yml`). Aligned team on single LLM path + reproducible local DB stack | Gemechis | `c4b3781` |
| 2026-04-14 | PR #6 merged: feat/agent → develop (38 files / 47,656 insertions). Full agent pipeline + KB + 21/21 injection tests integrated on one branch. Apr 11-13 integration blocker resolved | Eyor (author), Gemechis (merge) | `a416a95` |
| 2026-04-15 | End-to-end real DAB Yelp evaluation unblocked: fixed response formatting, LLM DB routing, MCP tool config. Best recent pass@1 85.7% (6/7 on 2-trial runs); stress run 42.86% (3/7 on 50-trial) — both values reported for reproducibility | Gemechis | `f274bdf` |
| 2026-04-15 | PR #8 merged: feat/openrouter-setup → develop. End-to-end Yelp run now on the main integration path | Gemechis | `5946a84` |

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
| 2026-04-11 | Meseret | The Silent Killer of AI Data Agents (And How We're Engineering Around It) | LinkedIn | -- | [link](https://www.linkedin.com/posts/meseret-bolled-8b395325b_aiengineering-dataengineering-aiagents-activity-7448667030389497856-bPq4) | -- |

### Reddit

| Date | Author | Subreddit | Title | Link | Upvotes | Comments |
|------|--------|-----------|-------|------|---------|----------|
| 2026-04-11 | Kirubel | r/learnmachinelearning | DataAgentBench shows the best frontier model hits 38% on realistic multi-DB data queries - what's actually causing the failures? | [link](https://www.reddit.com/r/learnmachinelearning/comments/1sieo3g/dataagentbench_shows_the_best_frontier_model_hits/) | -- | ⚠️ POST REMOVED by Reddit filters (account: Silly-Effort-6843) |
| 2026-04-11 | Kirubel | r/LocalLLaMA | I kept running into cases where retrieval was the bottleneck -- injection testing with Groq Llama (21/21 pass rate) | [link](https://www.reddit.com/r/LocalLLaMA/comments/1siqbda/i_kept_running_into_cases_where_retrieval_was/) | -- | ⚠️ POST REMOVED by Reddit filters (account: Silly-Effort-6843). u/matt-k-wong reply still visible but OP gone. |
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
| 2026-04-14 | Kirubel (KG) | Hugging Face #general | **5-message practitioner exchange with user H$Go (8:52 PM - 9:49 PM EAT)**. Opened with cross-DB join question framed against DAB; H$Go pushed back on the multi-DB premise. Walked H$Go through: DAB inherited-environment framing → 38% ceiling → MCP Discovery & Routing as the real hard part → agent-driven pattern detection + normalization codegen → KB Injection Testing as automated test gate (21/21 on sub-8B) → **Level 1 (Functional) vs Level 2 (Semantic / 0-rows-no-error) failure framing**. H$Go's "fuzzy AI" join suggestion validated our Correction Layer direction. First substantive Discord conversation of the project. | https://discord.com/channels/879548962464493619/879548962464493622/1493670392236212416 |

---
### Engagement Metrics

| Date | Platform | Content | Reactions | Comments | Impressions | Views |
|------|----------|---------|-----------|----------|-------------|-------|
| 2026-04-11 | LinkedIn | The Silent Killer of AI Data Agents (Meseret) | 28 | 1,132 | 52 |

### Internal Slack Daily Posts and Weekly summary

| Date | Author | Channel | Content Summary |
|------|--------|---------|-----------------|
| 2026-04-07 to 2026-04-11 | Meseret | #oracle-standup | Daily Signal Corps updates posted every working day covering: what shipped, what is stuck, what is next, community intel gathered. 5 posts total across Week 8. |

### Internal Slack Daily Posts

| Author | Channel | Period | Content Summary |
|--------|---------|--------|-----------------|
| Kirubel | #Team-llama | 2026-04-07 to 2026-04-11 | Posted daily Slack updates across all 5 days of Week 8 based on standup discussions and GitHub commit status. Updates covered: KB layer completions, injection test results, X thread publications, Medium article launch, Reddit posts, and Signal Corps infrastructure setup. 5 posts total. |
| Meseret | #Team-llama | 2026-04-07 to 2026-04-11 | Posted daily Slack updates across all days of Week 8 based on standup discussions Updates covered and shaed importan resources on the channel
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
| 2026-04-14 | Kirubel | Community Post | AI Agents community (14.7K): DAB 4 failure categories, context engineering framing | [link](https://x.com/kirubeltewodro2/status/2043992602979221805) | TBD | TBD |
| 2026-04-14 | Kirubel | Community Post | Machine Learning community: injection vs RAG, 21/21 on 8B, density > length | [link](https://x.com/kirubeltewodro2/status/2043994436850593947) | TBD | TBD |
| 2026-04-14 | Kirubel | Community Post | AI/Python/Data community: silent join mismatch, normalize_join_key() | [link](https://x.com/kirubeltewodro2/status/2043995579647439321) | TBD | TBD |
| 2026-04-14 | Kirubel | Community Post | Open Source Contributors: normalize_join_key() multi-DB utility | [link](https://x.com/kirubeltewodro2/status/2043996542756106502) | TBD | **Admin @jcubic replied asking if OSS** |
| 2026-04-14 | Kirubel | Community Post | AI Agents: HOT TAKE — RAG is wrong architecture for data agents, "change my mind" | [link](https://x.com/kirubeltewodro2/status/2044017533683196221) | TBD | TBD |
| 2026-04-14 | Kirubel | Community Post | Machine Learning: 38% ceiling isn't a model problem, it's context engineering | [link](https://x.com/kirubeltewodro2/status/2044017762004291818) | TBD | **@anandrishv replied asking topic — replied back with DAB + info density** |
| 2026-04-14 | Kirubel | Community Post | AI/Python/Data: WHERE LIKE '%wait%' overcounts 3-4x, "worst horror story?" | [link](https://x.com/kirubeltewodro2/status/2044039244964823216) | TBD | TBD |
| 2026-04-14 | Kirubel | Community Post | Open Source: open-sourced KB, 21 docs tested, PRs welcome + repo link | [link](https://x.com/kirubeltewodro2/status/2044039490868564383) | TBD | TBD |
| 2026-04-15 | Kirubel | Thread | Article B launch thread: injection-testing method (21 docs, 13 iterations, 8B model) and bounded-domain argument against retrieval-first defaults | [link](https://x.com/kirubeltewodro2/status/2044376586380546117) | TBD | TBD |

### Medium/LinkedIn

| Date | Author | Title | Platform | Word Count | Link | Views |
|------|--------|-------|----------|------------|------|-------|
| 2026-04-14 | Kirubel | Why Your AI Data Agent Silently Fails on Cross-Database Queries | Medium | ~1500 | (published Apr 14) | -- |
| 2026-04-15 | Kirubel | We Injection-Tested 21 Knowledge Base Documents on an 8B Model. Here's What Actually Worked. | Medium | ~1600 | [link](https://medium.com/@kirutew17654321/we-injectiontested-21-knowledge-base-documents-on-an-8b-model-here-is-what-actually-worked-9fbc99b22ac8) | -- |

### Reddit

| Date | Author | Subreddit | Title | Link | Upvotes | Comments |
|------|--------|-----------|-------|------|---------|----------|
| 2026-04-13 | Kirubel | r/learnmachinelearning | Silent cross database join failures: has anyone dealt with int vs prefixed string ID mismatches? | [link](https://www.reddit.com/r/learnmachinelearning/comments/1sknnoa/silent_cross_database_join_failures_has_anyone/) | TBD | TBD |
| 2026-04-15 | Kirubel | r/LocalLLaMA | Stop dumping docs into RAG and praying — here's a direct-injection test approach that converged to 21/21 on 8B | [link](https://www.reddit.com/r/LocalLLaMA/comments/1sm4s0y/stop_dumping_docs_into_rag_and_praying_heres_a/) | TBD | TBD |


### Reddit Replies (Week 9, account: u/Far-Comparison-9745)

| Date | Author | Subreddit | Thread | Reply Summary | Link |
|------|--------|-----------|--------|---------------|------|
| 2026-04-14 | Kirubel | r/LocalLLaMA | SQL benchmark (text-to-SQL model comparison, 25 questions) | Praised benchmark, surfaced DAB join-key failure mode (normalize_join_key pattern), suggested multi-DB routing + key normalization for v2, referenced DAB paper | [reply](https://www.reddit.com/r/LocalLLaMA/comments/1s7r9wu/comment/og3h2jx/) |
| 2026-04-14 | Kirubel | r/LocalLLaMA | Email-to-structured-context for AI agents (1M+ emails processed) | Connected to injection testing (21/21 on 8B), structured formats > prose at same context length, suggested pre-structuring before retrieval | [reply](https://www.reddit.com/r/LocalLLaMA/comments/1qg4d4t/comment/og3hnqv/) |
| 2026-04-14 | Kirubel | r/LocalLLaMA | Why AI Coding Agents Waste Half Their Context Window | Shared package-level injection methodology (21/21 on 8B); argued structured manifests > raw code dumps for context density | [reply](https://www.reddit.com/r/LocalLLaMA/comments/1sh8q39/comment/ogp9zh4/) |
| 2026-04-14 | Kirubel | r/learnmachinelearning | Multi-agent system for user behavior tracking (17yo builder) | Suggested date-anchored memory + injection testing methodology, linked DAB approach | [reply](https://www.reddit.com/r/learnmachinelearning/comments/1s9z7xa/comment/og54mzw/) |
| 2026-04-14 | Kirubel | r/learnmachinelearning | Semantic Chunking Pipelines for RAG | Counter-positioned direct injection as alternative for bounded domains, 21/21 finding | [reply](https://www.reddit.com/r/learnmachinelearning/comments/1sd17ie/comment/og586f0/) |
| 2026-04-14 | Kirubel | r/LocalLLaMA | EdgeVDB: On-Device Vector Database | Connected on-device search to bounded-domain injection testing, suggested pre-structured docs | [reply](https://www.reddit.com/r/LocalLLaMA/comments/1sl3rtg/comment/og5aid8/) |
| 2026-04-15 | Kirubel | r/LocalLLaMA | Own post: Stop dumping docs into RAG and praying | Added clarifying follow-up on what the injection test validates (document usability, not retrieval quality) and where it breaks (unbounded corpora) | [reply](https://www.reddit.com/r/LocalLLaMA/comments/1sm4s0y/comment/ogbj572/) |
| 2026-04-14 | Meseret | r/learnmachinelearning | Post asking about benchmark for reasoning stability in long LLM contexts | Answered with DAB reference: 54 queries across 4 DB types, 38% pass@1 ceiling, 40% of failures from incorrect planning compounding across steps. Referenced arxiv.org/abs/2603.20576. Connected to team's live experience building against DAB. | [reply](https://www.reddit.com/r/learnmachinelearning/comments/1slzhgy/multidatabase_query_in_agent/) |
| 2026-04-14 | Meseret | r/LocalLLaMA | Posted question about silent failure in mid-chain multi-database queries | Post deleted by Reddit moderators before receiving replies. Question asked: when agent fails switching from PostgreSQL to MongoDB does it surface as error or silent wrong result? | -- |
| 2026-04-15 | Meseret | r/learnmachinelearning | Posted the same question about silent failure in mid-chain multi-database queries | waiting for reply from the community. Question asked: when agent fails switching from PostgreSQL to MongoDB does it surface as error or silent wrong result? |[reply](https://www.reddit.com/r/learnmachinelearning/comments/1q9egkh/comment/og3jf00/?utm_source=share&utm_medium=web3x&utm_name=web3xcss&utm_term=1&utm_content=share_button) |

---

## Community Intelligence

_External responses or findings that changed the team's technical approach._

| Date | Source | Finding | Impact on Build |
|------|--------|---------|-----------------|
| 2026-04-11 | r/MachineLearning | Posting blocked: account too new or karma too low for r/MachineLearning. Pivoted to r/learnmachinelearning | Adjusted community targeting. r/learnmachinelearning has lower barrier, still relevant audience. Will build karma for r/MachineLearning access in Week 9. |
| 2026-04-11 | u/matt-k-wong (r/LocalLLaMA) | Validated "longer docs = lower quality" as universal LLM property; linked our injection test approach to Karpathy's wiki thesis | Reinforced Mikias's table-heavy, Q&A-anchored KB format; 21/21 injection test pass on llama-3.1-8b-instant (Apr 12) is a direct outcome |
| 2026-04-13 | DAB official channels (GitHub + leaderboard site) | No official DAB Discord exists. UC Berkeley EPIC Lab + Hasura PromptQL route community through GitHub issues only | Cohort community strategy redirected to Hugging Face / EleutherAI / LlamaIndex Discords per Practitioner Manual guidance. Three Discords joined. |
| ~~2026-04-13~~ | ~~u/Far-Comparison-9745 (r/learnmachinelearning)~~ | ~~REMOVED: This was Kirubel's own second Reddit account, not external community engagement~~ | -- |
| 2026-04-13 | @matanzutta on X (replied to Kirubel's Domain Knowledge Trap reply under @ashpreetbedi/Dash v2) | "the gap between what the schema says and what the business actually means is where most agent queries go wrong" — verbatim restatement of our Domain Knowledge thesis from a non-coordinated practitioner | **Strongest external validation in portfolio so far.** Confirms our framing lands with practitioners outside the cohort. Cite in final engagement summary and Week 9 X content. |
| 2026-04-14 | u/This-You-2737 (r/learnmachinelearning) | Replied to own join-failures post recommending Great Expectations + Scaylor Orchestrate as pipeline-validation tools | Reinforces our positioning of KB injection testing as agent-native alternative to pipeline-side validation. Validates the problem is well-understood; the agent-native solution path is the differentiator. |
| 2026-04-14 | H$Go (Hugging Face Discord #general, ~57 min thread) | Initial pushback on multi-DB premise → after DAB framing, suggested "fuzzy AI" matching as the way to handle ID mismatches → asked about test verification | **3rd external practitioner validation.** H$Go's "fuzzy AI" suggestion arrived independently at our Correction Layer architecture (agent-driven pattern detection + normalization codegen). Confirms the Discovery & Routing framing resonates. Closing exchange introduced **Level 1 (Functional) vs Level 2 (Semantic) failure** vocabulary — promising frame for Article B and benchmark thread. |

---

## Engagement Summary Stats

### Week 8 Totals
- **X posts:** 3
- **Medium articles:** 1 (Kirubel, ~1200 words, published)
- **LinkedIn articles:** 1 (Meseret, "Silent Killer of AI Data Agents", ~1800 words published Apr 11)
- **SC article deliverable:** ✅ 2/2
- **Reddit posts:** 2
- **Reddit comments/replies:** 2 substantive (u/matt-k-wong validation thread + "550 Free LLM Tools" comment)
- **Discord engagement:** Cohort class group (1, first-mover help) + 3 servers joined (HF, EleutherAI, LlamaIndex)
- **Slack engagement:** daily standup summary,sharing different resources related to the topics,daily update of what shipped,what stuck and future works.

### Week 9 In-Progress Totals (as of 2026-04-15)
- **X reply-placements (Apr 13):** 5 (one received reply from @matanzutta validating thesis)
- **X Community posts (Apr 14):** 8 across 4 communities (AI Agents, ML, AI/Python/Data, Open Source). 2 received practitioner replies (@jcubic, @anandrishv).
- **X Communities joined:** 4 (AI Agents 14.7K, ML, AI/Python/Data, Open Source Contributors)
- **Medium articles published (Week 9):** 2 (Kirubel: Apr 14 + Apr 15)
- **Reddit posts:** 3 (Kirubel: r/learnmachinelearning + r/LocalLLaMA; Meseret: r/learnmachinelearning)
- **Reddit replies (Apr 14-15, deployed by Kirubel as u/Far-Comparison-9745):** 7 substantive replies on r/LocalLLaMA + r/learnmachinelearning
- **Reddit replies received from external practitioners:** 1 (u/This-You-2737 on join-failures post recommending Great Expectations + Scaylor Orchestrate)
- **Discord:** 3 servers joined (HF, EleutherAI, LlamaIndex). 1 substantive HF #general practitioner exchange (5 messages, ~57 min) with user H$Go on cross-DB joins, MCP discovery, KB injection testing, Level 1 vs Level 2 failure framing.
- **Linkedin Second article:** Meseret-in progress
- **External validation logged:** 3 (@matanzutta thesis restatement Apr 13; u/This-You-2737 tooling exchange Apr 14; H$Go independently arrived at "fuzzy AI" matching = our Correction Layer pattern Apr 14)
- **daily Google Standup meeting and Slack Engagement:**Daily discussion on the topics and issues we are facing.
### Accounts Tracked for Reply-Threading

| Account | Platform | Focus Area | Link |
|---------|----------|------------|------|
| @shipp_ai | X | AI engineering | https://x.com/shipp_ai |
| @_avichawla | X | Data/ML engineering | https://x.com/_avichawla |
| @himanshustwts | X | AI/ML threads | https://x.com/himanshustwts |
| @sh_reya | X | AI engineering | https://x.com/sh_reya |
