# Community Participation Log - Oracle Forge

_Daily record of all substantive engagement: posts, comments, research, resource acquisition._
_Each entry names the specific technical focus and intelligence gathered._

**Categories:** Community Participation | Resource Acquisition | Technical Deep-Dive
**Technical Focus Tags:** Multi-DB | Join Keys | Unstructured Text | Domain Knowledge | Evaluation | Architecture

---

## 2026-04-07 (Day 1 - Infrastructure)

| Category | Platform | Technical Focus | Intelligence / Action | Link |
|----------|----------|-----------------|----------------------|------|
| Technical Deep-Dive | Internal | Architecture | KB v0.1 structure created. Initial directory layout: architecture/, domain/, correction/, evaluation/. Establishes Karpathy-method injection testing as validation approach. | -- |

---

## 2026-04-08 (Day 2 - KB v1 Architecture)

| Category | Platform | Technical Focus | Intelligence / Action | Link |
|----------|----------|-----------------|----------------------|------|
| Technical Deep-Dive | Internal | Architecture | KB v1 committed: 6 architecture docs covering Claude Code 3-layer memory, autoDream consolidation, tool scoping (40+ tight > 5 generic), OpenAI 6-layer context, conductor/worker multi-DB routing, evaluation harness schema. All 6/6 injection tests pass. | `8f6caf9` |
| Technical Deep-Dive | Internal | Multi-DB | Conductor/worker pattern documented: conductor parses NL query for DB references, spawns DB-specific workers with schema context, merges results. Failure recovery logs to correction layer. | kb/architecture/conductor_worker_pattern.md |
| Technical Deep-Dive | Internal | Evaluation | pass@1 scoring method documented: correct first answers / total queries, minimum 50 trials. Trace schema defined: {query, tool_calls, result, expected, score}. | kb/architecture/evaluation_harness_schema.md |

---

## 2026-04-09 (Day 3 - KB v2 Domain + Repo Init)

| Category | Platform | Technical Focus | Intelligence / Action | Link |
|----------|----------|-----------------|----------------------|------|
| Technical Deep-Dive | Internal | Join Keys | KB v2 domain layer committed: cross-DB join key mappings for Yelp (business_id direct match), Telecom (INT -> "CUST-{INT}"), Healthcare (INT -> "PT-{INT}"). resolve_join_key() function documented with code. 9/9 injection tests pass. | `76aa867` - `9cf152f` |
| Technical Deep-Dive | Internal | Unstructured Text | Text extraction patterns documented: regex for medication doses (mg vs mcg validation), sentiment lexicon with negation handling ("not good" = negative). | kb/domain/unstructured/ |
| Technical Deep-Dive | Internal | Domain Knowledge | Business glossary created: "active customer" = purchased in last 90 days (not just row exists), "churn" = churn_date IS NOT NULL, fiscal vs calendar quarters per dataset. | kb/domain/domain_terms/business_glossary.md |
| Community Participation | X | Multi-DB | Posted on PostgreSQL + MongoDB friction: ill-formatted join keys as the real production barrier. Linked DAB paper + repo. | [tweet](https://x.com/kirubeltewodro2/status/2042250450888503584) |
| Community Participation | X | Evaluation | Posted on DAB 38% pass@1 ceiling: framed as engineering gap signal, not benchmark flaw. | [tweet](https://x.com/kirubeltewodro2/status/2042263948691415485) |

---

## 2026-04-10 (Day 4 - KB v3 Corrections + Medium Article)

| Category | Platform | Technical Focus | Intelligence / Action | Link |
|----------|----------|-----------------|----------------------|------|
| Technical Deep-Dive | Internal | Multi-DB | KB v3 corrections layer committed: 8 failure entries across all 4 DAB categories. Resolved patterns with confidence scores: PG-INT to Mongo-String (14/14), case-insensitive match (9/10), trailing space removal (7/7), negative sentiment (23/25), fiscal quarter (8/8), three-step join (11/12). Total: 21/21 injection tests. | `4d976ef` |
| Technical Deep-Dive | Internal | Architecture | Inception document committed to planning/. Gashaw preparing, Mikiyas integrating as markdown. Team approval pending at next mob session. | `91634c1` |
| Technical Deep-Dive | Internal | Architecture | REFERENCEDOC.md added: team onboarding guide explaining KB structure, load order, and change workflow. All members asked to read. | `a6fbd59` |
| Community Participation | Medium | Join Keys | Published "Engineering Resilience: Solving the Cross-Database Join Key Format Mismatch in AI Agents" (~1200 words). Covers: INT vs prefixed-STRING mismatch, resolve_join_key() pattern, Telecom/Healthcare dataset examples. Directly validates kb/domain/joins/join_key_mappings.md. | [Medium](https://medium.com/@kirutew17654321/engineering-resilience-solving-the-cross-database-join-key-format-mismatch-in-ai-agents-ffb17b9d5a02) |
| Community Participation | X | Join Keys | Announced Medium article on cross-DB join key format mismatch. | [tweet](https://x.com/kirubeltewodro2/status/2042676161499570186) |

---

## 2026-04-11 (Day 5 - Signal Infrastructure + Reddit Launch + Linkedin Article)

| Category | Platform | Technical Focus | Intelligence / Action | Link |
|----------|----------|-----------------|----------------------|------|
| Community Participation | Reddit | Multi-DB, Evaluation | Posted to r/learnmachinelearning: DAB failure modes discussion. Summarized 4 failure categories, 38% ceiling, and injection-tested KB approach. (r/MachineLearning blocked posting -- karma/age requirement.) | [reddit](https://www.reddit.com/r/learnmachinelearning/comments/1sieo3g/dataagentbench_shows_the_best_frontier_model_hits/) |
| Community Participation | Reddit | Architecture, Evaluation | Posted to r/LocalLLaMA: injection testing methodology with Groq Llama, 21/21 pass rate, practical observations on document length and format. | [reddit](https://www.reddit.com/r/LocalLLaMA/comments/1siqbda/i_kept_running_into_cases_where_retrieval_was/) |
| Community Reply | Reddit | Architecture | Second reply from u/matt-k-wong: verified that "longer docs = lower quality" is a universal property. Linked our approach to Karpathy's viral "LLM wiki" topic (info density > raw context). Community validation of structured-doc methodology for high-density knowledge injection. | [thread](https://www.reddit.com/r/LocalLLaMA/comments/1siqbda/i_kept_running_into_cases_where_retrieval_was/) |
| Technical Deep-Dive | Internal | Architecture | Cloned repo, reviewed develop branch. Mapped full KB structure: 6 architecture + 9 domain + 4 correction + 3 evaluation docs. Verified 21/21 injection test results. | local |
| Resource Acquisition | Internal | -- | Created signal/ directory infrastructure: engagement_log.md, community_participation_log.md, resource_acquisition.md. Branch: feat/signal-corps-engagement. | local |
| Community Participation | X | -- | Curated 4 high-engagement accounts for reply-threading strategy: @shipp_ai, @_avichawla, @himanshustwts, @sh_reya. Identified Karpathy LLM wiki gist as content anchor. | -- |
| Community Participation | LinkedIn | Silent Failure, Join Keys, Architecture | SC1 published "The Silent Killer of AI Data Agents" (~1800 words) — covers cross-database join key mismatch (PG INT vs MongoDB CUST-string), DAB 38% ceiling, three-layer context architecture, ETL vs runtime resolution. Shared in team Slack for visibility. | [article](https://www.linkedin.com/pulse/silent-killer-ai-data-agents-how-were-engineering-around-bolled-rsg8f) |

---

## 2026-04-12 (Day 6 — Ethiopian Holiday, Rest Day)

| Category | Platform | Technical Focus | Intelligence / Action | Link |
|----------|----------|-----------------|----------------------|------|
| Technical Deep-Dive | Internal | Architecture | Mikias rebuilt KB injection test harness (kb/INJECTION_TEST_LOG.md committed). 13 iteration runs to converge at 21/21 (100%) on llama-3.1-8b-instant — confirms doc quality is model-size-agnostic when structure is right. | `2843265` |

---

## 2026-04-13 (Day 7 — Construction → Integration)

| Category | Platform | Technical Focus | Intelligence / Action | Link |
|----------|----------|-----------------|----------------------|------|
| Technical Deep-Dive | Internal | Architecture | Eyor merged develop ↔ feat/agent: agent pipeline + KB + 21/21 injection tests on one branch. Full integration verified. | `d5cd573` |
| Technical Deep-Dive | Internal | Multi-DB, Join Keys, Unstructured Text, Domain Knowledge | Mikias + Gashaw pushed probes (19, all 4 DAB categories), utilities (6 modules), tests (join_keys + routing), and new KB docs (authoritative_tables, fiscal_calendar, null_guards). Interim spec compliance: ✅ 19 probes (>15), ✅ 4 categories (>3), ✅ 6 utilities (>3). | `ad68f9a` |
| Resource Acquisition | DAB official channels | All | Confirmed via GitHub repo + leaderboard site that no official DAB Discord exists. UC Berkeley EPIC + Hasura PromptQL route community through GitHub issues only. | https://github.com/ucbepic/DataAgentBench |
| Community Participation | Cohort class group | All | First-mover help: peer asked for DAB Discord link, confirmed it doesn't exist, surfaced 3 verified alternative Discord invites (HF, EleutherAI, LlamaIndex) per Practitioner Manual guidance. | -- |
| Community Participation | Discord (Hugging Face) | All | Joined server. Substantive engagement deploying Apr 14-16. | https://discord.gg/JfAtkvEtRb |
| Community Participation | Discord (EleutherAI) | All | Joined server. Substantive engagement deploying Apr 14-16. | https://discord.gg/zBGx3azzUn |
| Community Participation | Discord (LlamaIndex) | All | Joined server. Substantive engagement deploying Apr 14-16. | https://discord.com/invite/eN6D2HQ4aX |
| Community Participation | X (Twitter) | Domain Knowledge | Reply-threaded Tweet 2 (Domain Knowledge Trap, churn rate definition) under @ashpreetbedi's Dash v2 thread (text-to-SQL agent context kit). 3 placement variants. | https://x.com/kirubeltewodro2/status/2043614126912500174 |
| Community Participation | X (Twitter) | Unstructured Text | Reply-threaded Tweet 5 (Negation Problem) on NLP/sentiment threads, including one under @0xcgn. 2 placement variants. | https://x.com/kirubeltewodro2/status/2043616814421180639 |
| Technical Deep-Dive | X (Twitter) | Domain Knowledge | **External validation received.** @matanzutta replied to Kirubel's Dash v2 reply: "the gap between what the schema says and what the business actually means is where most agent queries go wrong" — non-coordinated practitioner restating our thesis verbatim. Highest-signal engagement of the week. | https://x.com/matanzutta/status/2043620994544239077 |
| Community Participation | r/learnmachinelearning | Join Keys | New post: "Silent cross database join failures: has anyone dealt with int vs prefixed string ID mismatches?" Lead with failure mode (PG int ↔ Mongo "CUST-1234567"), asked community for OSS detection tools. | https://www.reddit.com/r/learnmachinelearning/comments/1sknnoa/silent_cross_database_join_failures_has_anyone/ |
| Technical Deep-Dive | r/LocalLLaMA | Architecture, Domain Knowledge | Follow-up comment on own injection-testing thread: 21/21 pass on llama-3.1-8b-instant (sub-8B), linked INJECTION_TEST_LOG.md on repo for verification. Closes loop on u/matt-k-wong's question about whether the methodology generalizes below 70B. | https://www.reddit.com/r/LocalLLaMA/comments/1siqbda/i_kept_running_into_cases_where_retrieval_was/ |
| Community Reply | r/learnmachinelearning | Domain Knowledge | u/Far-Comparison-9745 reply on the original DAB failure modes thread asked the right question: how to automate learning domain rules (e.g., "active = last 90 days") never in the schema. Validates need for business_glossary.md + SME capture loop. Response drafted. | https://www.reddit.com/r/learnmachinelearning/comments/1sieo3g/comment/ofwrtgq/ |
| Resource Acquisition | Internal | -- | Compiled Week 8 Engagement Portfolio (signal/week8_engagement_portfolio.md) for interim PDF inclusion. Drafted X thread 04 (integration milestone) for post-PR launch. | local |

---

## Summary by Technical Focus

| Focus Area | Total Entries | Platforms |
|------------|--------------|-----------|
| Multi-DB | 7 | Internal, X, Reddit, Medium |
| Join Keys | 7 | Internal, X, Reddit, Medium |
| Architecture | 8 | Internal, Reddit |
| Evaluation | 4 | Internal, X, Reddit |
| Team Coordination | 4 | Internal, Google Meet |
| Silent Failure | 2 | LinkedIn |
| Unstructured Text | 3 | Internal, X |
| Domain Knowledge | 5 | Internal, X, Reddit |

## Week 9 Status (as of 2026-04-13)
- **Discord:** ✅ 3 servers joined (HF, EleutherAI, LlamaIndex). Substantive engagement Apr 14-16.
- **Reddit replies:** Active — u/matt-k-wong thread continues, u/Far-Comparison-9745 reply received on original post.
- **X reply-threading:** ✅ 5 placements delivered Apr 13. **External validation received from @matanzutta.**
- **Unstructured text + Domain knowledge content:** Now well-represented (Tweet 2 + Tweet 5 deployed in real threads, @matanzutta validation on Domain Knowledge specifically).

## Remaining Gaps for Final Submission (Apr 18)
- **DAB benchmark scores:** Probes designed and runner exists, but real pass/fail scores from end-to-end agent runs still pending.
- **Discord substantive comments:** Servers joined, first comment in HF/EleutherAI/LlamaIndex deploying Apr 14-16.
- **Final engagement summary:** Compile reach metrics + notable responses for final PDF.
