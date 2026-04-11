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

## 2026-04-11 (Day 5 - Signal Infrastructure + Reddit Launch)

| Category | Platform | Technical Focus | Intelligence / Action | Link |
|----------|----------|-----------------|----------------------|------|
| Community Participation | Reddit | Multi-DB, Evaluation | Posted to r/learnmachinelearning: DAB failure modes discussion. Summarized 4 failure categories, 38% ceiling, and injection-tested KB approach. (r/MachineLearning blocked posting -- karma/age requirement.) | [reddit](https://www.reddit.com/r/learnmachinelearning/comments/1sieo3g/dataagentbench_shows_the_best_frontier_model_hits/) |
| Community Participation | Reddit | Architecture, Evaluation | Posted to r/LocalLLaMA: injection testing methodology with Groq Llama, 21/21 pass rate, practical observations on document length and format. | [reddit](https://www.reddit.com/r/LocalLLaMA/comments/1siet5q/testing_whether_your_knowledge_base_documents/) |
| Technical Deep-Dive | Internal | Architecture | Cloned repo, reviewed develop branch. Mapped full KB structure: 6 architecture + 9 domain + 4 correction + 3 evaluation docs. Verified 21/21 injection test results. | local |
| Resource Acquisition | Internal | -- | Created signal/ directory infrastructure: engagement_log.md, community_participation_log.md, resource_acquisition.md. Branch: feat/signal-corps-engagement. | local |
| Community Participation | X | -- | Curated 4 high-engagement accounts for reply-threading strategy: @shipp_ai, @_avichawla, @himanshustwts, @sh_reya. Identified Karpathy LLM wiki gist as content anchor. | -- |

---

## Summary by Technical Focus

| Focus Area | Total Entries | Platforms |
|------------|--------------|-----------|
| Multi-DB | 6 | Internal, X, Reddit, Medium |
| Join Keys | 5 | Internal, X, Medium |
| Architecture | 5 | Internal |
| Evaluation | 4 | Internal, X, Reddit |
| Unstructured Text | 1 | Internal |
| Domain Knowledge | 1 | Internal |

## Gaps to Address in Week 9
- **Discord:** Zero engagement. Target: DAB community Discord, Hugging Face
- **Reddit replies:** Zero comment engagement. Deploy reply templates in active threads
- **X reply-threading:** Zero replies under larger accounts. Shift from broadcast to reply strategy
- **Unstructured text + Domain knowledge content:** Underrepresented in external posts. Both are DAB failure categories with rich KB material.
