# 14-day study plan — Data Engineer Professional · NEW EXAM (starting Oct 9, 2026)

> Current guide: **Oct/2026, New Exam column** · **60 questions** · 120 min · **English only** · passing ~70%.
> Philosophy: you already have the foundation. This is **weight-driven recap + close net-new topics + many practice exams**.
> ⚠️ Exam is **in English**: train terminology in English (terms below are in exam language).
> Golden rule: **every practice exam error becomes a tracker entry** (§ Tracker) + review the objective doc.

---

## Priority by weight (new exam — 9 sections)

| Block | Sections | Weight sum | Priority |
|------|--------|-------------|-----------|
| **A — Code/pipelines** | S1 (23%) | 23% | 🔴 Maximum |
| **B — Cost/Performance** | S5 (15%) | 15% | 🔴 Maximum |
| **C — Ingestion + Manipulation** | S2 (12%) + S3 (12%) | 24% | 🟠 High |
| **D — Monitoring + Debug/Deploy** | S4 (10%) + S8 (10%) | 20% | 🟠 High |
| **E — Security + Governance + Modeling** | S6 (8%) + S7 (5%) + S9 (5%) | 18% | 🟡 Medium |

Practical rule: if time is tight, **never cut A and B** (38% of exam).

---

## Schedule (2 weeks)

### Week 1 — Diagnostic + recap by domain

**Day 1 — Diagnostic + realign vocabulary (2–3h)**
- Read the **Oct/2026 guide (New Exam column) entirely** + take the **10 new official sample questions** (in § Cheat sheet, with answers). It's the most faithful material available.
- Cold practice exam on Udemy (⚠️ it's from the old exam — good to measure foundation, but ignore net-new topics it doesn't cover). Record % per section in tracker.
- Build 2 glossaries: (a) **renames** (DLT→Lakeflow Declarative Pipelines, DABs→Declarative Automation Bundles, APPLY CHANGES→AUTO CDC, Repos→Git folders); (b) **English terms** for net-new (watermark, checkpoint, deletion vectors, governed tag, etc.).
- **Schedule the exam** (Webassessor) for date ≥ Oct 9; exam in English.

**Day 2 — S1 Code, part 1 (Block A, 23%) (2–3h)**
- Python project structure for Declarative Automation Bundles (modular + CI/CD).
- Dependency troubleshooting (PyPI/wheels/source archives) **on serverless, pipeline, and bundle-deployed**.
- UDFs: **Pandas, Python and SQL, including Unity Catalog functions**.
- Lakeflow Declarative Pipelines + Auto Loader for streaming; **streaming table vs materialized view** (decide by latency/cost/refresh).
- Drill: 15–20 S1 questions.

**Day 3 — S1 Code, part 2 (2–3h) — HEAVY NET-NEW**
- **Structured Streaming stateful**: watermarks (limit state growth), output modes, `foreachBatch`, checkpoints → **exactly-once** recovery after driver failure.
- AUTO CDC APIs + **SCD Type 1 and Type 2 via `stored_as_scd_type`**.
- Structured Streaming **vs** Lakeflow Declarative Pipelines (when each one).
- Lakeflow Jobs with control flow (**If/Else, For Each**).
- Compute/config: **serverless compute** (environments, dependency mgmt, **performance mode**), high-memory notebook tasks, auto-optimization (disallow retries).
- Testing: `assertDataFrameEqual`, `assertSchemaEqual`, `DataFrame.transform`.
- Drill: 15–20 questions; review Day 1 S1 errors.

**Day 4 — S5 Cost & Performance (Block B, 15%) (2–3h)**
- Managed tables + **Predictive Optimization** + Liquid Clustering → reduce operational overhead.
- **Pick the right technique**: deletion vectors vs Liquid Clustering vs **CLUSTER BY AUTO** by access pattern.
- **Delta cache** for repeated reads.
- **CDF** to expose row-level changes (updates/deletes) and downstream incremental processing.
- Query profile: bottlenecks (poor data skipping, join strategy, shuffle).
- **Liquid Clustering vs partitioning/ZORDER** by table size and query pattern.
- Drill: 15–20 questions + Query Profiler in practice (Free Edition).

**Day 5 — S2 Ingestion & Acquisition (Block C, 12%) (2–3h) — HEAVY NET-NEW**
- Formats: Delta, Parquet, JSON, **Iceberg**, CSV, Binary; sources: message buses (**Kafka, Kinesis, Pub/Sub**) + cloud storage.
- Incremental CDC with Lakeflow Pipelines targeting **Delta OR Iceberg** format.
- **Lakeflow Connect**: managed CDC connectors for **SQL Server, MySQL, PostgreSQL** (includes deletes, minimal code).
- **OpenSharing** (D2D and Databricks-to-Open) + **Clean Rooms** (collaboration preserving privacy).
- Lakehouse Federation with governance (UC permissions + connection credentials).
- Drill: 15–20 S2 questions.

**Day 6 — S3 Data Manipulation (Block C, 12%) (2–3h) — HEAVY NET-NEW**
- Advanced transformations: window functions, joins, aggregations (Spark SQL + PySpark).
- **VARIANT**: model/query semi-structured with `parse_json`, `variant_get`, colon-path access.
- **AI functions**: `ai_query` for model inference in pipeline (enrichment/classification).
- **Data quality expectations** in Lakeflow Declarative Pipelines: quarantine / drop / fail on bad records.
- Drill: 15–20 S3 questions.

**Day 7 — S4 Monitoring (10%) + S8 Debug/Deploy (10%) + Practice Exam #2 (3h)**
- S4: **system tables** (billing, compute, access, lakeflow) for cost/audit/workload; REST API/CLI/**SDK**; pipeline event logs; **Databricks Lakehouse alerts** (governed metrics, quality, cost, SQL warehouse/query health, audit/security, AI agent quality, Lakeflow Job branching); Jobs UI/API.
- S8: diagnose via Spark UI/cluster logs/system tables/query profiles; **job repairs + parameter overrides**; event logs; deploy with **Declarative Automation Bundles**; **Git folders** for CI/CD.
- **Complete practice exam #2** (timed). Update tracker.

### Week 2 — Close gaps + back-to-back practice exams

**Day 8 — S6 Security (8%) + S7 Governance (5%) + S9 Modeling (5%) (2–3h)**
- S6: least-privilege ACLs on UC securables; **ABAC with governed tags** → row filters/column masks at scale (mask everything tagged 'pii', including future tables); anonymization/pseudonymization (hashing, tokenization, suppression, generalization); compliant batch+streaming pipeline with PII detection/masking; **data purging** (GDPR right-to-erasure / right-to-be-forgotten) with Delta + UC.
- S7: **UC tags and comments** for discoverability; **UC permission inheritance model** (grant on catalog inherited to schemas/objects, including ones created later).
- S9: Delta/Iceberg table layout (partition-to-grain, cluster by access pattern, compaction/file size); dimensional models with **Materialized Views** (pre-computed aggregation) + **UC Metric Views** (governed metric definition and reusable).
- Drill: 20 questions covering S6+S7+S9.

**Day 9 — Practice Exam #3 + deep error review (3h)**
- 60 timed questions (official samples + Udemy, filtering old topics). For **each error**: reread objective in guide + official doc. Record pattern in tracker.

**Day 10 — Gap day (2–3h)**
- Attack ONLY the 2–3 sections with worst % in tracker. Focused drills + docs. Prioritize net-new if weak.

**Day 11 — Practice Exam #4 + review (3h)** — target ≥80% consistent.

**Day 12 — Traps day (2h)**
- Review § Cheat sheet + your error log. Re-drill only what you missed in ≥2 practice exams.

**Day 13 — Practice Exam #5 in exam conditions (2.5h)**
- 120 min no breaks/reference. Target ≥85%.
- **Reconfirm official guide** (guide asks to check 2 weeks before).
- Logistics: Webassessor; **exam in English**; test online proctoring.

**Day 14 — Light eve (1–1.5h)**
- Cheat sheet + glossaries reread only. No new practice exam. Sleep.

---

## Cheat sheet — 10 OFFICIAL sample questions from new exam (with answers + why)
Exact pattern of how new exam is written. Memorize reasoning, not answer.

1. **Streaming table vs Materialized View** — MV when aggregate must reflect **full history + late updates** and scheduled refresh; streaming table is append/incremental exactly-once and **does not** recompute aggregate over changing history. *(ans. C)*
2. **Structured Streaming stateful** — **watermark** (limit state by discarding very late events) + **checkpoint** (restore offsets/state → exactly-once on restart). *(ans. A)*
3. **CDC from RDBMS** — **Lakeflow Connect** managed connector (Postgres/MySQL/SQL Server) ingests CDC incl. deletes with minimal code; full reload/CSV/OpenSharing don't work. *(ans. B)*
4. **VARIANT** — `parse_json` for VARIANT column + `variant_get`/colon-path: efficient storage and flexible query without fixed schema. *(ans. A)*
5. **Cost attribution** — `system.billing.usage` joined with UC system tables for compute/pricing (governed and queryable data). *(ans. C)*
6. **Write amplification in small MERGEs** — **deletion vectors** mark row-level updates/deletes without rewriting whole files; compaction reconciles later. *(ans. B)*
7. **Mask PII at scale** — **ABAC policy** on governed tag 'pii' applies column mask wherever tag exists, including future tables (not per-table masking nor view duplication). *(ans. D)*
8. **UC permission inheritance** — grant SELECT on **catalog** is inherited by schemas/tables, **including ones created later**. *(ans. A)*
9. **Multi-environment deploy** — bundle `databricks.yml` with per-target overrides + `databricks bundle deploy -t <target>` (reproducible, CI/CD). *(ans. B)*
10. **Consistent metric + slow dashboards** — **UC Metric View** (single governed definition) + **Materialized Views** (pre-computed aggregates that dashboards read). *(ans. C)*

### Other high-density items per objective
- **SCD Type 1 vs 2** via `stored_as_scd_type` in AUTO CDC.
- **CLUSTER BY AUTO / Predictive Optimization**: automatic layout/clustering maintenance.
- **Delta cache** ≠ query result; speeds up repeated reads of same data.
- **Clean Rooms**: partner collaboration without exposing raw data.
- **ai_query**: inference/classification inside SQL/DataFrame pipeline.
- **Iceberg as target**: CDC pipelines can write to Delta *or* Iceberg.
- **Lakehouse Federation**: cross-source query with UC governance (no data movement).

---

## Resources
**Official training (Databricks Academy) — aligned to guide:**
- Advanced Data Engineering with Databricks (ILT) — core course.
- Advanced Techniques with Apache Spark™ Declarative Pipeline.
- Databricks Data Privacy (→ S6).
- Databricks Performance Optimization (→ S5).
- Automated Deployment with Declarative Automation Bundles (→ S8).

**Official practice:**
- **AI Prep Guide** (official Databricks): transform a chatbot into a "primed" tutor with the guide — generates 6–10 hands-on task checklist mapped to objectives.
- **Databricks Free Edition**: do hands-on labs (VARIANT, ai_query, deletion vectors, DABs, Metric View, streaming stateful).
- **Official docs**: source of truth for net-new (Iceberg, Lakeflow Connect, ABAC, Metric Views) — third-party material not yet covered.

**Third-party practice exams (⚠️ OLD EXAM — use with filter):**
- Udemy "Practice Exams: Databricks Data Engineer Professional".
- GitHub Amrit-Hub — question hints. Vlad Siv — prep notes.
- Don't cover net-new (streaming stateful, Iceberg, Lakeflow Connect, VARIANT, AI functions, ABAC, Metric Views). Supplement with Academy + docs.

**Logistics:** Webassessor (webassessor.com/databricks). **New exam English only; schedule date ≥ Oct 9.**

---

## Practice exam tracker (new exam — 9 sections)
Target: reach and hold ≥85% before Day 13.

| Practice Exam | Day | Overall | S1 Code | S2 Ingest | S3 Manip | S4 Monit | S5 Cost/Perf | S6 Sec | S7 Gov | S8 Debug | S9 Model | Bottom 3 |
|----------|-----|-------|---------|-----------|----------|----------|--------------|--------|--------|----------|----------|----------|
| #1 (cold) | 1  |       |         |           |          |          |              |        |        |          |          |          |
| #2        | 7  |       |         |           |          |          |              |        |        |          |          |          |
| #3        | 9  |       |         |           |          |          |              |        |        |          |          |          |
| #4        | 11 |       |         |           |          |          |              |        |        |          |          |          |
| #5        | 13 |       |         |           |          |          |              |        |        |          |          |          |

### Recurring error log (missed in ≥2 practice exams)
| Topic/objective | Section | Why I missed it | Doc/correction note |
|-----------------|-------|---------------|----------------------|
|                 |       |               |                      |
