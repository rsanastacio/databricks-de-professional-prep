# Practice Exam 1 — Databricks Certified Data Engineer Professional (New Exam)

> Original practice questions, not official exam questions. Answers are based on the Databricks documentation linked under each answer; verify in the docs. Time yourself: 120 minutes.

## Questions

### 1. [Developing Code — Declarative Automation Bundles project layout]

Your team is building a reusable data pipeline using Declarative Automation Bundles (DAB) that will be deployed across 12 regional workspaces. The pipeline includes shared utility functions (data validation, error handling) used by multiple jobs, and each region has region-specific configuration. You want to minimize maintenance overhead by packaging shared code once and reusing it in all deployments. Which approach best satisfies this requirement while keeping the bundle structure maintainable?

A) Copy the shared modules into each region's folder, store them in a Databricks workspace folder as git files, and reference them via `%run` in each job notebook.  
B) Package shared code as a Databricks App and deploy it as a dependency; configure each job to import the app as a library reference in the job's requirements.  
C) Store all shared functions in a single SQL UDF in the metastore, then create workspace-level permissions so all jobs can reference it directly without importing.  
D) Create a Python wheel containing shared modules, reference it in `databricks.yml` via `artifact.libraries`, and use `targets` with region-specific variables to override configuration per deployment.  

---

### 2. [Developing Code — Pandas UDF vs SQL UC function choice]

Your data platform team is adding a new transformation function to compute a rolling risk score from trading activity. The function must (1) be callable from both PySpark and SQL queries, (2) be governed via Unity Catalog with consistent versioning and audit trails, (3) execute efficiently on serverless compute with no external dependencies, and (4) allow analysts to update the business logic without redeploying infrastructure. Which option best meets all four constraints?

A) Pandas UDF, because it supports both PySpark and SQL via `register_pandas_udf()` and integrates with UC for versioning.  
B) SQL function at the metastore level, wrapped in a Python UDF to expose it to PySpark; this provides full governance.  
C) Pandas UDF, because vectorization offers better performance than SQL UDFs on serverless compute.  
D) SQL function in UC, because it satisfies all constraints: governance, no external deps, dual-language access, and analyst-driven updates.  

---

### 3. [Developing Code — Streaming table vs materialized view for latency/cost/refresh]

Your team is building a real-time analytics pipeline. Raw events stream in at 100K events per second; a downstream aggregation table must be fresh within 5 minutes for executive dashboards (SLA), but the business can tolerate stale intermediate results if they save 60% on compute costs. The aggregation runs for ~2 seconds per execution. Should you use a streaming table or materialized view?

A) Materialized view on 1-minute refresh to guarantee freshness and exceed business requirements.  
B) Streaming table, because it maintains continuous incremental processing and guarantees sub-second freshness.  
C) Materialized view with 5-minute refresh, because it meets the SLA, avoids constant recomputation, and saves ~60% on compute vs. streaming.  
D) Streaming table for ingestion plus materialized view on top for aggregation to combine incremental and batch patterns.  

---

### 4. [Developing Code — AUTO CDC SCD Type 2 with sequence_by]

You are building a customer dimension table in a Lakeflow Declarative Pipeline. Source data contains customer attributes (name, email, tier) that change over time. You need to track the full history with effective/end dates and mark the current row. The CRM feed has an `updated_at` timestamp, and you must preserve the original record ID. Which AUTO CDC configuration is correct?

```sql
CREATE OR REFRESH STREAMING TABLE customer_dim
  FLOW AUTO CDC
  FROM (select * from cloud_files(...))
  KEYS (cust_id)
  SEQUENCE BY updated_at
  TRACK HISTORY ON (name, email, tier)
  STORED AS SCD TYPE 2
```

A) Use `APPLY CHANGES` instead of `AUTO CDC INTO` for modern syntax.  
B) Add `SCD TYPE 2` clause and `EFFECTIVE_DATE` config; `TRACK HISTORY` only works with manual change feeds.  
C) The SQL is correct; AUTO CDC automatically adds `_change_type`, `_start_date`, `_end_date` for SCD Type 2.  
D) Add `TRACK HISTORY COLUMN LIST (cust_id, name, email, tier)` to include the key in tracked columns.  

---

### 5. [Developing Code — Structured Streaming event-time window with watermark]

Your real-time analytics team processes IoT sensor data with Structured Streaming. Events arrive with `event_time` timestamps and must be grouped into 5-minute time windows. Your SLA requires that late-arriving events (up to 10 minutes late) update existing window aggregations, and events beyond 10 minutes are dropped without error. You want to emit window updates efficiently, sending only changed rows to downstream consumers to minimize network and storage overhead. Which watermark and output mode combination is correct?

A) No watermark; use `append` mode and manually filter records older than 10 minutes.  
B) Watermark of 10 minutes, 5-minute window, and `complete` output mode for full aggregates.  
C) Watermark of 10 minutes on `event_time`, 5-minute window, and `append` output mode for finalized results.  
D) Watermark of 10 minutes on `event_time`, 5-minute window, and `update` output mode to emit only changed rows.  

---

### 6. [Developing Code — Lakeflow Jobs For Each task with task values]

You are building a Lakeflow Jobs pipeline that processes data for multiple customers in parallel. Each customer record has `customer_id` and `customer_name`. You need to run a transformation notebook for each customer, passing the customer ID as a parameter. Which DAB configuration using `for_each` is correct?

```yaml
tasks:
  process_per_customer:
    for_each:
      items: SELECT customer_id, customer_name FROM {{var.customer_table}}
    task:
      notebook_task:
        notebook_path: /transform_customer
        base_parameters:
          CUSTOMER_ID: "{{task.customer_id}}"
```

A) The YAML is correct; `for_each.items` will iterate, and `task.customer_id` auto-binds.  
B) Change `base_parameters.CUSTOMER_ID` to `{{each.value.customer_id}}`.  
C) The YAML is correct; `task.customer_id` correctly references the column from the iteration.  
D) Add explicit `loop_variable` config; `for_each` does not auto-bind column names.  

---

### 7. [Developing Code — Serverless environment dependency management]

Your data engineering team runs a daily sentiment analysis job on serverless compute to classify 100K support tickets. The job uses a 500 MB Python NLP library (spacy + transformers). Profiling shows 15 seconds of every 30-second run is spent in environment initialization and dependency installation. You've already packaged the library as a wheel. Your team needs to reduce initialization overhead without switching to classic clusters (to maintain cost efficiency). Which approach best achieves this?

A) Use `init_scripts` to pre-cache dependencies; serverless will apply the script on every run.  
B) Move to a classic cluster; serverless is not suited for heavy library workloads.  
C) Use serverless **performance mode** (`performance_mode: performance` in DAB) and pre-package the library as a wheel; performance mode caches environments.  
D) Deploy the library as a Databricks App asset workspace-wide; serverless auto-loads cached versions.  

---

### 8. [Data Ingestion & Acquisition — Auto Loader schema evolution and _rescued_data]

Your compliance team's Auto Loader ingestion pipeline processes daily compliance reports in JSON format. After weeks of stable ingestion, a new upstream system adds a `metadata.compliance_flags` nested field that your schema doesn't define. The pipeline must preserve all records (not skip or fail) so the compliance team can audit the data later. Your transformation jobs downstream are not yet updated to handle the new field. How does Auto Loader handle unexpected fields by default?

A) Auto Loader drops them silently; you must add the field to the schema or use `schema_evolution_mode: 'addNewColumns'`.  
B) Auto Loader automatically rescues unexpected fields into the `_rescued_data` column as JSON for safe preservation.  
C) Unexpected fields quarantine the entire record in `_quarantine_table`; use Auto Loader's recovery API to recover.  
D) Auto Loader rejects the file and halts ingestion; use `cloudFiles.rescueData = false` to accept unknown fields.  

---

### 9. [Data Ingestion & Acquisition — Kafka ingestion to streaming table]

Your platform team is building a real-time event ingestion pipeline using Lakeflow Declarative Pipelines to consume JSON events from a Kafka topic (10K events/sec, 8 partitions, growing). The topic contains transaction records with nested user and product fields. Your pipeline must start from the latest offset (not historical backlog), auto-discover new partitions if the Kafka broker adds them, and parse JSON into structured typed columns for downstream analytics. Which Lakeflow configuration correctly satisfies all three requirements?

A) Use `kafka_bootstrap_servers` with `value.deserializer='StringDeserializer'` to parse JSON.  
B) Use `cloudFiles` with a Kafka source and wrap JSON in `from_json()` to deserialize.  
C) Use `kafka_bootstrap_servers`, add `startingOffsets='latest'` and `auto.offset.reset='latest'` in options.  
D) Use `kafka_bootstrap_servers` with `startingOffsets='latest'` (built-in default); new partitions auto-discovered; deserialize via `from_json()`.  

---

### 10. [Data Ingestion & Acquisition — Lakeflow Connect SQL Server CDC]

Your enterprise's legacy order management system runs on an on-premises SQL Server behind a corporate firewall. Your data team must ingest all customer and order changes into Databricks daily, including deletes (SCD Type 2 history tracking). You have a VPN tunnel bridging your on-prem network to AWS VPC. You want to use Lakeflow Connect's native CDC capabilities to track inserts, updates, and deletes without manual log parsing. What architecture is required to enable this integration?

A) Configure SQL Server source in Lakeflow Connect UI, enable CDC at the database, select `change_data_capture=true` in the pipeline.  
B) Deploy a Lakeflow Connect Ingestion Gateway (Relay) in your on-prem network, configure SQL Server source, enable database CDC, create ingestion pipeline in Databricks.  
C) Use a Databricks SQL endpoint to query SQL Server via JDBC; Lakeflow Jobs periodically exports CDC logs.  
D) Route SQL Server transaction logs to Kafka, then ingest Kafka into Databricks via Lakeflow.  

---

### 11. [Data Ingestion & Acquisition — Delta Sharing to non-Databricks recipient]

Your organization (Databricks-based) wants to share real-time customer analytics data with a key business partner who uses Snowflake and has no Databricks workspace. The partner's analysts need self-service query access without joining your workspace or undergoing lengthy credential setup. Your team needs minimal operational overhead. The partner SLA requires data freshness within 1 hour. Which Delta Sharing model enables this collaboration?

A) Use Lakehouse Federation to allow Snowflake to query Databricks catalogs directly.  
B) Invite the partner to your Databricks workspace and grant `EXECUTE` permission on a shared external location.  
C) Delta Sharing requires both parties on Databricks; export to Parquet and sync via cloud storage instead.  
D) **Direct Delta Sharing** (open sharing): Configure a share in Databricks, and the partner queries via Snowflake's native Delta Sharing connector.  

---

### 12. [Data Manipulation — Window function to keep latest record per key]

Your finance team's settlement process requires deduplicating a raw transactions table to identify the final state of each customer's daily activity. The table has `account_id`, `transaction_date`, `amount`, and `sequence_id` (monotonically increasing within each account/day combination to track event order). Multiple transactions can occur on the same date for a single account; you must pick the transaction with the highest `sequence_id` to get the true final state. Your settlement job must select exactly one row per account per day. Write the SQL query that accomplishes this.

```sql
SELECT account_id, transaction_date, amount, sequence_id
FROM (
  SELECT account_id, transaction_date, amount, sequence_id,
         ROW_NUMBER() OVER (PARTITION BY account_id, transaction_date ORDER BY sequence_id DESC) AS rn
  FROM transactions
)
WHERE rn = 1
```

A) Add `ORDER BY transaction_date DESC` to prioritize latest date first.  
B) The query is correct; `ROW_NUMBER()` with `sequence_id DESC` ranks by highest sequence (latest), keeping `rn=1`.  
C) Replace `ROW_NUMBER()` with `RANK()` to handle ties in `sequence_id`.  
D) Use `ORDER BY sequence_id ASC` to get the latest (most recent) record.  

---

### 13. [Data Manipulation — VARIANT with parse_json / try_variant_get]

Your data pipeline ingests clickstream events as JSON strings in an `event_metadata` column. You need to extract the nested `user.id` and `action` fields for analytics. Some records have malformed JSON or missing fields. Your downstream BI tool requires NULL for any extraction errors (not exceptions). You're building a transformation that must handle 10M records reliably without job failures. Which SQL approach correctly extracts these fields safely?

```sql
SELECT 
  try_variant_get(parse_json(event_metadata), 'user.id', 'string') AS user_id,
  try_variant_get(parse_json(event_metadata), 'action', 'string') AS action
FROM events
```

A) Use `variant_get()` instead of `try_variant_get()` for the modern API.  
B) Use `get_json_object()` instead for simpler path extraction.  
C) Call `parse_json()` once as a temp variable; calling twice is inefficient.  
D) The query is correct; `parse_json()` converts string to VARIANT, `try_variant_get()` safely extracts and returns NULL on failure.  

---

### 14. [Data Manipulation — Expectations-based quarantine pattern]

Your order fulfillment team runs a Lakeflow pipeline ingesting 50K daily orders. Validation rules require `order_date` (non-null, within past year) and `total_amount > 0`. Orders violating these rules must be identified and flagged with a `validation_status` column for manual review by compliance, but the pipeline must continue processing. Your team wants automated audit trails showing which records failed which rules. Which pattern correctly implements this using Lakeflow's data quality features?

```sql
CREATE STREAMING TABLE orders_validated
  @expect(description='Valid order date', 
          constraint='order_date IS NOT NULL AND order_date >= CURRENT_DATE() - INTERVAL 365 DAY')
  @expect(description='Positive amount',
          constraint='total_amount > 0')
AS
SELECT *,
  CASE WHEN (order_date IS NULL OR order_date < CURRENT_DATE() - INTERVAL 365 DAY OR total_amount <= 0) 
       THEN 'Failed' ELSE 'Passed' END AS validation_status
FROM orders_raw
```

A) Create two tables: valid rows (WHERE filter) and invalid rows (NOT conditions); monitor the invalid table manually.  
B) Use @expect decorators with individual constraint descriptions; add a validation_status column for flagging; pipeline continues and metrics are recorded.  
C) Use EXPECT with `ON VIOLATION DROP ROW` to explicitly remove invalid rows before writing; this ensures data quality.  
D) Use WHERE clause to filter valid rows; EXPECT is only for monitoring and doesn't flag individual records.  

---

### 15. [Monitoring and Alerting — Cost attribution by job tag]

Your finance team needs to chargeback compute costs to each business unit (finance, marketing, engineering) for monthly reporting. Every Databricks job is tagged with `cost_center` metadata. You'll query `system.billing.usage` to retrieve billing data by usage type and resource. Your CFO requires a weekly dashboard showing spend breakdown by cost_center. You need to join billing data with job/cluster metadata to associate costs with tags. What's the correct SQL pattern?

A) Query `system.billing.usage` filtered to `usage_type='COMPUTE'`, join on `job_id` to `system.jobs.list_jobs()` output, extract cost_center tag.  
B) Query `system.billing.usage`, join to `system.compute.clusters` on `cluster_id` to get cluster tags; map clusters to jobs.  
C) Use `system.billing.list_prices()` to calculate cost per DBU, then manually parse job config files for tags.  
D) Query `system.billing.usage` for `usage_type='COMPUTE'`, join to job/cluster metadata to access tags, group by `cost_center` tag value.  

---

### 16. [Monitoring and Alerting — Pipeline event log for expectation metrics]

Your order processing Lakeflow pipeline has an EXPECT clause that validates `order_total > 0` for data quality. Your operations team wants a daily dashboard showing how many orders violated this expectation (trend analysis for anomalies). Your SLA requires detecting quality drops within 1 hour. You need to query system tables to extract expectation violation counts, grouped by day. Which system table and query pattern retrieves this metric?

A) Query `system.pipelines.event_log` filtered to `event_type='expectation'` and `expectation_name='order_total > 0'`; group by day and count.  
B) Query `system.tables.table_lineage` to find tables depending on the expectation; count rows in the quarantine table by day.  
C) Use `SHOW METRICS` on the streaming table to get expectation summaries from `system.metrics.expectations` by date.  
D) Query the Delta transaction log (`_delta_log`) of the pipeline's storage location; parse event records for expectation failures.  

---

### 17. [Monitoring and Alerting — Alerting on data-quality condition]

Your master data team maintains a `customers` table that should have exactly one record per customer ID (unique constraint). Lately, data quality issues have introduced duplicate customer IDs. Your team wants a Databricks SQL alert that fires if duplicate count exceeds 100 on any day, notifying the team via email to investigate source data. You need the alert to compare a metric against a threshold. What configuration correctly implements this alert?

A) Use `MONITOR` command on `customers` to set anomaly detection; configure duplicate rate threshold.  
B) Write a UDF checking for duplicates; alert triggers if UDF returns true during query execution.  
C) Create a query returning a single row with duplicate count; create an alert that triggers if result > 100.  
D) Create a query grouping by `customer_id` and counting; alert triggers if any group has count > 1.  

---

### 18. [Cost & Performance Optimization — Deletion vectors for frequent small MERGEs]

Your fraud detection system performs incremental reconciliation on a `transactions` table (2M rows). The system runs 100 MERGE operations daily, each updating ~1000 rows with correction flags. After 30 days of these small updates, you observe table fragmentation: 3000 small files, slow queries (5x slower than baseline). You enable deletion vectors (DVs) at the table level. How do DVs help optimize this workload?

A) DVs pre-compute which files to rewrite; MERGE only rewrites files marked in the vector.  
B) DVs automatically partition the table to avoid small-file issues.  
C) DVs store delete markers instead of rewriting files; they reduce fragmentation and speed up MERGE. Queries apply DV filters transparently.  
D) DVs compress the transaction log, reducing metadata overhead and speeding up MERGE/planning.  

---

### 19. [Cost & Performance Optimization — Liquid Clustering vs partitioning]

Your analytics platform has a `user_events` table (500 GB, 10M unique users, 1B rows) tracking user behavior. Analysts query different user subsets daily (no predictable pattern). Partitioning by `user_id` would create 10M partitions, causing severe metadata overhead and slow catalog operations. Most queries filter on `user_id` for efficiency. Your team wants sub-second query latency without managing explicit partitions. Which clustering approach minimizes overhead while preserving performance?

A) Partition by `user_id`; 10M partitions is high but acceptable for predictable queries.  
B) Use Liquid Clustering on `user_id`; it organizes data at block level without explicit partitions, reducing overhead.  
C) Use partitioning on hash of `user_id` (modulo 1000); this reduces partition count and provides similar speedup.  
D) High-cardinality filters require secondary indexes, which Databricks doesn't support; accept full-table scans.  

---

### 20. [Cost & Performance Optimization — Predictive Optimization on UC managed tables]

Your data warehouse team manages 200+ tables in Unity Catalog (totaling 50 TB). Query SLAs require sub-10-second latency for 95% of user queries. Your team lacks resources to manually profile each table and recommend indexes/clustering. Your CFO wants to reduce compute costs by 20%+ without sacrificing performance. You enable Predictive Optimization on your UC catalog. What automatic actions does it perform, and what is the cost model for this optimization?

A) Automatically identifies tables that would benefit from clustering; you review and apply recommendations. It's free.  
B) Automatically applies clustering/ZORDER optimizations to all tables without review. Free on UC.  
C) Automatically optimizes tables in the background (clustering, ZORDER, etc.); background compute is billable.  
D) Available only on classic workspaces, not UC.  

---

### 21. [Cost & Performance Optimization — Query profile showing skewed join]

A 1 GB `users` table joined with 500 GB `events` table on `user_id` takes 5 minutes. Spark UI shows high-skew condition: a few partitions process 100x more data than others. Join is BroadcastHashJoin but events is too large to broadcast. What should you investigate first?

A) Increase cluster memory; BroadcastHashJoin needs more memory to fit the broadcast.  
B) Add a column filter on events (e.g., `WHERE event_date >= CURRENT_DATE - 30`) to reduce the dataset.  
C) Force SortMergeJoin instead; BroadcastHashJoin is inappropriate for large tables.  
D) Check if `user_id` has null values or skew; uneven join-key distribution causes imbalance.  

---

### 22. [Cost & Performance Optimization — Change Data Feed with table_changes]

Your master data team maintains a `customer_master` table (1 GB, 10M records) with daily updates. Six downstream systems (reporting, analytics, CRM, billing, audit, ML) must stay synchronized with changes. Currently, you export the entire table daily—wasting 99% bandwidth for 1% change. You enable Change Data Feed (CDF) to stream only inserts, updates, and deletes. Your integration layer needs to query changes incrementally since the last sync point. Which SQL function and approach correctly retrieves only incremental changes?

A) Use `SELECT * FROM table_changes('customer_master', 0)` to get all changes since version 0.  
B) Use `SELECT * FROM table_changes('customer_master', version_number)` where version_number is the last sync version; CDF returns inserts, updates, deletes.  
C) Enable CDF, then query `SELECT * FROM customer_master WHERE _change_type IN ('insert', 'update', 'delete')`.  
D) Use Lakeflow Declarative Pipelines `FROM table_changes()` to subscribe; manual SQL queries don't expose CDF.  

---

### 23. [Data Security and Compliance — ABAC policy with governed tag]

Your compliance team requires that the `phone_number` column in the `customers` table be masked for all non-Finance users (data minimization principle). You want to implement this via Attribute-Based Access Control (ABAC) using Unity Catalog's column masking with governed tags to identify sensitive columns and mask them based on user department. Which configuration correctly implements this masking policy?

A) Use a row filter instead; apply `is_account_group_member('Finance_group')` to control record access.  
B) Create dynamic column mask in table definition; tag the table with `department=Finance` for automatic filtering.  
C) Assign the `pii` governed tag to the `phone_number` column; create a masking UDF; apply ABAC policy matching column tag and checking `is_account_group_member('Finance')`.  
D) Apply `ALTER COLUMN phone_number SET TAGS ('pii' = 'sensitive')`; create masking UDF `mask_phone(val STRING) RETURNS STRING`; apply policy with `HAS_TAG_VALUE('pii', 'sensitive')` in the MATCH COLUMNS clause.  

---

### 24. [Data Security and Compliance — Row filter function with is_account_group_member]

Your finance team maintains a `transactions` table with both public (marketing spend, board-approved budgets) and confidential (executive compensation, M&A) rows marked by `department` column. Finance group members must see all rows (for audit purposes). All other employees see only `department='Public'` transactions. You implement row-level security via Unity Catalog row filter functions. Which row filter function definition correctly enforces this access control policy?

A) CREATE FUNCTION filter_transactions() RETURN is_account_group_member('Finance') OR department='Public'  
B) CREATE FUNCTION filter_transactions() RETURNS BOOLEAN RETURN IF(is_account_group_member('Finance'), TRUE, department='Public')  
C) CREATE ROW FILTER filter_transactions ON transactions USING (is_account_group_member('Finance') OR department='Public')  
D) CREATE ROW FILTER ON transactions WHERE is_account_group_member('Finance') OR department='Public'  

---

### 25. [Data Governance — UC privilege inheritance]

Your data governance team is setting up Unity Catalog privileges for a new data analyst. You grant the analyst `USE CATALOG` on your main catalog but deliberately do not grant any `USE SCHEMA` or `SELECT` privileges. The analyst attempts to run `SELECT * FROM main_catalog.sales_schema.orders`. Based on Unity Catalog's privilege model, what occurs?

A) The user can SELECT the table; `USE CATALOG` implies `USE SCHEMA` on all schemas.  
B) The user cannot SELECT; they need explicit `USE SCHEMA` and `SELECT` on the table.  
C) The user can list the catalog but not access schemas; they need `USE SCHEMA` AND `SELECT` to access tables.  
D) The user can SELECT only if they have `SELECT` privilege at catalog level (inherited).  

---

### 26. [Data Governance — Comments and tags for discoverability]

Your data catalog team is improving table discoverability for 2000 tables across sales, marketing, and engineering domains. You add a human-readable comment explaining each table's purpose, transformations, and SLA. You also tag tables with governed tag `data_domain=sales` (or marketing, engineering). Your goal is to help analysts self-serve data discovery without requiring documentation crawling or team queries. What benefits do comments and tags provide in this context?

A) Comments appear in Databricks Search and catalog browser; tags enable filtering in UI and are queryable in system tables.  
B) Comments and tags filter search results equally; no impact on data access.  
C) Comments are indexed by Genie AI for natural-language search; tags are for access control only.  
D) Comments appear in query results; tags organize tables in the workspace UI.  

---

### 27. [Debugging and Deploying — Repairing failed job run]

Your production ETL job runs nightly at 2 AM and processes customer transactions. Tonight's run fails halfway through due to a transient network timeout (downstream API issue). You diagnose and fix the root cause. You want to re-run the failed job with the exact same input parameters, job config, and cluster settings for backfill without affecting tomorrow's scheduled run. You need the most straightforward repair mechanism. Which approach is correct?

A) Use `databricks jobs update-job-run --run-id <ID> --status RUNNING` to resume from failure.  
B) Use CLI: `databricks jobs run-now --job-id <ID> --jar-params <params>` to replay.  
C) In the Databricks UI, click the failed run, then click 'Repair Run' to re-execute with the same parameters.  
D) Manually re-run the job via the Jobs UI; the UI auto-populates the last-run parameters.  

---

### 28. [Debugging and Deploying — Bundle targets and deploy]

Your DevOps team maintains a Declarative Automation Bundle (DAB) with two targets: `dev` (2 cluster cores, 5-minute job timeout) and `prod` (8 cores, 30-minute timeout) to balance costs and performance. After testing in dev, you need to deploy the bundle to production with prod's configuration. You're using the `databricks bundle` CLI tool. Which command correctly deploys using the prod target's variables?

```yaml
targets:
  dev:
    variables:
      cluster_cores: 2
      job_timeout: 5
  prod:
    variables:
      cluster_cores: 8
      job_timeout: 30
```

A) `databricks bundle deploy -t prod` will deploy using the prod target's variables.  
B) `databricks bundle deploy --target prod` is required; short form `-t` is not valid.  
C) `databricks bundle deploy prod` (positional argument) will deploy to prod.  
D) `databricks bundle deploy` with `export DATABRICKS_TARGET=prod` env var.  

---

### 29. [Debugging and Deploying — Spark UI diagnosis of spill/skew]

Your data platform team runs a complex aggregation job on 500 GB of event data. Spark UI shows Stage 5 (a shuffle/join stage) with 200 tasks. Task duration varies wildly: most complete in 2–5 seconds, but a few take 120+ seconds. Peak memory usage on executor nodes hits the limit, and task logs show spill warnings. Your team needs to diagnose whether this is a cluster-capacity issue (too little memory) or a data-distribution problem. What is the most likely root cause?

A) Insufficient cluster memory: all tasks are spilling; add more nodes.  
B) Network congestion during shuffle; variance reflects network latency.  
C) Executor GC pauses; long tasks stalled by garbage collection.  
D) Partition skew: a few tasks received most data; they spilled due to memory pressure.  

---

### 30. [Data Modeling — Dimensional model with MV + metric view]

Your analytics team is building a dimensional ecommerce data model for real-time BI dashboards. The raw `fact_orders` table (1B rows, updated every 5 minutes) joins with dimension tables (products, categories, dates). Analysts need pre-aggregated metrics (revenue by category per month, top SKUs, customer lifetime value) with sub-second query latency, governed measures, and versioning for audit. Your team wants to separate the data modeling layer (performance optimization) from the semantic/governance layer. How do you structure this architecture?

A) Create MVs for metric aggregations (e.g., `mv_revenue_by_category`); create a UC metric view on top that references MVs and defines governed measures.  
B) Create a single fact table with all dimensions denormalized; define metric view on top for aggregations.  
C) Use Lakeflow Declarative Pipelines to pre-compute aggregates; expose as views (not metric views).  
D) Query fact + dimensions directly in metric view definitions; avoid MVs to reduce storage.  

---

### 31. [Developing Code — Unit testing with assertDataFrameEqual and DataFrame.transform]

Your team is building a data transformation library to standardize customer data across 50+ sources. The transformation logic must be testable in isolation, with quick feedback during CI/CD. You decide to package each transformation step as a `DataFrame.transform()` call. Which testing approach ensures the transformation is semantically correct without running it on production data, and works with a serverless SQL warehouse context where only Lakeflow SQL tests are available?

```python
from databricks.sdk.sql import sql
from pyspark.testing import assertDataFrameEqual

def test_normalize_email():
    df = spark.createDataFrame(
        [("John", "JOHN@EXAMPLE.COM")],
        ["name", "email"]
    )
    result = df.transform(normalize_email)
    expected = spark.createDataFrame(
        [("John", "john@example.com")],
        ["name", "email"]
    )
    assertDataFrameEqual(result, expected)
```

A) Write Lakeflow SQL unit tests with `CREATE TEMPORARY TABLE` for inputs and compare outputs using `SELECT COUNT(CASE WHEN ... THEN 1)` to validate row counts and nulls.  
B) Use PySpark `assertDataFrameEqual` in a local Pytest suite; the transform logic is Python/SQL agnostic and can run on a serverless default compute cluster to validate before CI/CD merge.  
C) Deploy the transform as a temporary SQL view in a production warehouse and run `SELECT * EXCEPT` to spot differences; manually inspect the output daily.  
D) Use Databricks Notebooks with interactive runs, save the result to a temporary table, and manually inspect with `DESCRIBE` on each run—no automation needed.  

---

### 32. [Data Ingestion & Acquisition — Auto Loader vs COPY INTO vs read_files for different scales]

Your team receives 500 GB of compressed Parquet files daily from a vendor SFP drop into an S3 bucket with inconsistent naming. The ingestion SLA is 15 minutes end-to-end (detect, ingest, transform). You cannot modify the vendor's upload schedule or file format. Which approach minimizes latency and operational overhead while scaling to future 2 TB+ volumes?

A) Auto Loader with `cloudFiles` in file-notification mode (SNS/SQS); automatically triggers Lakeflow Declarative Pipeline on new file detection.  
B) `COPY INTO` with recursive glob pattern; Lakeflow job scheduled every 5 minutes to check for new files.  
C) `read_files` in a Lakeflow Declarative Pipeline with table schema inference; batch every 10 minutes.  
D) Manual Python script with `df.read.parquet()` on all files; run via job every 5 minutes and filter already-loaded files by modification time.  

---

### 33. [Monitoring and Alerting — Failure analysis with pipeline run metrics and error messages]

A Lakeflow Declarative Pipeline that ingests 10M rows daily failed mid-run at 08:15 UTC. You need to determine which task failed, the error message, and whether the failure is transient (retry) or permanent (requires fix). Your monitoring dashboard only shows a generic 'FAILED' status. Which query pattern enables root-cause analysis from pipeline run events?

A) Query `system.lakeflow.runs` to find the failed pipeline run; then inspect `run.start_time` and `run.end_time` to estimate row throughput.  
B) Query pipeline run event logs to find failed task steps; extract `task_name`, `status`, and `error_message` to identify the failing step and error type.  
C) Enable Spark driver logs via cluster logging; search for 'FAILED' in logs and cross-reference with `system.jobs.runs` for timing.  
D) Check the Lakeflow UI's 'Run Details' page and manually click through each task; record task names and error codes from displayed tiles.  

---

### 34. [Data Manipulation — Higher-order functions on arrays (transform, filter, aggregate)]

You have a table of customer transactions with a nested `items` array, each item containing `(product_id, price, discount_percent)`. You need to compute the net amount per item (price * (1 - discount_percent / 100)) for each transaction, filter items where net amount > $10, and sum the result. You must do this in a single SQL expression without exploding the array. Which higher-order function chain achieves this?

```sql
SELECT
  customer_id,
  items,
  -- Option: use transform, filter, and aggregate
  AGGREGATE(
    FILTER(
      TRANSFORM(items, x -> x.price * (1 - x.discount_percent / 100)),
      x -> x > 10
    ),
    0,
    (acc, x) -> acc + x
  ) AS total_net
FROM transactions
```

A) TRANSFORM to compute net amounts, FILTER to keep > $10, AGGREGATE to sum; all nested as `AGGREGATE(FILTER(TRANSFORM(...)))`.  
B) Use MAP to compute net amounts, then REDUCE to filter and sum in one pass.  
C) Explode the array into rows, compute net amount, filter with WHERE, GROUP BY customer_id and SUM.  
D) Use a window function with OVER (PARTITION BY customer_id) and aggregate the array inline.  

---

### 35. [Cost & Performance Optimization — Query result cache for identical repeated queries]

Your BI team runs 200 identical summary queries per day on a 500 GB fact table, each querying the same 10 GB of hot data. Query latency SLA is < 5 seconds. You need to choose a caching strategy that minimizes operational overhead and cost. Which approach is most suitable for identical repeated queries?

A) Enable Delta disk cache on a dedicated cluster; it persists across sessions and scales with fact-table size.  
B) Use both Delta disk cache and query result cache; dual caching ensures fastest repeated queries and lowest total cost.  
C) Neither cache is worth the overhead; just run queries on a larger instance type to speed up computation directly.  
D) Use query result cache; it caches the final result after all computations and automatically invalidates on table changes.  

---

### 36. [Data Security and Compliance — Pseudonymization for GDPR compliance]

Your company handles PII (email, SSN, phone) and must comply with GDPR's pseudonymization requirement for analytics. You need to transform PII in a way that: (1) is deterministic (same input → same output for joins), (2) cannot be reversed by analysts, (3) passes regulatory audit. Which approach is correct for GDPR pseudonymization?

A) Tokenization; it replaces PII with random tokens stored in a secure vault, and analysts cannot reverse it without vault access.  
B) Salted MD5 hashing with a team secret; it is deterministic, cannot be reversed, and analysts cannot recover the original value.  
C) Tokenization; salted hashing can be brute-forced with a rainbow table if the salt is discovered.  
D) Salted SHA-256 hashing with strong salt; it is deterministic, cryptographically strong, irreversible for analysts, and meets GDPR pseudonymization.  

---

### 37. [Debugging and Deploying — Diagnosing schema mismatches in Spark logs]

Your Lakeflow Declarative Pipeline updated from DBR 14.2 to 15.3 and fails on the first transformation with error 'StructField at index 0 is missing'. The pipeline did not change code; only the DBR changed. You need to identify which table schema changed between versions. Where do you look first?

A) Check the Databricks event log in `system.billing.audit_logs`; filter by `event_category = 'SCHEMA_CHANGE'` and `resource_type = 'TABLE'` to find schema differences.  
B) Query the pipeline task history in `system.lakeflow.runs`; look for the 'FAILED' task and inspect the error details.  
C) Review the Spark driver logs via cluster logging in the Compute settings; search for 'StructField' errors to pinpoint the affected column.  
D) Query `system.object_lineage` filtered by your pipeline's output table; trace back upstream to the first task and inspect source schema changes.  

---

### 38. [Developing Code — Idempotent foreachBatch MERGE using batch id]

Your Structured Streaming pipeline ingests event logs into a Delta table. You use `foreachBatch` to MERGE new events with existing data, deduplicating on (user_id, event_timestamp). However, on stream restart (e.g., after a failure), the same batch of events are reprocessed, causing duplicate merges. You need idempotency without reprocessing the entire history. How do you make the MERGE idempotent per batch?

```python
def foreachBatch_merge(batch_df, batch_id):
    # Idempotent merge: use batch_id in the merge condition
    batch_df.write.format('delta').mode('merge').options(
        mergeSchema='true'
    ).option('idempotencyKey', batch_id).foreachBatch(
        lambda df, bid: df.merge(
            delta_table,
            condition=f'delta_table.user_id = df.user_id AND delta_table.event_ts = df.event_ts AND delta_table._batch_id = {bid}'
        ).whenMatched().updateAll().whenNotMatched().insertAll().execute()
    ).start()
```

A) Add a `_batch_id` column to the batch DataFrame and include it in the MERGE condition; if (user_id, event_timestamp, _batch_id) match an existing row, skip the merge.  
B) Store the batch_id in a separate checkpoint table; before each MERGE, query the checkpoint to skip already-processed batches.  
C) Use `foreachBatch` with `idempotencyKey` option set to `batch_id`; this tells the Streaming API to deduplicate by batch ID automatically.  
D) Enable `MERGE ... IF NOT EXISTS` and rely on Primary Key constraints to prevent duplicates across batches.  

---

### 39. [Data Governance — Ownership and grant permissions on tables]

A data analyst has `SELECT` + `MODIFY` permissions on a critical `sales.transactions` table (not the owner). Your governance policy requires the analyst to be able to share read access with specific colleagues without involving the data owner or IT. What privilege must you grant the analyst to enable this, and who has authority to transfer table ownership if the current owner leaves?

A) Grant the analyst `ADMIN` role; this allows them to grant/revoke any permission. The current owner can transfer ownership via `ALTER TABLE ... OWNER = analyst`.  
B) Grant the analyst `OWNERSHIP` on the table; they can then use `GRANT SELECT TO ...` on colleagues. Only the current owner (or account admin) can transfer ownership via `ALTER TABLE ... OWNER`.  
C) Grant the analyst `GRANT_OPTION` on `SELECT`; they can delegate `SELECT` to colleagues. The current owner retains transfer authority; account admins can force a transfer.  
D) Analysts cannot grant permissions by policy; only owners and admins can. Transfer ownership requires IT approval and re-governance from scratch.  

---

### 40. [Data Modeling — SCD Type 2 point-in-time join using `__START_AT` / `__END_AT`]

You have a `dim_customer` SCD Type 2 table with effective-dated rows (customer_id, name, status, __START_AT, __END_AT), and a `fact_orders` table with (order_id, customer_id, order_date). You need to join orders to the customer dimension as it existed at order date (not current snapshot). How do you write the point-in-time join?

```sql
SELECT
  o.order_id,
  d.name,
  d.status
FROM fact_orders o
JOIN dim_customer d ON
  o.customer_id = d.customer_id
  AND o.order_date >= d.__START_AT
  AND (o.order_date < d.__END_AT OR d.__END_AT IS NULL)
ORDER BY o.order_id
```

A) Use `o.order_date BETWEEN d.__START_AT AND d.__END_AT`; this ensures the order sees the correct customer state.  
B) Use `o.order_date >= d.__START_AT AND (o.order_date < d.__END_AT OR d.__END_AT IS NULL)`; this handles the time window correctly (start inclusive, end exclusive).  
C) Group by order_id and customer_id, then take the MAX(__START_AT) row where MAX(__START_AT) <= order_date; simplest.  
D) Use `o.order_date = d.__START_AT` to get the exact row created on order date; more precise than range queries.  

---

### 41. [Cost & Performance Optimization — Automatic clustering selection based on query patterns]

Your `fact_events` table has 100B rows with usage monitoring enabled on a Unity Catalog managed table. Query patterns show: 60% filter by `user_id`, 25% by `region + date_range`, 15% by `event_type`. You want to optimize without manually specifying clustering columns. What automatic clustering capability can learn from these patterns and adapt over time?

A) Delta Lake can automatically learn which columns most frequently filter queries and cluster by those; it adapts as patterns change and requires only enabling the feature.  
B) Manual clustering configuration is required; you must explicitly set `CLUSTER BY (user_id, region, date_range)` based on your analysis.  
C) Automatic clustering is a Lakeflow optimization that runs OPTIMIZE every hour; no configuration needed.  
D) Automatic clustering is not suitable for tables with 100B rows; only tables < 10B rows benefit from clustering.  

---

### 42. [Data Ingestion & Acquisition — Exposing tables to external engines as Iceberg (managed vs external)]

Your organization runs Databricks for analytics and has external tools (Presto, Trino, Spark on EC2) that need direct read access to your tables. You must use the Iceberg format for interoperability. Should you use managed Iceberg (store tables in Databricks managed storage), Iceberg REST catalog (centralized metadata), or external Iceberg reads (point external engines at your Delta Lake directly)?

A) Managed Iceberg; Databricks stores Iceberg metadata in managed storage, and external engines query via REST catalog API.  
B) External Iceberg reads; external engines read your Delta tables directly as Iceberg without converting; simplest setup.  
C) Iceberg REST catalog; you run a dedicated REST server that serves Iceberg metadata to external engines pointing at your S3/ADLS2 storage.  
D) Managed Iceberg + Iceberg REST catalog together; dual setup ensures both Databricks and external engines can query Iceberg format.  

---

### 43. [Developing Code — Stream-stream join with watermarks on both sides]

You have two Structured Streaming sources: `stream_clicks` (user_id, click_time) and `stream_purchases` (user_id, purchase_time). You need to join them to find purchases within 10 minutes after a click. Both streams are late-arriving (up to 30 minutes). How do you set watermarks on both streams and define the join condition to prevent state explosion?

```python
clicks = (
    spark.readStream.kafka(...)
    .withWatermark('click_time', '30 minutes')
)
purchases = (
    spark.readStream.kafka(...)
    .withWatermark('purchase_time', '30 minutes')
)
joined = clicks.join(
    purchases,
    (clicks.user_id == purchases.user_id)
    & (purchases.purchase_time >= clicks.click_time)
    & (purchases.purchase_time <= clicks.click_time + interval '10 minutes'),
    'leftOuter'
)
```

A) Watermark both streams at 30 minutes; set join condition to `purchase_time BETWEEN click_time AND click_time + 10 minutes`; use left outer join to keep all clicks.  
B) Watermark only clicks at 30 minutes; purchases don't need a watermark since they are matched against clicks.  
C) Watermark both streams; use `joinWithStateTimeout` to auto-drop state older than 10 minutes; prevents state explosion.  
D) Watermark clicks at 10 minutes, purchases at 30 minutes; join on user_id only, then filter with WHERE for time range in the result.  

---

### 44. [Monitoring and Alerting — Monitoring runs via Jobs API / Databricks CLI]

Your team runs 50 Lakeflow jobs daily and needs automated alerting when a job fails (email on failure, JIRA ticket creation). You want to centralize the monitoring logic in a Python script on your laptop that runs every 5 minutes. Which monitoring approach is most reliable and requires the least infrastructure?

A) Query `system.jobs.runs` via Databricks SQL; filter by `state = 'FAILED'` and recent runs; post failures to Slack via webhook.  
B) Use Databricks CLI `databricks jobs list-runs` in a cron script; parse JSON output and post alerts; run from your laptop or a simple EC2 instance.  
C) Enable Databricks notifications in Workspace settings and subscribe to email alerts; no code needed.  
D) Poll the Jobs API (`GET /api/2.1/jobs/runs/list`) in a Python script; check for failed runs and trigger alerts via HTTP POST to your monitoring service.  

---

### 45. [Data Manipulation — `ai_query` batch inference inside a pipeline with cost/latency considerations]

Your Lakeflow Declarative Pipeline processes 1M product reviews daily and must classify sentiment using a Foundation Model. You have two options: (1) `ai_query` function on each row during transformation, or (2) batch the rows, call the model's batch endpoint, and join results back. Each model inference costs $0.001 per call. Which approach minimizes cost and meets a 2-hour SLA?

A) Use `ai_query` on each row; Lakeflow automatically batches calls internally, so cost is the same as manual batching but code is simpler.  
B) Manually batch rows in chunks of 100; call the Foundation Model batch endpoint per chunk; join results back; costs less than `ai_query` per-row.  
C) Use `ai_query`; it streams results as they arrive, so latency is predictable and cost is per-call (no batching overhead).  
D) Cache the model predictions in a reference table; for new reviews, query the cache first; only call the model for new unique review texts.  

---

### 46. [Cost & Performance Optimization — Small files from streaming writes: auto-compaction, optimized writes, OPTIMIZE]

Your Lakeflow Declarative Pipeline ingests 100 GB daily from Kafka, writing to a Delta table via `foreachBatch`. After 7 days, the table has 700 GB of data scattered across 70,000 small files (1 MB each). Query latency has degraded 10x. Which optimization minimizes small files while avoiding excessive compaction cost?

A) Run `OPTIMIZE TABLE` every hour; it will compact files automatically into larger partitions.  
B) Enable `optimizedWrite=true` and `autoCompact=true` in the DataFrameWriter; Lakeflow will automatically compact small files on write.  
C) Increase the `foreachBatch` batch size from 100 GB to 1 TB; larger batches produce fewer, larger files naturally.  
D) Run a weekly `OPTIMIZE TABLE` job; it compacts 70k files into ~100 larger files, balancing cost and performance.  

---

### 47. [Data Security and Compliance — GDPR erasure with deletion vectors (REORG TABLE ... APPLY PURGE + VACUUM)]

Your team manages a critical `customers` table with 50B rows and 5 years of historical data in Delta Lake. A GDPR erasure request arrives: delete all data for customer_id = 12345. Your backup retention SLA is 7 days, and compliance audits occur monthly. Re-ingesting 50B rows weekly is prohibitively expensive. You need to erase the customer's data efficiently while preserving audit trails and meeting the 7-day retention SLA. How do you accomplish this?

```sql
-- Mark rows for deletion using deletion vectors
DELETE FROM customers WHERE customer_id = 12345;

-- Compact and apply deletion vectors
REORG TABLE customers APPLY (PURGE);

-- Remove old versions and unneeded files
VACUUM customers RETAIN 0 HOURS;
```

A) Use `DELETE FROM customers WHERE customer_id = 12345`; Delta's deletion vectors mark rows without re-writing files.  
B) Use `DELETE`, then `OPTIMIZE` to compact files and purge deleted rows; this is the standard GDPR-erasure pattern.  
C) Use `DELETE`, then `REORG TABLE ... APPLY (PURGE)` to physically remove deleted rows; finally `VACUUM` to clean old versions.  
D) Use `DROP TABLE` and re-ingest data excluding the customer; only reliable way to guarantee no traces.  

---

### 48. [Data Modeling — Choosing clustering keys for a large fact table based on query patterns]

Your `fact_sales` table (100B rows) is queried by: regional managers filtering on `region + date_range` (60% of queries), finance teams querying `product_category + fiscal_quarter` (30%), and ad-hoc analysts on various columns (10%). Query latency SLA is 10 seconds. Which clustering key is best?

A) Cluster by `(region, date_range, product_category)`; covers all query patterns without missing the top queries.  
B) Cluster by `(region, date_range)` only; 60% query coverage is the strongest signal; ad-hoc queries will be slower but acceptable.  
C) Cluster by `(date_range, product_category, region)`; order by frequency, not selectivity.  
D) Do not cluster; run `OPTIMIZE` instead; for 10-second SLA on 100B rows, clustering is insufficient.  

---

### 49. [Developing Code — Append flows to fan-in several sources into one streaming table]

You have three independent data sources (Kafka topic 1, Kafka topic 2, and S3 uploads) that all produce the same schema (id, timestamp, value). You need to ingest all three into a single `unified_events` streaming table. Which approach is cleanest and most maintainable in Lakeflow Declarative Pipelines?

```python
# Option: Append multiple sources to one streaming table
@flow
def ingest_unified():
    # Source 1: Kafka topic 1
    kafka1 = read_kafka('topic1')
    
    # Source 2: Kafka topic 2
    kafka2 = read_kafka('topic2')
    
    # Source 3: S3 uploads
    s3_data = read_files('s3://bucket/uploads')
    
    # Append all three sources
    unified = kafka1.union(kafka2).union(s3_data)
    
    # Write to streaming table
    unified.into('unified_events').write()
```

A) Create three separate streaming tables, one per source, then union them in a view for downstream queries.  
B) Use `union` to append all three source DataFrames in a single transformation; write once to `unified_events` streaming table.  
C) Create one streaming task per source, each writing to `unified_events` with `mode='append'`; Lakeflow merges writes.  
D) Set up three separate pipelines, each writing to its own table; join them downstream as needed.  

---

### 50. [Debugging and Deploying — CI/CD with Git folders + Declarative Automation Bundles in a CI system using service principal]

Your team develops Lakeflow pipelines in a Git repo (main and feature branches). You need CI/CD automation: deploy pipelines from feature branches to a staging workspace, run tests, merge to main, deploy to production. You want to avoid manual approval for production deploys by non-admins. Which CI/CD pattern is safe and least operational?

```yaml
# Declarative Automation Bundle for production deployment
resources:
  jobs:
    production_pipeline:
      name: "Production ETL Pipeline"
      tasks:
        - task_key: ingest
          pipeline_task:
            pipeline_id: "${pipeline_id}"
  pipelines:
    etl_pipeline:
      name: "ETL Pipeline"
      configuration:
        source: git
        path: "pipelines/etl.py"
        branch: main
```

A) Use Declarative Automation Bundles (DAB) in Git with `databricks bundle deploy`; DABs encode permissions and ownership, preventing unauthorized changes to production resources.  
B) Store pipeline code in a Git folder; CI runs `databricks pipelines create/update` on feature branches; manual approval gate for production merge.  
C) Set up a CI system (GitHub Actions) that authenticates with a service principal; deploy DABs from feature branches to staging, then to production on merge to main.  
D) Push code directly to the Git folder linked in Workspace; Lakeflow auto-deploys on any commit without CI/CD scaffolding.  

---

### 51. [Data Manipulation — `expect_or_fail` vs `expect_or_drop` vs warn semantics in quality checks]

Your Lakeflow Declarative Pipeline reads raw data and applies quality expectations: `email IS NOT NULL`, `age BETWEEN 18 AND 120`, `status IN ('active', 'inactive')`. Some data is corrupted (5% null emails, 2% age > 120). You have three quality modes for each expectation: fail the pipeline, drop invalid rows, or warn. Which strategy minimizes downtime while maintaining data integrity?

A) Use `expect_or_fail` for all expectations; stop the pipeline if any data is invalid; force upstream to fix data.  
B) Use `expect_or_drop` for all expectations; silently drop invalid rows; ensures pipeline always succeeds.  
C) Use `expect_or_fail` for critical constraints (email IS NOT NULL); `expect_or_drop` for edge cases (age range); `warn` for advisory rules.  
D) Use `warn` for all expectations; log invalid rows but process them anyway; audit later.  

---

### 52. [Monitoring and Alerting — Auditing who read a table with `system.access.audit`]

Your organization handles HIPAA-regulated customer data in Databricks. Your compliance team must audit table access monthly for SOC2 requirements. You need to generate daily audit reports showing: who accessed sensitive tables, when, from which workspace, which SQL was executed, and detect unauthorized access patterns. Previous monitoring gaps allowed undetected reads for 3 days. Which system table provides comprehensive access telemetry for compliance?

A) `system.access.audit` contains user identity, timestamp, resource type (table), action (SELECT), workspace_id, and query text.  
B) `system.query_history` shows queries run on a table; filter by table name and time range; extract user and SQL.  
C) `system.event_log` records all workspace events including table access; filter by action = 'READ'.  
D) `system.admin.jobs.runs` tracks job executions; query details show table access within each job step.  

---

### 53. [Cost & Performance Optimization — Materialized view incremental refresh requirements (serverless compute) and full-recompute fallback]

You create a materialized view on a 500 GB fact table, computing daily aggregations by region and product with UNION ALL and joins. You want incremental refresh (only recompute changed rows). Which compute requirement enables incremental refresh, and when does it fall back to full recompute?

A) Serverless SQL Warehouse compute is required; falls back to full recompute if source table schema changes or cost analysis determines full recompute is cheaper.  
B) SQL compute (not serverless) is required; row tracking is optional. Falls back if the source table grows > 1 TB.  
C) Serverless compute plus row tracking enabled on the source; falls back to full if the materialized view is dropped and recreated.  
D) Classic cluster compute with CLUSTER BY on source table; falls back if data changes at all after initial creation.  

---

### 54. [Data Security and Compliance — Least-privilege job identity (run as service principal)]

Your data pipeline runs as a Lakeflow job and must read from two tables (sales_data, customer_data) but not from a third (internal_costs). Currently, the job runs under your user identity, which has full access. How do you enforce least privilege so the job accesses only the two required tables?

A) Create a service principal with `SELECT` permission only on sales_data and customer_data; configure the Lakeflow job to run as this service principal via job ACL.  
B) Use role-based access control (RBAC); assign the job to a `data_pipeline_reader` role with SELECT on the two tables; job inherits role permissions.  
C) Create a view that selects from sales_data and customer_data; grant the job SELECT on the view only; the view enforces data access scope.  
D) Use Unity Catalog namespace isolation; put the two tables in a `public` schema, internal_costs in `private`; job identity is automatically restricted by schema.  

---

### 55. [Developing Code — Lakeflow Jobs If/else condition task based on task value]

Your Lakeflow job ingests 10M customer records daily with strict SLA: 99.5% uptime, failures trigger alerts within 1 minute to Slack and PagerDuty, recovery teams must auto-retry within 30 seconds. Your data quality check task outputs a boolean (quality_pass: true/false). If quality passes, run downstream ingestion; if it fails, send an alert and skip ingestion without cascading failures. How do you implement this conditional logic in Lakeflow Jobs?

A) Use an If/else task type; set the condition to `tasks.quality_check.quality_pass == true`; branch to ingestion if true, alert if false.  
B) Use a Notebook task with logic: if quality_pass, trigger another job run for ingestion; else, post to Slack.  
C) Use a SQL task with a conditional INSERT/UPDATE; write results to a `job_status` table; downstream tasks poll the table.  
D) Lakeflow Jobs does not support branching; use two separate jobs and a scheduler script to run job 2 conditionally based on job 1 output.  

---

### 56. [Data Ingestion & Acquisition — Lakehouse Federation connection + foreign catalog with UC permissions]

Your organization has a legacy Teradata data warehouse with customer data. You want to expose this data to analysts in Databricks without copying (expensive). Lakehouse Federation allows querying foreign data sources via a foreign catalog. What permissions must you set up for analysts to read the Teradata data?

A) Create a foreign catalog pointing to Teradata; grant analysts `SELECT` permission on the foreign catalog and its tables; no Teradata-side permissions needed.  
B) Create a Lakehouse Federation connection to Teradata; set up a foreign catalog; grant analysts `USE_CATALOG` + `SELECT` on tables; Teradata roles auto-map to Databricks.  
C) Create a Lakehouse Federation connection with Teradata credentials; analysts query via UC-controlled foreign catalog; Teradata permissions are checked by the connection's identity.  
D) No UC permissions needed; analysts connect directly via ODBC/JDBC gateway; Lakehouse Federation is transparent to permission model.  

---

### 57. [Developing Code — Library installation failure on serverless (environment version / dependency conflict)]

Your Lakeflow Declarative Pipeline uses a custom Python package (my_lib v2.0) that requires numpy >= 1.24. The pipeline runs on serverless default compute, which has a fixed environment. Your package conflicts with the pre-installed numpy 1.23. The pipeline fails with `ImportError: numpy version conflict`. How do you resolve this?

A) Update my_lib to support numpy 1.23; downgrading the dependency resolves conflict and avoids environment-version changes.  
B) Install numpy 1.24 at pipeline runtime using `pip install --upgrade numpy`; this forces the required version.  
C) Switch to SQL-only pipelines; serverless does not support Python with custom dependencies; use a classic cluster instead.  
D) Use a specific environment version (e.g., 15.4-GPU-ML) with newer numpy; configure the Lakeflow job to use this environment instead of default serverless.  

---

### 58. [Data Manipulation — Complex aggregation with GROUPING SETS / ROLLUP / CUBE]

Your analytics team queries a daily revenue table (50B rows, growing 5% weekly) at peak with 200 concurrent users. Query SLA is under 30 seconds. You need to compute aggregations at multiple levels: (1) by region, (2) by product, (3) by region + product, (4) global total. Writing four separate queries with UNION is verbose and violates SLA under load. Which SQL aggregation function computes all four levels in a single pass?

```sql
SELECT
  region,
  product,
  SUM(revenue) AS total_revenue
FROM sales
GROUP BY GROUPING SETS (
  (region),
  (product),
  (region, product),
  ()
)
ORDER BY region, product
```

A) `GROUP BY GROUPING SETS ((region), (product), (region, product), ())`; computes all four aggregations in one pass.  
B) `GROUP BY region, product` with UNION ALL to separate aggregates; requires four separate queries.  
C) `GROUP BY ROLLUP (region, product)`; computes hierarchical aggregations: region+product, region, global.  
D) `GROUP BY CUBE (region, product)`; computes all combinations of region and product, plus global.  

---

### 59. [Debugging and Deploying — Driver OOM caused by `collect()` / `toPandas()` diagnosed from cluster logs]

Your Lakeflow Declarative Pipeline crashes with `java.lang.OutOfMemoryError: Java heap space`. The pipeline reads 100 GB from a table, applies a filter (reducing rows by 50%), then calls `toPandas()` to return results to Python. The driver node has 16 GB RAM. Why does the crash happen and how do you diagnose it from cluster logs?

A) `toPandas()` brings all filtered rows (50 GB) into the driver's JVM heap; 50 GB > 16 GB RAM, causing OOM. Cluster logs show 'GC overhead limit exceeded' or explicit OOM on the driver.  
B) The filter operation is incorrectly distributed; 100% of rows are shipped to the driver before filtering. Cluster logs show skewed task distribution on the driver node.  
C) `toPandas()` is inefficient; use `collect()` instead to keep data in Spark. Cluster logs show no OOM; issue is Pandas serialization overhead.  
D) OOM is caused by large metadata; driver must hold all column statistics. Switch to a larger driver instance type (64 GB+) to resolve.  

---

### 60. [Developing Code — Choosing Structured Streaming vs Lakeflow Declarative Pipelines under operational constraints]

Your team must build a real-time data ingestion pipeline with these constraints: (1) operational team has no PySpark experience, only SQL; (2) pipeline must auto-recover from failures without manual restarts; (3) must run 24/7 with minimal ops overhead; (4) data volume is 500k events/sec. Should you use Structured Streaming or Lakeflow Declarative Pipelines and why?

A) Structured Streaming with Python; it is powerful and flexible, suitable for 500k events/sec, but requires senior PySpark engineers.  
B) Lakeflow Declarative Pipelines with SQL; SQL-friendly, built-in fault tolerance, auto-recovery, minimal ops. Suitable for 500k events/sec if using optimized connectors.  
C) Structured Streaming on a classic cluster; better than Lakeflow for ops simplicity and fault tolerance.  
D) Lakeflow Declarative Pipelines are only for low-volume batch; use Structured Streaming for 500k events/sec.  

---

## Answer Key and Explanations

**1. Answer: D**

DAB supports packaging Python code as a wheel artifact and referencing it via `artifact.libraries` to enable code reuse across deployments. Using `targets` and variables allows region-specific config without duplicating code. the second approach relies on git files (fragile and hard to maintain at scale), the third approach treats apps as a deployment unit (not a code dependency), and the fourth approach limits logic to SQL-only and creates tight coupling.

- **A:** Incorrect: Copying code across regions violates DRY and scales poorly.
- **B:** Incorrect: Databricks Apps are not designed as code dependency packages.
- **C:** Incorrect: SQL UDFs alone cannot express the full utility logic, and metastore-level sharing doesn't scale operationally.
- **D:** Correct: This pattern enables code reuse via wheel artifacts and targets, reducing maintenance overhead.

References: <https://docs.databricks.com/aws/en/dev-tools/bundles/artifact-private> · <https://docs.databricks.com/aws/en/dev-tools/bundles/library-dependencies>

**2. Answer: D**

A SQL UC function meets all constraints: it's governed by UC with audit trails and versioning, callable from both SQL and PySpark (via `spark.sql()`), requires no external dependencies, and analysts can update it independently. Pandas UDFs require Python serialization overhead, external dependencies (PyArrow), and don't scale well on serverless. the second approach is the constraint-satisfying answer.

- **A:** Incorrect: Pandas UDFs require Python environments and dependencies, violating serverless and no-external-deps constraints.
- **B:** Incorrect: Adding an unnecessary Python wrapper defeats the purpose and reintroduces complexity.
- **C:** Incorrect: Vectorization gains don't apply uniformly; SQL is often faster on serverless for simple transformations.
- **D:** Correct: SQL UC functions satisfy governance, cross-language access, serverless compatibility, and independent updates.

References: <https://docs.databricks.com/aws/en/pyspark/reference/functions/pandas_udf> · <https://docs.databricks.com/aws/en/designer/tutorial-send-slack-message-sql-udf>

**3. Answer: C**

A materialized view with 5-minute refresh perfectly satisfies both the SLA (freshness within 5 minutes) and cost constraints (60% savings vs. continuous streaming). Streaming tables cost more (per-second compute). the first approach exceeds requirements (overkill). the third approach adds unnecessary complexity. the fourth approach over-specifies refresh frequency and wastes budget.

- **A:** Incorrect: More frequent refresh than needed violates cost constraints.
- **B:** Incorrect: Streaming tables cost more (continuous compute) and exceed the SLA requirement.
- **C:** Correct: MV refresh every 5 minutes meets SLA and saves 60% vs. streaming.
- **D:** Incorrect: Redundant layering adds operational overhead without benefit.

References: <https://docs.databricks.com/aws/en/ldp/concepts/streaming-tables> · <https://docs.databricks.com/aws/en/ldp/concepts/materialized-views>

**4. Answer: C**

`FLOW AUTO CDC` with `KEYS`, `SEQUENCE BY updated_at`, `TRACK HISTORY ON`, and `STORED AS SCD TYPE 2` is the correct modern Lakeflow syntax for SCD Type 2. The system automatically manages `__START_AT` and `__END_AT` columns for history tracking. Using APPLY CHANGES represents legacy syntax; misapplying SCD Type 2 configuration wastes effort.

- **A:** Incorrect: APPLY CHANGES is legacy; FLOW AUTO CDC is the modern approach.
- **B:** Incorrect: TRACK HISTORY COLUMN LIST is standard in AUTO CDC.
- **C:** Correct: FLOW AUTO CDC with KEYS, TRACK HISTORY ON, and SCD TYPE 2 is the modern syntax that automatically adds __START_AT and __END_AT.
- **D:** Incorrect: Primary keys belong in the source, not in TRACK HISTORY COLUMN LIST.

References: <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table-auto-cdc>

**5. Answer: D**

A 10-minute watermark allows late events up to 10 minutes; beyond that they drop. Update mode emits only changed rows (efficient for late arrivals), whereas complete mode re-sends all windows with every trigger (wasteful), and append mode never emits updates after a window's watermark is reached. Update mode directly satisfies the requirement to update windows when late data arrives.

- **A:** Incorrect: Manual filtering bypasses Structured Streaming's stateful semantics.
- **B:** Incorrect: Complete mode re-emits all windows every trigger; wasteful.
- **C:** Incorrect: Append mode finalizes windows at the watermark and never emits further updates.
- **D:** Correct: Watermark + update mode allows late events to update windows, emitting only changed rows for efficiency.

References: <https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/withWatermark> · <https://docs.databricks.com/aws/en/structured-streaming/output-mode>

**6. Answer: C**

In Lakeflow Jobs `for_each`, the `items` SQL query auto-binds row columns to `task.<column>` in the task context. The YAML shown is correct. the first approach is correct but imprecise. the second approach uses non-existent syntax. the fourth approach overstates the need for explicit config.

- **A:** Incorrect: While the YAML works, the auto-binding of row columns to task context is the more precise and standard pattern.
- **B:** Incorrect: `each.value` is not Lakeflow syntax; use `task.column_name`.
- **C:** Correct: Auto-binding of row columns to task context variables is standard.
- **D:** Incorrect: Auto-binding is built-in; no explicit loop_variable needed.

References: <https://docs.databricks.com/aws/en/jobs/how-to/foreach-sql-lookup-tutorial> · <https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types>

**7. Answer: C**

Serverless performance mode caches Python environments across runs within a time window, eliminating repeated initialization. Wheels enable caching. Abandoning serverless without exploring optimization is premature; misapplying features like Apps and init scripts won't solve the problem.

- **A:** Incorrect: Init scripts run every initialization; they don't persist caches.
- **B:** Incorrect: Serverless can be optimized via performance mode; abandoning it is unnecessary.
- **C:** Correct: Performance mode caches environments; wheels avoid repeated downloads.
- **D:** Incorrect: Apps are not dependency package repositories.

References: <https://docs.databricks.com/aws/en/compute/serverless/dependencies> · <https://docs.databricks.com/aws/en/dev-tools/bundles/library-dependencies>

**8. Answer: B**

Auto Loader automatically rescues unexpected data into the `_rescued_data` column as JSON, preventing ingestion failure. You can inspect and decide whether to add the field to the schema. the first approach understates capability. the third approach confuses quarantine (data quality, not schema). the fourth approach misrepresents default behavior.

- **A:** Incorrect: Auto Loader has automatic rescue behavior; it doesn't silently drop.
- **B:** Correct: Unexpected fields go to `_rescued_data` as JSON for safe preservation.
- **C:** Incorrect: Quarantine is for data quality expectations, not schema detection.
- **D:** Incorrect: Auto Loader accepts unknown fields by default via `_rescued_data`.

References: <https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema> · <https://docs.databricks.com/aws/en/data-engineering/schema-evolution>

**9. Answer: D**

Lakeflow's Kafka connector starts from `startingOffsets='latest'` by default and auto-discovers new partitions. Deserialize JSON via `from_json()` after casting the value to string. the first approach conflates serializer config (not exposed at SQL level). the second approach misapplies `cloudFiles`. the third approach redundantly adds default config.

- **A:** Incorrect: Serializer config isn't exposed at the Lakeflow SQL level.
- **B:** Incorrect: `cloudFiles` is for cloud object storage, not Kafka.
- **C:** Incorrect: `latest` is already default; `auto.offset.reset` is redundant.
- **D:** Correct: Kafka connector with latest offset, auto-discovery, and from_json() deserialization.

References: <https://docs.databricks.com/aws/en/connect/streaming/kafka/> · <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/kafka>

**10. Answer: B**

On-premises databases require an Ingestion Gateway deployed in your network to securely bridge the firewall. Enable SQL Server CDC, configure the source in Lakeflow Connect, define an ingestion pipeline. Other approaches either omit the gateway (needed for firewall access) or add unnecessary complexity by bypassing CDC.

- **A:** Incorrect: Misses the Ingestion Gateway/Relay needed for on-prem access through firewall.
- **B:** Correct: Relay + database CDC + Lakeflow Connect pipeline handles on-prem CDC.
- **C:** Incorrect: JDBC queries are not CDC; they don't track deletes or history.
- **D:** Incorrect: Kafka adds unnecessary complexity; Lakeflow Connect handles CDC natively.

References: <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/>

**11. Answer: D**

Delta Sharing's open sharing model allows non-Databricks recipients (Snowflake included) to access shared Delta tables via a URL and lightweight connector. No workspace login required. the second approach requires workspace access (not applicable). the third approach forces manual export (defeats real-time). the fourth approach is federation (opposite direction).

- **A:** Incorrect: Federation allows Snowflake to query Databricks; sharing is the opposite.
- **B:** Incorrect: Requires workspace access; not applicable to external partners.
- **C:** Incorrect: Delta Sharing works with non-Databricks systems via open sharing.
- **D:** Correct: Open sharing enables non-Databricks recipients like Snowflake to query Delta tables.

References: <https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/opensharing>

**12. Answer: B**

`ROW_NUMBER()` with `ORDER BY sequence_id DESC` correctly assigns rank 1 to the highest sequence_id within each partition. the second approach wrongly suggests `RANK()` (for ties, unnecessary). the third approach reverses the sort (DESC is correct). the fourth approach adds redundant date sorting (already partitioned).

- **A:** Incorrect: Date is already in PARTITION BY; redundant ordering.
- **B:** Correct: ROW_NUMBER with DESC sequence_id order correctly selects latest record.
- **C:** Incorrect: RANK() is for ties; ROW_NUMBER() is simpler and correct here.
- **D:** Incorrect: DESC gets the highest (latest) sequence_id, not the lowest.

References: <https://docs.databricks.com/aws/en/sql/language-manual/functions/row_number>

**13. Answer: D**

`parse_json()` converts string to VARIANT; `try_variant_get()` safely extracts and returns NULL on error. This is the standard and idiomatic approach. the second approach reverses modern API (try_variant_get is safer). the third approach uses older API. the fourth approach is micro-optimization (SQL engines optimize this).

- **A:** Incorrect: variant_get() throws errors; try_variant_get() is safer.
- **B:** Incorrect: get_json_object is older; VARIANT is more flexible.
- **C:** Incorrect: Calling parse_json twice is negligible; SQL optimizes this.
- **D:** Correct: parse_json + try_variant_get is the standard safe pattern.

References: <https://docs.databricks.com/aws/en/ingestion/variant> · <https://docs.databricks.com/aws/en/pyspark/reference/classes/variantval>

**14. Answer: B**

Using @expect decorators (Python API for Lakeflow) with explicit constraint descriptions allows the pipeline to record metrics for audit trails while continuing to process all rows. Adding a validation_status column flags invalid records for manual review. the first approach requires manual maintenance. the third approach would DROP the rows (not flag them). the fourth approach loses audit trail capability.

- **A:** Incorrect: Manual dual-table approach bypasses Lakeflow's built-in expectation tracking and audit trails.
- **B:** Correct: @expect decorators with validation_status column flag invalid rows while recording metrics for audit.
- **C:** Incorrect: ON VIOLATION DROP ROW removes records; doesn't flag them for manual compliance review.
- **D:** Incorrect: WHERE filtering alone loses audit trail; EXPECT provides metric recording for compliance.

References: <https://docs.databricks.com/aws/en/ldp/expectations> · <https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-expectations>

**15. Answer: D**

Join billing data to job/cluster metadata via IDs, extract the `cost_center` tag, and group by tag to attribute costs. the first approach over-relies on APIs (less maintainable). the second approach adds unnecessary cluster indirection. the third approach conflates pricing with tagging. the fourth approach is the clearest SQL-native approach.

- **A:** Incorrect: APIs are less maintainable than SQL queries on system tables.
- **B:** Incorrect: Cluster-level tagging is less reliable; use job-level tags.
- **C:** Incorrect: Pricing calculation is separate from tag attribution.
- **D:** Correct: Join billing to jobs/clusters, extract cost_center tag, group and sum.

References: <https://docs.databricks.com/aws/en/admin/system-tables/billing> · <https://docs.databricks.com/aws/en/admin/system-tables/jobs-cost>

**16. Answer: A**

Lakeflow pipelines record events in system.pipelines.event_log. Expectation failures are logged with event_type='expectation'. Querying and grouping by day provides the metric. System table lineage and Delta transaction logs are not suitable for extracting structured expectation metrics.

- **A:** Correct: system.pipelines.event_log has expectation events; group by date to track.
- **B:** Incorrect: Lineage doesn't track expectation metrics directly.
- **C:** Incorrect: SHOW METRICS is not standard; system.pipelines is the right source.
- **D:** Incorrect: Parsing Delta transaction logs is unsupported and fragile.

References: <https://docs.databricks.com/aws/en/admin/system-tables/materialization>

**17. Answer: C**

Databricks SQL alerts compare a single aggregated query result against a threshold. A query returning the count of duplicates can be monitored; if exceeding 100, the alert fires. Row-level details or multiple rows are not suitable for threshold comparison.

- **A:** Incorrect: MONITOR is for profiling, not duplicate detection.
- **B:** Incorrect: UDFs don't directly trigger SQL alerts.
- **C:** Correct: Single-value query result is compared against threshold to trigger alert.
- **D:** Incorrect: Multiple-row results are not directly comparable to thresholds.

References: <https://docs.databricks.com/aws/en/jobs/tasks/alert>

**18. Answer: C**

Deletion vectors store delete markers (bit vectors) instead of rewriting files during MERGE, avoiding small-file fragmentation and speeding up both MERGE and queries. Queries transparently apply DV filters. DVs address fragmentation indirectly by eliminating the need for constant rewriting.

- **A:** Incorrect: DVs mark deletes; they don't control which files rewrite.
- **B:** Incorrect: DVs and partitioning serve different purposes.
- **C:** Correct: DVs store markers, avoid rewrites, reduce fragmentation.
- **D:** Incorrect: DVs are not about log compression; they address file fragmentation.

References: <https://docs.databricks.com/aws/en/tables/features/deletion-vectors> · <https://docs.databricks.com/aws/en/admin/workspace-settings/deletion-vectors>

**19. Answer: B**

Liquid Clustering is designed for high-cardinality columns. It organizes data at block level without explicit partitions, avoiding 10M partition overhead. Queries on high-cardinality keys scan fewer blocks efficiently. Traditional partitioning causes unacceptable metadata overhead; hash partitioning still creates many partitions per query.

- **A:** Incorrect: 10M partitions cause unacceptable metadata overhead.
- **B:** Correct: Liquid Clustering handles high-cardinality efficiently without partition explosion.
- **C:** Incorrect: Hash partitioning doesn't scale; still many partitions per query.
- **D:** Incorrect: Liquid Clustering is designed exactly for this scenario.

References: <https://docs.databricks.com/aws/en/delta/best-practices>

**20. Answer: C**

Predictive Optimization on UC tables automatically runs optimization tasks in the background (clustering, VACUUM, ANALYZE). However, this background compute is billable through serverless SKUs. The automation eliminates manual optimization recommendations and review.

- **A:** Incorrect: Automatic (not manual review); and it's not free.
- **B:** Incorrect: Automatic is correct, but background compute is billable.
- **C:** Correct: Auto-optimization on UC tables; background compute is billable.
- **D:** Incorrect: Predictive Optimization works on UC managed tables.

References: <https://docs.databricks.com/aws/en/optimizations/predictive-optimization> · <https://docs.databricks.com/aws/en/admin/system-tables/predictive-optimization>

**21. Answer: D**

High variance in task duration (2s to 120s) combined with spilling on some tasks indicates data skew. When few join-key values appear millions of times, all rows for those keys land in one partition. Skewed tasks receive more data, run out of memory, spill. Investigating join-key distribution (nulls, hot keys) is the first diagnostic step.

- **A:** Incorrect: Memory shortage affects all tasks uniformly.
- **B:** Incorrect: Network issues would affect all tasks similarly.
- **C:** Incorrect: GC pauses don't cause consistent spilling on skewed tasks.
- **D:** Correct: Skew causes some tasks to receive more data and spill.

References: <https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/> · <https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/long-spark-stage>

**22. Answer: B**

The table_changes() function is the standard CDC access pattern. Pass the last sync version and it returns all changes (inserts, updates, deletes with _change_type) since that version. Replaying entire history is inefficient. CDF requires table_changes() syntax rather than WHERE filtering. Legacy approaches add unnecessary complexity.

- **A:** Incorrect: Version 0 replays entire history; use the last sync version.
- **B:** Correct: table_changes(table_name, version) returns incremental changes.
- **C:** Incorrect: CDF requires table_changes(), not just WHERE filtering.
- **D:** Incorrect: SQL queries can use table_changes() directly.

References: <https://docs.databricks.com/aws/en/tables/features/change-data-feed> · <https://docs.databricks.com/aws/en/oltp/projects/lakebase-cdf>

**23. Answer: D**

The correct ABAC pattern: (1) Tag columns directly via ALTER COLUMN SET TAGS (column tags don't inherit from parent tables); (2) Create mask UDF that receives column value and returns masked output; (3) Create policy with COLUMN MASK clause matching tagged columns and checking group membership. Row filters restrict records, not fields. Tags on columns (not users) determine what gets masked.

- **A:** Incorrect: Row filters restrict records; column masks restrict field-level access.
- **B:** Incorrect: ABAC doesn't auto-mask without explicit policy; masking is manual.
- **C:** Incorrect: Uses old identity attribute pattern; doesn't specify the policy structure.
- **D:** Correct: Tags applied to columns via ALTER COLUMN; UDF receives value; policy matches column tags.

References: <https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/core-concepts> · <https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/tutorial>

**24. Answer: B**

Row filter functions must return BOOLEAN. The IF statement correctly implements: if user is in Finance group, return TRUE (show all rows); else return the condition (department='Public'). Missing RETURN keyword or wrong clause structure prevents compilation. Non-standard syntax like CREATE ROW FILTER ON...WHERE is invalid.

- **A:** Incorrect: Missing RETURNS BOOLEAN and RETURN keyword.
- **B:** Correct: Function returns BOOLEAN; IF logic correct.
- **C:** Incorrect: CREATE ROW FILTER syntax is wrong; use CREATE FUNCTION.
- **D:** Incorrect: CREATE ROW FILTER ON ... WHERE syntax is not valid.

References: <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-row-filter>

**25. Answer: C**

Unity Catalog privileges don't automatically inherit. USE CATALOG allows listing but not schema access. To SELECT, user needs explicit USE SCHEMA on the specific schema AND SELECT on the table. Each privilege level requires independent authorization.

- **A:** Incorrect: USE CATALOG does not imply USE SCHEMA; no automatic inheritance.
- **B:** Incorrect: While technically true, this option is less precise than the distinction in the correct answer.
- **C:** Correct: USE SCHEMA and SELECT needed; USE CATALOG alone doesn't grant schema access.
- **D:** Incorrect: SELECT at catalog level is not inherited.

References: <https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/>

**26. Answer: A**

Comments are searchable metadata appearing in Databricks Search and catalog browser. Governed tags enable discovery filtering in the UI and are queryable in system tables. Together they improve analyst self-service discoverability without manual documentation crawling.

- **A:** Correct: Comments appear in search/browser; tags enable discovery filtering.
- **B:** Incorrect: Both provide descriptive metadata; tags enable filtering.
- **C:** Incorrect: Comments are primarily metadata, not AI-powered search.
- **D:** Incorrect: Comments don't appear in query results.

References: <https://docs.databricks.com/aws/en/comments/> · <https://docs.databricks.com/aws/en/admin/governed-tags/>

**27. Answer: C**

Databricks provides 'Repair Run' feature in Jobs UI that re-executes a failed run with the exact same parameters and config. This is the standard backfill pattern for recovering from transient failures.

- **A:** Incorrect: Resuming from failure is not typically supported.
- **B:** Incorrect: CLI syntax is job-type-specific; not the standard repair pattern.
- **C:** Correct: Repair Run re-executes with original parameters.
- **D:** Incorrect: Requires manual entry; not reproducible.

References: <https://docs.databricks.com/aws/en/jobs/run-now>

**28. Answer: A**

The databricks bundle deploy -t prod command correctly deploys using the prod target's variables. The -t flag is the valid short form for specifying the target. Environment variables and positional syntax are not standard; the full form is --target.

- **A:** Correct: -t flag specifies the target for deployment.
- **B:** Incorrect: -t is the valid short form for --target.
- **C:** Incorrect: Positional syntax is not valid for bundle deploy.
- **D:** Incorrect: Environment variable is not standard.

References: <https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes>

**29. Answer: D**

Task duration variance (2s to 120s) plus spilling on subset of tasks indicates data skew. Skewed tasks with more data run out of memory and spill. Cluster-wide memory shortage would cause uniform slowness; network issues would affect all tasks similarly.

- **A:** Incorrect: Uniform slowness suggests cluster-wide memory issues.
- **B:** Incorrect: Network issues would affect all tasks similarly.
- **C:** Incorrect: GC pauses aren't directly correlated with task skew and spill.
- **D:** Correct: Skew causes some tasks to receive more data, spill.

References: <https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/long-spark-stage>

**30. Answer: A**

Standard dimensional model: materialized views pre-aggregate fact table joins with dimensions (performance optimization). UC metric views provide governance and versioned measures on top of aggregates (semantic layer). This separates modeling concerns from governance. Denormalization, pipeline-based pre-computation, and direct querying all violate dimensional model principles or performance requirements.

- **A:** Correct: MVs for aggregates + metric view for governance.
- **B:** Incorrect: Denormalization is not dimensional modeling.
- **C:** Incorrect: Pipelines and views serve different purposes.
- **D:** Incorrect: Direct querying of large fact tables kills performance.

References: <https://docs.databricks.com/aws/en/dashboards/manage/data-modeling/local-metric-views> · <https://docs.databricks.com/aws/en/ldp/concepts/materialized-views>

**31. Answer: B**

`assertDataFrameEqual` from `pyspark.testing` is the standard unit-test function for validating DataFrame semantics (schema, rows, values) in isolation, independent of warehouse context. It runs on default compute, integrates with Pytest/CI, and is the exam-standard approach. SQL validation alone misses semantic errors. Manual inspection on production violates safety and governance. Automation is essential for CI gate protection.

- **A:** Incorrect: SQL-based row-count validation misses semantic errors in column transformations; incomplete for transformation unit tests.
- **B:** Correct: `assertDataFrameEqual` + Pytest is the testable, automatable, CI-friendly approach for transform validation on default compute.
- **C:** Incorrect: Manual inspection on production data violates safety and governance; no programmatic assertion or CI gate.
- **D:** Incorrect: Manual inspection is not scalable and provides no CI automation or regression protection.

References: <https://docs.databricks.com/aws/en/ldp/unit-testing> · <https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/transform>

**32. Answer: A**

Auto Loader with file-notification mode provides event-driven ingestion (millisecond detection), scales to 2TB+ volumes without rescanning, and eliminates polling latency. File-notification mode (SNS/SQS) is the fastest and most cost-efficient for continuous high-volume ingestion. COPY INTO and read_files require polling; manual scripts lack resilience and scalability.

- **A:** Correct: Auto Loader + file-notification mode achieves millisecond latency, auto-scales, and integrates natively with Lakeflow.
- **B:** Incorrect: COPY INTO with polling every 5 min does not meet 15-min SLA reliably at 500 GB/day (can create bottlenecks); not event-driven.
- **C:** Incorrect: read_files with batch polling every 10 min adds unnecessary latency; not optimized for large volumes.
- **D:** Incorrect: Manual script with modification-time filtering is fragile, not idempotent, and lacks fault tolerance.

References: <https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/> · <https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-detection-modes> · <https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/best-practices>

**33. Answer: B**

Pipeline run event logs provide granular per-task telemetry including `task_name`, `status`, and `error_message`. This enables pinpointing which task failed and the error type (transient vs permanent). Pipeline-level runs show only aggregate status. Driver logs require manual searching and are unstructured. Manual UI inspection is not scalable and does not enable automated alerting.

- **A:** Incorrect: Pipeline-level runs show only aggregate status; lack per-task error details.
- **B:** Correct: Pipeline run event logs provide task-level telemetry including task name, status, and error message for RCA.
- **C:** Incorrect: Driver logs are unstructured and time-consuming to search; not the intended monitoring path.
- **D:** Incorrect: Manual UI inspection is not scalable and does not enable automated alerting.

References: <https://docs.databricks.com/aws/en/ldp/concepts/spark-declarative-pipelines>

**34. Answer: A**

TRANSFORM computes net amounts, FILTER selects items > $10, and AGGREGATE sums them—all without exploding. This is the idiomatic Databricks approach for nested-array operations. MAP and REDUCE are not standard SQL higher-order functions in Databricks SQL. Exploding rows negates the benefit of arrays and adds shuffle complexity. Window functions do not operate on nested arrays; this is the wrong pattern.

- **A:** Correct: TRANSFORM → FILTER → AGGREGATE is the standard, readable chain for nested-array transformations without explosion.
- **B:** Incorrect: MAP and REDUCE are not standard SQL higher-order functions in Databricks SQL.
- **C:** Incorrect: Exploding rows negates the benefit of arrays; adds shuffle and complexity.
- **D:** Incorrect: Window functions do not operate on nested arrays; wrong pattern.

References: <https://docs.databricks.com/aws/en/semi-structured/higher-order-functions>

**35. Answer: D**

Query result cache is ideal for identical repeated queries: it caches the final result (not intermediate I/O), auto-invalidates on table changes, and is simple to enable. Delta disk cache is better for varied queries that share the same base data. Larger instances increase compute cost without addressing the root issue. For identical queries, caching the result eliminates all computation.

- **A:** Incorrect: Delta disk cache is better for varied queries; query result cache is simpler for identical queries.
- **B:** Incorrect: Dual caching adds unnecessary complexity; query result cache alone suffices for identical queries.
- **C:** Incorrect: Larger instances increase compute cost; caching the result is the right optimization.
- **D:** Correct: Query result cache is designed for identical repeated queries and auto-invalidates on changes.

References: <https://docs.databricks.com/aws/en/sql/language-manual/delta-cache>

**36. Answer: D**

Salted SHA-256 hashing is the correct balance: deterministic (preserves joins), strong hashing (resists brute force), and analysts cannot reverse without the salt. GDPR pseudonymization accepts strong hashing. Tokenization requires vault infrastructure and breaks deterministic joins. MD5 is cryptographically weak and vulnerable to rainbow tables, even with salt.

- **A:** Incorrect: Tokenization requires vault infrastructure and breaks deterministic joins needed for analytics.
- **B:** Incorrect: MD5 is cryptographically weak and vulnerable to rainbow tables, even with salt.
- **C:** Incorrect: Tokenization adds operational complexity; salted SHA-256 is sufficient for GDPR.
- **D:** Correct: Salted SHA-256 is deterministic, strong, irreversible, and meets GDPR pseudonymization requirements.

References: <https://docs.databricks.com/aws/en/security/secrets/>

**37. Answer: C**

Driver logs from the failed cluster run contain the full Spark stack trace, including which column/table caused the 'StructField' error. This provides immediate context on the schema mismatch. Audit logs track permission changes, not schema drift; pipeline task history lacks detailed error context; lineage is for dependency tracking, not debugging.

- **A:** Incorrect: Audit logs track permissions and API calls, not schema changes on tables.
- **B:** Incorrect: Pipeline task history shows failure status but not detailed Spark error stack.
- **C:** Correct: Driver logs contain the full error trace with column/table details for immediate RCA.
- **D:** Incorrect: Object lineage tracks data flow, not schema version history.

References: <https://docs.databricks.com/aws/en/compute/configure>

**38. Answer: A**

`_batch_id` column in the MERGE condition ensures that if the same batch is reprocessed, the row already exists (matching user_id, event_timestamp, and batch_id), so WHEN MATCHED updates/skips, not inserts. This ensures idempotency without external state tracking. Manual checkpoint table adds operational overhead. `idempotencyKey` is not a valid PySpark DataFrameWriter option. `IF NOT EXISTS` is not standard MERGE syntax.

- **A:** Correct: `_batch_id` in MERGE condition ensures idempotency per batch without external state.
- **B:** Incorrect: Manual checkpoint table adds operational overhead; not the built-in approach.
- **C:** Incorrect: `idempotencyKey` is not a valid PySpark DataFrameWriter option.
- **D:** Incorrect: `IF NOT EXISTS` is not standard SQL MERGE syntax; Primary Keys alone don't guarantee idempotency on restart.

References: <https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/mergeInto> · <https://docs.databricks.com/aws/en/pyspark/reference/classes/datastreamwriter/foreachBatch>

**39. Answer: B**

Ownership grant provides authority to assign permissions to others and enables self-service access management. Only the owner (or account admin with ADMIN privilege) can transfer ownership via `ALTER TABLE`. This balances governance (controlled delegation) with operational efficiency (analysts manage their domain).

- **A:** Incorrect: ADMIN is overly broad; not required for selective grant authority.
- **B:** Correct: OWNERSHIP on the table enables selective `GRANT` by the analyst; transfer requires owner or account admin.
- **C:** Incorrect: GRANT_OPTION is not standard Databricks syntax; not the right granularity.
- **D:** Incorrect: Governance can enable analysts to grant permissions without IT involvement on each request.

References: <https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/>

**40. Answer: B**

The condition `order_date >= __START_AT AND (order_date < __END_AT OR __END_AT IS NULL)` correctly handles the SCD Type 2 time window: start is inclusive, end is exclusive (NULL = current/no end date). BETWEEN is ambiguous with NULL end dates and does not handle open-ended (current) rows correctly. GROUP BY + MAX approach is inefficient and may return the wrong row if multiple versions exist in the window. Exact match on __START_AT misses orders placed after dimension change.

- **A:** Incorrect: BETWEEN is ambiguous with NULL end dates; does not handle open-ended (current) rows correctly.
- **B:** Correct: `>= __START_AT AND (< __END_AT OR IS NULL)` is the standard SCD Type 2 time-window condition.
- **C:** Incorrect: GROUP BY + MAX approach is inefficient and may return wrong row if multiple versions exist in the window.
- **D:** Incorrect: Exact match on __START_AT misses orders placed after dimension change.

References: <https://docs.databricks.com/aws/en/delta/merge>

**41. Answer: A**

Delta Lake's automatic clustering learns which columns most frequently filter queries (your 60% user_id, 25% region+date) and clusters by those. It requires only enabling the feature and adapts as patterns change. Manual configuration would lose the benefit of automatic learning. Lakeflow OPTIMIZE is distinct from automatic clustering; it's a separate compaction feature. Large tables (100B+ rows) benefit significantly from clustering when query patterns exist; there is no size limit.

- **A:** Correct: Automatic clustering learns from query patterns and adapts over time; perfect for data with clear access patterns.
- **B:** Incorrect: Automatic clustering provides learning benefits that manual configuration lacks.
- **C:** Incorrect: Automatic clustering is a table property, not a Lakeflow scheduling feature; distinct from OPTIMIZE.
- **D:** Incorrect: Automatic clustering helps large tables (100B+ rows) when query patterns exist; no size limit.

References: <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-cluster-by>

**42. Answer: C**

Iceberg REST catalog provides a centralized metadata service for external engines to read Iceberg tables from your data lake (S3/ADLS2). External engines (Presto, Trino) connect to the REST API, not Databricks. Managed Iceberg is for Databricks-internal use; external engines cannot directly query Databricks-managed storage. External engines cannot directly read Delta tables as Iceberg without a REST catalog service. Dual setup is redundant; REST catalog alone is sufficient.

- **A:** Incorrect: Managed Iceberg is for Databricks-internal use; external engines cannot directly query Databricks-managed storage.
- **B:** Incorrect: External engines cannot directly read Delta tables as Iceberg without a REST catalog service.
- **C:** Correct: Iceberg REST catalog is the standard for external-engine interoperability; they query the REST API, not Databricks.
- **D:** Incorrect: Dual setup is redundant; REST catalog alone is sufficient.

References: <https://docs.databricks.com/aws/en/delta/iceberg-reads> · <https://docs.databricks.com/aws/en/external-access/iceberg>

**43. Answer: A**

Both streams need watermarks (30 min late-arrival tolerance). The join condition constrains the match window to 10 minutes, and Spark's state manager automatically cleans up state older than watermark + constraint window. Both streams need watermarks for Spark to clean up state safely; `joinWithStateTimeout` is not a standard Spark Structured Streaming API method. Post-filter approach is inefficient and does not guide state cleanup; time range must be in join condition.

- **A:** Correct: Dual watermarks + time-range join condition prevents state explosion by limiting match window.
- **B:** Incorrect: Both streams need watermarks for Spark to clean up state safely.
- **C:** Incorrect: `joinWithStateTimeout` is not a standard Spark Structured Streaming API method.
- **D:** Incorrect: Post-filter is inefficient and does not guide state cleanup; time range must be in join condition.

References: <https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/withWatermark>

**44. Answer: D**

Jobs API (`/api/2.1/jobs/runs/list`) is the programmatic, scalable approach for monitoring. It returns structured JSON (state, error_message, run_id), integrates easily with alert services, and is more reliable than parsing CLI output. SQL query requires a running SQL Warehouse, making it less efficient for polling. CLI output parsing is brittle and not reliable for production monitoring. Workspace email notifications lack customization and integration with JIRA and Slack.

- **A:** Incorrect: SQL query requires a running SQL Warehouse; less efficient for polling.
- **B:** Incorrect: CLI output parsing is brittle; not reliable for production monitoring.
- **C:** Incorrect: Workspace email notifications lack customization and integration with JIRA/Slack.
- **D:** Correct: Jobs API is the standard, reliable approach; returns structured data for easy integration.

References: <https://docs.databricks.com/aws/en/reference/jobs-api-2-1-updates> · <https://docs.databricks.com/aws/en/reference/jobs-api-2-2-updates>

**45. Answer: B**

Manual batching (100+ rows per call) reduces the number of calls from 1M to 10K, cutting cost by 99%. Batch endpoints are designed for throughput and meet the 2-hour SLA. `ai_query` does not automatically batch; each call is a separate API invocation ($1,000 cost for 1M calls). Per-row streaming adds unnecessary latency complexity. Caching is valuable but does not address the main cost driver (1M calls).

- **A:** Incorrect: `ai_query` does not automatically batch; each call is a separate API invocation ($1,000 cost).
- **B:** Correct: Manual batching (100 rows/call) reduces calls to 10K; dramatic cost saving; batch endpoints meet 2h SLA.
- **C:** Incorrect: Per-row streaming is not cost-optimized; results arrive asynchronously, not predictably.
- **D:** Incorrect: Caching is valuable but does not address the main cost driver (1M calls).

References: <https://docs.databricks.com/aws/en/large-language-models/batch-inference-pipelines>

**46. Answer: D**

Weekly `OPTIMIZE` compacts 70k files into fewer, larger files (~100), reducing query overhead. Hourly `OPTIMIZE` is too costly; `optimizedWrite=true` helps future writes but does not fix historical small files; batch size cannot be arbitrarily increased. Weekly `OPTIMIZE` is the standard practice for streaming-write compaction.

- **A:** Incorrect: Hourly `OPTIMIZE` on 700 GB is prohibitively expensive; weekly is sufficient.
- **B:** Incorrect: `autoCompact` addresses future writes; does not fix 70k existing small files.
- **C:** Incorrect: Batch size is constrained by stream throughput; artificial increase is not feasible.
- **D:** Correct: Weekly `OPTIMIZE` is the balanced, standard practice for streaming-write small-file cleanup.

References: <https://docs.databricks.com/aws/en/tables/operations/optimize>

**47. Answer: C**

`DELETE` marks rows with deletion vectors. `REORG TABLE ... APPLY (PURGE)` physically removes marked rows from files. `VACUUM` cleans up old versions and unreferenced files. This is the GDPR-compliant, efficient approach. Incorrect approaches: using DELETE alone only marks rows without purging; using OPTIMIZE compacts but may not physically purge deleted rows; dropping and re-ingesting is wasteful and unnecessary with deletion vectors.

- **A:** Incorrect: `DELETE` alone marks rows; does not physically remove them from files.
- **B:** Incorrect: `OPTIMIZE` compacts but may not physically purge deleted rows; not sufficient for GDPR.
- **C:** Correct: `DELETE` → `REORG APPLY (PURGE)` → `VACUUM` ensures physical deletion and compliance.
- **D:** Incorrect: `DROP TABLE` and re-ingest is wasteful and unnecessary with deletion vectors.

References: <https://docs.databricks.com/aws/en/admin/workspace-settings/deletion-vectors> · <https://docs.databricks.com/aws/en/sql/language-manual/delta-reorg-table> · <https://docs.databricks.com/aws/en/tables/features/deletion-vectors>

**48. Answer: B**

Cluster by the top 2–3 query columns: `(region, date_range)` covers 60% (highest-impact queries). Adding more columns (product_category) dilutes the clustering benefit and increases memory overhead. Ad-hoc queries (10%) degrade gracefully without clustering. Over-clustering dilutes benefit; top 2–3 columns are optimal for impact. Order clustering by selectivity (cardinality reduction), not query frequency. Clustering + `OPTIMIZE` together meet 10-second SLA on large tables.

- **A:** Incorrect: Over-clustering dilutes benefit; top 2–3 columns are optimal.
- **B:** Correct: Cluster by top 60% queries (`region`, `date_range`); balance between impact and overhead.
- **C:** Incorrect: Order clustering by selectivity (cardinality reduction), not query frequency.
- **D:** Incorrect: Clustering + `OPTIMIZE` together meet 10-second SLA on large tables.

References: <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-cluster-by>

**49. Answer: B**

`union` appends rows from multiple DataFrames with the same schema, and writing once to a streaming table is idiomatic and efficient. Separate tables + view adds unnecessary indirection and is not idiomatic for fan-in. Multiple concurrent writes to one table risk conflicts and are not recommended. Separate pipelines duplicate infrastructure and defeat the purpose of fan-in.

- **A:** Incorrect: Separate tables + view adds indirection; not idiomatic for fan-in.
- **B:** Correct: `union` all sources and write once to one streaming table; clean and efficient.
- **C:** Incorrect: Multiple concurrent writes to one table risk conflicts; not recommended.
- **D:** Incorrect: Separate pipelines duplicate infrastructure; defeats the purpose of fan-in.

References: <https://docs.databricks.com/aws/en/ldp/concepts/spark-declarative-pipelines>

**50. Answer: C**

Service-principal CI/CD with DABs (GitHub Actions or similar) is the standard pattern: feature branches auto-deploy to staging, merge to main triggers production deploy with audited service-principal identity. DABs encode configuration but require explicit CI workflow; not automatic on their own. Manual approval gates for every production merge are operationally heavy. Auto-deploy on any commit risks accidental production changes and lacks governance.

- **A:** Incorrect: DABs encode configuration but require explicit CI workflow; not automatic.
- **B:** Incorrect: Manual approval gates for every production merge are operationally heavy.
- **C:** Correct: CI system + service principal + DABs = automated, audited, safe CI/CD without manual intervention.
- **D:** Incorrect: Auto-deploy on any commit risks accidental production changes; no governance.

References: <https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes> · <https://docs.databricks.com/aws/en/repos/git-folders-concepts>

**51. Answer: C**

`expect_or_fail` enforces non-negotiable constraints (email NOT NULL = functional requirement); `expect_or_drop` handles edge cases (age > 120 is rare, drop is safe); `warn` is for advisory rules. This mix balances data quality with operational resilience. `expect_or_fail` for all expectations causes excessive pipeline failures and is not operational. Silent dropping of invalid rows is a data-integrity risk and loses visibility. Processing invalid data violates data governance; audit-only is insufficient.

- **A:** Incorrect: `expect_or_fail` for all expectations causes excessive pipeline failures; not operational.
- **B:** Incorrect: Silent dropping of invalid rows is a data-integrity risk; loses visibility.
- **C:** Correct: Tiered expectations (fail for critical, drop for edge cases, warn for advisory) balance quality and resilience.
- **D:** Incorrect: Processing invalid data violates data governance; audit-only is insufficient.

References: <https://docs.databricks.com/aws/en/ldp/unit-testing>

**52. Answer: A**

`system.access.audit` is the compliance audit table: tracks who (user_id), what (resource, action), when (timestamp), and where (workspace_id). Incorrect approaches: query_history shows queries but lacks user identity and workspace context; event_log tracks infrastructure events, not detailed table-read events; jobs.runs tracks job execution, not direct user table access.

- **A:** Correct: `system.access.audit` is the standard compliance table for user data-access auditing.
- **B:** Incorrect: `system.query_history` shows queries but lacks user identity and workspace context.
- **C:** Incorrect: `system.event_log` tracks infrastructure events, not detailed table-read events.
- **D:** Incorrect: `system.admin.jobs.runs` tracks job execution, not direct user table access.

References: <https://docs.databricks.com/aws/en/admin/system-tables/audit-logs>

**53. Answer: A**

Serverless compute is the mandatory requirement for incremental refresh. The system falls back to full recompute when source schema changes, cost analysis favors full recompute, or unsupported query structures exist. Row tracking is recommended and required for certain operations but not universally mandatory. Classic compute always performs full recompute.

- **A:** Correct: Serverless is the universal requirement for incremental refresh; falls back on schema changes or cost optimization.
- **B:** Incorrect: Serverless (not SQL classic compute) is required for incremental refresh.
- **C:** Incorrect: Row tracking is recommended but not universally required; drop/recreate does not trigger fallback.
- **D:** Incorrect: Serverless compute is required, not classic cluster; data changes do not trigger automatic fallback.

References: <https://docs.databricks.com/aws/en/ldp/incremental-refresh> · <https://docs.databricks.com/aws/en/ldp/concepts/materialized-views>

**54. Answer: A**

Service principals with minimal object-level permissions is the least-privilege pattern. Create a dedicated service principal with SELECT only on the two tables, configure the job to run as that principal. This enforces hard boundaries. RBAC roles are coarser and do not enforce per-job least privilege. Views are a boundary, not least-privilege; job still has permissions on underlying tables if assigned. Schema isolation is too coarse; per-object permissions are required.

- **A:** Correct: Dedicated service principal with minimal object-level permissions is the least-privilege standard.
- **B:** Incorrect: RBAC roles are coarser; does not enforce per-job least privilege.
- **C:** Incorrect: Views are a boundary, not least-privilege; job still has permissions on underlying tables if assigned.
- **D:** Incorrect: Schema isolation is too coarse; per-object permissions are required.

References: <https://docs.databricks.com/aws/en/admin/users-groups/service-principals> · <https://docs.databricks.com/aws/en/dev-tools/auth/service-principals>

**55. Answer: A**

Lakeflow Jobs' If/else task type evaluates a condition (e.g., output of a prior task) and routes to different downstream tasks. Incorrect approaches: using a Notebook task with external job triggering requires manual orchestration; using a SQL task with polling is inefficient; claiming Lakeflow Jobs doesn't support branching is outdated (Lakeflow Jobs supports If/else tasks).

- **A:** Correct: If/else task type is the native Lakeflow Jobs feature for conditional routing.
- **B:** Incorrect: Notebook + external triggering is not the built-in Lakeflow Jobs pattern.
- **C:** Incorrect: Polling a status table is inefficient; not how Lakeflow branching works.
- **D:** Incorrect: Lakeflow Jobs supports If/else tasks; no need for external scheduling.

References: <https://docs.databricks.com/aws/en/jobs/control-flow> · <https://docs.databricks.com/aws/en/jobs/tasks/if-else>

**56. Answer: C**

Lakehouse Federation connection authenticates to Teradata with a stored identity (e.g., service account). UC permissions control analyst access to the foreign catalog/tables. Teradata-side access is checked by the connection's identity, not each analyst. Teradata-side permissions are still enforced via the connection's identity. Teradata role auto-mapping is not part of Lakehouse Federation; UC permissions are primary. Lakehouse Federation is UC-integrated; UC permissions are mandatory.

- **A:** Incorrect: Teradata-side permissions must still be enforced via the connection's identity.
- **B:** Incorrect: Teradata role auto-mapping is not part of Lakehouse Federation; UC permissions are primary.
- **C:** Correct: Connection's identity accesses Teradata; UC permissions control analyst access to foreign catalog.
- **D:** Incorrect: Lakehouse Federation is UC-integrated; UC permissions are mandatory.

References: <https://docs.databricks.com/aws/en/query-federation/foreign-catalogs>

**57. Answer: D**

Serverless default compute has a fixed, immutable environment. To use a newer numpy, specify a non-serverless environment version (e.g., single-user cluster with GPU-ML environment). Downgrading loses required features and is not sustainable. Runtime pip install fails on serverless; environment is locked. SQL-only is not a solution; Python is central to pipelines.

- **A:** Incorrect: Downgrading loses required features; not sustainable.
- **B:** Incorrect: Runtime pip install fails on serverless; environment is locked.
- **C:** Incorrect: SQL-only is not a solution; Python is central to pipelines.
- **D:** Correct: Use a specific, non-serverless environment version with required numpy; configure Lakeflow job to use it.

References: <https://docs.databricks.com/aws/en/error-messages/pipeline-environment-version-not-allowed-error-class>

**58. Answer: A**

`GROUPING SETS` explicitly lists the aggregation levels you want, executing all in one pass. Incorrect approaches: using UNION requires four separate queries and is inefficient; ROLLUP is hierarchical (computes region+product, region, global but misses product-only); CUBE generates all 2^n combinations, which is redundant and less clear than GROUPING SETS for your specific four levels.

- **A:** Correct: `GROUPING SETS` with your four exact levels is efficient and clear.
- **B:** Incorrect: Four queries are inefficient; defeats the purpose of aggregation functions.
- **C:** Incorrect: `ROLLUP` is hierarchical; generates region+product, region, global (missing product-only).
- **D:** Incorrect: `CUBE` generates all 2^2 combinations; redundant and less clear than `GROUPING SETS` for your specific levels.

References: <https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/groupingSets> · <https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/rollup>

**59. Answer: A**

`toPandas()` deserializes all filtered rows into Python memory (50 GB DataFrame). The driver's 16 GB heap cannot fit it; OOM results. Cluster logs show GC overhead or explicit `OutOfMemoryError` on driver task. Filter is distributed; all rows are not moved to driver during filter. `collect()` has the same OOM issue; both pull all rows to driver. Larger driver is a workaround; root cause is `toPandas()` itself.

- **A:** Correct: `toPandas()` pulls all 50 GB into driver heap; 50 GB > 16 GB = OOM. Logs show driver-side OOM.
- **B:** Incorrect: Filter is distributed; all rows are not moved to driver during filter.
- **C:** Incorrect: `collect()` has the same OOM issue; both pull all rows to driver.
- **D:** Incorrect: Larger driver is a workaround; root cause is `toPandas()` itself.

References: <https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/collect> · <https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/toPandas>

**60. Answer: B**

Lakeflow Declarative Pipelines are SQL-native, auto-recover from failures, scale to 500k+ events/sec with optimized connectors, and minimize ops overhead. This matches all four constraints. Structured Streaming is powerful but requires PySpark expertise; Lakeflow is the operationally friendlier choice.

- **A:** Incorrect: Structured Streaming requires PySpark expertise; violates constraint 1 (ops team is SQL-only).
- **B:** Correct: Lakeflow is SQL-friendly, auto-recovers, handles 500k events/sec, and minimizes ops overhead.
- **C:** Incorrect: Structured Streaming on classic cluster still requires PySpark; not operationally simpler.
- **D:** Incorrect: Lakeflow scales to high-volume streaming; not just batch.

References: <https://docs.databricks.com/aws/en/ldp/concepts/spark-declarative-pipelines>
