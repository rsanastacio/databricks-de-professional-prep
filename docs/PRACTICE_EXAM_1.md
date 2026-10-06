# Practice Exam 1 — Databricks Certified Data Engineer Professional (New Exam)

> **Disclaimer:** These are original practice questions, not official exam questions. Answer keys are based on Databricks official documentation; always verify in the official docs if you have doubts. **Recommended time: 120 minutes** (2 minutes per question).

---

## Questions

### 1. [Developing Code — Structured Streaming Stateful]
You are implementing a real-time event aggregator with Structured Streaming that counts events by user_id in 10-minute windows. The pipeline receives events with up to 2 hours of delay. Which combination of configurations ensures that late-arriving events are captured without duplication upon resumption?

A) Output mode `complete` with `watermark = 2 hours` and remote checkpoint  
B) Output mode `append` with `watermark = 10 minutes` and state checkpoint  
C) Output mode `update` with `watermark = 2 hours` without checkpoint  
D) Output mode `append` with `state_retention = 2 hours` and remote checkpoint

---

### 2. [Data Ingestion & Acquisition — Lakeflow Connect]
A client needs to replicate inserts, updates, and deletes from a PostgreSQL table to Databricks in near-real time while maintaining a change history by date. Which solution requires the LEAST custom code?

A) Apache Kafka + Python Delta Writer  
B) Lakeflow Connect managed connector with automatic CDC  
C) DMS (AWS) + ADLS → Lakeflow API  
D) Airflow + JDBC source + Spark batch

---

### 3. [Data Manipulation — VARIANT]
A JSON API returns a `metadata` field that can have dynamic structure (an array with varying objects or a simple string). You need to extract the first element if it's an array, or the full value if it's a string. Which function allows this without a type error?

A) `get_json_object(metadata, '$[0]')` with try-catch  
B) `from_json(metadata, 'array<string>')` after schema validation  
C) `variant_get(parse_json(metadata), ':0')` or `variant_get(parse_json(metadata), '')` depending on type  
D) `json_extract_array(metadata)[0]` with `coalesce`

---

### 4. [Monitoring and Alerting — Query Performance]
A SQL Warehouse has slow queries (P99 increasing). You suspect data freshness or ineffective caching. Which metric from Query History and Warehouse Stats do you check FIRST to rule out stale data?

A) `scan_bytes` and `bytes_spilled` from the query  
B) `cache_hit_ratio` of the warehouse + `total_rows_scanned` per table  
C) `query_start_time` vs `table_updated_at` + verify Delta Cache status  
D) `remote_IO_bytes` and `local_IO_bytes` from the Photon profile

---

### 5. [Cost & Performance Optimization — CLUSTER BY AUTO]
You have a fact table (`sales`) with 2B rows that is queried by `region` and `date_id` 80% of the time. The current job reads 50% of the table per query. Which strategy reduces cost most significantly?

A) Partitioning by `region` + `date_id`  
B) `CLUSTER BY region, date_id` (manual, without automatic reordering)  
C) `CLUSTER BY AUTO region, date_id` with Predictive Optimization  
D) Liquid clustering without Predictive Optimization

---

### 6. [Data Security and Compliance — ABAC with Governed Tags]
You need to apply row filters and column masks to 50+ tables based on user department. Doing this table by table is unsustainable. Which approach scales better?

A) Create a parameterized view by `CURRENT_USER()` for each table  
B) ABAC with managed tags (governed tags) + declarative row filters/column masks  
C) Replicate data by department across separate catalogs with GRANT per catalog  
D) UDFs that filter by `CURRENT_ROLE()` in each query

---

### 7. [Data Governance — UC Metric Views]
An analyst needs to monitor data delivery SLA: "how many tables in the catalog were updated in the last 24 hours?". Which object allows declaring this declaratively, auditing who accessed it, and being optimized by the system?

A) A SQL view that queries `information_schema.table_updates`  
B) UC Metric View that aggregates count and last_modified_time per table  
C) A Lakeview dashboard that queries UC lineage logs  
D) A Delta table that writes UPDATE timestamps via a trigger

---

### 8. [Debugging and Deploying — Git Folders]
You have a workspace that syncs SQL queries from a Git repo. You modified a query that was deployed to production. What is the risk and how to mitigate it?

A) Query will run with repo version — use Git tags + branch protection to control release  
B) Query can become orphaned if repo is deleted — version backups in /Workspace/archive  
C) Change syncs immediately to all running the query — use Declarative Automation Bundles with change review  
D) Workspace stays out-of-sync if repo changes — use read-only Git Folders and promote via pull request

---

### 9. [Data Modeling — Slowly Changing Dimension]
A customer dimension has history (new email, new address, status change). Which pattern with Databricks requires FEWER I/O operations when doing an update?

A) Simple SCD Type 1 (overwrite); retain history in backup table  
B) SCD Type 2 with `MERGE` + insert new row with `is_current` flag  
C) Automatic SCD Type 2 with `stored_as_scd_type(2)` in MERGE  
D) Immutable event log + view that materializes current snapshot in MV

---

### 10. [Developing Code — Python UDF]
A Python UDF `process_text` uses the `spacy` library that is not pre-installed on the cluster. For production use where the UDF must be reused across multiple jobs, which approach is most scalable?

A) Install `spacy` via `pip install` in each notebook before defining the UDF (only for that session)  
B) Define the UDF in a shared notebook and rely on dynamic library loading  
C) Manually manage dependencies via `spark.jars.packages` in each job configuration  
D) Use a cluster init script to install `spacy` at startup (one-time setup, automatically available to all jobs)

---

### 11. [Data Ingestion & Acquisition — Iceberg Format Target]
You are ingesting data from an S3 data lake that is queried by multiple workspaces (shared via OpenSharing). The schema changes frequently. Which target format ensures schema enforcement and shared versioning?

A) Delta with `schema_evolution = true`  
B) Iceberg with default catalog registered in the workspace  
C) Parquet with schema inferred by Glue/Hive metastore  
D) Iceberg with UC catalog and shared warehouse access

---

### 12. [Data Manipulation — Deletion Vectors]
A Delta table has 10B records. You need to delete 100M rows (1% of total). Without deletion vectors, what would be the impact on a query after the DELETE?

A) Query reads only non-deleted rows (automatic rewrite)  
B) Query reads all 10B rows and filters in the Photon layer  
C) Query reads 10B rows and evaluates deletion bitmap for each row  
D) Immediate rewrite rewrites all files in seconds

---

### 13. [Monitoring and Alerting — Predictive Optimization]
Your warehouse receives queries that vary greatly in pattern (sometimes OLTP, sometimes OLAP). Which Predictive Optimization metric indicates that you need to adjust compaction hint or liquid clustering?

A) Query latency degradation > 20% vs baseline  
B) Scan bytes vs bytes skipped ratio below 5%  
C) Liquid clustering overhead (shuffle bytes) exceeding pruning benefit  
D) Prediction of query time degradation based on historical access patterns

---

### 14. [Cost & Performance Optimization — Delta Cache]
A dimension table (`dim_product`) is queried in 95% of warehouse queries. Which configuration maximizes Delta Cache hit rate and reduces cost?

A) `spark.databricks.io.cache.enabled = true` for all clusters  
B) SQL Warehouse with cache enabled + `spark.databricks.io.cache.type = MEMORY` in warehouse settings  
C) Force table in broadcast with `BROADCAST(dim_product)` in each query  
D) Replicate dimension in MEMORY-ONLY table and reference via alias

---

### 15. [Data Security and Compliance — Data Redaction at Scale]
You need to redact PII (email, CPF) in 200+ tables in a catalog accessed by multiple groups. Which approach is more sustainable?

A) Create views with masked PII for each combination of table × group  
B) Use UC column masks declaratively + ABAC to apply in bulk  
C) Apply schema evolution with inline masking UDFs  
D) Separate ETL that creates "safe" tables without PII for external groups

---

### 16. [Debugging and Deploying — Declarative Automation Bundles]
You are migrating multiple SQL jobs from Data Factory to Databricks. Which approach reduces manual toil and enables version control + automatic retry?

A) Register jobs via Databricks REST API with Python loop  
B) Use Declarative Automation Bundles (DAB) in YAML with Git sync  
C) Copy SQL notebook to workspace and trigger via webhook  
D) Use Azure Data Factory linked service with Databricks notebook activity

---

### 17. [Debugging and Deploying — Cluster Init Script Error]
An init script installing package `xgboost` is failing silently. The cluster starts, but jobs fail with ImportError. What is the MOST likely cause and fix?

A) `pip` is not in PATH — use `/usr/bin/python3 -m pip install`  
B) Init script returns error but cluster `exit 0` anyway — add `set -e` and fail-fast  
C) XGBoost installed by user, not root — add `sudo` before pip  
D) Package incompatible with DBR version — check pypi and test locally

---

### 18. [Data Modeling — Fact Table Grain]
A fact table (`events`) has grain `[timestamp, event_type, user_id]` but receives late-arriving corrections for events from 48 hours ago with field `is_correction = true`. Which design avoids aggregation duplication?

A) Insert corrections as new rows; MV groups `is_correction = false` only  
B) MERGE with `WHEN MATCHED AND is_correction = true THEN UPDATE` based on composite key  
C) Separate fact tables: `events_live` + `events_corrections` with UNION in view  
D) Add `_dbt_valid_from` + `_dbt_valid_to` and materialize versioned snapshot

---

### 19. [Developing Code — AI Functions]
You need to classify hundreds of millions of texts (`description`) into 5 categories using an LLM. Which approach combines performance and cost?

A) `ai_query('gpt-4o', 'Categorize: {description}')` in Python application with batch API  
B) `ai_query()` UDF with `api_key` stored in UC secret, applied via Spark SQL batched  
C) API call via `requests` in Python UDF with embedding cache  
D) Fine-tuned local model in Databricks Model Registry, inference via model serving endpoint

---

### 20. [Data Ingestion & Acquisition — AUTO CDC + SCD]
You have a `customer_snapshot` table that is overwritten daily with full extract from an ERP. Which setup captures changes (insert, update, delete) automatically without complex CDC?

A) MERGE with `WHEN NOT MATCHED BY SOURCE THEN DELETE` + `stored_as_scd_type(1)`  
B) CDF (Change Data Feed) + job that compares snapshots and generates delta  
C) `AUTO` MERGE with SCD logic: `stored_as_scd_type(2)` captures insert/update/delete  
D) Lakeflow Connect auto-CDC with snapshot compare and DELETE tracking

---

### 21. [Data Manipulation — CDF + Streaming]
A Delta table has CDF enabled. You want to consume deletes via Structured Streaming `readStream` to maintain cache invalidation. Which option works?

A) `spark.readStream.format('delta').option('withChangeDataFeed', true)`  
B) `spark.readStream.format('delta').table('my_table').withColumn('_change_type')`  
C) `spark.readStream.format('delta').option('startVersion', 0).load()` with CDF enabled in catalog  
D) Option A, but requires CDF to be enabled beforehand via `ALTER TABLE ... SET TBLPROPERTIES`

---

### 22. [Monitoring and Alerting — Data Freshness SLA]
A table that should be updated every 4 hours was not updated for 6 hours. Which alert fires?

A) Lakeview dashboard with `CASE WHEN age_minutes > 240 THEN 'ALERT'`  
B) UC Metric View that monitors `table_modified_time` + Databricks Alert SQL  
C) Background refresh on Streaming Table with timeout  
D) Both A and B; B is more intelligent

---

### 23. [Cost & Performance Optimization — Liquid Clustering vs Partitioning]
A `transactions` table has 100B rows, queried by `user_id`, `date`, and `amount` in different combinations. Partitioning by `date` left many date folders empty. Which is the best alternative?

A) `CLUSTER BY user_id, date` with manual recompaction weekly  
B) Liquid clustering: `CLUSTER BY user_id, date, amount`; Predictive Optimization reorders automatically  
C) Dynamic partitioning with `PARTITION BY (user_id, date)`  
D) Hash bucketing: `BUCKETED BY (user_id) INTO 1024 BUCKETS`

---

### 24. [Data Security and Compliance — GDPR / Right to Erasure]
A GDPR erasure request is handled with `DELETE FROM customers WHERE customer_id = ?` on a Delta table that has deletion vectors enabled. What must the team do so the customer's data is physically removed, including from older table versions, without breaking concurrent readers?

A) Nothing else: the `DELETE` removes the rows from the data files immediately  
B) Wait for time travel to expire; files of old versions are deleted automatically after the retention period  
C) Set `delta.deletedFileRetentionDuration = '0 hours'`, disable the retention check, and run `VACUUM` right after the `DELETE`  
D) Run `REORG TABLE customers APPLY (PURGE)` to rewrite files that carry deletion vectors, then run `VACUUM` once the retention window has passed

---

### 25. [Developing Code — foreachBatch + State Management]
You are using `foreachBatch` in Structured Streaming to write to Delta. You need to ensure that a restarted microbatch (after failure) does not cause duplication in the table. How?

A) Use `mergeSchema = true` and rely on automatic transactions  
B) Idempotent sink with `MERGE` based on composite key + checkpoint recovery  
C) Disable retry in batch sink  
D) Register batch ID before writing, verify after

---

### 26. [Data Ingestion & Acquisition — Streaming Table Maintenance]
A Streaming Table (ST) in Unity Catalog that ingests from Kafka has a checkpoint that grew to 100 GB in 30 days. What is the cause and solution?

A) State accumulation without watermark; add watermark + state retention time  
B) Checkpoint not cleaned automatically; run `ALTER TABLE ... RESET CHECKPOINT`  
C) ST recompilation overhead; redesign query  
D) Iceberg metadata accumulates; run `VACUUM` regularly

---

### 27. [Data Manipulation — MERGE vs INSERT OVERWRITE Performance]
You need to do SCD Type 1 (overwrite customer.email) on 500M rows using 100K update records. Which is more efficient?

A) `INSERT OVERWRITE` the entire table after join with updates  
B) `MERGE INTO customer WHEN MATCHED THEN UPDATE SET email` limited to 100K affected rows  
C) DELETE updates, then INSERT new; VACUUM  
D) Batch insert with window function and row_number repartitioning

---

### 28. [Monitoring and Alerting — SQL Warehouse Autoscaling]
A SQL Warehouse has autoscaling of 2–10 clusters. During off-peak (2 clusters active), queries are slow. Which metric directly indicates insufficient cluster capacity rather than slow query execution?

A) Query execution time; if all queries take >5 seconds individually, the issue is compute  
B) Spark executor memory usage from Spark UI; high memory indicates compute bottleneck  
C) `query_queue_time` in warehouse stats; sustained queue time > 5 minutes indicates insufficient clusters  
D) Query cache hit ratio; low cache hits indicate inefficient caching

---

### 29. [Cost & Performance Optimization — Serverless SQL Warehouse]
You have a dashboard that runs 20 queries/minute during business hours. Which option reduces cost for "pay-per-query"?

A) Serverless SQL Warehouse with `spot_instances = true`  
B) Classic cluster with autoscaling disabled and 1 single-node  
C) Serverless SQL Warehouse (managed compute, no cluster management)  
D) Photon-enabled classic warehouse with `spark.databricks.photon.ml.enabled = true`

---

### 30. [Debugging and Deploying — Spark SQL Query Plan]
An `EXPLAIN PLAN` shows `BroadcastHashJoin` on a dimension of 5B rows, and queries fail with out-of-memory errors. What should you do?

A) Add `/*+ BROADCAST(dim) */` hint to force broadcast (explicitly request broadcast)  
B) Check only `spark.sql.autoBroadcastJoinThreshold`; if it is < 5GB, increase it to fit the table  
C) Reorder the join so the larger table is on the left  
D) Disable automatic broadcast via `spark.sql.autoBroadcastJoinThreshold = 0` and use SortMergeJoin with skew hints

---

### 31. [Data Security and Compliance — Clean Rooms]
You want to share data with an external partner but without exposing individual identities — only aggregations. Which Databricks solution enables this?

A) Export CSV to partner  
B) UC schema shared with row filters applying aggregation  
C) Databricks Clean Rooms: shared SQL analytics without exposing raw data  
D) Delta Share with external recipient (OpenSharing)

---

### 32. [Developing Code — Window Function + Partitioning]
A query calculates `ROW_NUMBER()` OVER `(PARTITION BY user_id ORDER BY timestamp)`. Imbalanced partitions exist: one user_id has 1B rows, others have few. What risks emerge?

A) Window function logic breaks; results are non-deterministic across imbalanced partitions  
B) A single task stalls processing 1B rows while other tasks finish, and subsequent stages wait for the straggler  
C) Performance impact is minimal; Spark parallelizes each partition independently  
D) Both straggler delay and latency increase; consider repartitioning or skew hints to redistribute load

---

### 33. [Developing Code — Schema Drift Detection]
Kafka ingestion with Structured Streaming detects new field `new_field` in JSON events. You want to fail the pipeline if any schema drift occurs. Which approach(es) will catch the new field?

A) `failOnNewColumnViolation = true` in Spark readStream  
B) Just set `mergeSchema = false` (default is already false)  
C) Pre-ingestion JSON schema validation that rejects events with unexpected fields  
D) A and C together; B alone does not prevent new fields from entering as nulls

---

### 34. [Data Manipulation — VARIANT Type Performance]
Query `WHERE variant_get(col, ':field') = 'value'` on a 1B row VARIANT column is slow. After profiling, you find Spark performs a full table scan with no predicate push-down. What is the best permanent solution?

A) Add index on the VARIANT column via `CREATE INDEX col (field)`  
B) Rewrite the query using `get_json_object(to_json(col), '$field')` instead of `variant_get()`  
C) Increase executor memory and parallelize with more partitions  
D) Extract the field as a separate normal column during ingestion; apply predicate to the extracted column

---

### 35. [Data Ingestion & Acquisition — Iceberg Table Evolution]
An Iceberg table has existing data with schema `{id: int, name: string}`. You add `created_date: timestamp` but need to ensure old rows (which have no value for this column) can still be read without schema conflicts. Which is correct?

A) Use `ALTER TABLE ... ADD COLUMN created_date TIMESTAMP` with no default (let Iceberg handle missing values)  
B) Use `ALTER TABLE ... ADD COLUMN created_date TIMESTAMP NOT NULL` to enforce consistency  
C) Use `ALTER TABLE ... ADD COLUMN created_date TIMESTAMP DEFAULT NULL` so old rows resolve to NULL  
D) Schema evolution in Iceberg does not support optional columns; must rewrite all data

---

### 36. [Developing Code — Watermark in Multi-Stream Join]
You are joining two streams (events and actions) in Structured Streaming. How to ensure late-arriving events do not cause incorrect join?

A) Add watermark to both streams with same delay (e.g., 1 hour)  
B) State timeout + recheckpoint; do not use watermark in join  
C) Output mode `append` enforces correct join  
D) Join is not possible in streams; use micro-batch MERGE instead

---

### 37. [Developing Code — Streaming Performance Degradation]
A Structured Streaming job ingesting 100K events/second from Kafka slowed to 50K/second after 3 days. Latency also increased from 10 seconds to 5 minutes. What are the likely causes and how would you diagnose?

A) Kafka topic under-partitioned; increase partition count  
B) Ineffective watermark causing state to grow unbounded  
C) Checkpoint files accumulating due to state size  
D) Both B and C cause degradation; monitor state_bytes and checkpoint size in Spark Streaming UI to confirm

---

### 38. [Cost & Performance Optimization — Compaction Strategy]
A table receives 100K inserts/day via streaming. Small files accumulate. Your goal: minimize operational overhead while maintaining performance. Which strategy works best?

A) Manual daily `OPTIMIZE TABLE` + `VACUUM` (requires scheduled jobs)  
B) Manual weekly bucketing reconfiguration to fix file size  
C) Classic partitioning by date (does not prevent small file problem)  
D) Liquid clustering + Predictive Optimization for automatic, continuous compaction (minimal overhead)

---

### 39. [Data Security and Compliance — UC Secret Management]
A job needs a password to access an external database. Where to store and retrieve it?

A) Hardcode in notebook (NEVER)  
B) UC Secrets via `dbutils.secrets.get(scope='db_scope', key='db_password')`  
C) Store in `spark.conf` via cluster environment var  
D) Both B and C work; B is more secure (audit trail)

---

### 40. [Debugging and Deploying — Notebook Parameter Injection]
A parameterized notebook receives `{{date}}` that should be 2024-01-15. Job executes with literal value `{{date}}`. What is the problem?

A) Databricks does not interpolate literals in jobs; use `dbutils.widgets` instead  
B) Job config passing wrong value; check `job_parameters` in job definition  
C) Notebook created via Git Folder; Git Folder does not interpolate parameters  
D) Both A and B; prefer `dbutils.widgets.get()` with default

---

### 41. [Data Governance — Permission Inheritance in UC]
You granted `USAGE` on a schema. Does it make a difference if you later grant `SELECT` on a specific table? (inheritance perspective)

A) Both require grant; schema USAGE is prerequisite for table access  
B) Table grant inherits automatically from schema grant; redundant  
C) Schema grant is necessary but not sufficient; table grant is also required  
D) Table grant ignores schema grant; only table level matters

---

### 42. [Data Manipulation — Time Travel + Rollback]
You accidentally executed `DELETE FROM sales` without WHERE. Last backup is 2 hours old. Which Databricks feature enables rollback?

A) `SELECT * FROM sales@0` (version 0)  
B) `RESTORE TABLE sales TO VERSION x` where x = version from 2 hours ago  
C) Delta time travel: `SELECT * FROM sales TIMESTAMP AS OF '...'` but how to restore?  
D) Run `RESTORE` + recycle bin (28 days), but may be too late if VACUUM was run

---

### 43. [Cost & Performance Optimization — Table Statistics and Query Planning]
An EXPLAIN shows `HashAgg` with high spill instead of the more efficient `SortAgg`. You suspect Catalyst made the wrong aggregation choice. What must you do first to help Catalyst choose correctly?

A) Enable `spark.sql.statistics.histogramEnabled = true` in cluster config  
B) Manually rewrite the query to use `GROUP BY ... WITH SORT AGGREGATE`  
C) Run `ANALYZE TABLE ... COMPUTE STATISTICS` to populate rowCount/totalSize stats in metastore  
D) Increase `spark.sql.shuffle.partitions` to reduce per-partition data size

---

### 44. [Data Manipulation — Aggregate Functions + Null Handling]
Query: `SELECT SUM(amount) FROM sales WHERE amount IS NOT NULL`. Alternative: `SELECT SUM(COALESCE(amount, 0)) FROM sales`. Which is correct?

A) First is correct; second sums 0s as fake values  
B) Second is correct; does not change result if no nulls  
C) Both correct if schema guarantees NO NULL; first is more readable  
D) Depends on business: first ignores nulls, second treats as 0

---

### 45. [Monitoring and Alerting — Job Failure Alerting]
A critical job running MERGE fails silently; you discover it only when business data is missing. You need to alert the team within 1 minute of failure with audit trail. What is the recommended approach?

A) Configure job-level notifications to Slack; fires immediately upon failure  
B) Databricks Alerts feature querying `system.jobs.runs` and filtering `state = FAILED`; integrates with alerts infrastructure  
C) Custom Python script polling `system.jobs.runs` every minute and sending manual alerts  
D) Combine job notifications (A) with Databricks Alerts (B) for redundant alerting with full audit trail

---

### 46. [Cost & Performance Optimization — Column Mask Performance Impact]
You need to mask SSN in 500+ queries. You want to avoid both: (1) runtime masking overhead on every query, and (2) duplication of data. What is the best strategy?

A) Pre-compute `AES_ENCRYPT()` at ingress to create encrypted column alongside raw data  
B) Apply UC column masks directly; Databricks handles with negligible overhead if predicate push-down works  
C) Create materialized view pre-masked for all sensitive columns; users query the MV instead  
D) Use UC column masks (B) for most queries plus materialized views (C) for critical high-frequency queries to avoid repeated masking cost

---

### 47. [Developing Code — Job Cluster vs Attached Cluster]
A production job runs on an all-purpose cluster that also hosts interactive analyst notebooks. The job's runtime is unpredictable and it occasionally fails when analysts restart the cluster. What is the most appropriate change?

A) Keep the shared cluster; Databricks isolates scheduled jobs from notebooks automatically  
B) Raise the shared cluster's maximum autoscaling workers so both workloads have enough capacity  
C) Ask analysts not to restart the cluster during the job's schedule window  
D) Run the job on its own job compute (a job cluster or serverless jobs compute) so it gets isolated resources and a lifecycle independent of interactive users

---

### 48. [Developing Code — PySpark vs Pandas Performance]
An operation on 100M rows: standard PySpark UDF takes 30 seconds; rewriting it as a Pandas UDF takes 5 seconds (6x faster). What is the root cause of the dramatic improvement?

A) PySpark serializes data to Python pickle; Pandas UDF uses Arrow columnar format (no JVM overhead)  
B) PySpark only runs on one executor; Pandas UDF automatically parallelizes  
C) Pandas UDF avoids Python execution entirely; runs compiled machine code  
D) Combination: Pandas UDF reduces serialization overhead (A) AND Catalyst optimizes it as columnar operation (B)

---

### 49. [Developing Code — SQL Dynamic Query Generation]
A report needs to group by a user-chosen column. You receive `group_by = user_input`. Which approach prevents SQL injection?

A) Direct string concatenation: `f"SELECT {group_by}, SUM(amount) FROM sales GROUP BY {group_by}"`  
B) Use parameterized binding: `spark.sql(f"SELECT ? AS col, SUM(amount) FROM sales GROUP BY ?", [user_input, user_input])`  
C) Validate user_input against a hardcoded allowlist of permitted column names; throw error if not in list  
D) Combination: validate against allowlist (C) and use parameterized queries (B) for defense in depth

---

### 50. [Data Ingestion & Acquisition — Streaming Ingestion Performance]
A stream of 1M events/second is ingested into Delta via Structured Streaming. Latency is increasing. What is the MOST likely cause?

A) Spark executor memory overflow  
B) State accumulation without watermark; checkpointing grows indefinitely  
C) Partition count low; data accumulates in partitions  
D) Delta schema evolution on each micro-batch

---

### 51. [Cost & Performance Optimization — Index-free Pruning via Iceberg]
A 500B event record table is frequently queried by high-cardinality date ranges (e.g., "last 6 hours", "yesterday"). Which approach optimizes pruning without building explicit B-tree indexes?

A) Partition by month: `PARTITION BY MONTH(event_timestamp)` (creates one folder per month)  
B) Rely on Iceberg's manifest statistics: each file stores min/max event_timestamp; Iceberg prunes files with non-matching ranges  
C) Liquid clustering via `CLUSTER BY event_timestamp` only; do not use Predictive Optimization  
D) Combine fixed partitioning (A) for coarse filtering with Iceberg manifest statistics (B) for fine-grained pruning (most efficient)

---

### 52. [Developing Code — Encryption Key Management in Spark]
A Spark job needs to retrieve an encryption key. Which approach provides both security and audit logging?

A) Hardcode the key in the job source code  
B) Pass the key via cluster environment variable; rotate manually  
C) Embed the key in the `spark.conf` configuration (user-visible in job UI)  
D) Retrieve via `dbutils.secrets.get(scope, key)` from UC Secrets (audit trail + no logging exposure)

---

### 53. [Developing Code — Type Casting Errors]
During ingestion, `CAST(date_string AS DATE)` encounters 1 invalid value in 1B rows and fails the entire job. You want to handle the error gracefully and continue processing. What is the best approach?

A) Replace with `TRY_CAST(date_string AS DATE)`, which returns NULL for invalid values and allows the job to continue  
B) Add a CASE statement with regex pre-validation: `CASE WHEN date_string MATCHES regex THEN CAST(...) ELSE NULL END`  
C) Use `CAST(...FORMAT 'yyyy-MM-dd')` to be strict about format and fail fast  
D) Skip invalid rows via `WHERE REGEXP_LIKE(date_string, '^\\d{4}-\\d{2}-\\d{2}$')`

---

### 54. [Data Ingestion & Acquisition — Multiformat Ingestion with Lakeflow]
You ingest Parquet, JSON, and CSV from S3 into Delta. Schema evolves with each format. What is the most robust solution?

A) Spark auto schema inference with `mergeSchema = true` (requires manual job per format)  
B) Unity Catalog table specifications (does not auto-detect format differences)  
C) Lakeflow Connect with automatic per-format schema detection and compatibility checks  
D) A and C combined: Spark baseline plus Lakeflow for format-specific handling (most robust)

---

### 55. [Monitoring and Alerting — Predictive Optimization Metrics]
Selective queries on a large Delta table are slow. Which signal in the query profile best indicates that the table's data layout should be clustered (for example with `CLUSTER BY AUTO`)?

A) Shuffle write bytes are high in a join stage  
B) Almost all files and bytes are read for a selective filter, with very few pruned  
C) An "optimizer recommendation score" below 80%  
D) A high number of active executors in the Executors tab

---

### 56. [Debugging and Deploying — Incremental Pipeline Issues]
A daily incremental job: `SELECT * FROM src WHERE updated_at > '{{yesterday}}'` runs daily, picking only yesterday's updates. One day, a data quality fix retroactively updated records from 30 days ago with updated_at = today. What happens to the pipeline and how to fix?

A) Idempotent: the job picks the 30 days and merges cleanly with existing data (no issue)  
B) Breaking: the job now picks 30 days of data today; aggregate metrics are duplicated/wrong (late-arriving updates break simple incremental)  
C) Job skips early; triggers alert (fails fast due to data anomaly)  
D) Late-arriving updates break simple timestamp incremental (B); use idempotent MERGE with watermark to handle late updates correctly

---

### 57. [Data Modeling — Conformed Dimensions]
Multiple fact tables inconsistently reference `customer` (customer_id vs customer_pk, different attributes). You want to consolidate without breaking history or existing queries. Which approach works?

A) Create new conformed `dim_customer`; both old and new fact tables reference it (parallel operation during transition)  
B) Modify existing fact table schemas directly with `ALTER TABLE ... DROP/ADD` columns  
C) Recreate fact tables with new dimension; archive old versions (clean but risky cutover)  
D) Both A and C work together: create new dimension, gradually migrate each fact table (A), eventually recreate final ones (C)

---

### 58. [Developing Code — Broadcast Join Optimization]
A query joins `sales` (100B rows) and `dim_product` (10K rows). Spark chooses BroadcastNestedLoopJoin (slow). How to force BroadcastHashJoin?

A) `/*+ BROADCAST(dim_product) */` hint  
B) Increase `spark.sql.autoBroadcastJoinThreshold` to > 10K MB  
C) Reorder join: `SELECT * FROM dim_product JOIN sales`  
D) Both A and B; A is more direct

---

### 59. [Cost & Performance Optimization — Predictive Optimization Fine-Tuning]
A table with Predictive Optimization enabled runs automatic reordering every 24h, consuming 2h of compute. It is excessive. Which option reduces frequency?

A) Disable Predictive Optimization completely  
B) Adjust `OPTIMIZE` frequency via `CREATE TABLE ... TBLPROPERTIES (...)`  
C) Configure `spark.databricks.predictiveOptimization.maxFrequency = weekly`  
D) Both B and C; C is more effective

---

### 60. [Data Governance — UC Asset Inventory]
An internal audit requires a comprehensive, queryable list of all tables in the catalog with: owner, last modification timestamp, and data classification tags. Which source provides this as queryable records (not UI)?

A) Query Unity Catalog system table `system.information_schema.tables` joined with UC tags; export to audit report  
B) Glean search API to list tables and retrieve metadata  
C) Databricks metadata REST API (beta) without tag information  
D) Combination of multiple sources (A + metadata API) because no single source has all fields

---

---

## Answer Key and Explanations

**1. Answer: A**
Watermark of 2 hours allows late-arriving events up to 2h. Remote checkpoint ensures resumption without duplication (output mode `complete` is correct for aggregations). Mode `append` with 10 minutes watermark would lose events outside the window; `update` without checkpoint loses state on crash.

**2. Answer: B**
Lakeflow Connect managed connector for PostgreSQL CDC is officially supported and reduces code vs Kafka + custom writer. DMS and Airflow require more configuration; both are valid but B has "less code".

**3. Answer: C**
`variant_get(parse_json(...), ':0')` and variants handle VARIANT without pre-existing type error. `from_json` would force fixed schema (throw error on mismatch). `json_extract_array` fails if not an array.

**4. Answer: C**
`query_start_time` vs `table_updated_at` reveals whether data is stale. Delta Cache status shows hit rate. Both metrics are critical; C is most practical for ruling out stale data issue.

**5. Answer: C**
`CLUSTER BY AUTO` with Predictive Optimization reorders automatically based on access patterns, reducing cost more than fixed partitioning (which does not scale to dynamic query patterns).

**6. Answer: B**
ABAC with governed tags enables row filters and column masks declaratively in bulk. Parameterized views do not scale well (50+ tables × multiple groups = combinatorial explosion).

**7. Answer: B**
UC Metric Views are managed SQL objects that aggregate metadata and can audit access. Information schema is read-only; dashboards are passive; Delta has no built-in triggers.

**8. Answer: D**
Git Folders sync workspace with repo. Changes in repo sync to workspace. Risk: accidental changes propagate. Mitigation: read-only Git Folder + promote via PR + branch protection.

**9. Answer: C**
`stored_as_scd_type(2)` in MERGE automates SCD Type 2, reducing I/O vs manual operations + compaction.

**10. Answer: D**
Init script (D) is most scalable: one-time installation at cluster startup, automatically reused across all jobs. Notebook-level pip (A) is session-only; manual dependency management (C) requires per-job configuration.

**11. Answer: D**
Iceberg with UC catalog ensures schema enforcement, shared versioning, and OpenSharing integration. Delta + OpenSharing works, but Iceberg is more robust for multi-workspace.

**12. Answer: C**
Without deletion vectors, Spark maintains a bitmap of deleted rows and evaluates each of the 10B rows (reading all 10B lines with filter bitmap). With DV, skips deleted files physically.

**13. Answer: D**
Predictive Optimization predicts query latency degradation based on historical access patterns, indicating when reordering/compaction is needed.

**14. Answer: B**
SQL Warehouse with cache enabled + `MEMORY` type maximizes hit rate. Broadcast is for specific joins, not general dimension caching.

**15. Answer: B**
UC column masks declaratively + ABAC scale better than manual views. Views = N tables × M groups.

**16. Answer: B**
Declarative Automation Bundles (DAB) in YAML + Git sync is modern standard for version control + automatic deployment.

**17. Answer: B**
Init script needs `set -e` to fail-fast. Without it, cluster starts even with error.

**18. Answer: B**
MERGE with composite key and `is_correction` flag prevents duplication. Inserting as new row does not aggregate easily.

**19. Answer: B**
`ai_query()` UDF with secret + batch processing scales. Direct GPT-4 would cost more; local model is alternative but LLM is more flexible.

**20. Answer: C**
`AUTO` MERGE with `stored_as_scd_type` automates CDC without external tools.

**21. Answer: D**
CDF must be enabled via `ALTER TABLE ... SET TBLPROPERTIES`. Then `readStream` with CDF option works.

**22. Answer: B**
UC Metric Views + Alert SQL is native Databricks form. Dashboard is passive; B is more intelligent.

**23. Answer: B**
Liquid clustering + Predictive Optimization scales better than fixed partitioning for varied queries.

**24. Answer: D**
With deletion vectors, `DELETE` only marks rows as deleted; the bytes stay in the existing files. `REORG TABLE ... APPLY (PURGE)` rewrites those files without the deleted rows, and `VACUUM` (after the retention window) removes the old files that older versions still reference. Forcing a zero-hour retention is unsafe for concurrent readers and long-running queries.

**25. Answer: B**
Idempotent sink with MERGE on composite key + checkpoint recovery prevents duplication on retry.

**26. Answer: A**
Watermark + state retention time control checkpoint growth.

**27. Answer: B**
MERGE limited to 100K updates is more efficient than rewriting 500M rows.

**28. Answer: C**
Queue time directly indicates cluster capacity: sustained queueing > 5 minutes proves insufficient clusters. Execution time (A) does not distinguish between queue time and actual compute. Memory (B) and cache (D) are compute/efficiency metrics, not capacity indicators.

**29. Answer: C**
Serverless SQL Warehouse is "pay-per-query" with automatically managed compute. Spot instances do not exist in serverless.

**30. Answer: D**
A 5B row table cannot be broadcast. The fix is SortMergeJoin (set `autoBroadcastJoinThreshold = 0` to disable broadcast) combined with skew hints to handle data skew that may occur in sort-merge joins. Join reordering (A) and increasing memory (C) do not solve the fundamental issue of table size.

**31. Answer: C**
Clean Rooms enable shared SQL analytics without exposing raw data.

**32. Answer: D**
Imbalanced partitions cause both straggler delay (B—one task processing 1B rows blocks stage completion) and overall latency increase (D). Repartitioning or skew hints redistribute data to even out partition sizes and improve parallelism.

**33. Answer: D**
Option A (`failOnNewColumnViolation`) and Option C (external schema validation) both work for catching schema drift. Option B (`mergeSchema = false`) is already the default but does not prevent new fields—they are added as null columns (does not fail the pipeline).

**34. Answer: D**
VARIANT does not support indexes (A) or efficient predicate push-down (B). Query rewrites (C) provide no permanent benefit. The best solution is to extract the field as a separate normal column at ingestion (D), enabling efficient filtering on the extracted column.

**35. Answer: C**
Option C uses DEFAULT NULL, allowing old rows to safely read as NULL for the new column. Option B (NOT NULL without default) fails because old rows lack a value for a NOT NULL column. Option A without explicit default can cause type conflicts on reads.

**36. Answer: A**
Watermark on both streams with same delay ensures correct join.

**37. Answer: D**
State explosion without watermark (B) and checkpoint growth (C) together cause the observed throughput drop and latency increase after 3 days. Monitoring state_bytes and checkpoint size in Spark UI confirms the diagnosis. Kafka rebalancing (A) would show immediate impact, not gradual 3-day degradation.

**38. Answer: D**
Liquid clustering + Predictive Optimization provides automatic, hands-off compaction without requiring manual OPTIMIZE jobs or static bucketing configuration. Manual OPTIMIZE (A) requires operational overhead; bucketing (B) is static and inflexible.

**39. Answer: B**
UC Secrets with audit trail is more secure than env vars.

**40. Answer: B**
Job config must pass parameter correctly. Parameterized notebook uses `dbutils.widgets.get()` to receive value.

**41. Answer: C**
Schema USAGE is necessary prerequisite; table grant also required (does not inherit automatically).

**42. Answer: B**
`RESTORE TABLE ... TO VERSION x` or `SELECT ... TIMESTAMP AS OF` for time-travel. RESTORE writes new version; time-travel is read-only.

**43. Answer: C**
Catalyst uses table statistics (rowCount, totalSize) to decide between HashAgg and SortAgg. Running `ANALYZE TABLE ... COMPUTE STATISTICS` populates these stats in the metastore. Without stats, Catalyst uses heuristics which may be wrong.

**44. Answer: D**
First ignores nulls; second treats as 0. Business decides; first is more typical.

**45. Answer: D**
Combining job notifications (A) for immediate alert with Databricks Alerts (B) for managed, audited alerting provides both rapid notification and compliance audit trail. Job notifications alone may be unreliable; custom polling (C) is not recommended.

**46. Answer: D**
UC column masks (B) provide centralized, auditable masking for most queries with negligible overhead. Materialized views (C) add pre-masked copies for critical high-frequency queries to eliminate repeated masking cost. Combining both optimizes both coverage and performance.

**47. Answer: D**
A shared all-purpose cluster exposes the job to resource contention, cluster restarts and shared Spark state from interactive users. Dedicated job compute starts for the run, has its own resources and terminates afterwards, which also makes it cheaper than keeping an all-purpose cluster up for scheduled work.

**48. Answer: D**
Pandas UDF's 6x speedup comes from two factors: (A) Arrow columnar format eliminates JVM-to-Python row serialization overhead, and (B) Catalyst planner treats it as a columnar operation, enabling vectorized execution and optimization. Both are necessary for the performance gain.

**49. Answer: D**
Defense in depth: validate user input against a hardcoded allowlist (C) and use parameterized queries (B). Allowlist ensures only known columns are allowed; parameterized queries provide additional protection. Direct concatenation (A) is vulnerable to SQL injection.

**50. Answer: B**
Without watermark, state grows indefinitely; checkpoints grow with state.

**51. Answer: D**
Combining fixed partitioning (A) for coarse directory pruning with Iceberg manifest statistics (B) for file-level pruning provides the most efficient approach. Manifest statistics prune individual Parquet files based on min/max; fixed partitioning is coarser but reduces directory scans.

**52. Answer: D**
UC Secrets via `dbutils.secrets.get()` provides both security (never logged in Spark UI or configs) and audit trail. Hardcoding (A), environment vars (B), and spark.conf (C) all expose secrets in logs, UI, or configs.

**53. Answer: A**
`TRY_CAST` returns NULL for invalid values and allows the job to continue without error. Pre-validation (B, D) adds complexity; strict format checking (C) fails fast rather than handling gracefully.

**54. Answer: D**
Combining Spark auto-schema (A) with Lakeflow Connect (C) provides the most robust multi-format ingestion: Spark handles basic schema inference while Lakeflow detects format-specific issues and validates compatibility across Parquet, JSON, and CSV.

**55. Answer: B**
If a selective filter still reads nearly every file, data skipping is not working for that column, which is exactly what clustering on the filtered columns (or letting `CLUSTER BY AUTO` choose them) fixes. Shuffle volume and executor count describe joins and compute sizing, not data layout.

**56. Answer: D**
Simple timestamp-based incremental breaks when historical data is retroactively updated (scenario B). MERGE with idempotent logic and watermark handles late updates by matching on composite key and updating existing records instead of duplicating.

**57. Answer: D**
Combining approach A (create conformed dimension + gradual fact table migration) with approach C (eventual fact table recreation) provides low-risk consolidation. Fact tables can reference the new dimension incrementally while old schemas remain intact, then eventually clean up via recreation.

**58. Answer: A**
`/*+ BROADCAST(dim_product) */` hint forces BroadcastHashJoin; B auto-increases threshold.

**59. Answer: C**
Configure `spark.databricks.predictiveOptimization.maxFrequency` to reduce excessive recompaction.

**60. Answer: A**
Querying `system.information_schema.tables` directly provides table names, owners, and timestamps in a queryable SQL table. UC tags are joinable for classification. This is the native, audit-ready source. Glean (B) is search-based, not queryable; metadata API (C) lacks tag information.
