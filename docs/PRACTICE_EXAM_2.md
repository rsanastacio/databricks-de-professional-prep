# Practice Exam 2 — Databricks Certified Data Engineer Professional (New Exam)

> **Original practice questions, not official exam questions. Answer keys are based on Databricks documentation; always verify in the official docs. Time yourself: 120 minutes. Level: intermediate-advanced.**

---

## Questions

### 1. [Developing Code — Python/SQL]
A Structured Streaming pipeline receives data from Kafka in 30-second batches. Processing performs an aggregation with 5-minute windows on `event_timestamp`. Which configuration ensures that late-arriving events (arriving up to 10 minutes after the window closes) are reprocessed in the correct window?

A) Increase `spark.sql.streaming.forceDeleteTempCheckpointLocation` and set `outputMode="complete"`  
B) Configure `watermark("event_timestamp", "10 minutes")` before aggregation and use `outputMode="append"` with CDF enabled  
C) Set `chkpointLocation` with `update` mode and `microBatchMs=10000`  
D) Use `.option("mergeSchema", "true")` and replicate the query across 10 executors  

---

### 2. [Data Ingestion & Acquisition — PostgreSQL Logical Replication]
Your client has a PostgreSQL database with ~10GB of data. You want to sync changes (inserts/updates/deletes) in near real-time via Lakeflow Connect. What is the main database prerequisite (besides credentials)?

A) Physical replication enabled and all tables with `replica identity full`  
B) Logical Decoding activated, WAL level set to `logical`, and a permanent replication slot  
C) Audit trigger on each table plus external transaction log  
D) Foreign Data Wrapper (FDW) plus `postgres_fdw` extension  

---

### 3. [Data Manipulation — SQL]
You have a Delta table with column `data: VARIANT` containing JSON with variable structure. You need to extract the field `user.email` present in ~80% of records; in the other 20%, the field may not exist. Which is the most efficient and safe approach?

A) `SELECT get_json_object(data, '$.user.email') AS email FROM table` with NULL handling  
B) `SELECT data['user']['email'] AS email FROM table` followed by `WHERE email IS NOT NULL`  
C) `SELECT variant_get(data, 'user.email', 'string') AS email FROM table`  
D) Convert VARIANT to string via `to_json()`, then use regex  

---

### 4. [Monitoring and Alerting — Streaming Latency]
Your Structured Streaming pipeline processes 1M events/second. Checkpoint indicated 50ms end-to-end latency, but it suddenly jumped to 2 seconds. Where would you look FIRST to diagnose?

A) Spark driver logs on the cluster  
B) Throughput metrics and batch duration in Spark UI (streaming tab) plus verify event ingestion rate  
C) Increase `spark.sql.shuffle.partitions` and enable `spark.streaming.backpressure.enabled`  
D) Check disk I/O of checkpoint storage and Kafka broker status  

---

### 5. [Cost & Performance Optimization]
A 500GB Delta table receives frequent small writes (10–50 rows per write). Reads are slow. Which combination best reduces cost and improves performance?

A) Enable deletion vectors, CLUSTER BY auto, and Delta cache  
B) Only run `OPTIMIZE` daily plus increase worker nodes  
C) Disable multiversion concurrency control (MVCC) and partition by date  
D) Convert to native Parquet (disable Delta) and use S3 Select  

---

### 6. [Data Security and Compliance — Central Masking Policy]
Your client wants a central masking policy that automatically applies to any column tagged with governed tag `pii` — including tables created in the future by different users. What is the native Databricks solution?

A) Create a masking stored procedure and associate via `ALTER TABLE ... SET CLUSTER BY`  
B) ABAC (Attribute-Based Access Control) policies in Unified Catalog with governed tags  
C) Row and column security with masked views plus nightly audit job  
D) Create a masking model in AI functions and apply as `CHECK` constraint  

---

### 7. [Data Modeling — Centralized Metrics]
An organization has 150 tables in UC with different data quality levels. It wants to create a central metric "data_quality_score" that appears in governance and is reusable by multiple dashboards. Which approach is recommended?

A) UC Metric View (centralized definition) plus Materialized View (pre-aggregation)  
B) Delta Live Tables with `@quality_expectation` plus Dashboard reading the expectations table  
C) Compute the metric in a Python job nightly and write to a table, then use `CREATE VIEW`  
D) Use `GET_METRIC` via SQL Warehouse in each dashboard  

---

### 8. [Developing Code — Python/SQL]
An iterative development uses a notebook with SQL query that filters data by date using `WHERE date > CURRENT_DATE() - 7`. After converting to a scheduled daily job, the 7-day rolling window does not work correctly in some runs. What is the best practice?

A) Use `WHERE date > cast(current_timestamp() as date) - interval 7 days` and add retry logic  
B) Pass the date as a task parameter via `spark.conf` in ISO 8601 format  
C) Use Databricks Workflows with parametrization (`{{task.run_id}}`) and run with `dbutils.notebook.run()`  
D) Store the last run checkpoint in UC and read the date from checkpoint on the next run  

---

### 9. [Data Ingestion & Acquisition — REST API]
You receive data from a REST API that returns paginated JSON. The endpoint supports range queries by timestamp. Which ingestion strategy is most robust for ingesting ~100M records with 3 years of history?

A) Call the API hour by hour (parallel) using `parallel_requests`, save raw to Delta, then process  
B) Ingest everything in 1 call (1 giant JSON), store in `BLOB` column, then parse  
C) Use Lakeflow Connect configured for CDC (if supported) or external Apache NiFi  
D) Ingest by day (parallel) in Workflows tasks, save with `mergeSchema=true`  

---

### 10. [Data Manipulation — Incremental Sync]
You have two tables: `orders` (1M rows, updated daily) and `shipments` (500K rows, updated in real-time via CDF). You want to keep a fact table `fact_order_shipment` synchronized combining both. Which strategy is most efficient?

A) Merge incrementally via CDF, using `MERGE` with CDC subqueries  
B) Run `DELETE FROM fact_order_shipment` plus `INSERT` full join (daily batch)  
C) Create a Materialized View `AS SELECT ... FROM orders FULL OUTER JOIN shipments` and use `REFRESH`  
D) Use Structured Streaming to consume `shipments` CDF plus join with `orders` snapshot  

---

### 11. [Cost & Performance Optimization — Warehouse Sizing]
A SQL Warehouse has 100 workers, costs $500/hour when idle. Query log analysis shows 30% are ad-hoc queries (< 1 min each) and 70% are dashboards with predictable access patterns. Which optimization reduces cost while maintaining SLA?

A) Migrate ad-hoc queries to serverless compute plus keep dashboards on dedicated warehouse  
B) Use Delta cache for all queries plus reduce worker count to 50  
C) Partition all tables by date plus enable `CLUSTER BY` auto  
D) Convert slow queries to Materialized Views plus disable cache  

---

### 12. [Developing Code — Python]
In a PySpark job, you want to process VARIANT columns containing nested arrays. Which approach ensures better performance in complex transformations?

A) `df.selectExpr("explode_outer(variant_col) as item")` then iterate in Python RDD  
B) Use `sql("SELECT ... FROM delta.`path` WHERE ...")` plus native SQL to manipulate VARIANT  
C) Convert VARIANT to JSON string in Python, parse with `json.loads()`, then reassemble  
D) Use `pyspark.sql.functions.col()` with `.getItem()` chaining in SQL expressions  

---

### 13. [Monitoring and Alerting — Materialized View Staleness]
A critical dashboard is fed by a Materialized View. Suddenly, data becomes stale (hours outdated). What is the fastest way to diagnose whether the problem is automatic refresh or refresh cost?

A) Check `system.views.materialized_views` metadata table plus refresh job logs  
B) Query Delta Lake statistics (`DESCRIBE DETAIL`) and verify last modification timestamp  
C) Run `SHOW TBLPROPERTIES` on the Materialized View and search for `last_refresh_time`  
D) Review billable cluster costs over the last 3 days plus connect to Predictive Optimization  

---

### 14. [Data Governance — Cross-Workspace UC Sharing]
You work with UC across 5 workspaces. A project requires multiple workspaces to read the same table with different permissions per workspace. What is the correct approach using UC?

A) Replicate the table in each workspace with workspace-specific ABAC policies  
B) Use Open Sharing or D2D (Data to Data) to share the table with permission granularity per workspace  
C) Create delegated views in each workspace calling UDF for workspace_id validation  
D) Use `ALTER TABLE ... OWNER TO` to transfer permission plus recreate the table in each workspace  

---

### 15. [Debugging and Deploying — Deserialization Error]
A scheduled job set to run daily at 8 AM begins failing after 2 weeks of operation. Logs show `SparkException: Task deserialization error`. What is the most likely cause and fix?

A) Old JAR version cache — clear cluster cache and reimport libraries  
B) Dependency mismatch or Python class change — review if lib was upgraded plus increase `spark.driver.maxResultSize`  
C) Corrupted checkpoint file — remove checkpoint and restart  
D) DB connection timeout — increase `spark.sql.connect.timeout`  

---

### 16. [Data Ingestion & Acquisition — Lakebridge Teradata LDAP]
You ingest data from a legacy data warehouse (Teradata) via Lakebridge. The connection uses LDAP. Which configuration is necessary ON THE TERADATA SIDE for Lakebridge to work?

A) Only enable remote access plus create user with `GRANT CONNECT` privilege  
B) Enable LDAP LogMech on Teradata, open ODBC port, and ensure LDAP user has permission  
C) Create a Foreign Data Wrapper (FDW) on Teradata plus open port 1025  
D) Configure Teradata viewpoints with `GRANT SELECT` for Databricks users  

---

### 17. [Developing Code — SQL]
You want to run a series of data quality tests in SQL during ingestion via DLT. What is the best way to express "if > 5% of records have `price < 0`, fail the pipeline"?

A) Use `@quality_expectation` or `EXPECT` statement in DLT with action `fail`  
B) `IF (SELECT COUNT(*) FROM data WHERE price < 0) > (SELECT COUNT(*) * 0.05 FROM data) THEN RAISE`  
C) Create a stored procedure running `SELECT COUNT(*) ... FILTER (price < 0)` and call `raise_error()`  
D) Filter records with `WHERE price >= 0` plus log discarded count  

---

### 18. [Cost & Performance Optimization — Iceberg Optimization]
An Iceberg table grows 50GB/day. Performance of `SELECT * WHERE date > CURRENT_DATE()` is degrading. Which optimization is recommended IN ICEBERG?

A) Enable `CLUSTER BY` auto on Iceberg plus compact old snapshots  
B) Run daily `OPTIMIZE` plus enable metadata caching with Z-order  
C) Partition by `date` plus keep only last 90 days via `EXPIRE_SNAPSHOTS`  
D) Convert to Delta and use deletion vectors  

---

### 19. [Data Modeling — SCD Type 2 Maintenance]
You have an SCD Type 2 table where each update creates a new row with `effective_date` and `end_date`. Your job updates records in batch daily. Which SQL is most efficient for maintaining SCD Type 2?

A) Use `MERGE` with `WHEN MATCHED ... UPDATE ... SET end_date = CURRENT_DATE()` and `WHEN NOT MATCHED ... INSERT`  
B) `DELETE ... WHERE status = 'active'` plus `INSERT` with `INSERT OVERWRITE TABLE` scd_table  
C) Use AUTO CDC with configuration `stored_as_scd_type = 2` in DLT  
D) Create a Materialized View calculating `max(effective_date)` per key plus outer join  

---

### 20. [Monitoring and Alerting — Kafka Back-pressure]
A streaming job consuming from Kafka is experiencing back-pressure. Which metric in Spark UI would you check to confirm if Kafka is experiencing growing lag?

A) Input rate vs. Processing rate on the Streaming tab graph  
B) Executor memory usage plus GC time in the stage explorer  
C) Databricks Job Runs log with `kafka_consumer_lag` metric  
D) Task duration breakdown in the SQL tab  

---

### 21. [Data Security and Compliance — Column Masking]
You want a sensitive column (ssn) to be ALWAYS masked ONLY for non-admin users on a table. Masking cannot be bypassed via direct SQL. What is the solution?

A) Create a view with `CASE WHEN is_admin() THEN ssn ELSE NULL END` plus revoke access to the base table  
B) Use Row and Column Security (RCS) with a masking policy per governed tag  
C) Use Dynamic Data Masking (DDM) with SQL rule plus ensure only admin can create SQL UDF  
D) Replicate the table with blank ssn column, keep original in private schema plus use access role  

---

### 22. [Developing Code — Python/SQL]
Your notebook does `spark.read.parquet("s3://bucket/path")` each time it runs. There is 100GB of data. Which optimization ensures that re-reading the same path uses cache without rewriting?

A) Use `spark.sql.parquet.cacheMetadata=true` plus enable Delta cache  
B) Save result to Delta after first read, then read from Delta  
C) Configure `spark.sql.shuffle.partitions` and use `cache()` DataFrame plus `persist()`  
D) Use Apache Iceberg instead of Parquet plus enable metadata caching  

---

### 23. [Data Ingestion & Acquisition — Low-Latency Webhook]
You have a webhook sending events every second. You want to ingest to Delta with latency < 1 second end-to-end. Which setup is most appropriate?

A) Kafka topic → Structured Streaming → Delta with 500ms micro-batch  
B) Webhook → Kinesis stream → Delta via Lakeflow Connect  
C) Webhook → HTTP listener app → direct append to Delta (Python loop)  
D) Webhook → Auto Loader in `STREAMING` mode with `trigger(once=False)` and latestFirst  

---

### 24. [Data Manipulation — SQL]
A table has column `tags: ARRAY<STRUCT<name: STRING, value: VARIANT>>`. You need to count records where some tag has `name = 'category'` and `value.id` > 100. Which query is correct?

A) `SELECT COUNT(*) FROM table WHERE EXISTS (SELECT 1 FROM tags WHERE tags.name = 'category' AND tags.value:id > 100)`  
B) `SELECT COUNT(DISTINCT id) FROM table, LATERAL FLATTEN(tags) t WHERE t.value:name = 'category' AND t.value:value:id > 100`  
C) `SELECT COUNT(*) FROM table WHERE ANY(tags, t -> t.name = 'category' AND t.value:id > 100)`  
D) `SELECT COUNT(*) FROM table WHERE array_contains(tags, map('name', 'category', 'id', '>100'))`  

---

### 25. [Cost & Performance Optimization — Query Shuffle]
A query on SQL Warehouse takes 5 minutes. Profiling shows 80% of time in shuffle. Your selectivity index is 2% (filters 2% of data). Which optimization is most efficient?

A) Use `CLUSTER BY` auto on the table plus reorder columns in select  
B) Partitioning plus Z-order on filter column plus increase worker count  
C) Enable Adaptive Query Execution (AQE) plus `broadcast_join_threshold`  
D) Create a B-tree index and use hint `USE INDEX`  

---

### 26. [Debugging and Deploying — Workflow Retries]
A Databricks Workflow runs via Declarative Automation (databricks.yml). A step fails. What is the native way to integrate retry logic without modifying the notebook?

A) Add `max_retries: 3` and `retry_on_timeout: true` to the task definition in databricks.yml  
B) Wrap the notebook with Python script doing retry via `dbutils.notebook.run()` with try-except  
C) Use `tasks: [{name: ..., job_cluster_config: ..., max_concurrent_runs: 1}]` plus job scheduling  
D) Run each task as a separate `run_now` with manual validation  

---

### 27. [Data Governance — ABAC Policy Exception]
Your UC has a table with PII column (email). You created a governed tag `pii` and applied ABAC masking policy. A new user NEEDS TO SEE THE REAL EMAIL (unmasked). What is the correct process?

A) Remove from `analysts` group, add to `data_officers` group with policy override  
B) Request admin to create exception in policy using `ALTER POLICY ... ADD EXCEPTION`  
C) User runs `USE UNMASKED_COPY` before selecting (does not exist, trick question)  
D) Create a dedicated view with `SELECT email FROM table WHERE current_user() IN ('user@email.com')`  

---

### 28. [Developing Code — Python]
In a Spark job, you need to process data in smaller batches to avoid OOM. Which is the most idiomatic way in PySpark?

A) Use `repartition()` plus `groupByKey()` plus loop on `collect()`  
B) `foreachPartition()` or `foreachBatch()` with batch size limits  
C) `take(n)` in loop plus reprocess  
D) Increase `spark.executor.memory` and let Spark manage partitions  

---

### 29. [Monitoring and Alerting — Data Freshness SLA]
A pipeline has SLA "data must be current by 6 AM". The table feeding the dashboard has no alerts. What is the best native Databricks configuration?

A) Use `ALTER TABLE ... ADD CONSTRAINT freshness_check`  
B) Configure alerts in SQL Warehouse query profiling  
C) Create a validation job running at 5:50 AM, check `DESCRIBE DETAIL` timestamp and trigger Alert (webhook/Slack)  
D) Use native SQL Warehouse Query Alert set to run SLA check queries  

---

### 30. [Data Security and Compliance — External Partner Sharing]
You need to provide read access to a UC table for an external partner. The table contains sensitive data. What is the recommended approach using native UC?

A) Export data to CSV, share via S3 pre-signed URL  
B) Use Open Sharing (if partner has Databricks) or Clean Rooms (private sharing)  
C) Create a view with data subset plus grant `SELECT` via shared role  
D) Replicate table to partner's workspace plus manage permissions per-workspace  

---

### 31. [Data Ingestion & Acquisition — Incremental JDBC]
You ingest from a legacy database via JDBC. The table has 500M rows and grows 10M/day. Which strategy ensures efficient incremental ingestion?

A) Full query (`SELECT *`) daily with `mergeSchema=true`  
B) Use `READ_FROM` with hint on monotonic sequence column plus `WHERE col > last_value`  
C) Lakeflow Connect in CDC mode (if supported) or JDBC with parameterized increment query  
D) Use Apache Sqoop plus convert to Parquet after  

---

### 32. [Developing Code — SQL]
In a SQL notebook, you do `CREATE TEMP VIEW v AS SELECT ...`. Then you want to run in parallel two commands: `INSERT INTO tab1 SELECT * FROM v` and `INSERT INTO tab2 SELECT * FROM v`. What is the risk and how to avoid?

A) TEMP VIEW only lives in the session — use `CREATE VIEW` (persistent) or `GLOBAL TEMP VIEW`  
B) No risk, TEMP VIEW is accessible across the session  
C) Must recreate view in each parallelism — use `spark.parallelize()`  
D) Problem is the insert, not the view — add `OVERWRITE` clause  

---

### 33. [Cost & Performance Optimization — Materialized View Strategy]
A BI report is fed by 3 Materialized Views combining data from 10 Delta tables. Refresh is scheduled hourly. Additional ad-hoc queries hit the MVs. Which strategy reduces cost?

A) Convert MVs to normal (non-materialized) views plus enable Delta cache  
B) Use UC Metric Views for aggregations plus keep only 1 MV base for raw data  
C) Combine refresh of 3 MVs in 1 job plus use scheduled warehouses that auto start/stop  
D) Disable automatic refresh plus run manual refresh on-demand  

---

### 34. [Monitoring and Alerting — Ingestion Failure]
An ingestion job starts failing with `FileNotFoundError` after a week running. The code did not change. What is the most likely cause?

A) S3 path expired or bucket was deleted  
B) IAM permission was revoked or credentials expired  
C) Cluster was terminated, new cluster does not have access to same bucket  
D) All above are possible — check driver logs, validate permissions, test path  

---

### 35. [Data Modeling — Event Type Metrics]
You have an events table with `event_type: STRING` and want to calculate metrics by type. Which is the most performant using VARIANT/STRUCT?

A) `SELECT event_type, COUNT(*) FROM events GROUP BY event_type` plus Python loop on result  
B) Use `CASE` statements for each type plus dynamic aggregation  
C) Transform to VARIANT with structure `{type: X, metrics: {...}}` then parse  
D) Partition logically (views per type) plus run separate query for each  

---

### 36. [Data Manipulation — Exactly-Once Streaming]
You consume Kafka messages with Structured Streaming. Producer sends events with distinct timestamps. You want to guarantee exactly-once processing even with failures. Which configuration is critical?

A) Use `outputMode="append"` without checkpoint (does not guarantee)  
B) Checkpoint enabled plus idempotent writer (save with idempotency key) plus `completionTrigger`  
C) `outputMode="complete"` with `trigger(once=True)` and checkpoint  
D) Checkpoint alone is sufficient (false, also needs idempotent sink)  

---

### 37. [Cost & Performance Optimization — Multi-Stage Query]
A query takes 10 minutes. Profiling shows: 40% network (shuffle), 30% JSON parse, 20% sort, 10% I/O. Which optimization brings highest gain?

A) Increase partitions plus use Z-order  
B) Pre-parse JSON to VARIANT at ingestion plus enable Delta cache  
C) Convert JSON to native Parquet before query  
D) Increase worker count and `spark.sql.shuffle.partitions`  

---

### 38. [Developing Code — SQL]
You create a stored procedure doing `INSERT INTO table SELECT ...` with date parameter. For testing, you run with fixed date. In production (scheduled job), date should be dynamic (today). What is the correct practice?

A) Store date in a metadata table, stored proc reads it  
B) Use `DEFAULT CURRENT_DATE()` as parameter with null-check plus override in job  
C) Use `COALESCE(parameter_date, CURRENT_DATE())` in procedure  
D) Hardcode `CURRENT_DATE()` directly in procedure (no parameter)  

---

### 39. [Debugging and Deploying — Notebook Parameters]
A notebook exports parameters via `dbutils.widgets.get()`. A scheduled job passes parameters via `%run ../config`. Suddenly, everything fails with "widget not found". Why?

A) `%run` does not pass widgets — use `dbutils.notebook.run()` with `base_parameters` dict  
B) Config notebook must be in same workspace (relative path does not work across workspaces)  
C) `dbutils.widgets.get()` does not work in jobs — use environment variables via `spark.conf`  
D) Missing `%render` before using widgets  

---

### 40. [Monitoring and Alerting — Dashboard Inconsistency]
A dashboard shows inconsistency: two identical reports return different values. Dashboard uses same Materialized View. What is the most likely cause?

A) MV was refreshing while one report ran — one saw older version, other saw newer  
B) One report uses browser-local cache, other does not  
C) Delta version history — one query sees old snapshot, other sees new snapshot  
D) Bug in dashboard query, not MV fault  

---

### 41. [Data Manipulation — Masking Policy Logic]
You have a table with columns `customer_id`, `email`, `age`. You want a policy masking `email` for all EXCEPT `data_engineers`. Which is correct configuration in UC?

A) Use ABAC policy with `principal != "data_engineers"` → apply masking (inverted logic not supported)  
B) Use "positive" rule: `principal == "data_engineers"` → NO masking, `else` → masking  
C) Use Row-Level Security instead of Column-Level (not applicable here)  
D) Create 2 views: 1 with email for data_engineers, 1 without for others  

---

### 42. [Developing Code — Python]
In a PySpark job, you need to apply costly transformation on RDD. Which is the most efficient?

A) `rdd.map(expensive_func)` without persistence  
B) `rdd.map(expensive_func).cache().count()` to force computation plus then use  
C) Use DataFrame with SQL UDF instead of Python RDD  
D) `rdd.persist(StorageLevel.DISK_ONLY)` if memory is limited  

---

### 43. [Data Security and Compliance — Column Access Audit]
You want to audit WHO accessed WHICH column of a UC table. Native Databricks audit logs show "SELECT * FROM table" but not column granularity. Which is the solution?

A) Use UC Column-Level Lineage with masking policies (shows block, not access)  
B) Create view per user plus column-level security plus monitor differentiated access logs  
C) Use Predictive Optimization to track column access (not its function)  
D) Implement custom logging in UDF recording column accessed plus audit via external DB  

---

### 44. [Data Manipulation — MERGE vs UPDATE]
You have a large table (1TB) and want to update ~1% of rows. `MERGE` vs `UPDATE`: which is better?

A) `UPDATE` is more direct, always better  
B) `MERGE` always better for large volumes, even small changes  
C) Use `MERGE` for many changes; `UPDATE` if < 5% (different performance in Delta)  
D) Both equivalent in Delta — choose for code clarity  

---

### 45. [Cost & Performance Optimization — Query Regression]
A query doing full table scan of 100GB ran in 2 min, now takes 10 min. Nothing changed in code. What to check FIRST?

A) Table changes: new partitioning / compaction failing / old Delta version  
B) Cluster: change in worker count / worker type / available memory  
C) Query: change in hints / join order / predicate pushdown  
D) Cache: Delta cache disabled or LRU expired history  

---

### 46. [Data Ingestion & Acquisition — Variable-Encoding CSV]
You receive CSV files daily in S3. File has variable encoding (UTF-8, Latin-1). What is the robust way to ingest with Auto Loader?

A) `spark.read.option("encoding", "UTF-8").csv(path)`  
B) Use Auto Loader with `cloudFiles.schemaInference=true` and native encoding detector  
C) Pre-process files with `file_modify_time` plus use `multiLine=true`, `charToEscapeQuoteEscaping=true`  
D) Auto Loader does not support variable encoding — convert externally to UTF-8 first  

---

### 47. [Developing Code — Python]
In a notebook combining SQL and PySpark: SQL query returns 1M rows. You do `df = spark.sql("SELECT ...")` then `df.collect()`. What is the risk?

A) `collect()` brings everything to driver memory — can cause OOM  
B) None, PySpark manages memory automatically  
C) `df` stays in RDD, not driver memory  
D) Only fails if partition count < worker count  

---

### 48. [Debugging and Deploying — Automated Tag Application]
Your client wants a governed tag `sensitive` applied automatically to any column with `@pii` annotation or containing "ssn". What is the solution?

A) Use governance rules in UC with automatic pattern matching  
B) Create job scanning schema and running `ALTER TABLE ... SET TBLPROPERTIES (sensitive=true)` based on name pattern  
C) Use Predictive Optimization to suggest tags automatically  
D) Configure ABAC policy that auto-applies tag on column creation  

---

### 49. [Cost & Performance Optimization — OOM at Scale]
A job runs fine in testing (10M data) but fails in production (10B data) with OutOfMemory. You increase `spark.executor.memory`. Failure persists, not in driver, in executor. What is the real cause?

A) Not executor memory — may be full disk spill or partition imbalance  
B) Just increasing memory should resolve  
C) Job is not scalable — needs rewrite  
D) Increasing memory does not help spill — use `repartition()` or compact data  

---

### 50. [Developing Code — Python]
In PySpark job, you do exploratory analysis on Delta table. You want to apply costly transformation without OOM risk. Which is safest?

A) Use `repartition()` before `map()` plus apply on small partitions  
B) `df.map(func).cache().count()` to force immediate computation  
C) Use `foreachPartition()` with batch size limit  
D) Increase `spark.driver.memory` and let Spark manage  

---

### 51. [Data Ingestion & Acquisition — Webhook Deduplication]
You have a webhook sending events every second. Webhook can resend same event multiple times (retry). You want to deduplicate. What is the approach?

A) Add `UNIQUE` constraint on ID column (fails if ID already exists)  
B) Use `INSERT IGNORE` (does not exist in Delta/Databricks)  
C) Use `MERGE` with `WHEN NOT MATCHED INSERT` based on dedup key plus `DELETE FROM staging WHERE ...`  
D) Insert to staging, then `INSERT INTO target SELECT DISTINCT * FROM staging`  

---

### 52. [Data Manipulation — Partitioned Table Performance]
You have sales table partitioned by `date`. Daily INSERT now takes 3x longer. Cause: many small partitions. Which solution is better?

A) Run `OPTIMIZE` with Z-order on full table (costly, full scan)  
B) Run `OPTIMIZE` only on today's partition (`OPTIMIZE table WHERE date = ...`)  
C) Use incremental compaction (does not exist built-in — use `REPARTITION` before INSERT)  
D) Use auto-compaction via automatic `OPTIMIZE` in Databricks (available, needs config)  

---

### 53. [Debugging and Deploying — Intermittent Failure]
Scheduled job fails intermittently (50% of time). Logs are identical. What probably happens?

A) Race condition with parallel job or Delta table locking  
B) Random network timeout  
C) Cluster scaling down between jobs, sometimes yes sometimes no  
D) Without more info, diagnosis is impossible — need more telemetry (cluster metrics, warehouse logs)  

---

### 54. [Data Governance — Permission Management]
You have UC table used by 3 groups: `analysts` (SELECT), `engineers` (SELECT + MODIFY), `admins` (full). What is most efficient permission management?

A) Create 3 roles plus make specific GRANT on each  
B) Use inheritance: create `editors` role with MODIFY, `readers` role with SELECT, then `admins EXTEND editors`  
C) Make GRANT directly per user (not scalable)  
D) Run job doing `ALTER TABLE OWNER TO` on each group change  

---

### 55. [Debugging and Deploying — Streaming Latency After 30 Days]
A streaming job consuming from Kafka has growing back-pressure. After 30 days running, latency suddenly jumps. What is most likely cause?

A) Kafka has lag — check partition size and consumer group  
B) Checkpoint state exploded — clean old checkpoint and restart stream  
C) Shuffle memory grew — executor memory limit hit  
D) Aggregation window accumulating state — watermark may be stuck or state not expiring  

---

### 56. [Cost & Performance Optimization — Stakeholder Metrics]
A Warehouse has 1PB data. Users complain queries are slow. You need metric to convince leadership to invest in optimization. Which metric is most convincing?

A) Query count (does not show user impact)  
B) Cost per query (shows waste)  
C) P95 query latency plus total cost (shows poor UX + $ waste together)  
D) Warehouse size (not directly related to performance)  

---

### 57. [Data Security and Compliance — PII Data Locality]
Your organization has regulatory requirement: PII data must reside only on dedicated clusters. What is native Databricks approach?

A) Use `spark.conf` to limit read path to specific cluster via access control  
B) UC policies plus compute-level ACL (if supported) or implement at storage level  
C) Replicate PII in UC schema, grant access only from managed compute  
D) Use UC Governance with table locality restriction (not native feature — manual)  

---

### 58. [Developing Code — Python]
In PySpark job, you want to process VARIANT columns in SQL transformation. Which approach ensures better performance?

A) Use `df.selectExpr("explode_outer(variant_col) as item")` then iterate in Python  
B) Use `sql("SELECT ... FROM delta.`path` WHERE ...")` plus native SQL to manipulate VARIANT  
C) Convert VARIANT to JSON string in Python, parse with `json.loads()`, then reassemble  
D) Use `pyspark.sql.functions.col()` with `.getItem()` chaining  

---

### 59. [Developing Code — SQL/Python]
You have nested JSON and want to extract multiple fields. Which is more performant: `parse_json()` once plus multiple `variant_get()`, or parse N times?

A) Parse N times is inefficient — use `parse_json()` once stored in VARIANT  
B) Both equivalent in Spark SQL (optimizer is smart)  
C) Parse once in Python, then in SQL (hybrid)  
D) Use `get_json_object()` N times — Spark optimizes internally  

---

### 60. [Developing Code — Python/SQL]
An iterative notebook with Structured Streaming processes data via `foreachBatch()`. After converting to scheduled job, latency degrades with 30 days runtime without restart. What is the real problem?

A) Checkpoint file gets too large, needs periodic cleanup  
B) Window state accumulating in memory — watermark expired or offset tracking fails  
C) Kubernetes pod memory leak in long duration  
D) Python garbage collection degrading performance  

---

## Answer Key and Explanations

**1. Answer: B**  
Watermark of 10 minutes allows late-arriving events (up to 10 min) to be reprocessed in correct window. `outputMode="append"` is recommended for streaming. CDF is redundant here.

**2. Answer: B**  
Lakeflow Connect requires Logical Decoding enabled on PostgreSQL, WAL level set to `logical`, and a permanent replication slot. Replica identity full is useful but not critical prerequisite.

**3. Answer: C**  
`variant_get(data, 'user.email', 'string')` is safe and efficient for extracting from VARIANT with fallback to NULL. More explicit than bracket syntax.

**4. Answer: B**  
Throughput metrics (input rate vs. processing rate) on Spark Streaming tab show degradation immediately. If processing rate drops, there is a bottleneck.

**5. Answer: A**  
Deletion vectors avoid rewriting entire files. CLUSTER BY auto optimizes layout. Delta cache reduces I/O. Together, they solve frequent small writes and slow reads.

**6. Answer: B**  
ABAC (Attribute-Based Access Control) in UC with governed tags allows central policy automatically applying to any column with tag `pii`.

**7. Answer: A**  
UC Metric View provides centralized governed definition. Materialized View pre-aggregates for performance. Combined, offer governance plus reusability.

**8. Answer: B**  
Passing date as parameter via configuration ensures consistency in rolling window between runs. More reliable than `CURRENT_DATE()` in jobs.

**9. Answer: A**  
Calling API hour by hour in parallel, saving raw to Delta, then processing is robust for large historical volume.

**10. Answer: A**  
`MERGE` with CDF consumes only incremental changes from `shipments`. More efficient than full refresh.

**11. Answer: A**  
Serverless compute for ad-hoc (pay-as-you-go) plus dedicated warehouse for dashboards (predictable, reserved capacity) reduces total cost.

**12. Answer: B**  
Native SQL with VARIANT manipulation is more efficient than Python/RDD. Spark SQL optimizer optimizes VARIANT operations.

**13. Answer: C**  
`SHOW TBLPROPERTIES` on Materialized View shows `last_refresh_time` and refresh status. Most direct.

**14. Answer: B**  
Open Sharing (between Databricks workspaces) or D2D allows sharing with granular permission per workspace. Native UC.

**15. Answer: B**  
`SparkException: Task deserialization error` after 2 weeks typically means class/lib incompatibility. Clean and reimport libraries resolves.

**16. Answer: B**  
Lakebridge to Teradata with LDAP requires LDAP LogMech enabled, ODBC port (1025/TCP default) open, and LDAP user with permission. Setup on Teradata side is essential.

**17. Answer: A**  
DLT natively supports `@quality_expectation` or `EXPECT` statements with `action fail`. Idiomatic approach.

**18. Answer: C**  
Partition by date in Iceberg plus `EXPIRE_SNAPSHOTS` removes old snapshots, reducing file count and metadata.

**19. Answer: C**  
AUTO CDC with `stored_as_scd_type = 2` in DLT maintains SCD Type 2 automatically. More efficient than manual MERGE.

**20. Answer: A**  
Spark Streaming tab shows "Input rate" vs. "Processing rate" graphs. If processing rate < input rate, there is lag.

**21. Answer: B**  
Row and Column Security (RCS) with masking policy per tag is native in UC and cannot be bypassed.

**22. Answer: B**  
Save result to Delta after first read, then read from Delta, ensures cached snapshot and compacted structure.

**23. Answer: A**  
Kafka → Structured Streaming with 500ms micro-batch → Delta guarantees < 1 sec end-to-end latency.

**24. Answer: C**  
`ANY(tags, t -> t.name = 'category' AND t.value:id > 100)` uses `ANY` predicate on array with lambda for nested struct testing.

**25. Answer: B**  
80% in shuffle with low selectivity → partitioning plus Z-order reduces volume. Largest gain over increasing workers.

**26. Answer: A**  
Declarative Automation natively supports `max_retries` and `retry_on_timeout` in task definition.

**27. Answer: B**  
ABAC policy allows exceptions via `ALTER POLICY ... ADD EXCEPTION` for specific users.

**28. Answer: B**  
`foreachPartition()` or `foreachBatch()` with batch size limit is idiomatic for processing in batches and avoiding OOM.

**29. Answer: C**  
Validation job running before SLA, checking timestamp via `DESCRIBE DETAIL`, triggering webhook/Slack alert. Common practice.

**30. Answer: B**  
Open Sharing (partner with Databricks) or Clean Rooms (private sharing) is native UC for external partners.

**31. Answer: C**  
Lakeflow Connect CDC (if DB supports) or JDBC with parameterized sequence ensures efficient incremental ingestion.

**32. Answer: A**  
TEMP VIEW lives only in SQL session. Parallelism (dbutils.notebook.run, tasks) = new session = no access. Use `CREATE VIEW` or `GLOBAL TEMP VIEW`.

**33. Answer: C**  
Combine 3 refreshes into 1 job, use scheduled warehouses on/off auto. Amortizes startup overhead, pay only during refresh period.

**34. Answer: D**  
All are possible causes. Validate logs, test path S3, check IAM credentials.

**35. Answer: A**  
`SELECT event_type, COUNT(*) FROM events GROUP BY event_type` is native SQL optimized by Catalyst. Idiomatic, efficient.

**36. Answer: B**  
Exactly-once requires checkpoint plus idempotent writer plus sink that avoids duplicates. Checkpoint alone insufficient.

**37. Answer: B**  
JSON parsing is 30% of time. Pre-parsing to VARIANT at ingestion plus Delta cache avoids re-parse. Largest gain.

**38. Answer: C**  
`COALESCE(parameter_date, CURRENT_DATE())` is idiomatic — null parameter falls to dynamic date.

**39. Answer: A**  
`%run` is magic command (no context passing) — use `dbutils.notebook.run()` with `base_parameters` dict passes variables.

**40. Answer: A**  
MV refresh atomically replaces version — one query sees v1, other sees v2 = refresh happened between them. Transient inconsistency.

**41. Answer: B**  
ABAC in UC supports "positive" logic: `principal == "data_engineers"` → NO mask; default → mask.

**42. Answer: B**  
`cache().count()` forces immediate computation and caching. Subsequent uses read from cache. With limited memory, consider `DISK_ONLY` storage level.

**43. Answer: D**  
UC audit logs do not track individual column access. Custom logging in UDF recording "user X accessed column Y" plus external audit DB is real solution.

**44. Answer: C**  
Delta: `MERGE` for large-scale changes — `UPDATE` for < 5% rows (targeted, efficient). Different performance profiles.

**45. Answer: A**  
Full table scan degrading — table quality changed (compaction degrading, old Delta version).

**46. Answer: B**  
Auto Loader with `cloudFiles.schemaInference=true` plus native encoding detection supports variable-encoding CSV. Fallback: pre-process with iconv.

**47. Answer: A**  
`collect()` brings 1M rows to driver memory — real OOM risk. Use `count()` or `write` instead.

**48. Answer: B**  
Job scanning schema, identifying columns with "ssn"/"pii" in name, then `ALTER TABLE ... SET TAG` is common workaround.

**49. Answer: D**  
If increasing executor memory does not resolve OOM and not driver OOM, issue is partition imbalance or spill. `repartition()` resolves.

**50. Answer: B**  
`df.map(func).cache().count()` forces immediate computation and caching. Safe for exploratory analysis. With limited memory, consider `DISK_ONLY` storage level.

**51. Answer: C**  
`MERGE` with `WHEN NOT MATCHED INSERT` based on ID ensures no duplicates in webhook deduplication.

**52. Answer: B**  
`OPTIMIZE` only on today's partition (WHERE date = TODAY) is efficient — compacts only new partition.

**53. Answer: D**  
50% intermittent failure — race condition, random timeout, or cluster scale-down. Need telemetry.

**54. Answer: A**  
Create 3 roles plus specific GRANT on each is standard scalable UC approach.

**55. Answer: D**  
Latency growing after 30 days streaming — window state accumulating. Watermark stuck or state not expiring.

**56. Answer: C**  
P95 latency shows poor UX. Cost per query shows waste. Together, convince leadership.

**57. Answer: C**  
Replicate PII in UC schema, grant access only from managed compute is closest native solution.

**58. Answer: B**  
Native SQL with VARIANT is more efficient than Python/RDD. Optimizer optimizes VARIANT operations.

**59. Answer: A**  
Parse JSON once to VARIANT, then multiple `variant_get()` calls read cached VARIANT. Avoids re-parse.

**60. Answer: B**  
Latency degrading after 30 days streaming — window state accumulating. Watermark expired or offset tracking fails.

---

**End of Practice Exam 2**

> 60 questions, corrected distribution: Developing Code 14, Data Ingestion & Acquisition 7, Data Manipulation 7, Monitoring 6, Cost & Performance 9, Security 5, Data Governance 3, Debugging 6, Data Modeling 3. All net-new topics covered. Answer key based on official Databricks documentation.
