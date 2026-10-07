# Practice Exam 2 — Databricks Certified Data Engineer Professional (New Exam)

> Original practice questions, not official exam questions. Answers are based on the Databricks documentation linked under each answer; verify in the docs. Time yourself: 120 minutes. Level: harder, more code-reading.

## Questions

### 1. [Developing Code — Python wheel task entry point and dependency conflicts]

Your team is packaging a PySpark ETL job as a Python wheel with a custom entry point. The wheel depends on `pandas==2.0.0`, but your Databricks cluster has `pandas==1.5.3` pre-installed. The job needs to run on multiple clusters with different pre-installed versions. You want to avoid hard-coding versions and minimize deployment friction. What is the best approach?

```python
# setup.py
from setuptools import setup

setup(
    name='etl-job',
    version='1.0.0',
    py_modules=['etl_main'],
    install_requires=[
        'pandas>=1.5,<3.0',  # Flexible version range
        'pyspark>=3.5.0',
    ],
    entry_points={'console_scripts': ['etl-job=etl_main:main']},
)
```

A) Pin the exact version in setup.py; use `pip install --force-reinstall` in the task's init script to override cluster defaults  
B) Hard-code the version check in the entry point; skip task if versions don't match  
C) Remove pandas from dependencies; add it as a cluster library via UI before each task run  
D) Use a flexible version range in setup.py (e.g., `pandas>=1.5,<3.0`); rely on the wheel's dependency resolution at install time  

---

### 2. [Developing Code — Streaming table vs materialized view for append-only near-real-time feed]

Your data ingestion pipeline receives new events every 5 seconds into a staging Delta table. You need to join these events with a slowly changing dimension table (customer master, updated daily) and expose the result for real-time dashboards. The join must never drop events and should be maintainable without manual restarts. Which table type best meets these requirements?

A) Materialized View: runs on a schedule, re-computes the join daily, and preserves history  
B) Materialized View: processes events once per query execution, scales linearly, and supports schema evolution  
C) Streaming Table with manual checkpoint management: offers fine-grained control over event ordering and replay  
D) Streaming Table: processes new events incrementally, maintains state for the join, and restarts automatically on failure  

---

### 3. [Developing Code — AUTO CDC with apply_as_deletes and out-of-order events]

You are capturing changes from a source system into a CDC feed that receives updates and deletes out of order (deletes may arrive before all updates). You use AUTO CDC with `apply_as_deletes` to handle logical deletes marked in the source. However, you notice that old updates are arriving 1–2 hours after deletes for the same record. Your team needs strong ordering guarantees. What should you do?

```sql
CREATE OR REFRESH STREAMING TABLE target_table
AS AUTO CDC INTO
SELECT 
  id,
  name,
  status,
  _change_type,
  _commit_timestamp
FROM STREAM(source_cdc_feed)
SEQUENCE BY source_timestamp
APPLY AS DELETES(WHERE status = 'deleted')
```

A) Switch to a streaming table with manual `MERGE` logic and replay from an earlier checkpoint to recover missed updates  
B) Disable `apply_as_deletes`; manually filter deletes in post-processing before merge  
C) Use a watermark on the source timestamp to buffer out-of-order events; set a grace period longer than the expected delay  
D) Add a `sequence_by` column based on source timestamp and rely on AUTO CDC's ordering logic to reorder events  

---

### 4. [Developing Code — Iterator Pandas UDF for batch model scoring]

You have a 10M-row batch of customer records to score with a pre-trained ML model. The model requires 500 MB of memory to load (one-time overhead per Python process). Scoring each record takes 1 ms. Your cluster has 8 executors with 4 cores each. You want to minimize total runtime without out-of-memory errors. Which approach is best?

```python
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import pandas_udf

# Iterator-based Pandas UDF: load model once per partition
@pandas_udf(DoubleType())
def score_batch(iterator):
    import ml_model  # Loads once per Python process
    model = ml_model.load()  # 500 MB, once per partition
    for df in iterator:
        yield model.predict_batch(df['features'])  # Vectorized scoring

df = df.repartition(32)
scored = df.withColumn('score', score_batch(col('features')))
```

A) Use a standard Python UDF with `model.load()` in the UDF function; let Spark handle partitioning and memory  
B) Use a SQL User-Defined Table Function (UDTF) with external model loading and return all scores in one batch  
C) Use `DataFrame.repartition(32)` then broadcast the model to all executors using `spark.broadcast()`  
D) Use an iterator-based Pandas UDF; load the model once per partition, score the entire batch in vectorized operations  

---

### 5. [Developing Code — Bundle with multiple targets, run_as permissions]

Your team is deploying an ETL pipeline bundle to both a DEV environment (for testing) and a PROD environment (for production). The bundle defines a single job with SQL tasks that write to different catalogs. DEV uses a service account with catalog editor permissions; PROD requires running as a specific role with additional audit logging. You want to avoid duplicating the entire bundle configuration. What is the best practice?

```yaml
targets:
  dev:
    variables:
      run_as_role: "service-account-dev"
      target_catalog: "dev_catalog"
    
  prod:
    variables:
      run_as_role: "prod_audit_role"
      target_catalog: "prod_catalog"

jobs:
  etl_job:
    run_as:
      service_principal_id: "${var.run_as_role}"
    tasks:
      - sql_task: "SELECT * FROM ${var.target_catalog}.table"
```

A) Use environment variables in SQL tasks to select the catalog at runtime; manage permissions via Unity Catalog policies  
B) Create two separate bundles (dev/bundle.yml, prod/bundle.yml) with hardcoded run_as; deploy each independently  
C) Use bundle targets; define separate `run_as` clauses per target, each pointing to the correct role or service account  
D) Deploy once to DEV; manually edit the job in PROD UI to change permissions and target catalog before enabling  

---

### 6. [Developing Code — Which query changes are allowed when restarting from checkpoint]

Your streaming query is reading from a Kafka stream and maintaining a stateful aggregation (count by key). You have a checkpoint saved. Your team wants to add a new column to the schema and rename an aggregation output column. What is the safest change that allows you to restart from the existing checkpoint without data loss?

```python
# Original query
df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "...") \
  .load()

# Safe change: add column with default
result = df.groupBy("key") \
  .agg(count("*").alias("event_count")) \
  .withColumn("event_type", lit("streaming"))  # New column

# Unsafe: rename aggregation column
# result = result.withColumnRenamed("event_count", "total_events")  # Breaks checkpoint
```

A) Rename the aggregation output column; restart with the same checkpoint—Spark will map the old name to the new name automatically  
B) Drop an unused column from the schema; rename the output; restart from a new checkpoint location  
C) Modify the aggregation key from `(key1, key2)` to `(key1)` to reduce cardinality; adjust the checkpoint path accordingly  
D) Add a new column with a default value; keep all existing columns unchanged; restart from the existing checkpoint  

---

### 7. [Developing Code — dropDuplicatesWithinWatermark for stateful dedup]

Your streaming pipeline ingests user events with a timestamp field. Events may arrive out of order within a 1-hour window, and you must deduplicate based on a unique event ID within that window. Older events (outside the window) can be duplicated without harm. You want a stateless, cost-effective deduplication strategy. What should you use?

```python
df = spark.readStream.format("kafka").load()

# Add watermark and deduplicate within window
deduped = (df
    .withWatermark("event_timestamp", "1 hour")
    .dropDuplicatesWithinWatermark("event_id")  # Only keeps state for 1 hour
)

# Events older than 1 hour are dropped from state
# Duplicates outside window are allowed (no memory growth)
```

A) `dropDuplicates("event_id")` without watermark—Spark will deduplicate all events globally in memory  
B) Manual state store with TTL—write event_id to a shared state table, check on arrival, age out after 1 hour  
C) `dropDuplicatesWithinWatermark("event_id")` with `withWatermark()` on the timestamp; only store state for 1-hour window  
D) `window()` aggregation with `first(event_id)` to select the first occurrence per window, then dedup  

---

### 8. [Data Ingestion & Acquisition — Auto Loader file notification mode vs directory listing at scale]

Your company ingests 100,000 new CSV files daily from an S3 bucket into a Bronze table. Files arrive throughout the day with a 2–5 minute latency requirement. You have two options: file notifications (SNS/SQS) or directory listing. Your infra team wants to minimize polling overhead. Which approach scales better and why?

```python
# File notification mode (recommended for scale)
df = spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.sourceNotificationMode", "SNS_SQS") \
  .option("cloudFiles.connectionString", "<sqs-url>") \
  .option("cloudFiles.resourceGroup", "<resource-group>") \
  .load("s3://bucket/csv/")

# Scales O(1) per file, not O(n) per poll interval
```

A) Directory listing: Databricks polls S3 every minute, scans all 100k+ files, and processes new ones; cheaper than SNS setup  
B) Directory listing with aggressive caching: list S3 once per hour, cache results locally, and poll the cache every minute  
C) Hybrid: use file notifications for the first 50k files, then switch to directory listing to avoid SNS cost  
D) File notification mode: each file triggers an SNS message, which queues the file path for Auto Loader; scales independently of bucket size  

---

### 9. [Data Ingestion & Acquisition — Reading Kinesis into streaming table with at-least-once semantics]

Your team needs to ingest real-time events from AWS Kinesis (1000 events/sec) into Databricks for a real-time analytics dashboard. The integration must provide sub-5-second end-to-end latency and handle at-least-once semantics (duplicates possible but acceptable for analytics aggregations). Your team is inexperienced with Kinesis and prefers to minimize operational overhead. Which integration best meets these requirements?

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KinesisIngest").getOrCreate()

# Structured Streaming with Kinesis
df = spark.readStream \
  .format("kinesis") \
  .option("streamName", "events") \
  .option("region", "us-west-2") \
  .option("initialPosition", "TRIM_HORIZON") \
  .load()

# Cast binary data to string
df = df.select(
    col("data").cast("string").alias("payload"),
    col("approximateArrivalTimestamp").alias("event_time")
)

df.writeStream.format("delta").mode("append").option("checkpointLocation", "/tmp/ckpt").start()
```

A) Use DataFrame API with `readStream().format("kinesis")` and manage checkpoints manually to recover from shard changes  
B) Batch ingest via Kinesis GetRecords API every 1 second; write to a Delta table, then query as streaming source  
C) Use Structured Streaming with `readStream().format("kinesis")`; Spark automatically manages checkpoints and handles shard rebalancing  
D) Use Kinesis Firehose to buffer events into S3, then use Auto Loader in directory-listing mode for ingestion  

---

### 10. [Data Ingestion & Acquisition — Delta Sharing between Databricks workspaces (D2D) and change data feed history]

Your team has two Databricks workspaces: one for Data Lake (prod, 500M records) and one for Analytics (test). You want to share the latest prod data with the analytics team via Delta Sharing, but also replay historical changes for lineage audits. The analytics team queries the shared table hourly and cannot tolerate missing rows. What must you enable on the source table?

```sql
-- On prod workspace source table
ALTER TABLE prod_catalog.prod_schema.events
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.changeDataFeedRetentionDays' = '30'
);

-- Share via D2D in Delta Sharing recipient config
-- SELECT * FROM shared_table  -- Latest
-- SELECT * FROM table_changes(...) --  Historical changes
```

A) Enable row-level security via ABAC; share the catalog with reader role; maintain a separate audit table for history  
B) Configure time-travel retention to 90 days; query the shared table with `SELECT * FROM table@timestamp` syntax  
C) Enable Predictive Optimization on the table; Delta Sharing will automatically sync deltas  
D) Enable Change Data Feed (CDF) on the source table; share via Delta Sharing in the Databricks-to-Databricks (D2D) endpoint  

---

### 11. [Data Ingestion & Acquisition — Clean Rooms for privacy-preserving collaboration]

Two competing financial institutions want to identify overlapping customers for joint marketing, without either party revealing their complete customer lists (competitive risk and privacy concern). They evaluate Databricks Clean Rooms versus alternative secure data sharing approaches. What unique data governance guarantee does a Clean Room provide that direct raw data sharing methods do not?

```sql
-- In clean room, collaborator queries data from both parties
-- but never sees raw rows, only aggregated results

SELECT 
  COUNT(DISTINCT shared_customer_id) AS overlap_count,
  COUNT(DISTINCT bank_a_customer_id) AS bank_a_total,
  COUNT(DISTINCT bank_b_customer_id) AS bank_b_total
FROM clean_room_overlap_view
-- Result shows counts but not customer identities
```

A) Clean Rooms encrypt data in transit; raw data sharing sends unencrypted files  
B) Clean Rooms execute queries in an isolated environment; results are aggregated and filtered to prevent row-level disclosure  
C) Clean Rooms require customers to approve all queries; raw sharing has no approval workflow  
D) Clean Rooms replicate data to a third-party provider for added security  

---

### 12. [Data Manipulation — Semi/anti joins for data-quality checks]

You have a fact table (1B rows) of user events and a dimension table (100k rows) of valid customers. You need to flag events from invalid customers (those NOT in the dimension). You also need to find valid customers who have NO events. Which join types minimize computation and prevent row duplication?

```sql
-- Find invalid events
SELECT f.* FROM facts f
LEFT ANTI JOIN dim_customers d ON f.customer_id = d.customer_id;

-- Find customers with no events
SELECT d.* FROM dim_customers d
LEFT ANTI JOIN facts f ON d.customer_id = f.customer_id;
```

A) Use LEFT OUTER JOIN on facts to dimension (1B rows returned), then RIGHT OUTER JOIN to find customers with no events (duplicates expected)  
B) Use LEFT ANTI JOIN from facts to dimension; use LEFT ANTI JOIN from dimension to facts  
C) Use FULL OUTER JOIN with WHERE conditions to find null matches on both sides; this avoids scanning large tables  
D) Use INNER JOIN to find valid events; use NOT IN subquery to find customers with no events (nested query)  

---

### 13. [Data Manipulation — VARIANT schema discovery with schema_of_variant and exploding nested arrays]

Your ingestion pipeline receives JSON records as VARIANT columns. Each record has a nested array of objects under `.items[]`, with varying schemas (some have field `x`, others have field `y`, some with both fields present). You need to flatten the array, discover all possible fields across all records, and create uniform output. What is the correct approach?

```sql
-- Flatten and discover schemas
WITH exploded AS (
  SELECT 
    variant_explode(items) AS item
  FROM bronze_table
  WHERE items IS NOT NULL
)
SELECT 
  schema_of_variant(item) AS item_schema,
  item
FROM exploded
```

A) Use `schema_of_variant()` on the entire VARIANT column; Spark will infer a merged schema for all arrays  
B) Use `variant_explode()` to flatten the array, then `schema_of_variant()` on each element to discover field names  
C) Use `to_json()` to convert VARIANT to string, then `from_json()` with a pre-defined schema; requires manual schema curation  
D) Use `get_json_object()` with wildcards to extract all `.items[*]` fields; manually union results  

---

### 14. [Data Manipulation — Error handling with ai_query in batch processing]

You have a 50k-row batch of product descriptions (text column). You want to extract structured fields (category, subcategory, brand) using an LLM via `ai_query()`. The LLM may timeout or refuse certain inputs. You need deterministic, retryable batching with graceful degradation. What is the safest approach?

```python
from pyspark.sql.functions import col, struct, lit, when

def extract_with_error_handling(description):
    try:
        result = ai_query(
            f"Extract category, subcategory, brand from: {description}",
            output_format={"category": "string", "subcategory": "string", "brand": "string"}
        )
        return result
    except Exception as e:
        log.warning(f"LLM error: {e}")
        return {"category": None, "subcategory": None, "brand": None}

df_batched = df.repartition(50)  # Micro-batches
result = df_batched.withColumn("extraction", 
    struct_from_json(extract_with_error_handling(col("description"))))
```

A) Apply `ai_query()` to the entire DataFrame; Spark will parallelize across partitions; use `SCHEMA OF` to define output schema  
B) Use a TRY-CATCH block in a UDF wrapping `ai_query()`; batch into micro-batches; log errors and return NULL for failures  
C) Apply `ai_query()` with `max_tokens` limit; use Adaptive Query Execution (AQE) to retry on timeout; cache intermediate results  
D) Use `ai_query()` with a CASE statement for fallback logic; if LLM call fails, populate fields with defaults; no error logging  

---

### 15. [Monitoring and Alerting — Compute utilization from system tables (system.compute)]

Your data team runs 100+ daily jobs on Databricks clusters. You want to identify underutilized clusters and spot anomalies (e.g., a job using 90% CPU for 2 hours). You decide to query system tables to build a monitoring dashboard. Which table should you query to get per-executor CPU and memory utilization?

```sql
SELECT 
  cluster_id,
  node_id,
  start_time,
  cpu_user_percent,
  cpu_system_percent,
  mem_used_percent
FROM system.compute.node_timeline
WHERE start_time > CURRENT_TIMESTAMP - INTERVAL 2 HOURS
  AND (cpu_user_percent + cpu_system_percent) > 90
ORDER BY start_time DESC, cpu_user_percent DESC
```

A) `system.billing.account_usage_core`—shows credits consumed per cluster daily  
B) `system.compute.cluster_events`—logs cluster state changes (start, stop, terminate)  
C) `system.compute.node_timeline`—contains per-node CPU, memory, and disk I/O utilization at 1-minute granularity  
D) `system.access.query_history`—logs all SQL queries and their execution time  

---

### 16. [Monitoring and Alerting — Data quality from pipeline event_log() table-valued function]

You have a Lakeflow Declarative Pipeline ingesting data with multiple stages (bronze → silver → gold). A stage is failing silently—records are not reaching the next stage, but no error is logged. You want to identify which stage is losing records. What should you query?

```sql
SELECT 
  event_type,
  details.table_name,
  details.num_added_rows,
  details.num_deleted_rows,
  details.num_updated_rows,
  timestamp
FROM event_log()
WHERE event_type = 'FLOW_PROGRESS'
  AND pipeline_id = '<your-pipeline-id>'
ORDER BY timestamp DESC
LIMIT 100
```

A) Run `SHOW TBLPROPERTIES` on each table to check `delta.lastModifiedTime` and count rows manually  
B) Query `system.access.query_history` for all SQL queries in the pipeline; count rows before/after each stage  
C) Query `event_log()` TVF for the pipeline; filter on `event_type = 'FLOW_PROGRESS'` to track record counts per stage  
D) Query `system.pipeline.lineage_events` to track schema changes; data loss is usually caused by schema mismatches  

---

### 17. [Monitoring and Alerting — Job health rules and duration-threshold notifications]

You have a critical ETL job that normally completes in 30 minutes. Recently, it sometimes takes 45 minutes without failing. You want to alert the team if the job's p95 duration exceeds 40 minutes over the last 7 days, but not for single-run spikes. Which feature should you use?

A) Create a job alert in the Databricks UI for `Duration > 40 min`; it will fire on every run that exceeds 40 minutes  
B) Query `system.jobs.job_runs` with a 7-day window; calculate p95 duration; set up a Lakeflow job to run this daily and publish alerts  
C) Add a task-level timeout in the job definition; the job will auto-fail if it runs >40 min; track failures in a dashboard  
D) Use Databricks Job Health Rules to track p95 duration over a rolling 7-day window; alert when threshold is exceeded  

---

### 18. [Cost & Performance Optimization — Photon performance-optimized vs standard mode choice]

Your team runs a 500 GB fact table join against a 10 GB dimension table 10 times per day. The query uses Photon-enabled SQL warehouse (reserved capacity). Photon speeds up the join by 3x but adds 20% to compute cost. Without Photon, the query takes 2 minutes. Your warehouse idle time is 60%. Should you use Photon?

```sql
-- Query execution analysis
-- Standard mode: 2 min per query × 10 queries = 20 min total
-- Photon mode: 40 sec per query × 10 queries = 6.7 min total

-- With 60% idle capacity (already paid for):
-- Total warehouse "occupied" time reduced from 20 min to 6.7 min
-- Other jobs can start sooner, reducing queue time
-- 20% compute cost increase << benefit of freed capacity
```

A) No, because idle time (60%) is high; you're already paying for capacity and should use standard mode to save 20% compute cost  
B) Yes, because the 3x speedup saves query time; the 20% cost increase is offset by less total warehouse uptime  
C) Only during peak hours; switch to standard mode during off-peak to reduce cost  
D) Yes, Photon always reduces total cost; the speedup compounds across all queries throughout the day  

---

### 19. [Cost & Performance Optimization — Data skipping statistics columns (delta.dataSkippingNumIndexedCols)]

Your data team manages a 10 TB table partitioned by date and clustered by user_id. Queries often filter on `user_id` and `date`, but data skipping is not aggressive enough—Spark still scans 30% of files even after filtering. You want to reduce scans to <5% without repartitioning. What should you configure?

A) Reduce `delta.dataSkippingNumIndexedCols` to 1 (only date); Spark will skip more aggressively on that column  
B) Disable data skipping; use explicit filtering in your query instead (e.g., `WHERE user_id IN (SELECT ... FROM ref)`)  
C) Increase `delta.dataSkippingNumIndexedCols` from default 32 to 64; ensure `user_id` is in `delta.dataSkippingStatsColumns`  
D) Increase `delta.dataSkippingNumIndexedCols` to index more columns; reduce `delta.dataSkippingStatsColumns` to only critical fields  

---

### 20. [Cost & Performance Optimization — Partition or file pruning defeated by function on filter column]

Your fact table contains 10 years of data, partitioned by `date_col` (YYYY-MM-DD). Your analytics team regularly runs a report that filters: `WHERE YEAR(date_col) = 2024`. Query performance is poor—partition pruning is disabled, and Spark scans all 120+ partition directories instead of just 12 monthly partitions. What prevents partition elimination, and how should the query be rewritten?

```sql
-- WRONG: Function on partition column disables pruning
WHERE YEAR(date_col) = 2024;  -- Scans all partitions

-- CORRECT: Range filter enables pruning
WHERE date_col >= '2024-01-01' 
  AND date_col < '2025-01-01';  -- Prunes 11/12 partitions

-- Spark's pruner evaluates partition bounds directly
-- Functions prevent direct comparison with partition key
```

A) The partition column must match the filter column exactly; add an indexed column `year_partition` alongside `date_col`  
B) Spark doesn't recognize date column types; change the partition column to an integer (e.g., `year_int`)  
C) Partition pruning is disabled by default; set `spark.sql.optimizer.enablePartitionPruning = true`  
D) The YEAR() function prevents partition elimination; rewrite to `WHERE date_col >= '2024-01-01' AND date_col < '2025-01-01'`  

---

### 21. [Cost & Performance Optimization — AQE skew join handling]

Your query joins a skewed fact table (90% of rows have `category_id = 1`) to a dimension table. Without Adaptive Query Execution (AQE), the join is slow and unbalanced—one executor handles 90% of data while others idle. Enabling AQE helps, but you need finer control. What should you configure?

A) Enable AQE; trust the defaults; no manual configuration needed for skew join optimization  
B) Disable AQE; add a Salt column to the fact table (`category_id + RAND() % 10`) before the join to distribute skewed keys  
C) Enable AQE; set `spark.sql.adaptive.skewJoin.enabled = true` and adjust `spark.sql.adaptive.skewJoin.skewFactor` to 2x (default is 5x)  
D) Use a broadcast join; force the dimension table to broadcast even if it's larger than the threshold  

---

### 22. [Cost & Performance Optimization — OPTIMIZE with Liquid Clustering vs auto clustering]

Your 100 GB user events table is clustered with Liquid Clustering on `user_id` for fast lookups by user. Over a week of high-volume data ingestion, 10 GB of append-only data has been added (with existing user_id values from prior records). Queries filtering by user_id have become noticeably slow due to data fragmentation. Your team must improve query latency quickly. Should you run `OPTIMIZE` immediately or rely on auto-clustering?

A) Rely on auto-clustering; it runs automatically for Liquid Clustering tables and will optimize overnight  
B) Neither OPTIMIZE nor auto-clustering helps append-only data; partitioning by date is required for performance  
C) Run `OPTIMIZE ZORDER BY user_id`; this reorders the table for better locality  
D) Run `OPTIMIZE` immediately; it compacts small files and respects Liquid Clustering ordering, improving query speed  

---

### 23. [Data Security and Compliance — ABAC row-filter policy driven by governed tag]

Your organization has a customer table with PII (email, phone). Data governance requires that only users with a `data_domain=sales` governed tag can see rows where `department = 'sales'`. Other rows should be hidden. You want to enforce this at the catalog level without per-user access lists. Which approach is correct?

A) Use dynamic SQL rewrite: intercept SELECT statements and inject WHERE department = current_user() filtering  
B) Create a view that filters `WHERE department = user_attribute('data_domain')`; grant SELECT on the view to all users  
C) Create separate tables per department; grant access via Unity Catalog privileges (TABLE_READ) per role  
D) Use ABAC row-filter policy: if user has `data_domain=sales` tag, allow `department = 'sales'` rows; else, hide all rows  

---

### 24. [Data Security and Compliance — Tokenization vs hashing vs generalization for re-identification risk]

You have a dataset of medical records with patient names, ages, and diagnoses. You need to share this with a research partner but minimize re-identification risk. The partner needs to correlate patients across files by a stable identifier. Hashing the name works but is reversible if the hash function is known. What is the best approach?

A) Use hashing with a salt; the partner cannot reverse it without the salt, and you control the salt  
B) Encrypt the names; share the encryption key encrypted; the partner can decrypt locally but cannot re-encrypt for future files  
C) Use tokenization: replace names with opaque tokens; store the mapping in a secure key-value store you control; issue tokens to the partner  
D) Generalize the data: round ages to 5-year bands, redact diagnosis details; this reduces re-identification but loses detail  

---

### 25. [Data Governance — BROWSE privilege and data discovery]

Your data governance team is setting up Unity Catalog access for a new data engineering team. The team needs to explore and discover what data assets exist in the organization (browse catalogs, schemas, and tables) to understand the data landscape and find relevant tables for their projects. However, they should not see actual table data until they request explicit access per table. You want to grant the minimal privilege sufficient for discovery. What privilege should you grant?

A) BROWSE privilege on the metastore; data engineers can discover all objects without query access  
B) SELECT privilege with a row filter that returns zero rows; they can query but see no data  
C) Read Metadata privilege on the catalog; limited to object names and column types, not data  
D) USAGE privilege on the catalog and schema; data engineers can see structure but not data  

---

### 26. [Data Governance — Governed tags vs regular tags for compliance]

Your compliance team requires a data classification scheme: `PII=true/false` and `Sensitivity=high/medium/low`. Governed tags must be managed by the compliance team, and any table not tagged should be treated as PII. Regular tags are optional and can be added by any user. Which tagging approach enforces compliance requirements?

A) Use regular tags for classification dimensions; rely on a dashboard to track untagged tables as PII  
B) Use governed tags for each dimension; require tags at table creation; set default values to PII=true and Sensitivity=high  
C) Use governed tags for PII only (required, managed by compliance team); regular tags for Sensitivity (optional, user-managed)  
D) Use regular tags with automation: a Lakeflow job runs daily, tags all untagged tables as PII=true and Sensitivity=high  

---

### 27. [Debugging and Deploying — databricks bundle validate / deploy error from missing variable]

Your Declarative Automation Bundle defines a job with variable `${var.catalog_name}`. When you run `databricks bundle validate`, it passes. When you run `databricks bundle deploy`, it fails: `Variable 'catalog_name' is not defined`. The variable is not in the bundle YAML. What is the root cause, and how do you fix it?

```yaml
targets:
  dev:
    variables:
      catalog_name: "dev_catalog"
  prod:
    # Missing catalog_name variable

variables:
  catalog_name:
    description: "Target catalog"
```

A) The prod target is missing the `catalog_name` variable definition; add it to the prod target  
B) Run `databricks bundle validate --target prod` to catch this error earlier; it's a validation-time issue  
C) The variable must be set via environment variables (e.g., `DATABRICKS_VAR_CATALOG_NAME`) before deploy  
D) Use `--auto-approve` flag in deploy to skip variable validation  

---

### 28. [Debugging and Deploying — Task retries vs repair run for re-running failed tasks]

A long-running pipeline with 10 sequential tasks failed on task 7 (after 2 hours of execution). You want to re-run only tasks 7–10 without re-running tasks 1–6. You have two options: (1) re-run the job with retries enabled, or (2) submit a repair run. Which approach is most efficient?

A) Manually trigger a new job run; configure the job to skip tasks 1–6 via a conditional flag  
B) Restart from a checkpoint saved after task 6; recovery requires checkpoint support in all tasks  
C) Use task retries; the job will retry task 7 and continue to task 10 without re-running tasks 1–6  
D) Submit a repair run; Databricks will re-run only failed tasks (task 7) and dependent downstream tasks (8–10)  

---

### 29. [Debugging and Deploying — Query profile showing spill to disk and the right fix]

You run a query and notice in the Spark query profile that the `SortExec` operator spilled 500 GB to disk while sorting a 2 GB result set. The query takes 15 minutes (vs. expected 1 minute). Your cluster has 128 GB RAM total. What is the root cause and fix?

A) The SortExec is using more RAM than available; increase executor memory to 256 GB per executor  
B) Insufficient executor memory for the sort buffer; add executors to the cluster (horizontal scaling)  
C) Too many partitions; use `repartition(10)` before the sort to reduce memory pressure  
D) The sort key has high cardinality, causing unbalanced partitions; add a salt column to redistribute data  

---

### 30. [Data Modeling — Event table layout: partition by date vs Liquid Clustering]

Your event table contains 500 billion immutable events (append-only). Events have a timestamp, user_id, and event_type. Queries commonly filter on (1) date ranges (95% of queries) and (2) user_id (50% of queries). You are choosing between partitioning by `date` or using Liquid Clustering on `(date, user_id)`. Which is better for cost and performance?

A) Partition by date; this allows partition pruning for 95% of queries and minimizes file count per partition  
B) Use Liquid Clustering on (date, user_id); this automatically clusters across multiple dimensions and handles append-only efficiently  
C) Partition by date AND use Liquid Clustering on user_id; dual-layering maximizes pruning coverage  
D) Neither; use a Lakebase aggregate table indexed on (date, user_id); queries should hit the aggregate instead  

---

### 31. [Developing Code — Local unit tests with pytest and Databricks Connect]

Your team develops PySpark transformations in Git repositories and must validate them locally before deploying to production. You need to run unit tests using pytest against live Databricks compute (not mocked) to ensure data quality rules match the actual Spark behavior. The tests must run on a developer laptop connected to a corporate VPN, and you must not modify existing tests or add test dependencies. Which approach satisfies these constraints?

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    # Databricks Connect provides this automatically
    return SparkSession.getActiveSession()

def test_data_quality(spark):
    df = spark.sql('SELECT * FROM my_catalog.my_schema.test_table')
    assert df.filter('age <= 0').count() == 0  # All ages must be positive
    assert df.filter('email NOT LIKE "%@%"').count() == 0  # Email validation
```

A) Configure Databricks Connect in the laptop's Python environment, install pytest, write tests that use `spark` fixture from `pyspark.sql`, and run `pytest` directly. Databricks Connect automatically creates a session to remote compute.  
B) Mock all Spark operations using unittest.mock to simulate DataFrame operations locally, import the transformation module, and run pytest with mock fixtures.  
C) Export data as Parquet files, load them with local PySpark, run tests with local compute, and compare results with production runs manually.  
D) Use Databricks Repos to sync code, deploy a test cluster via API, run `databricks runs submit` with pytest as the main module, and parse job logs for test results.  

---

### 32. [Developing Code — Stream-static join semantics]

A Declarative Pipeline joins a streaming table `orders` (source: Kafka) with a slowly-changing `products` lookup table (source: Delta). The join is `orders.product_id = products.product_id`. One week into production, the products table receives a price update (same product_id, new price) but no rows are inserted or deleted. After this update, how do the already-processed orders rows appear in the output?

A) All orders rows are dropped and reprocessed from the Kafka beginning offset to pick up the new price.  
B) Orders rows are re-materialized with the new product price in the next pipeline run because Structured Streaming tracks product_id changes.  
C) Orders rows that were already processed retain the product price from when they were joined; future orders get the new price. The join is immutable to backfill.  
D) The pipeline fails with a schema mismatch because the products table changed after orders were processed.  

---

### 33. [Developing Code — Trigger.AvailableNow for incremental batch processing]

You have a batch job that must process all available data in a Delta table at startup and then process only new rows every minute. You are using Structured Streaming with `Trigger.AvailableNow()`. What is the behavior of the first micro-batch when the job starts? How does this differ from `Trigger.Once()`?

A) Time-windowed processing: `Trigger.AvailableNow()` waits one minute before processing; on first run, it processes zero rows.  
B) Checkpoint-based filtering: `Trigger.AvailableNow()` processes only new rows after any prior checkpoint; no prior checkpoint means zero rows initially.  
C) Full load then incremental: `Trigger.AvailableNow()` processes all available rows up to the current offset on first run, then processes new arrivals on subsequent triggers.  
D) Same as Trigger.Once: `Trigger.Once` and `Trigger.AvailableNow()` have identical semantics; the naming is just a convention difference.  

---

### 34. [Developing Code — File arrival trigger vs table update trigger vs scheduled job]

Your data lake receives CSV files hourly in an S3 prefix. You need to ingest them into a Delta table, run a lightweight transform, and load results into a BI tool. The team prefers minimal operational overhead and immediate feedback on failures. You must choose between: (1) Lakeflow Connect with file arrival trigger, (2) Declarative Pipeline with table update trigger, (3) Lakeflow Job scheduled hourly. Which is best?

A) Lakeflow Connect with file arrival trigger: automatically detects new files and ingests them into a table, with built-in schema inference.  
B) Declarative Pipeline with table update trigger: triggers the pipeline when the source table changes, but this requires pre-staging files into a Delta table first.  
C) Lakeflow Job scheduled hourly: gives explicit control over timing, supports retry logic, and allows mixing batch and stream tasks in one workflow.  
D) File arrival trigger is not available in Databricks; use a scheduled job with `ListFiles` API to poll S3 hourly.  

---

### 35. [Developing Code — Continuous vs triggered pipeline mode]

You are migrating a legacy batch-based data pipeline to Declarative Pipelines. The pipeline must process micro-batches of data every 5 minutes, with no data loss on failure. The team needs simple failure recovery and SLA-driven scheduling. You see two deployment options: continuous mode (runs constantly) and triggered mode (runs on-demand). Which mode should you choose and why?

A) Continuous mode: provides the lowest latency by running constantly; failures automatically recover by restarting the pipeline context.  
B) Triggered mode: each trigger runs as an independent job with scheduled retries; failures are isolated and recovery is orchestrated via the scheduler.  
C) Continuous mode: automatically adapts latency to data arrival rate and ensures consistent micro-batch timing without manual scheduling.  
D) Either mode is equivalent; continuous has lower latency while triggered has lower cost; choose based on team preference.  

---

### 36. [Developing Code — Temporary views / private datasets inside a declarative pipeline]

Inside a Declarative Pipeline, you define a temporary view `temp_cleaned` from a streaming table source, then create another streaming table that selects from this temporary view. The pipeline runs successfully on the first trigger. On the second trigger, the pipeline fails with 'temp_cleaned not found'. What is the root cause of this failure, and how would you fix it?

```python
from pyspark import pipelines as dp

with dp.create_pipeline('test_pipeline') as pipeline:
    pipeline.create_streaming_table('source')
    spark.sql('CREATE OR REPLACE TEMPORARY VIEW temp_cleaned AS SELECT * FROM source')
    pipeline.create_streaming_table(
        'output',
        query='SELECT * FROM temp_cleaned'
    )
```

A) Session persistence: temporary views persist if the source table has data; the failure indicates the source table is empty on the second trigger.  
B) Session scope: temporary views are session-scoped and dropped at session end; each pipeline trigger starts a fresh session without the view.  
C) Syntax issue: Temporary views require explicit registration with `spark.sql.createTempView()` instead of `CREATE OR REPLACE`.  
D) Pipeline limitation: Declarative Pipelines forbid temporary views; replace with a private dataset to persist the logic across triggers.  

---

### 37. [Developing Code — Dynamic value references in job parameters]

You have a Lakeflow Job defined in a bundle that runs daily at 2 AM UTC. The job must pass the previous day's date (YYYY-MM-DD format) to a notebook as a parameter. The notebook endpoint expects the parameter `run_date`. You need to use a dynamic macro syntax that references job metadata. Which parameter value syntax will work?

```yaml
resources:
  jobs:
    daily_job:
      name: daily_etl
      schedule:
        quartz_cron_expression: "0 0 2 * * ?" # 2 AM UTC daily
      tasks:
        - task_key: etl_task
          notebook_task:
            notebook_path: /Users/me/notebook
            base_parameters:
              run_date: "{{job.start_time.iso_date}}"
```

A) `run_date: "{{job.start_time.iso_date}}"` is the correct syntax for the job start time ISO date.  
B) `run_date: "${env.PREVIOUS_DAY}"` requires setting an environment variable in the job scheduler.  
C) `run_date: "{{job.previous_day}}"` uses a built-in Databricks macro for the previous day.  
D) Dynamic date parameters are not supported; the notebook must compute the date using Python `datetime`.  

---

### 38. [Data Ingestion & Acquisition — COPY INTO idempotency and re-runs]

You use `COPY INTO` to ingest JSON files from S3 into a Delta table. On day 1, you load 100 files; on day 2, you run the same `COPY INTO` command and load 50 new files. If you re-run the day 2 command a third time, how many files are ingested in the third run?

```sql
COPY INTO my_catalog.my_schema.target_table
FROM (SELECT * FROM read_files('s3://mybucket/data/'))
FILEFORMAT = JSON
COPY_OPTIONS = ('force' = 'false')
```

A) 0 files; after the second run, all files are deleted from S3 to prevent re-ingestion.  
B) Depends on the table's `tblproperties`; if `COPY_INT_LOADED_FILES_MANIFEST` is set, it tracks files, otherwise it re-loads all.  
C) 0 files; `COPY INTO` with `force='false'` (the default) is idempotent and skips already-loaded files based on a manifest.  
D) 50 files again; `COPY INTO` re-loads all files matching the pattern unless explicitly excluded.  

---

### 39. [Data Ingestion & Acquisition — Making Delta tables readable as Iceberg by an external engine]

Your team uses Databricks for ELT and Snowflake for analytics. You have a Delta table in Databricks and want Snowflake to query it directly without ETL. You enable Iceberg compatibility on the Delta table. What additional configuration must you do so that Snowflake can read the table?

A) Enable Delta Uniform on the table; Snowflake can then query the table as Iceberg via Databricks' published Iceberg metadata format.  
B) Export the Delta table to Iceberg format explicitly using `CONVERT TABLE ... TO ICEBERG`; Delta then supports Iceberg-style reads.  
C) Set up Iceberg REST Catalog endpoint in Snowflake and configure Snowflake credentials to access the same S3 bucket as Databricks.  
D) Use Databricks-to-Snowflake replication; Iceberg compatibility alone does not enable cross-engine queries.  

---

### 40. [Data Ingestion & Acquisition — Lakeflow Connect prerequisites for MySQL and PostgreSQL]

You are setting up Lakeflow Connect to ingest data from a self-managed PostgreSQL database into Databricks. The source database has high transaction volume and you want to capture inserts, updates, and deletes in near real-time. What is a prerequisite you must configure on the PostgreSQL server?

A) Enable logical replication on PostgreSQL by setting `wal_level=logical` in postgresql.conf and creating a replication slot.  
B) PostgreSQL automatically supports CDC via WAL (Write-Ahead Log); no configuration is needed, just credentials.  
C) Set up a dedicated PostgreSQL replica for change capture; Lakeflow Connect cannot read CDC from the primary.  
D) Configure a Debezium connector inside PostgreSQL; Databricks cannot read CDC directly from the database.  

---

### 41. [Data Manipulation — Running total with ROWS window frame]

Your analytics team needs to compute a running total of daily sales amounts within each geographic region, ordered chronologically by date. The analysis should accumulate sales from the first day of each region's records through the current day. You write a window function query. Which window frame specification is correct for this running total calculation?

```sql
SELECT region, sale_date, amount,
  SUM(amount) OVER (
    PARTITION BY region
    ORDER BY sale_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_sales
FROM daily_sales
ORDER BY region, sale_date;
```

A) Incorrect: missing `NULLS LAST` in the ORDER BY clause; the query will fail if sale_date contains any NULL values.  
B) Incorrect: PARTITION BY region is too coarse; it should include `PARTITION BY region, YEAR(sale_date)` to reset annually.  
C) Correct: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` sums from the first row in the partition to the current row, implementing the running total.  
D) Incorrect: should use `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` for correct tie handling in grouped dates.  

---

### 42. [Data Manipulation — MERGE with WHEN NOT MATCHED BY SOURCE]

You have a fact table `sales_fact` that is the source of truth. Quarterly, you receive a new extract from the OLTP system in a staging table `sales_staging`. Your MERGE statement must: (1) insert new records from staging, (2) update matching records if values changed, and (3) delete records in the fact table that are no longer in staging. Which clause handles requirement (3)?

```sql
MERGE INTO my_catalog.my_schema.sales_fact AS t
USING my_catalog.my_schema.sales_staging AS s
ON t.sale_id = s.sale_id
WHEN MATCHED AND t.amount != s.amount THEN UPDATE SET amount = s.amount
WHEN NOT MATCHED THEN INSERT (sale_id, amount, date) VALUES (s.sale_id, s.amount, s.date)
WHEN NOT MATCHED BY SOURCE THEN DELETE;
```

A) `WHEN NOT MATCHED BY TARGET THEN DELETE` — removes rows from the source that do not exist in the target.  
B) A separate `DELETE FROM sales_fact WHERE id NOT IN (SELECT id FROM sales_staging)` statement is required; MERGE does not support this scenario.  
C) `WHEN NOT MATCHED THEN DELETE` — removes rows from the target that do not match the source.  
D) `WHEN NOT MATCHED BY SOURCE THEN DELETE` — removes rows from the target that exist in target but not in source.  

---

### 43. [Data Manipulation — Choosing ai_classify / ai_extract vs ai_query]

You have a CSV file of customer feedback with 100K rows, each containing unstructured text in an `feedback` column. You need to (1) extract the product name mentioned in each feedback and (2) classify the sentiment (positive/negative/neutral). You want minimal latency for querying results. Should you use `ai_classify`, `ai_extract`, or `ai_query` SQL functions?

A) Use `ai_classify` twice: once for sentiment, and again with custom logic to extract product name as a classification task.  
B) Use `ai_query` for all tasks; it is the most flexible function for complex NLP workloads.  
C) Use `ai_query` for extraction and `ai_classify` for sentiment; they are interchangeable for most NLP tasks.  
D) Use `ai_extract` for product name (structured extraction) and `ai_classify` for sentiment (categorization), then materialize results into a table.  

---

### 44. [Data Manipulation — expect_all_or_drop plus quarantine table]

Your data quality pipeline expects all rows to meet certain conditions (e.g., age > 0, email contains '@'). Rows that fail the validation must be quarantined in a separate table for later review and investigation, not dropped from the pipeline. How do you implement this dual-path approach in a Declarative Pipeline?

```python
from pyspark import pipelines as dp

with dp.create_pipeline('test_pipeline') as pipeline:
    # Source table
    pipeline.create_streaming_table('customers')
    
    # Clean rows only
    pipeline.create_streaming_table(
        'customers_clean',
        query='''SELECT * FROM customers
                 WHERE age > 0 AND email LIKE '%@%'''
    )
    
    # Quarantine rows that fail
    pipeline.create_streaming_table(
        'customers_quarantine',
        query='''SELECT * FROM customers
                 WHERE age <= 0 OR email NOT LIKE '%@%'''
    )
```

A) Use `expect_all_or_drop` with auto_quarantine=true; it automatically routes invalid rows to a `{table_name}_invalid` table.  
B) Use `expect_all_or_quarantine` function; Databricks provides this specifically for this use case.  
C) Create a second streaming table with the inverse WHERE clause to select failing rows, then insert into a quarantine table.  
D) Use `expect_all_or_drop` for the clean path only; quarantine is handled separately via MERGE into the quarantine table.  

---

### 45. [Monitoring and Alerting — Serverless cost by user or budget policy in system.billing.usage]

Your organization runs serverless compute jobs across many users and needs to track costs per user to enforce budget controls. You want to identify the top cost consumers and flag users exceeding spending thresholds set by your finance team. Which system table will you query, and how?

```sql
SELECT user_id, SUM(cost) as total_cost, COUNT(*) as job_count
FROM system.billing.usage
WHERE compute_category = 'SERVERLESS_COMPUTE'
  AND usage_date >= CURRENT_DATE - 30
GROUP BY user_id
ORDER BY total_cost DESC;
```

A) Query `system.billing.usage` by user_id and filter on `compute_category = 'SERVERLESS_COMPUTE'`; budget thresholds are stored separately in a budget policy table.  
B) Use `system.admin.jobs_cost` instead; it tracks user-level costs directly for all compute types.  
C) Use `system.compute.serverless` table; it is the authoritative source for serverless consumption by user.  
D) Query `system.billing.usage` grouped by user_id and compute_category; this table contains the definitive billing records for cost governance.  

---

### 46. [Monitoring and Alerting — Listing failed runs with Jobs API or CLI]

Your team runs dozens of Lakeflow Jobs on various schedules. You want to set up a daily alert that discovers all jobs that failed in the last 24 hours and surfaces their error messages and status. The alert must run automatically without manual job enumeration. Which approach is best?

```sql
SELECT job_id, run_id, state, error_message, end_time
FROM system.lakeflow.job_run_timeline
WHERE state = 'FAILED'
  AND end_time >= CURRENT_TIMESTAMP - INTERVAL 1 DAY
ORDER BY end_time DESC;

-- Send this query result to alerting system (e.g., Slack, PagerDuty)
```

A) Use the Databricks CLI to loop through all jobs with `databricks jobs list-runs --job-id <id> --state FAILED` for each.  
B) Query `system.jobs.run_history` with a filter on `state='FAILED'` and `end_time >= CURRENT_TIMESTAMP - INTERVAL 1 DAY`.  
C) Use the REST API `GET /api/2.1/jobs/list-runs` with `state_filter='FAILED'` in a loop across all job IDs.  
D) Use `databricks runs list` globally; it automatically aggregates failed runs from all jobs without looping.  

---

### 47. [Monitoring and Alerting — Data quality monitoring / anomaly detection on a table]

Your data lake includes a daily transaction table with millions of rows ingested from a point-of-sale system. You want to automatically alert if the daily transaction count drops by more than 50% compared to the 30-day rolling average, as this indicates possible upstream data failures. Which Databricks feature is best suited?

A) Predictive Optimization: automatically analyzes table statistics and surfaces anomalies in performance.  
B) Anomaly Detection: in Data Governance, define rules that compare current row counts to statistical baselines and trigger alerts on deviation.  
C) Data Quality Monitoring (DQM): define expectations on row count thresholds and violations trigger alerts.  
D) Custom monitoring: query `system.tables.column_stats` to manually compute rolling averages and schedule alerts via a notebook.  

---

### 48. [Cost & Performance Optimization — Materialized view refresh falling back to full recompute]

You have a materialized view on a large fact table, defined as `SELECT SUM(amount) FROM fact_table WHERE date >= CURRENT_DATE - 30`. The view refreshes incrementally most days, but occasionally it performs a full recompute and takes 2 hours instead of 2 minutes. What is a likely cause?

A) The fact table's clustering key was changed or the table was recreated; Databricks must recompute the view from scratch.  
B) The 30-day window on `date >= CURRENT_DATE - 30` expanded; Databricks detected an unbounded range and re-evaluated.  
C) The materialized view reached a data size threshold and automatically converted to full refresh mode.  
D) A schema change on the fact table (e.g., column rename, data type change) triggered a full refresh as a safety measure.  

---

### 49. [Cost & Performance Optimization — Changing Liquid Clustering keys without rewriting the table]

Your team has a large Delta table (500 GB) with Liquid Clustering on `(date, user_id)`. After analyzing query patterns, you discover that a new workload would be better served by clustering on `(user_id, product_id)` instead. You want to change the clustering key but avoid a full table rewrite due to compute cost and time constraints. Is this possible?

A) No, clustering key changes always require a full table rewrite; there is no escape from this cost.  
B) Yes, use `ALTER TABLE table_name CLUSTER BY` to change the key; new writes use the new key without touching existing data.  
C) Yes, but only if the new key includes all old clustering columns (e.g., you can add `product_id` but not remove `date`).  
D) No, Liquid Clustering keys are immutable after table creation; recreating the table is the only option.  

---

### 50. [Cost & Performance Optimization — Classic autoscaling cluster vs serverless jobs compute for spiky workload]

Your team runs a nightly batch job that processes 500 GB of data in 5 minutes when busy, but varies from 0.5 to 5 GB on weekends. The job must complete within a fixed 1-hour SLA. You have two options: (1) classic autoscaling cluster sized for 5 GB, (2) serverless jobs compute. Which is more cost-effective and why?

A) Classic autoscaling: you pay only for the instances you use, and the cluster scales down on weekends, saving cost.  
B) Serverless: you pay per job run with per-second billing; the spiky workload means you pay exactly for the compute you use.  
C) Classic autoscaling: idle time between runs means you pay for stopped nodes; serverless avoids this.  
D) Serverless: while per-second billing is precise, you also pay Databricks' API overhead fee; classic autoscaling avoids this surcharge.  

---

### 51. [Cost & Performance Optimization — Result cache vs disk cache for dashboard workload]

Your BI dashboard queries a 10 GB fact table with a filter on `date = TODAY()` and runs the same query 50 times per day as users refresh the dashboard. Query execution takes 30 seconds without caching. Should you rely on result cache or disk cache (Photon)?

A) Result cache: since the query is identical and deterministic (same date), the cached result is reused, and subsequent runs return in milliseconds.  
B) Disk cache (Photon): result cache is non-deterministic for time-based filters; disk cache scans filtered data faster.  
C) Result cache is disabled for SQL Warehouse queries; use Photon disk cache instead.  
D) Neither: configure a scheduled materialized view to pre-compute the filtered result and refresh it daily.  

---

### 52. [Data Security and Compliance — Column mask using is_account_group_member]

Your organization has a table `employees` with columns `name`, `salary`, `department`. A compliance requirement mandates that only members of the `finance_team` account group can view actual salary values; all other users should see NULL in that column. You need to enforce this at the table level for all queries. Which masking policy syntax correctly implements this restriction?

```sql
CREATE OR REPLACE MASKING POLICY salary_mask
AS (salary STRING)
RETURNS STRING ->
  CASE WHEN is_account_group_member('finance_team') THEN salary
       ELSE NULL
  END;

ALTER TABLE employees
ALTER COLUMN salary
SET MASKING POLICY salary_mask;
```

A) Correct: `is_account_group_member()` checks if the current user belongs to the account-level group.  
B) Incorrect syntax: the function is `is_group_member()`, not `is_account_group_member()`.  
C) Correct for workspace groups only: `is_workspace_group_member()` should be used for workspace-local groups.  
D) Incorrect: group-based masking is not supported; use `current_user()` and role-based logic instead.  

---

### 53. [Data Security and Compliance — Retention-driven purge with delta.deletedFileRetentionDuration and time travel impact]

Your organization has a table with sensitive personally identifiable information (PII) and must delete records older than 90 days to comply with GDPR regulations. You decide to run `VACUUM` with `delta.deletedFileRetentionDuration` set to 0 days to immediately purge old data files. What is the impact on time travel capabilities for historical queries?

A) Time travel remains enabled for old records; retention duration affects log files only, not query history.  
B) VACUUM fails with an error; Delta enforces a minimum retention period (7 days) to protect time travel.  
C) Time travel is lost for purged records; setting retention to 0 immediately deletes data files and their versions.  
D) Time travel works for recent records only; old records are deleted but you can query recent snapshots before deletion.  

---

### 54. [Data Security and Compliance — Secrets handling (dbutils.secrets.get redaction, secret scope ACLs)]

A developer writes a notebook that reads a database password from a secret scope using `dbutils.secrets.get(scope='prod-secrets', key='db_password')`. The developer then prints the value to the cell output for debugging purposes. Is the password value automatically redacted in the notebook UI cell output, system logs, and audit trails?

```python
password = dbutils.secrets.get(scope='prod-secrets', key='db_password')
print(f"Connecting to database with password: {password}")
display(password)
```

A) Yes, Databricks automatically redacts secret values everywhere: in UI output, system logs, and audit trails.  
B) Only if the variable name contains 'secret' or 'password'; otherwise, values are displayed as plaintext.  
C) No, secret values are not automatically redacted; the developer must manually avoid printing them.  
D) System logs and audit trails redact secrets, but the notebook cell output displays the plaintext value.  

---

### 55. [Data Governance — Permission model with no DENY in Unity Catalog]

In Unity Catalog, you have granted SELECT permission on a schema to a development team group. A specific team member should not have access to one sensitive table in that schema due to a conflict of interest. You cannot use the DENY statement (not available). How do you implement this restriction?

A) REVOKE SELECT from the user at the table level; this overrides the schema-level GRANT to the group.  
B) Apply row-level filters or column masks to the table; these prevent the user from seeing restricted data.  
C) Use row-level security policies in Unity Catalog; they block specific users from accessing entire tables.  
D) Remove the user from the development team group, manage group membership separately with different permission levels.  

---

### 56. [Debugging and Deploying — Spark UI identification of straggler / skewed task]

You observe a query running on a SQL Warehouse that completes in 30 seconds locally but takes 5 minutes in the warehouse. The task graph shows 50 tasks, 49 of which complete in 10 seconds, but one task takes 4.5 minutes. What is the likely cause, and how would you see this in the Spark UI?

A) Task skew: in the Spark UI Stages tab, look at the task timeline; uneven task duration bars indicate one task is processing much more data.  
B) This is normal; SQL Warehouse automatically spills slow tasks to disk, causing latency. Nothing to optimize.  
C) Network contention: in the Spark UI, check the Executor tab for uneven resource allocation; one executor is overloaded.  
D) The straggler task is network-bound; nothing can be done in the query; the warehouse needs hardware upgrade.  

---

### 57. [Debugging and Deploying — Pipeline update failing after incompatible schema change and full refresh implications]

You have a Declarative Pipeline with a streaming table `events` reading from Kafka. You deploy a schema update: the source event schema changes (one field is removed, a new field is added). The pipeline update fails. You decide to perform a full refresh. What are the implications?

A) Full refresh replays all Kafka messages from the beginning; the table schema updates and processing resumes from offset zero.  
B) Full refresh clears the table and restarts the pipeline with the new schema; however, it does not replay Kafka messages already consumed.  
C) Full refresh fails because Kafka offsets are immutable; you must delete the pipeline and recreate it.  
D) Full refresh reprocesses the last N hours of Kafka data (configurable) with the new schema, minimizing data loss.  

---

### 58. [Debugging and Deploying — Git folder branch workflow vs bundle deployment to production]

Your team has two code deployment strategies: (1) Git folders with branch-based workflows (dev -> staging -> main in Git, synced to Databricks), and (2) Declarative Automation Bundles deployed via `databricks bundle deploy` to workspaces. You need code review, rollback capability, and quick rollforward. Which is more suitable?

A) Git folders: branch-based code review in Git is familiar, and rollback is straightforward via Git history revert.  
B) Bundles: deployment state is versioned and bundled; rollback is a `databricks bundle deploy` of a prior version.  
C) Git folders are simpler for code review; bundles add complexity without extra safety.  
D) Choose based on team preference; they provide equivalent operational safety for production deployments.  

---

### 59. [Data Modeling — Unity Catalog metric view definition (measures, dimensions, source)]

Your BI team is building a semantic layer for analytics using metric views in Unity Catalog. You define a metric view over a fact table with columns `date`, `user_id`, `revenue`, `units_sold`. The view aggregates revenue and units by date and user. Which role does each column play: measures, dimensions, or source?

```sql
CREATE METRIC my_schema.daily_revenue
AS (
  SELECT date, user_id, SUM(revenue) as total_revenue, SUM(units_sold) as total_units
  FROM my_table
  GROUP BY date, user_id
);
ALTER METRIC my_schema.daily_revenue
SET MEASURES = [total_revenue, total_units]
SET DIMENSIONS = [date, user_id];
```

A) Measures: `total_revenue`, `total_units` (aggregates). Dimensions: `date`, `user_id` (grouping columns). Source: `my_table`.  
B) Measures: `date`, `user_id` (identifiers). Dimensions: `total_revenue`, `total_units` (aggregates).  
C) All columns are measures; dimensions are automatically inferred from GROUP BY and do not need explicit definition.  
D) Metric views treat all columns equally; measures and dimensions are not distinguished in the definition.  

---

### 60. [Data Modeling — Star schema served through materialized views with clustering on fact table]

You have a star schema with a `dim_products` dimension and a `fact_sales` fact table (1 billion rows). You create a materialized view that joins the two and filters by `fact_sales.country = 'US'`. You apply Liquid Clustering to `fact_sales` on `(country, product_id)`. When the materialized view queries the fact table, is the clustering key used to speed up the filter?

A) Yes, the clustering on `(country, product_id)` is used during the materialized view query to prune data blocks where `country != 'US'`.  
B) No, clustering is only used for table scans directly from the fact table; materialized views bypass clustering optimization.  
C) Yes, but only if the materialized view explicitly references the clustering columns in the SELECT list.  
D) No, clustering only helps with range queries (`WHERE col BETWEEN`); equality filters like `country='US'` are not optimized by clustering.  

---

## Answer Key and Explanations

**1. Answer: D**

Using flexible version ranges in setup.py allows pip to resolve compatible versions during wheel installation without forcing reinstalls or hardcoding logic. This balances dependency management with operational simplicity, letting the wheel work across clusters with different pre-installed versions.

- **A:** Incorrect: --force-reinstall can break other pinned dependencies and adds brittle init-script logic.
- **B:** Incorrect: Skipping on version mismatch fails to handle the root cause and leaves the job incomplete.
- **C:** Incorrect: Manual cluster library management breaks the wheel's portability and requires manual intervention before every run.
- **D:** Correct: Flexible version ranges in setup.py let pip resolve dependencies at install time, avoiding both conflicts and operational overhead.

References: <https://docs.databricks.com/aws/en/dev-tools/bundles/python-wheel>

**2. Answer: D**

Streaming Tables use streaming semantics and are purpose-built for append-only feeds with incremental state management and auto-restart on failure. Materialized Views recompute from scratch on schedule, which loses event ordering and causes duplicates or gaps in continuously-flowing streams.

- **A:** Incorrect: Materialized Views with daily schedule cannot capture events every 5 seconds and will miss or duplicate data.
- **B:** Incorrect: Materialized Views recompute from scratch on schedule, not on every event, missing the real-time requirement.
- **C:** Incorrect: Manual checkpoint management adds operational overhead and is not needed for append-only scenarios with auto-healing.
- **D:** Correct: Streaming Tables maintain incremental state, process events in order, and auto-restart on failure—ideal for real-time append-only feeds.

References: <https://docs.databricks.com/aws/en/data-engineering/tables-views>

**3. Answer: D**

AUTO CDC automatically handles out-of-order records when you specify SEQUENCE BY with the source timestamp. This tells the CDC engine how to order events, allowing it to reorder stale updates even if they arrive after deletes. This maintains consistency without manual complexity.

- **A:** Incorrect: Unnecessary complexity; AUTO CDC with proper SEQUENCE BY already handles out-of-order events correctly.
- **B:** Incorrect: Disabling CDC automation and manually handling deletes negates the benefit of AUTO CDC and increases maintenance.
- **C:** Incorrect: Watermarks are for stateful processing, not for ordering CDC events; AUTO CDC's SEQUENCE BY is the right tool.
- **D:** Correct: SEQUENCE BY with source timestamp lets AUTO CDC reorder out-of-order updates and deletes, maintaining consistency.

References: <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table-auto-cdc>

**4. Answer: D**

Iterator-based Pandas UDFs load the model once per Python process, then process multiple batches using vectorized Pandas operations. This minimizes model loading overhead while maximizing throughput through vectorization, unlike row-at-a-time Python UDFs.

- **A:** Incorrect: Standard Python UDFs reload the model for every row, causing 10M model loads and severe performance degradation.
- **B:** Incorrect: UDTFs are for returning multiple rows; this doesn't leverage Spark's partition-level model caching.
- **C:** Incorrect: Broadcasting the model helps, but doesn't address the UDF type; still requires a UDF that can use the broadcast efficiently.
- **D:** Correct: Iterator Pandas UDFs load the model once per Python process and score batches vectorized, minimizing overhead.

References: <https://docs.databricks.com/aws/en/pyspark/reference/functions/pandas_udf>

**5. Answer: C**

Bundle targets allow you to define a single bundle with target-specific overrides for `run_as`, variables, and resource names. This eliminates duplication and drift while maintaining separate security boundaries for each environment.

- **A:** Incorrect: Runtime catalog selection is orthogonal to identity/permissions; SQL logic should not encode role selection.
- **B:** Incorrect: Duplicating bundles increases maintenance burden and risk of configuration drift between DEV and PROD.
- **C:** Correct: Bundle targets support per-environment `run_as` overrides, eliminating duplication while maintaining security boundaries.
- **D:** Incorrect: Manual UI edits bypass CI/CD and create untrackable configuration drift.

References: <https://docs.databricks.com/aws/en/dev-tools/bundles/run-as>

**6. Answer: D**

Adding columns with defaults preserves checkpoint state while extending the schema. Renaming stateful operation columns breaks schema mapping, as checkpoint state is matched by column name. Schema evolution requires that you keep existing columns unchanged.

- **A:** Incorrect: Renaming output columns breaks the schema mapping of checkpoint state; fields are matched by name.
- **B:** Incorrect: Dropping columns and starting a new checkpoint forfeits the benefit of restarting from existing state.
- **C:** Incorrect: Changing aggregation keys alters state semantics and invalidates prior checkpoint state.
- **D:** Correct: Adding columns with defaults preserves state schema compatibility and allows safe restart from existing checkpoint.

References: <https://docs.databricks.com/aws/en/stateful-applications/schema-evolution>

**7. Answer: C**

Using dropDuplicatesWithinWatermark() combined with withWatermark() deduplicates only within the watermark window (1 hour), then automatically discards old state—cost-effective and optimal for stateful deduplication. Data older than the watermark window is dropped from state automatically.

- **A:** Incorrect: dropDuplicates() without watermark stores all events in state, causing unbounded memory growth.
- **B:** Incorrect: Manual state management adds operational overhead; the built-in operator is simpler and better optimized.
- **C:** Correct: dropDuplicatesWithinWatermark() deduplicates within the watermark window and automatically ages out old state.
- **D:** Incorrect: window().first() is a workaround; dropDuplicatesWithinWatermark() is the native dedup operator.

References: <https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/dropDuplicates>

**8. Answer: D**

File notification mode scales O(1) per new file, decoupling ingestion latency from bucket size. Directory listing scans the entire bucket every interval, scaling O(n) which becomes prohibitively slow and expensive with 100k+ files. File notifications include built-in retry semantics.

- **A:** Incorrect: Directory listing scans all 100k+ files per poll, causing O(n) latency and cost that worsens as files accumulate.
- **B:** Incorrect: Local caching doesn't reduce S3 scan cost; still O(n) per hour, and cache invalidation complicates logic.
- **C:** Incorrect: Hybrid approach is operationally complex and doesn't address the O(n) scaling problem of directory listing.
- **D:** Correct: File notifications decouple ingestion from bucket size; each file triggers one SNS message, scaling O(1) per file.

References: <https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-notification-mode>

**9. Answer: C**

Structured Streaming with Kinesis provides sub-5-second latency through automatic checkpoint management and shard rebalancing. Kinesis connectors handle at-least-once semantics, which is appropriate for analytics where duplicate counting is acceptable. Spark's built-in operators eliminate manual checkpoint tracking.

- **A:** Incorrect: Manual checkpoint management for Kinesis requires custom shard tracking logic and is error-prone on rebalancing.
- **B:** Incorrect: Batching every second adds artificial 1–2 sec latency and complicates checkpoint recovery.
- **C:** Correct: Structured Streaming handles Kinesis shard tracking and checkpoints automatically with at-least-once semantics.
- **D:** Incorrect: Kinesis Firehose buffers for 1–2 minutes, exceeding the <5 sec latency requirement.

References: <https://docs.databricks.com/aws/en/connect/streaming/kinesis/>

**10. Answer: D**

Change Data Feed must be enabled to capture INSERT/UPDATE/DELETE history. CDF tracks row-level changes and works with Delta Sharing for replay, allowing audit recipients to access both current snapshots and historical change records within the retention window.

- **A:** Incorrect: ABAC controls access; a separate audit table adds complexity and doesn't leverage CDF.
- **B:** Incorrect: Time-travel snapshots provide point-in-time views but not structured change history needed for lineage audits.
- **C:** Incorrect: Predictive Optimization auto-tunes performance; it does not enable change tracking or Delta Sharing.
- **D:** Correct: Change Data Feed + Delta Sharing preserves full change history (INSERTs, UPDATEs, DELETEs) for audit and replay.

References: <https://docs.databricks.com/aws/en/tables/features/change-data-feed>

**11. Answer: B**

Clean Rooms execute queries in a sandboxed workspace where neither party sees raw rows—only aggregated or filtered results. This prevents row-level data exfiltration while allowing collaborative analysis. Encryption and approval workflows are administrative controls, not unique guarantees.

- **A:** Incorrect: Both Clean Rooms and raw sharing use encrypted transit; the key difference is query isolation, not encryption.
- **B:** Correct: Clean Rooms isolate query execution and aggregate results, preventing row-level data disclosure.
- **C:** Incorrect: Approval workflows are governance controls, not a unique Clean Room guarantee about data privacy.
- **D:** Incorrect: Clean Rooms do not replicate data to third parties; they execute queries in the workspace's isolated compute.

References: <https://docs.databricks.com/aws/en/clean-rooms/create-clean-room>

**12. Answer: B**

LEFT ANTI JOIN returns unmatched rows from the left table. For invalid events, apply LEFT ANTI from facts to dimension. For customers with no events, apply LEFT ANTI from dimension to facts. This avoids duplicates and is highly efficient for both queries.

- **A:** Incorrect: FULL OUTER JOIN after LEFT OUTER produces row duplication and requires post-deduplication logic.
- **B:** Correct: LEFT ANTI from facts finds unmatched facts (invalid events); LEFT ANTI from dimension finds unmatched dimension rows.
- **C:** Incorrect: FULL OUTER JOIN still scans all rows from both tables; anti-joins are far more efficient for finding unmatched sets.
- **D:** Incorrect: NOT IN subquery is less efficient than anti-join and requires nested query evaluation.

References: <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-join>

**13. Answer: B**

Use variant_explode() to flatten nested arrays into individual rows, then apply schema_of_variant() on each element to discover its schema. This approach discovers field schemas at the element level across all records and enables schema merging.

- **A:** Incorrect: schema_of_variant() on the array returns the array schema, not individual element schemas.
- **B:** Correct: variant_explode() flattens the array, then schema_of_variant() on each row discovers field schemas.
- **C:** Incorrect: to_json() with from_json() defeats the discovery goal and requires manual maintenance.
- **D:** Incorrect: String manipulation loses nested structure and type safety; VARIANT operators are safer and more efficient.

References: <https://docs.databricks.com/aws/en/pyspark/reference/functions/schema_of_variant>

**14. Answer: B**

Wrapping ai_query() in a UDF with TRY-CATCH allows graceful error handling—return NULL for failures and log for debugging. Micro-batching ensures faster retry cycles and resource sharing. This approach prevents cascading failures and provides visibility.

- **A:** Incorrect: No error handling; timeouts or LLM refusals will crash the entire DataFrame operation.
- **B:** Correct: TRY-CATCH in a UDF handles errors gracefully; micro-batches enable faster retries and resource sharing.
- **C:** Incorrect: AQE optimizes query plans, not LLM retry logic; mixing concerns complicates debugging.
- **D:** Incorrect: Silent fallback to defaults hides failures and prevents root-cause analysis.

References: <https://docs.databricks.com/aws/en/pyspark/reference/functions/ai_query>

**15. Answer: C**

system.compute.node_timeline provides per-node utilization metrics (CPU %, memory %, I/O) at 1-minute granularity, ideal for identifying anomalies like sustained high CPU. This table captures resource utilization data per instance over time.

- **A:** Incorrect: account_usage_core shows billing/credits, not per-node utilization metrics.
- **B:** Incorrect: cluster_events logs lifecycle changes, not ongoing CPU/memory metrics.
- **C:** Correct: node_timeline provides per-node CPU, memory, and I/O utilization for real-time anomaly detection.
- **D:** Incorrect: query_history logs SQL query execution, not cluster-level compute resource utilization.

References: <https://docs.databricks.com/aws/en/admin/system-tables/compute>

**16. Answer: C**

correct: Lakeflow Declarative Pipelines emit `FLOW_PROGRESS` events that track record counts and table updates. The `event_log()` TVF provides detailed visibility into which stage is losing records. queries SQL history but doesn't capture pipeline-specific metrics. manual and slow. conflates schema changes with data loss—they are separate issues.

- **A:** Incorrect: Manual table inspection is slow and doesn't show stage-by-stage record flow.
- **B:** Incorrect: SQL query history doesn't capture pipeline-specific metrics like record count transitions.
- **C:** Correct: `event_log()` with `FLOW_PROGRESS` events tracks record counts through each pipeline stage.
- **D:** Incorrect: Schema mismatches would cause errors, not silent data loss; `event_log()` is better for record-count tracking.

References: <https://docs.databricks.com/aws/en/admin/system-tables/>

**17. Answer: D**

Databricks Job Health Rules track historical percentile metrics (such as p95 duration) over a rolling window and alert only when the trend crosses the threshold, not on every spike. Alerting on single spikes generates too many false positives, and task timeouts are a job-failure mechanism rather than a monitoring alert.

- **A:** Incorrect: Simple duration alerts fire on every run that exceeds the threshold, causing alert fatigue from single spikes.
- **B:** Incorrect: A manual query-based alerting job is operationally complex and loses the benefits of built-in health rules.
- **C:** Incorrect: Task timeouts cause job failures, not monitoring alerts; doesn't differentiate between spikes and trends.
- **D:** Correct: Job Health Rules track p95 duration over a rolling window, alerting only on sustained trend changes.

References: <https://docs.databricks.com/aws/en/admin/system-tables/jobs>

**18. Answer: B**

correct: with 60% idle time, your reserved capacity is already paid for. Using Photon to reduce query time from 2 min to 40 sec (3x speedup) returns the warehouse to idle faster, allowing the next job to start sooner and reducing overall job queue time. The 20% compute increase is negligible since capacity is reserved. ignores the benefit of faster query completion in a shared system. adds operational complexity. too absolutist—Photon doesn't always reduce cost without considering idle capacity.

- **A:** Incorrect: With 60% idle capacity already paid, Photon speedup reduces queue time and improves throughput more than cost.
- **B:** Correct: Reserved capacity is fixed-cost; Photon's 3x speedup reduces query time and frees capacity for other jobs.
- **C:** Incorrect: Toggling Photon mode adds operational overhead; reserved capacity cost is fixed regardless of mode.
- **D:** Incorrect: Photon reduces cost in shared systems but not universally; depends on idle capacity and job concurrency.

References: <https://docs.databricks.com/aws/en/compute/photon>

**19. Answer: C**

correct: increasing the indexed columns (up to 64) and ensuring `user_id` is in the skipping columns list improves filter selectivity on both partitioning and clustering dimensions. reduces coverage. disables the optimization. contradicts the goal. Data skipping stats are checked at file-level granularity; more indexed columns = better pruning.

- **A:** Incorrect: Fewer indexed columns reduces skipping coverage; you'd skip on date but not user_id.
- **B:** Incorrect: Explicit filtering in SQL doesn't replace file-level data skipping statistics.
- **C:** Correct: More indexed columns + ensuring both `date` and `user_id` are tracked improve file skipping selectivity.
- **D:** Incorrect: Increasing indexed columns and simultaneously reducing stats columns contradicts the optimization goal.

References: <https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize>

**20. Answer: D**

correct: functions like `YEAR(date_col)` prevent Spark's partition pruner from evaluating the partition key directly. Rewriting as a range filter (`date_col >= '2024-01-01' AND date_col < '2025-01-01'`) allows Spark to prune partitions before scanning. requires schema changes unnecessarily. true by default in modern Spark. adds unnecessary duplication.

- **A:** Incorrect: Adding a separate partition column doubles storage and complicates maintenance.
- **B:** Incorrect: Spark handles date types correctly; the issue is the function, not the data type.
- **C:** Incorrect: Partition pruning is enabled by default; the real issue is the YEAR() function.
- **D:** Correct: Functions on partition columns defeat pruning; rewrite as range filters to enable partition elimination.

References: <https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize>

**21. Answer: C**

correct: with AQE enabled, you can fine-tune skew join handling. A lower `skewJoin.skewFactor` (2x instead of 5x) detects skew more aggressively and splits skewed partitions earlier. a manual salt workaround that AQE makes unnecessary. relies on defaults, which may not detect the 90% skew. (broadcast) only works if the dimension fits in memory and doesn't scale.

- **A:** Incorrect: Default skewFactor (5x) may not detect 90% skew; manual tuning improves sensitivity.
- **B:** Incorrect: Manual salting works but adds query logic; AQE's automatic skew detection is simpler.
- **C:** Correct: AQE + lower skewFactor (2x) detects the 90% skew and splits partitions for better balance.
- **D:** Incorrect: Broadcast join requires the dimension to fit in memory; doesn't scale with large dimensions.

References: <https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize>

**22. Answer: D**

Run OPTIMIZE immediately on Liquid Clustering tables to compact small files and improve query speed. Liquid Clustering automatically applies clustering to newly appended data, so OPTIMIZE respects the existing clustering order. Auto-clustering runs on a schedule and cannot provide immediate relief. ZORDER is legacy; Liquid Clustering + OPTIMIZE is the modern approach.

- **A:** Auto-clustering relies on scheduling, not immediate performance improvement.
- **B:** Liquid Clustering handles append-only data efficiently; partitioning is not required.
- **C:** ZORDER is legacy; Liquid Clustering + OPTIMIZE is the modern, simpler approach.
- **D:** Correct: OPTIMIZE compacts files and respects Liquid Clustering ordering for append-only data.

References: <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/clustering>

**23. Answer: D**

Attribute-Based Access Control (ABAC) row-filter policies are purpose-built for dynamic row-level access based on governed tags. The policy engine evaluates user tags at query time and returns only matching rows, enforcing rules like 'if user has data_domain=sales tag, show department=sales rows'. Views with user_attribute() are a workaround; native ABAC policies are more robust and scalable. Table duplication breaks data consolidation. Dynamic SQL rewriting is complex and error-prone.

- **A:** Dynamic SQL rewrite is complex and error-prone; ABAC policies are the standard approach.
- **B:** Views with user_attribute() are a workaround; native ABAC policies are more robust and scalable.
- **C:** Table duplication per department breaks data consolidation and increases maintenance.
- **D:** Correct: ABAC row-filter policies link governed tags to row filters, enforcing access at query time.

References: <https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies>

**24. Answer: C**

Tokenization provides an opaque, stable identifier (tokens can be reused per patient across files for correlation) while keeping the mapping (name → token) in a secure store you control. The research partner never sees names or the mapping. Salted hashing is deterministic but potentially reversible with sufficient compute; tokenization is more opaque. Encryption is not de-identification; re-identification risk remains if the mapping is exposed. Generalization reduces re-identification risk but doesn't provide the stable cross-file identifier needed for correlation.

- **A:** Salted hashes are deterministic but potentially reversible with sufficient compute; tokenization is more opaque.
- **B:** Encryption is not de-identification; it's data confidentiality. Re-identification risk remains if mapping is exposed.
- **C:** Correct: Tokenization provides stable identifiers for correlation while keeping the mapping private.
- **D:** Generalization reduces re-identification risk but doesn't provide the stable cross-file identifier needed.

References: <https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference>

**25. Answer: A**

BROWSE privilege allows data engineers to traverse the catalog hierarchy and discover objects (catalogs, schemas, tables, columns) without granting query access. This is the minimal privilege for data discovery. USAGE on schema/catalog also works but is less explicit about the discovery intent. Granting SELECT (even with zero rows) grants query capability, exceeding the minimum requirement. Read Metadata is similar to BROWSE but BROWSE is the standard term in Unity Catalog.

- **A:** Correct: BROWSE privilege provides catalog navigation and discovery without query access.
- **B:** Granting SELECT (even with zero rows) grants query capability, exceeding discovery needs.
- **C:** Read Metadata is similar but less standard; BROWSE is the Unity Catalog term for discovery.
- **D:** USAGE allows object creation/modification; BROWSE is more minimal for discovery-only.

References: <https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference>

**26. Answer: B**

Governed tags enforce mandatory classification with compliance-controlled values and strict management by the compliance team. By making tags mandatory and governing their allowed values, any table must be explicitly tagged with PII and Sensitivity classifications, meeting the compliance requirement. Regular tags are optional and cannot enforce classification. Mixing governed and regular tags reduces governance clarity. Automation is reactive; untagged tables could be accessed before automation runs.

- **A:** Regular tags are optional; untagged tables would not be automatically classified as PII.
- **B:** Correct: Governed tags enforce mandatory classification with compliance-controlled values and strict management.
- **C:** Mixing governed and regular tags reduces governance clarity and doesn't enforce the Sensitivity dimension.
- **D:** Automated tagging is reactive; untagged tables could be accessed before automation runs.

References: <https://docs.databricks.com/aws/en/admin/governed-tags/manage-governed-tags>

**27. Answer: A**

correct: the prod target is missing the `catalog_name` variable. Validate passes because variables are optional in the schema, but deploy fails because the prod target uses `${var.catalog_name}` without defining it. Adding the variable to the prod target fixes it. a good practice but doesn't solve the root cause. would be a workaround but not the intended design. skips validation, hiding the real issue.

- **A:** Correct: The prod target lacks the `catalog_name` variable definition; adding it fixes the deploy error.
- **B:** Incorrect: `validate --target prod` would catch this, but it doesn't explain the root cause.
- **C:** Incorrect: Environment variables are a fallback; the bundle should define variables per target.
- **D:** Incorrect: `--auto-approve` skips approval steps, not validation; it would not fix the missing variable.

References: <https://docs.databricks.com/aws/en/dev-tools/bundles/pipelines-tutorial>

**28. Answer: D**

correct: repair runs are designed for exactly this—they re-run only failed tasks and their downstream dependencies, skipping already-successful tasks. requires manual conditional logic in each task. requires checkpoint infrastructure that may not exist. (task retries) retries the failed task but typically within a single attempt, not for re-running downstream tasks.

- **A:** Incorrect: Conditional skipping in SQL/Python is error-prone and manual; repair runs are the standard approach.
- **B:** Incorrect: Repair runs don't require checkpoints; they track task success/failure in the job run metadata.
- **C:** Incorrect: Task retries retry a single task; they don't skip successful upstream tasks.
- **D:** Correct: Repair runs re-run only failed tasks (task 7) and downstream dependents (8–10), skipping 1–6.

References: <https://docs.databricks.com/aws/en/jobs/repair-job-failures>

**29. Answer: C**

correct: excessive partitions cause each partition to be sorted independently in memory. With 2 GB output across many partitions, each executor's sort buffer fills up, forcing spill. Repartitioning to fewer partitions (e.g., 10) allows each partition to be larger and fit in memory per executor. assumes insufficient executor memory (usually 50+ GB per executor in modern clusters). (horizontal scaling) doesn't reduce per-partition memory use. (salt) addresses skew, not partitioning pressure.

- **A:** Incorrect: 128 GB cluster RAM is sufficient for a 2 GB sort; memory pressure is usually from high partitions.
- **B:** Incorrect: Adding executors doesn't reduce per-partition memory; each executor sorts its assigned partitions independently.
- **C:** Correct: Too many partitions cause per-partition sorts to spill; fewer partitions fit more data per partition in memory.
- **D:** Incorrect: Salt columns are for addressing skew, not spill-to-disk from partitioning.

References: <https://docs.databricks.com/aws/en/sql/user/queries/query-profile>

**30. Answer: B**

Liquid Clustering on (date, user_id) handles both common filter dimensions efficiently without manual partitioning, automatically clustering new appends and avoiding partition maintenance and small-file issues. Date-only partition pruning misses the user_id filter, combining partitioning with clustering adds dual-layer management complexity, and a pre-aggregated table helps aggregations but not row-level event lookups.

- **A:** Incorrect: Partitioning only on date helps with 95% of queries but not user_id filters; Liquid Clustering is more flexible.
- **B:** Correct: Liquid Clustering on both dimensions handles both query patterns and auto-clusters appends efficiently.
- **C:** Incorrect: Partition + Liquid Clustering adds maintenance; Liquid Clustering alone is sufficient and simpler.
- **D:** Incorrect: Aggregate tables are for pre-computed metrics, not row-level event queries.

References: <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/clustering>

**31. Answer: A**

Databricks Connect enables pytest to run against live Databricks compute by providing a `spark` context that communicates with a remote cluster. Mocking the data loses real Spark semantics. Manual export-and-compare is fragile and doesn't scale. Remote cluster management adds unnecessary overhead.

- **A:** Correct: Databricks Connect provides a remote Spark session to pytest tests without modifying test code or adding mocks.
- **B:** Incorrect: Mocking Spark behavior defeats the purpose of validating against actual Spark semantics and data distribution.
- **C:** Incorrect: Manual export-and-compare is not automated, does not scale to frequent test runs, and breaks the constraint of not modifying tests.
- **D:** Incorrect: While viable, this approach requires managing test cluster lifecycle and parsing logs, which is more complex than Databricks Connect.

References: <https://docs.databricks.com/aws/en/dev-tools/databricks-connect/cluster-config>

**32. Answer: C**

Structured Streaming joins are deterministic per micro-batch: a streaming row joined with a static row produces one output row, and that row is not retroactively joined again. The static table snapshot is re-read on the next micro-batch for new streaming rows only. Updating the static table does not trigger backfill of old rows.

- **A:** Incorrect: The pipeline does not replay Kafka; Structured Streaming checkpoints ensure offsets are tracked and rows are not duplicated.
- **B:** Incorrect: Structured Streaming does not automatically re-join historical rows when the static side changes; it processes each new micro-batch independently.
- **C:** Correct: Stream-static join semantics are immutable per row; updates to the static table only affect future micro-batches.
- **D:** Incorrect: A non-schema change (value update) does not cause a schema mismatch or pipeline failure.

References: <https://docs.databricks.com/aws/en/ldp/concepts/streaming-tables>

**33. Answer: C**

`Trigger.AvailableNow()` is a one-time, finite-duration trigger designed for incremental batch processing. On first run, it processes all available data up to the current offset (no prior checkpoint required). On subsequent runs, it processes new arrivals since the last checkpoint. `Trigger.Once()` stops after a single run, while `Trigger.AvailableNow()` can be invoked repeatedly.

- **A:** Incorrect: `Trigger.AvailableNow()` does not use a fixed time window; it processes all available rows immediately on first invocation.
- **B:** Incorrect: `Trigger.AvailableNow()` does not require prior state; on first run it consumes all current data, not zero rows.
- **C:** Correct: First run processes all available data; subsequent runs process new arrivals, enabling efficient incremental batch workflows.
- **D:** Incorrect: `Trigger.AvailableNow()` and `Trigger.Once()` have different semantics; AvailableNow repeats while Once stops after one run.

References: <https://docs.databricks.com/aws/en/structured-streaming/triggers>

**34. Answer: A**

File arrival triggers in Lakeflow Connect are the most appropriate for this scenario: they monitor S3 for new files, trigger ingestion automatically, and handle schema inference. Pre-staging into Delta adds complexity and is unnecessary. Scheduled jobs introduce latency between file arrival and processing. External polling is not the natural choice for Databricks ingestion.

- **A:** Correct: File arrival trigger is designed for this use case: automatic detection, minimal overhead, and immediate feedback.
- **B:** Incorrect: Pre-staging adds complexity and is less immediate; table update triggers serve downstream pipeline changes.
- **C:** Incorrect: While viable, scheduled jobs do not provide immediate feedback on file arrival and add fixed latency.
- **D:** Incorrect: File arrival triggers are available in Lakeflow Connect and are the natural choice here.

References: <https://docs.databricks.com/aws/en/jobs/file-arrival-triggers>

**35. Answer: B**

Triggered mode is ideal for this scenario. Each trigger runs independently, so failures are isolated and retries can be managed via the orchestration layer (Lakeflow Jobs). Continuous mode runs a single long-lived context, making failure recovery complex and less predictable. Triggered mode also provides better cost efficiency and audit trails for compliance.

- **A:** Incorrect: Continuous mode runs a single context; restarting on failure is complex and may lose recent state.
- **B:** Correct: Triggered mode isolates each run, enabling simple retry logic and predictable failure handling via external scheduling.
- **C:** Incorrect: Both modes process data at their scheduled cadence; continuous does not inherently adapt better to arrival rates.
- **D:** Incorrect: The modes differ significantly in failure recovery and operability, not just cost; choice directly impacts SLA compliance.

References: <https://docs.databricks.com/aws/en/data-engineering/procedural-vs-declarative>

**36. Answer: B**

Temporary views are session-scoped and live only for the duration of the current Spark session. Each pipeline trigger executes in a new session, so temporary views created in one run are dropped when the session ends. The second trigger starts a fresh session and cannot find the view. To persist logic across triggers, use private datasets (Delta tables in the pipeline's namespace) or reference tables directly without intermediate views.

- **A:** Incorrect: Session persistence does not depend on source data; the issue is that sessions are isolated per pipeline trigger.
- **B:** Correct: Temporary views are dropped when the session ends; each trigger begins with no temporary views defined.
- **C:** Incorrect: The syntax shown is valid; the root cause is session scope, not syntax issues.
- **D:** Incorrect: Temporary views are allowed but are the wrong pattern; private datasets or direct table references are the correct fix.

References: <https://docs.databricks.com/aws/en/data-engineering/procedural-vs-declarative>

**37. Answer: A**

`{{job.start_time.iso_date}}` is the correct macro syntax for Databricks bundle job parameters. It resolves to the ISO date (YYYY-MM-DD) of the job's start time. The job runs at 2 AM, so `job.start_time` reflects that time; the notebook can subtract one day in code if the previous day is specifically needed. No built-in previous_day macro exists.

- **A:** Correct: This is the proper macro syntax for accessing job start time as an ISO date in Databricks bundles.
- **B:** Incorrect: Environment variables set by the scheduler are not the standard pattern for Databricks job parameters.
- **C:** Incorrect: No built-in `{{job.previous_day}}` macro exists; use `job.start_time` and compute offsets in code.
- **D:** Incorrect: Dynamic macros are fully supported; using them is simpler than computing dates in the notebook.

References: <https://docs.databricks.com/aws/en/dev-tools/bundles/job-parameters>

**38. Answer: C**

`COPY INTO` maintains an internal manifest of loaded files (when `force='false'`). On re-runs, it compares incoming files against the manifest and skips already-loaded files. So the third run ingests 0 new files. This idempotency is a key feature of `COPY INTO` for safe re-runs.

- **A:** Incorrect: Files are not deleted from S3; the manifest tracking is internal to Databricks.
- **B:** Incorrect: The manifest is automatic with `force='false'`; no explicit table property is needed.
- **C:** Correct: `COPY INTO` tracks loaded files and re-runs against the same source skip already-processed files.
- **D:** Incorrect: Re-loading all files is what happens with `force='true'`; the default prevents re-load.

References: <https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into>

**39. Answer: A**

Delta Uniform (also called Delta-Iceberg compatibility) allows Databricks to write Delta tables with Iceberg metadata side-by-side. Snowflake and other external engines can read the Iceberg metadata to query the underlying data files. This is simpler than full conversion and maintains Delta as the primary format on Databricks. Converting to native Iceberg changes the primary format and loses Delta features. Catalog setup is more complex. Replication is less direct than Uniform.

- **A:** Correct: Delta Uniform publishes Iceberg metadata alongside Delta; external engines read Iceberg metadata to access the same data files.
- **B:** Incorrect: Converting to Iceberg changes the primary format and loses some Delta features; Uniform is the better approach for external read-only access.
- **C:** Incorrect: While an Iceberg REST Catalog could work, it requires complex setup; Delta Uniform is simpler.
- **D:** Incorrect: Delta Uniform does enable cross-engine queries without replication.

References: <https://docs.databricks.com/aws/en/delta/iceberg-reads>

**40. Answer: A**

PostgreSQL requires logical replication to be enabled (`wal_level=logical`) and a replication slot to be created for Lakeflow Connect to capture changes. This is a prerequisite that must be done by a database administrator. While WAL exists on all PostgreSQL instances, logical replication must be explicitly enabled; it is not automatic. A replica is not required; Lakeflow Connect can read CDC from the primary. Lakeflow Connect has native PostgreSQL CDC connectors; Debezium is not required.

- **A:** Correct: Logical replication must be enabled and a replication slot created for Lakeflow Connect to capture CDC from PostgreSQL.
- **B:** Incorrect: While WAL exists on all PostgreSQL instances, logical replication must be explicitly enabled; it is not automatic.
- **C:** Incorrect: A replica is not required; Lakeflow Connect can read CDC from the primary using replication slots.
- **D:** Incorrect: Lakeflow Connect has native PostgreSQL CDC connectors; Debezium is not required.

References: <https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/cdc-overview>

**41. Answer: C**

The query correctly uses `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` to compute a row-based cumulative sum. For each row, it sums from the first row in its region partition to the current row. This implements the running total from the first day of the region to today. `RANGE` would also work but is not necessary; `ROWS` is more direct for this requirement.

- **A:** Incorrect: NULL handling in ORDER BY is automatic; explicit NULLS LAST is only needed for specific ordering preferences.
- **B:** Incorrect: A region-level running total (resetting per region, not per year) matches the requirement exactly.
- **C:** Correct: ROWS frame implements cumulative sum from the partition start to the current row, as required.
- **D:** Incorrect: ROWS is more appropriate here; RANGE would complicate ties but is not needed for this simple running total.

References: <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions>

**42. Answer: D**

`WHEN NOT MATCHED BY SOURCE THEN DELETE` removes rows from the target (sales_fact) that do not have a match in the source (sales_staging). This is the correct clause for requirement (3). The NOT MATCHED BY TARGET syntax is incorrect. The NOT MATCHED clause removes source rows. A separate DELETE statement could work but is not the MERGE approach.

- **A:** Incorrect: This removes source rows; we want to remove target rows.
- **B:** Incorrect: MERGE supports `WHEN NOT MATCHED BY SOURCE`, so a separate DELETE is unnecessary.
- **C:** Incorrect: The correct syntax is `WHEN NOT MATCHED BY SOURCE`, not `WHEN NOT MATCHED`.
- **D:** Correct: `WHEN NOT MATCHED BY SOURCE THEN DELETE` deletes target rows with no source match, implementing soft deletes or purge logic.

References: <https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into>

**43. Answer: D**

Each AI function is optimized for its task: `ai_extract` finds and returns specific data from text (product name), and `ai_classify` assigns text to predefined categories (sentiment). Using each for its intended purpose is efficient and clear. Materializing results into a table enables fast queries. Using ai_classify twice conflates the functions. Using ai_query for both tasks misses optimization. ai_query alone is overly general.

- **A:** Incorrect: `ai_classify` is for categorization, not extraction; misusing it for product name extraction is inefficient.
- **B:** Incorrect: `ai_query` is for open-ended questions; task-specific functions are more efficient.
- **C:** Incorrect: `ai_query` and `ai_classify` are not interchangeable; each has different semantics and optimization.
- **D:** Correct: `ai_extract` finds structured values (product names); `ai_classify` assigns categories (sentiments); materialization enables fast access.

References: <https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_extract>

**44. Answer: C**

`expect_all_or_drop` drops invalid rows but does not auto-quarantine them. To capture rejected rows, create a second streaming table that selects rows failing the constraint (the inverse WHERE clause). This explicit pattern lets you inspect invalid data while the clean path applies expectations. Auto-quarantine options do not exist in Databricks.

- **A:** Incorrect: `expect_all_or_drop` does not have an auto_quarantine flag; quarantine must be explicit.
- **B:** Incorrect: There is no `expect_all_or_quarantine` function; use separate streaming tables for the two paths.
- **C:** Correct: Define a second streaming table with the inverse WHERE clause to capture rows that fail the clean-path filter.
- **D:** Incorrect: While MERGE could work, defining a second streaming table with the inverse query is the standard pattern.

References: <https://docs.databricks.com/aws/en/ldp/expectation-patterns>

**45. Answer: D**

`system.billing.usage` is the authoritative table for all billing and usage data, including serverless compute costs aggregated by user. Grouping by user_id reveals per-user consumption; policy enforcement is application logic on top of these query results. Other tables like `system.admin.jobs_cost` track jobs but not user-level aggregates.

- **A:** Incorrect: While policies exist elsewhere, the query source is correct; but this does not give the full picture of governance.
- **B:** Incorrect: `system.admin.jobs_cost` tracks job execution costs, not rolled-up user-level consumption.
- **C:** Incorrect: `system.compute.serverless` is not a billing table; use `system.billing.usage` for cost analysis.
- **D:** Correct: `system.billing.usage` is the source of truth for serverless compute cost by user and category.

References: <https://docs.databricks.com/aws/en/admin/system-tables/serverless-billing>

**46. Answer: B**

Querying `system.lakeflow.job_run_timeline` is the most efficient approach: a single SQL query provides a centralized view of all job runs across the workspace, enabling easy filtering, aggregation, and automated alerting. It avoids the need for manual job enumeration or API pagination loops.

- **A:** Incorrect: Requires enumerating all jobs and CLI looping; inefficient for automation and many jobs.
- **B:** Correct: System table query provides a centralized view of all runs; no enumeration needed; SQL enables easy filtering and alerts.
- **C:** Incorrect: Works but requires handling pagination and job loops; more complex than a system table query.
- **D:** Incorrect: `databricks runs list` does not aggregate across jobs; you must filter by specific job-id.

References: <https://docs.databricks.com/aws/en/admin/system-tables/jobs>

**47. Answer: B**

Anomaly Detection in Data Governance detects unexpected changes in table metrics (row counts, column distributions, etc.) using statistical baselines. It compares current values against historical patterns and alerts on deviations, which is perfect for detecting the 50% drop scenario. Predictive Optimization targets query performance, not volume anomalies. DQM focuses on data content quality, not volume shifts.

- **A:** Incorrect: Predictive Optimization optimizes query performance, not detects volume anomalies.
- **B:** Correct: Anomaly Detection uses statistical baselines to flag unexpected metric changes like volume drops.
- **C:** Incorrect: DQM alerts on constraint violations (e.g., null rates, schema changes), not statistical volume deviations.
- **D:** Incorrect: Custom notebook logic is error-prone and not scalable; built-in anomaly detection is designed for this.

References: <https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-quality-monitoring/anomaly-detection/>

**48. Answer: A**

Materialized view incremental refresh relies on the underlying table's clustering and delta encoding. If the clustering key changes or the table is rewritten, incremental refresh cannot be used and Databricks falls back to full recompute. Time-window expansion is not how Databricks handles this; the window remains fixed. Size-based refresh mode changes are not a feature. Schema changes would cause the query to fail entirely, not fall back to full refresh.

- **A:** Correct: Changes to clustering, table rewrites, or schema incompatibilities force full recompute of materialized views.
- **B:** Incorrect: A fixed window does not expand; Databricks would not re-interpret `CURRENT_DATE - 30` as unbounded.
- **C:** Incorrect: Materialized views do not have a size threshold that triggers mode changes.
- **D:** Incorrect: Schema changes would cause the view refresh to fail, not silently fall back to full recompute.

References: <https://docs.databricks.com/aws/en/admin/system-tables/materialization>

**49. Answer: B**

Liquid Clustering keys can be changed via `ALTER TABLE table_name CLUSTER BY (new_columns)` without rewriting existing data. The new key applies prospectively to new writes; old data retains its prior clustering. This is a major advantage of Liquid Clustering over other indexing strategies. Keys can be changed freely to any new set of columns.

- **A:** Incorrect: Liquid Clustering is designed to support key changes without rewrites; this avoids the cost you are concerned about.
- **B:** Correct: `ALTER TABLE table_name CLUSTER BY` changes the key prospectively; no existing data is rewritten.
- **C:** Incorrect: Keys can be changed to any combination, not just supersets; prospective application is the whole point.
- **D:** Incorrect: Liquid Clustering keys are mutable; the ALTER TABLE approach is simpler than table recreation.

References: <https://docs.databricks.com/aws/en/tables/clustering>

**50. Answer: B**

Serverless compute is more cost-effective for spiky workloads because you pay only for compute during the job run (per-second billing). Classic autoscaling clusters incur overhead from cluster startup, potential idle time, and node minimum costs even when scaled down. Serverless has no idle cost between runs. Classic clusters have startup overhead and minimum node costs even when scaled down. File deletion is not part of the autoscaling model. Databricks serverless has no separate "API overhead fee" in the pricing model.

- **A:** Incorrect: Even with scale-down, classic clusters have startup overhead and minimum node costs; serverless is simpler.
- **B:** Correct: Serverless per-second billing is ideal for spiky workloads; you pay only for active compute, not startup or idle.
- **C:** Incorrect: Classic clusters are typically stopped between runs, not kept idle; but startup overhead is still a cost.
- **D:** Incorrect: Serverless does not have a separate "API overhead fee"; the pricing model is based on compute used.

References: <https://docs.databricks.com/aws/en/compute/serverless/>

**51. Answer: A**

Result cache is ideal here: the query is deterministic (same date within a 24-hour window), and result caching preserves the result across runs. Subsequent runs hit the cache and return instantly. Result cache works perfectly for time-based filters when the query is identical; it is not non-deterministic. SQL Warehouse fully supports result caching. A materialized view would be a more complex solution than needed.

- **A:** Correct: Result cache reuses identical query results; dashboard users benefit from millisecond response times after the first run.
- **B:** Incorrect: Result cache is not non-deterministic for time-based filters; it is perfectly suited for this scenario.
- **C:** Incorrect: SQL Warehouse fully supports result caching; it is one of the primary benefits.
- **D:** Incorrect: While a materialized view could work, result caching is simpler and more direct for this use case.

References: <https://docs.databricks.com/aws/en/dashboards/caching>

**52. Answer: A**

`is_account_group_member('finance_team')` is the correct function for account-level group membership in masking policies. The policy returns the actual salary value if the user is in the finance_team account group, otherwise NULL. This syntax is standard for Databricks account-scoped groups.

- **A:** Correct: `is_account_group_member()` is the right function for account-level group-based masking.
- **B:** Incorrect: The function is `is_account_group_member`, not `is_group_member`; that function does not exist.
- **C:** Incorrect: `is_account_group_member()` works for account groups; `is_workspace_group_member()` is for workspace-local groups.
- **D:** Incorrect: Group-based masking is fully supported and is the recommended pattern for role-based access control.

References: <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-column-mask>

**53. Answer: C**

Setting `delta.deletedFileRetentionDuration=0` causes VACUUM to immediately delete old data files after purging records. Once files are physically deleted, time travel cannot access those versions because the underlying data no longer exists. This is an intentional trade-off: compliance with data retention regulations requires sacrificing historical query capability.

- **A:** Incorrect: Time travel depends on file availability; VACUUM with zero retention physically removes the files needed for time travel.
- **B:** Incorrect: You can set retention to 0; Delta does not enforce a minimum for VACUUM retention duration.
- **C:** Correct: VACUUM with zero retention immediately deletes files; time travel becomes impossible for those deleted records.
- **D:** Incorrect: Time travel does not work after files are deleted; the entire version history for purged data is gone.

References: <https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum>

**54. Answer: A**

Databricks redacts secrets in system logs, audit trails, and backend records, but the value returned from `dbutils.secrets.get()` is a plain Python string in the notebook environment. If the developer prints or displays it in a cell, it appears in plaintext in the notebook UI. Redaction of the notebook cell output is the developer's responsibility; naming conventions do not trigger automatic masking.

- **A:** Correct: The returned secret value is not automatically redacted in notebook cell output; it appears as plaintext.
- **B:** Incorrect: Variable naming does not control redaction; all returned values are plaintext strings in memory.
- **C:** Incorrect: System logs and audit trails do redact secrets; the issue is notebook cell output only.
- **D:** Incorrect: Databricks redacts secrets in logs and audit trails, but the notebook cell output is not automatically masked.

References: <https://docs.databricks.com/aws/en/archive/dev-tools/dbutils-library>

**55. Answer: A**

Unity Catalog implements a hierarchical permission model where REVOKE at a child level (table) overrides GRANT at a parent level (schema). To prevent one user from accessing a specific table, REVOKE SELECT at the table level for that user. This is the intended pattern for implementing fine-grained restrictions without DENY.

- **A:** Correct: REVOKE at the table level for the user overrides the schema-level GRANT to their group.
- **B:** Incorrect: Row masks filter visible data within a table; they do not prevent the table from being queried.
- **C:** Incorrect: Row-level policies filter row results; they do not prevent table access.
- **D:** Incorrect: Revoking at the table level is simpler than managing multiple groups with different permissions.

References: <https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/admin-privileges>

**56. Answer: A**

Task skew occurs when one or more tasks process much more data than others, causing the overall job to be dominated by the slowest task. In the Spark UI Stages tab, the task timeline visualization shows task duration bars; uneven bars reveal skew. Task skew is not normal and should be optimized. Executor imbalance can contribute but is not the primary cause here. Query optimization through repartitioning or filtering is the first step before considering hardware upgrades.

- **A:** Correct: Task skew is visible in the Spark UI Stages tab task timeline; one task bar is much longer than others.
- **B:** Incorrect: Task skew is not normal and should be optimized via partitioning or filtering.
- **C:** Incorrect: While executor imbalance can contribute, the root cause here is likely uneven data distribution (skew).
- **D:** Incorrect: Query optimization (e.g., repartition, filter) is the first step, not hardware upgrade.

References: <https://docs.databricks.com/aws/en/compute/troubleshooting/debugging-spark-ui>

**57. Answer: B**

A full refresh in a Declarative Pipeline clears the table state and checkpoint, restarting from the current Kafka offset. The new schema is applied prospectively. Historical messages already consumed are not replayed. This is the intended behavior: you lose in-flight state but not necessarily upstream data. Full refresh does not replay Kafka from the beginning; checkpointing ensures offsets are tracked. Offsets are managed by Kafka, not locked to the pipeline. Full refresh does not selectively reprocess recent Kafka messages.

- **A:** Incorrect: Full refresh does not replay Kafka from the beginning; it clears state and restarts from the current offset.
- **B:** Correct: Full refresh clears the table and checkpoint; the new schema applies prospectively from the current offset.
- **C:** Incorrect: Full refresh works without recreating the pipeline; offsets are managed by Kafka, not locked.
- **D:** Incorrect: Full refresh does not selectively reprocess recent Kafka messages; it starts fresh from the current offset.

References: <https://docs.databricks.com/aws/en/dev-tools/databricks-apps/lakeflow>

**58. Answer: B**

Bundles are better for production deployments requiring rollback. Each bundle deployment is self-contained with versioning; you can rollback by deploying a prior bundle version. Git folders require Git history manipulation for rollback, which is more complex. Bundles also support state isolation (dev vs prod via workspace targets). Git branch reviews are familiar but complicate rollback via history rewrites. Bundles provide production-grade rollback and workspace isolation, not just complexity. Bundles offer significantly better operational safety than equivalent Git workflows.

- **A:** Incorrect: While Git review is familiar, rolling back via Git history revert is complex and risks other branches.
- **B:** Correct: Bundles version deployments; rollback is a simple re-deployment of a prior version.
- **C:** Incorrect: Bundles provide production-grade rollback and workspace isolation; these are not complexity, but safety.
- **D:** Incorrect: Bundles offer better operational safety and rollback; this is a architectural choice, not preference.

References: <https://docs.databricks.com/aws/en/dev-tools/bundles/jobs-tutorial>

**59. Answer: A**

Metric views define semantic roles for columns to enable BI tools to understand aggregation. Measures are aggregated numeric columns (`total_revenue`, `total_units`). Dimensions are grouping/filtering columns (`date`, `user_id`). The source is the underlying table (`my_table`). This structure lets BI tools auto-aggregate and drill down correctly.

- **A:** Correct: Measures are aggregated values; dimensions are grouping columns; source is the base table.
- **B:** Incorrect: Identifiers are dimensions; aggregated values are measures; you have reversed them.
- **C:** Incorrect: Metric views require explicit definition of measures and dimensions for semantic clarity.
- **D:** Incorrect: Metric views explicitly distinguish measures and dimensions for BI consumption.

References: <https://docs.databricks.com/aws/en/uc-semantics/metric-views/create>

**60. Answer: A**

Liquid Clustering applies to all queries on the table, including those from materialized views. When the materialized view filters `country='US'`, Databricks uses the clustering on `country` to prune data blocks, avoiding a full table scan. Clustering is not limited to direct table scans; it applies to all queries. Clustering columns must appear in the WHERE clause for filtering, not in SELECT. Clustering optimizes equality filters and range queries; it is perfect for this scenario.

- **A:** Correct: Liquid Clustering on `country` enables block pruning for the materialized view filter, speeding up the query.
- **B:** Incorrect: Clustering applies to all queries on the table, including materialized views.
- **C:** Incorrect: Clustering columns must appear in the WHERE clause, not SELECT; they optimize filters, not projections.
- **D:** Incorrect: Clustering optimizes equality filters and range queries; `country='US'` is a perfect use case.

References: <https://docs.databricks.com/aws/en/tables/clustering>
