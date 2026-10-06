> This is original study material (not official exam questions or content). Source of truth: docs.databricks.com and the official October 2026 exam guide (New Exam column). Verify features before the exam.

---

# Databricks Certified Data Engineer Professional — New Exam

## 1. Developing Code for Data Processing using Python and SQL (23%)

### Declarative Automation Bundles (DABs) — Structure and CI/CD

**What is it / when to use:**
- Bundles (formerly DABs) = declarative IaC with `databricks.yml`, Python/SQL templates rendered via Jinja2.
- Define jobs, pipelines, clusters, dashboards as code. Supports multiple *targets* (dev/staging/prod) with per-target overrides.
- Essential for reproducible deployment, versioning in Git, and CI/CD.

**Heuristic:**
- Use bundles whenever you need reproducible automation. Repo pattern (Git folders) + bundles = modern default.
- Targets allow the same definition, parameterized for each environment.

**Key syntax:**
```yaml
# databricks.yml
bundle:
  name: my-pipeline
  targets:
    dev:
      workspace:
        host: https://dev.cloud.databricks.com
    prod:
      workspace:
        host: https://prod.cloud.databricks.com
      variables:
        job_max_concurrent_runs: 1
resources:
  jobs:
    my_job:
      name: etl-job
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ./src/ingest
```
- `databricks bundle deploy -t prod` = deploys prod target with overrides.

**Gotcha:**
- Secrets in bundles: use `{{env "MY_SECRET"}}` or UC secret scopes, not hardcode.
- Variable references with `${var.varname}` need to be in `variables:` or CLI.

---

### Troubleshooting Dependencies

**What is it / when to use:**
- PyPI packages, local wheels, source archives can fail on serverless compute (restricted, sandboxed).
- Lakeflow Declarative Pipelines, Jobs, bundles — each has a different dependency resolution context.

**Heuristic:**
- Serverless compute: only stable PyPI packages and pre-built (wheels). C-extension-heavy packages fail if there is no compiled driver.
- Classic clusters: can use source archives, but increases startup time.
- **Prefer high-memory clusters for tasks that need significant RAM** during installation.

**Key syntax:**
```python
# Lakeflow Declarative Pipeline: pip_requirements
# dlt.yml:
{%- if target == 'prod' %}
pip_requirements: "requirements-prod.txt"
{%- else %}
pip_requirements: "requirements-dev.txt"
{%- endif %}

# PySpark UDF with dependencies:
# Uses --py-files or job clusters config.
spark.conf.set("spark.pyspark.python", "/usr/bin/python3")
```
- Job cluster config: `python_wheel_distributions` or `pip_packages` in cluster spec.

**Gotcha:**
- Serverless does not support `--py-files` with source .py directly; needs .zip or import via package.
- Python version mismatch between local build and serverless runtime (verify DBR).

---

### UDFs — Pandas, Python, SQL + Unity Catalog

**What is it / when to use:**
- **Pandas UDF**: vectorized, fast (Apache Arrow), recommended for column transformations.
- **Python UDF**: slow, row-by-row serialization, avoid in streaming.
- **SQL UDF**: deterministic, integrated in catalog, reusable cross-workspace (UC).

**Heuristic:**
- Pandas UDF for complex logic in batch.
- SQL UDF for simple, reusable transformations, no external state.
- Python UDF last resort (call-out to Python, external I/O).

**Key syntax:**
```python
# Pandas UDF
import pandas as pd
from pyspark.sql.functions import pandas_udf
@pandas_udf("double")
def multiply_by_two(s: pd.Series) -> pd.Series:
    return s * 2

# UC SQL UDF (stored, versioned)
CREATE OR REPLACE FUNCTION my_catalog.my_schema.my_func(x INT)
RETURNS INT
LANGUAGE SQL
AS 'SELECT x * 2'

# Call UC function
SELECT my_catalog.my_schema.my_func(col) FROM table
```

**Gotcha:**
- Pandas UDF with imports inside the function = overhead. Move imports to top-level.
- UC SQL UDFs: cannot have arbitrary SQL logic, only simple SELECT.

---

### Lakeflow Declarative Pipelines (ex-DLT) + Auto Loader

**What is it / when to use:**
- Declarative SQL/Python for modular ETL with automatic dependency tracking.
- Auto Loader = managed connector for cloud storage (S3, ADLS, GCS) with schema inference and CDC.
- Streaming tables vs materialized views = latency/cost/history trade-off.

**Minimal structure:**
```python
# pipeline.py (or .sql)
import dlt
from pyspark.sql.functions import *

# Source: Auto Loader from S3
@dlt.table(comment="Raw events from S3")
def raw_events():
    return spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", "/tmp/schema") \
        .load("s3://bucket/path")

# Streaming table: append-only, exactly once
@dlt.table(
    name="events",
    path="/mnt/events",
    comment="Cleaned events"
)
def events():
    return dlt.read_stream("raw_events") \
        .filter(col("event_id").isNotNull())

# Materialized view: full history, refreshable
@dlt.view
def daily_summary():
    return dlt.read("events") \
        .groupBy(date_trunc("day", "event_time")) \
        .agg(count("*"))
```

**Streaming table vs MV heuristic:**
| Aspect | Streaming Table | Materialized View |
|--------|-----------------|-------------------|
| Mode | Append-only, exactly-once | Recalculated each refresh |
| Latency | Low (incremental) | Batch (refresh period) |
| Cost | Lower (new data only) | Higher (recomputes all) |
| Uses | CDC, event streams | Aggregations, full history |
| Downtime | No backfill if failure | Automatic backfill on refresh |

**Gotcha:**
- Auto Loader with `mode="FAILONMALFORMEDDATA"` (default) aborts pipeline if bad files exist. Use `"PERMISSIVE"` + filter nulls.
- Streaming table with late-arriving data: checkpoint may be far ahead; consider `initialCheckpointLocation`.

---

### CDC with AUTO CDC APIs (APPLY CHANGES)

**What is it / when to use:**
- Capture changes (inserts, updates, deletes) in incremental source.
- `APPLY CHANGES` = Lakeflow Declarative Pipelines syntax for SCD Type 1 or Type 2.
- **NET-NEW**: stored_as_scd_type = stores full history or only latest version.

**Key syntax:**
```sql
-- Lakeflow Declarative Pipeline (APPLY CHANGES)
CREATE OR REFRESH STREAMING TABLE customer_scd AS
  APPLY CHANGES INTO live.customer_scd
  FROM raw_customer_changes
  KEYS (customer_id)
  SEQUENCE BY change_time
  COLUMNS * EXCEPT (operation, _rescued_data)
  STORED AS SCD TYPE 2
  -- TYPE 2: keeps history (start_version, end_version, is_current)
  -- TYPE 1: only current version (overwrites)
  
-- After, use via TIME TRAVEL or is_current filter:
SELECT * FROM live.customer_scd WHERE is_current
```

**Heuristic:**
- SCD Type 1 when only current state matters (e.g., address).
- SCD Type 2 when tracking when each piece of data changed (e.g., subscription plan).

**Gotcha:**
- `SEQUENCE BY` order MUST be in causal order, else application order is wrong.
- Deletes: mark with operation='DELETE' and source will ensure removal.

---

### Spark Structured Streaming vs Lakeflow Declarative Pipelines

**Heuristic:**
- Lakeflow Declarative Pipelines: recommended default, auto-scaling, automatic restart, native UI.
- Spark Structured Streaming: when you need custom stateful logic (state store management, complex windowing).

**NET-NEW Structured Streaming: Stateful Operations**

Watermarks, output modes, foreachBatch, checkpoints, exactly-once recovery.

```python
from pyspark.sql.functions import window, col

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .load()

# Watermark: late data arriving up to N minutes after event_time
query = df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "5 minutes")) \
    .count() \
    .writeStream \
    .outputMode("update")  # update = only rows that changed \
    .option("checkpointLocation", "/mnt/checkpoint") \
    .foreachBatch(process_batch)  # Custom logic per micro-batch \
    .start()

def process_batch(batch_df, batch_id):
    batch_df.write.mode("append").saveAsTable("output_table")

query.awaitTermination()
```

**Output modes:**
- `append`: only new data (default, recommended).
- `update`: changes in aggregate (rows that changed).
- `complete`: full state of aggregate (can grow large).

**Checkpoint = idempotence:**
- Automatic restart resumes from last checkpoint, guarantees exactly-once.
- Tip: separate checkpoint per DAG (don't share).

---

### Lakeflow Jobs with Control Flow (If/Else, For Each)

**What is it / when to use:**
- Lakeflow Jobs (ex-Workflows) = job orchestration with dependency tracking, retry, alerts.
- Control flow = branching (IF/ELSE), loops (FOR EACH), dynamic task generation.

**Key syntax (YAML):**
```yaml
# databricks.yml
resources:
  jobs:
    etl_orchestration:
      name: etl-workflow
      tasks:
        - task_key: check_source
          notebook_task:
            notebook_path: ./check
          
        - task_key: branch_logic
          depends_on:
            - task_key: check_source
          run_if: "ALL_DONE"  # or "AT_LEAST_ONE_SUCCESS"
          
        - task_key: if_true_branch
          condition_task:
            op: "EQUAL_TO"
            left: "{{tasks.check_source.values.status}}"
            right: "ready"
          depends_on:
            - task_key: branch_logic
            
        - task_key: process_data
          notebook_task:
            notebook_path: ./process
          depends_on:
            - task_key: if_true_branch
          when_condition: "{{task.if_true_branch.result}}"
```

**Gotcha:**
- Job parameters vs notebook parameters: use `dbutils.widgets.get()` to read from job.
- Retries default = 1; configure via `max_retries` on task.

---

### Serverless Compute — Environments, Dependency Mgmt, Performance Mode

**What is it / when to use:**
- Serverless compute = managed, autoscaling, no cluster overhead. Ideal for batch jobs and pipelines.
- **Environments** = pre-built Docker images with pre-installed dependencies, reusable.
- Performance mode = dedicated resources, predictable, more expensive.

**Key syntax:**
```python
# Configure environment (Docker)
# In databricks.yml
resources:
  compute:
    my_serverless_env:
      kind: "serverless"
      config:
        spark_conf:
          "spark.databricks.delta.preview.enabled": "true"
        environment_variables:
          MY_VAR: "value"
        pip_requirements:
          - numpy==1.21.0

# Use in job
resources:
  jobs:
    my_job:
      name: serverless-job
      compute_key: my_serverless_env
      tasks:
        - task_key: process
          notebook_task:
            notebook_path: ./process
```

**Performance mode:**
```python
# Enable performance mode via cluster config
spark.conf.set("spark.databricks.cloudprovider.performanceMode", "true")
```

**Heuristic:**
- Serverless for tasks < 30min, stateless.
- Classic clusters for long-running, state-heavy, or debugging.

**Gotcha:**
- Serverless = no driver local storage (~512MB). Use /tmp with limit.
- No `dbutils.fs.rm()` of large volumes in serverless (timeout).

---

### High-Memory Notebook Tasks

**What is it / when to use:**
- Notebook tasks in jobs can allocate separate compute (don't reuse driver from cluster).
- High memory for large DataFrame transformations.

**Syntax:**
```yaml
resources:
  jobs:
    memory_heavy_job:
      name: big-data-process
      tasks:
        - task_key: big_transform
          notebook_task:
            notebook_path: ./transform
            source: GIT
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "i3.2xlarge"  # high-memory instance
            num_workers: 4
            aws_attributes:
              availability: "SPOT"
```

---

### Testing: assertDataFrameEqual, assertSchemaEqual, DataFrame.transform

**What is it / when to use:**
- Unit testing for PySpark transformations.
- `assertDataFrameEqual` = compares data + schema.
- `assertSchemaEqual` = compares schema only.
- `DataFrame.transform()` = chaining transformations, testable.

**Syntax:**
```python
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql.functions import col

def add_greeting(df):
    return df.withColumn("greeting", lit("hello"))

def test_add_greeting():
    input_df = spark.createDataFrame([(1, "alice")], ["id", "name"])
    expected = spark.createDataFrame([(1, "alice", "hello")], ["id", "name", "greeting"])
    result = input_df.transform(add_greeting)
    assertDataFrameEqual(result, expected)

def test_schema():
    df = spark.createDataFrame([(1,)], ["id"])
    expected_schema = StructType([StructField("id", LongType())])
    assertSchemaEqual(df.schema, expected_schema)
```

**Gotcha:**
- assertDataFrameEqual is sensitive to order and types. Use `checkRowOrder=False` if order doesn't matter.

---

### Auto-Optimization and Retry Control

**What is it / when to use:**
- Auto-optimization = Delta Lab feature that rewrites small files, optimizes data skipping automatically.
- Disable retries for jobs that need to fail fast (fail-fast debugging).

**Syntax:**
```python
# Enable auto-optimization on table
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')")
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')")

# Job: disallow retries
# databricks.yml
resources:
  jobs:
    fail_fast:
      name: debug-job
      tasks:
        - task_key: debug
          max_retries: 0  # Fail immediately
```

---

## 2. Data Ingestion & Acquisition (12%) — NET-NEW

### Formats and Sources — Multi-Source Ingestion

**What is it / when to use:**
- Delta (v1/v2) = default; Iceberg = column-oriented, time-travel, evolution. Parquet = compressed storage. JSON, CSV = semi-structured. Binary = raw logs.
- Message buses = Kafka, Kinesis, Pub/Sub for streaming ingress.
- Cloud storage = S3, ADLS, GCS for batch.

**Heuristic:**
- Delta = default for OLAP lakehouse, best data skipping.
- Iceberg = when you need cross-workspace sharing, complex schema evolution, or performance queries on sparse columns.
- Kafka/Kinesis = event-driven ingress, high volume, low latency.

**Syntax:**
```python
# Auto Loader (cloudFiles) with multiple formats
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/path/schema") \
    .load("s3://bucket/path")

# Direct Kafka ingress
spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "topic") \
    .load()

# Iceberg (managed table)
spark.sql("CREATE TABLE my_catalog.schema.iceberg_table USING ICEBERG ...")
```

---

### CDC Incremental with Lakeflow + Delta/Iceberg

**What is it / when to use:**
- Capture source deltas (new/changed/deleted), apply downstream.
- Delta CDF (Change Data Feed) = row-level change history.

**Syntax:**
```sql
-- Enable CDF on source table
ALTER TABLE source_table
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Read changes
SELECT * FROM table_changes('source_table', 0)
WHERE _change_type IN ('insert', 'update_postimage', 'delete');

-- In Lakeflow Declarative Pipeline
CREATE STREAMING TABLE sink AS
  SELECT * FROM stream_changes('source_table')
```

---

### NET-NEW: Lakeflow Connect — Managed CDC Connectors

**What is it / when to use:**
- Lakeflow Connect = pre-built connectors, managed CDC for SQL Server, MySQL, PostgreSQL.
- No custom script needed, no scheduler, no offset management.

**Heuristic:**
- Use Lakeflow Connect when source is SQL Server/MySQL/PostgreSQL.
- Fallback to custom Spark job if source is exotic or CDC logic is very custom.

**Syntax (verify in the docs):**
```yaml
# In Lakeflow Declarative Pipeline config
connectors:
  postgres_source:
    connector_type: "postgresql"
    connection_config:
      host: "postgres.example.com"
      database: "mydb"
      user: "{{env 'POSTGRES_USER'}}"
      password: "{{env 'POSTGRES_PASSWORD'}}"
    tables:
      - table_name: "customers"
        cdc_enabled: true
```

---

### NET-NEW: OpenSharing (D2D and Databricks-to-Open) and Clean Rooms

**What is it / when to use:**
- **Databricks-to-Databricks (D2D) Sharing**: share tables between workspaces, managed credentials.
- **Databricks-to-Open**: share to Snowflake, BigQuery, etc. via open protocol.
- **Clean Rooms**: isolated workspace for confidential collaboration.

**Heuristic:**
- D2D for intra-org sharing.
- Databricks-to-Open for external partners.
- Clean Rooms for sensitive analysis (revenue-share, joint analytics).

**Syntax:**
```python
# Share table via D2D (requester-side)
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
share = w.shares.create(
    name="my_share",
    objects=[
        {
            "name": "my_catalog.my_schema.my_table",
            "data_object_type": "TABLE"
        }
    ]
)
w.shares.update_share(
    name="my_share",
    share_credentials_version=1,
    recipient={"shareCredentialsVersion": 1}
)
```

---

### Lakehouse Federation with Governance

**What is it / when to use:**
- Query federated data (Delta, Iceberg, External Services) as local tables.
- UC permissions = control access to federated objects.
- Connection-level credentials = integrated secret management.

**Syntax:**
```python
# Create connection for external warehouse
spark.sql("""
    CREATE CONNECTION external_db
    TYPE postgresql
    USING HOST 'db.example.com',
          PORT 5432,
          DATABASE 'mydb',
          USER 'postgres',
          PASSWORD 'secret'
""")

# Query federated table
spark.sql("""
    SELECT * FROM external_db.public.users
""")

# Apply UC access control
spark.sql("""
    GRANT SELECT ON CONNECTION external_db TO admin_group
""")
```

---

## 3. Data Manipulation (12%) — NET-NEW

### Advanced Transformations — Window, Join, Aggregation

**What is it / when to use:**
- Window functions = ranking, lead/lag, aggregates over partitions without collapsing rows.
- Joins = cartesian, left/right/inner/outer; reordered by Catalyst heuristic.
- Aggregations = groupBy+agg, distinct counts, percentiles.

**Syntax:**
```python
from pyspark.sql.functions import row_number, rank, lead, lag, sum as spark_sum, window, col

df = spark.createDataFrame(
    [(1, 'alice', 1000), (1, 'alice', 1200), (2, 'bob', 800)],
    ['dept_id', 'name', 'salary']
)

# Window: rank salaries per dept
from pyspark.sql import Window
w = Window.partitionBy('dept_id').orderBy(col('salary').desc())
result = df.withColumn('rank', rank().over(w))

# Lead/Lag: access to previous/next data
w_time = Window.partitionBy('dept_id').orderBy('salary')
result = df.withColumn('prev_salary', lag('salary').over(w_time))

# Aggregate
result = df.groupBy('dept_id').agg(
    spark_sum('salary').alias('total_salary'),
    count_distinct('name').alias('emp_count')
)
```

**Gotcha:**
- Window without ORDER BY partitions but doesn't order = unpredictable result.
- Join without hint: Spark reorders; use hint to broadcast small side: `/*+ BROADCAST(t2) */`.

---

### NET-NEW: VARIANT — parse_json, variant_get, Colon-Path

**What is it / when to use:**
- VARIANT = semi-structured data type in SQL for JSON without parsing.
- Lazy parsing = better performance for queries that touch few fields.

**Syntax:**
```sql
-- Parse JSON to VARIANT (lazy)
SELECT parse_json('{"a": 1, "b": {"c": 2}}') AS v;

-- Extract fields via colon-path
SELECT v:a, v:b:c FROM table;

-- variant_get for explicit extraction
SELECT variant_get(v, 'a', 'int') AS a_int FROM table;

-- Convert VARIANT to JSON string
SELECT to_json(v) FROM table;
```

**Gotcha:**
- `v:a` returns VARIANT; use `v:a::int` for casting.
- Colon-path is case-sensitive for keys.

---

### NET-NEW: AI Functions — ai_query for Inference in Pipeline

**What is it / when to use:**
- `ai_query` = call LLM (Claude, GPT, etc.) as UDF inside SQL/streaming.
- Use for enrichment, classification, NER in data pipeline.

**Syntax (verify in the docs):**
```sql
-- ai_query: classification example
SELECT 
    text,
    ai_query('SELECT classify_sentiment(@text)', MAP(ARRAY['text'], ARRAY[text])) AS sentiment
FROM text_table;

-- With model specification
SELECT ai_query(
    'my-model:endpoint',
    'Summarize this: @text',
    MAP(ARRAY['text'], ARRAY[description])
) AS summary
FROM documents;
```

**Gotcha:**
- Cost = per token sent; monitor usage.
- Latency = external call; consider batch vs row-by-row trade-off.

---

### Data Quality Expectations in Lakeflow Declarative Pipelines

**What is it / when to use:**
- DQ expectations = inline assertions in pipeline (quarantine/drop/fail).
- `expect` = inline validation; `assert` = fail pipeline.
- Invalid rows can go to quarantine zone (separate table) for debug later.

**Syntax:**
```python
# Lakeflow Declarative Pipeline (Python)
import dlt

@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_fail("valid_timestamp", "timestamp > '2020-01-01'")
@dlt.table
def clean_events():
    return dlt.read_stream("raw_events").where(...)

# Quarantine pattern: invalid rows to separate table
@dlt.table
def invalid_events():
    return dlt.read_stream("raw_events").where("id IS NULL OR timestamp IS NULL")
```

**Heuristic:**
- Use `expect_or_drop` for bad rows (automatic logging).
- Use `expect_or_fail` for critical (must fail).
- Quarantine table for audit trail.

---

## 4. Monitoring and Alerting (10%)

### System Tables — Billing, Compute, Access, Lakeflow

**What is it / when to use:**
- System tables = governed tables in `system` catalog, read-only access.
- `system.billing.list_prices` = prices.
- `system.compute.clusters` = cluster inventory.
- `system.access.audit_logs` = audit trail.
- `system.lakeflow.*` = pipeline runs, datasets, etc.

**Syntax:**
```sql
-- Cost per warehouse
SELECT 
    warehouse_id,
    SUM(usage_quantity * list_price) AS total_cost
FROM system.billing.list_prices p
JOIN system.billing.usage u ON p.sku_id = u.sku_id
GROUP BY warehouse_id;

-- Cluster configs
SELECT cluster_id, spark_version, num_workers
FROM system.compute.clusters
WHERE state = 'RUNNING';

-- Access audit
SELECT timestamp, action, user_name, object_name
FROM system.access.audit_logs
WHERE action IN ('DELETE', 'UPDATE')
ORDER BY timestamp DESC LIMIT 100;

-- Pipeline runs
SELECT pipeline_id, state, start_time, end_time
FROM system.lakeflow.pipeline_runs
WHERE state = 'FAILED'
ORDER BY start_time DESC;
```

---

### REST APIs / CLI / SDK to Monitor Jobs and Pipelines

**What is it / when to use:**
- Databricks REST API v2.1 = jobs, runs, pipelines endpoints.
- CLI = `databricks jobs get-run <run-id>`.
- Python SDK (`databricks.sdk.WorkspaceClient`) = programmatic access.

**Syntax:**
```python
from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta

w = WorkspaceClient()

# List job runs
runs = w.jobs.list_runs(job_id=123, limit=10)
for run in runs:
    print(f"{run.run_id}: {run.state}")

# Get specific run details
run = w.jobs.get_run(run_id=456)
print(f"Status: {run.state}, End time: {run.end_time}")

# Programmatic restart
w.jobs.repair_run(run_id=456)
```

**CLI:**
```bash
databricks jobs list-runs --job-id 123 --limit 10
databricks jobs get-run --run-id 456
```

---

### Pipeline Event Logs

**What is it / when to use:**
- Event logs = detailed timestamped log of each pipeline step.
- Accessible via REST API `/events` endpoint.
- Includes: task start, data flow, lineage, errors, durations.

**Syntax:**
```python
# Fetch event logs
w = WorkspaceClient()
events = w.pipelines.get_event_logs(pipeline_id='my_pipeline')
for event in events:
    print(f"{event.timestamp}: {event.event_type} - {event.details}")
```

---

### Databricks Lakehouse Alerts

**What is it / when to use:**
- Alerts = automatic trigger on governed metrics, quality, cost, SQL warehouse health, audit, AI agent quality, Lakeflow Job branching.
- Multiple channels: Slack, email, webhook.

**Heuristic:**
- Alert on cost when spend exceeds monthly threshold.
- Alert on DQ when rejection % rises.
- Alert on warehouse health when query latency degrades.

**Syntax (verify in the docs):**
```python
# Create alert via UI or API
# databricks alerts create \
#   --name "High warehouse cost" \
#   --metric "warehouse_cost" \
#   --condition ">" \
#   --threshold 10000 \
#   --notification_channel "slack"
```

---

### Lakeflow Jobs UI/API

**What is it / when to use:**
- Lakeflow Jobs = orchestration engine (ex-Workflows).
- UI = visual DAG, run history, retry.
- API = programmatic job management.

**Syntax:**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# List jobs
jobs = w.jobs.list()
for job in jobs:
    print(f"{job.job_id}: {job.settings.name}")

# Trigger job run
run = w.jobs.submit_run(
    job_id=123,
    notebook_params={"environment": "prod"}
)

# Poll run status
import time
while True:
    run = w.jobs.get_run(run_id=run.run_id)
    if run.state.state_message in ['TERMINATED', 'INTERNAL_ERROR']:
        break
    time.sleep(10)
```

---

## 5. Cost & Performance Optimization (15%)

### Managed Tables + Predictive Optimization + Liquid Clustering

**What is it / when to use:**
- **Managed tables** = ownership by UC, automatic optimization.
- **Predictive Optimization** = Delta Lab automatically analyzes and recommends optimization (z-ordering, compaction, clustering).
- **Liquid Clustering** = dynamic rebinning, better for evolving filter patterns than CLUSTER BY.

**Heuristic:**
- Managed tables default (UC governance + optimization).
- Predictive Optimization for tables > 1GB, irregular access.
- Liquid Clustering for queries with multiple evolving filters.

**Syntax:**
```python
# Managed table with UC
spark.sql("""
    CREATE TABLE my_catalog.my_schema.my_table (
        id INT,
        customer_id INT,
        date DATE
    )
    USING DELTA
    CLUSTER BY (customer_id)
""")

# Enable Predictive Optimization (on table)
spark.sql("""
    ALTER TABLE my_catalog.my_schema.my_table
    SET TBLPROPERTIES ('delta.predictiveOptimization.enabled' = 'true')
""")

# Liquid Clustering (NET-NEW syntax, verify)
spark.sql("""
    CREATE TABLE my_catalog.my_schema.liquid_table (
        id INT,
        customer_id INT,
        date DATE
    )
    USING DELTA
    CLUSTER BY LIQUID (customer_id, date)
""")
```

---

### Deletion Vectors vs Liquid Clustering vs CLUSTER BY AUTO

**What is it / when to use:**
- **Deletion Vectors (DV)** = logical delete markers, no file rewrite. Fast, minimal overhead.
- **Liquid Clustering** = dynamic rebinning. Better for skewed, evolving queries.
- **CLUSTER BY AUTO** = automatic clustering selection based on workload (verify in the docs).

**Heuristic:**
| Technique | Overhead | Use |
|---------|----------|-----|
| Deletion Vectors | Minimal | Frequent deletes, no rewrite |
| Liquid Clustering | Medium | Multiple evolving filters |
| CLUSTER BY AUTO | Medium | Adaptive workload, hands-off |

**Syntax:**
```python
# Enable Deletion Vectors
spark.sql("""
    ALTER TABLE my_table
    SET TBLPROPERTIES ('delta.deletionVectors.enabled' = 'true')
""")

# Check DV usage
spark.sql("SELECT count_deleted_files FROM system.tables_summary WHERE table_id = '...'")
```

---

### Delta Cache for Repeated Reads

**What is it / when to use:**
- Delta cache = in-memory cache of Parquet files on cluster nodes, survives query restarts.
- Reusable across queries, not invalidated if data hasn't changed.

**Heuristic:**
- Enable for read-heavy tables, size < 10x cluster RAM.
- Disable for tables in constant update (cache churn overhead).

**Syntax:**
```python
# Enable cache on a cluster
# Cluster config or notebook:
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "10gb")

# Force cache of specific table
spark.sql("""
    CACHE TABLE my_catalog.my_schema.my_table
""")

# Check cache status
spark.sql("SELECT * FROM system.cacheinfo")
```

---

### CDF (Change Data Feed) to Expose Row-Level Changes

**What is it / when to use:**
- CDF = change history (insert, update_preimage, update_postimage, delete).
- Incremental downstream processing without reprocessing everything.

**Syntax:**
```python
# Enable CDF on table
spark.sql("""
    ALTER TABLE my_table
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Read changes
changes = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("my_table")

# Changes include _change_type: insert, update_preimage, update_postimage, delete
changes.filter(col("_change_type") == "insert").show()
```

**Gotcha:**
- CDF storage cost = extra copies of changed data; monitor.

---

### Query Profile to Identify Bottlenecks

**What is it / when to use:**
- Query profile = breakdown of execution time by stage (data skipping, join strategy, shuffle).
- Access via Spark UI or SQL warehouses Query History.

**Heuristic:**
- Stage with shuffle > 90% time = issue. Consider repartitioning, increasing cluster RAM, or using Liquid Clustering.
- Data skipping stats < 10% = poor index. Consider ZORDER or Liquid Clustering.

**Syntax:**
```python
# Query profile on SQL warehouse (automatic history)
# Or, in notebook:
spark.sql("EXPLAIN FORMATTED SELECT ...").show()

# Detailed Spark UI: http://driver:4040/SQL/
```

---

### Liquid Clustering vs Partitioning / ZORDER

**Heuristic:**
| Technique | Incremental cost | Best for | Setup |
|---------|------------------|----------|-------|
| Partitioning | High (rewrite) | Low-cardinality (year, region) | Static, well-known |
| ZORDER | High (rewrite) | Static, known access pattern | Periodic optimize command |
| Liquid Clustering | Medium (incremental) | Evolving, high-cardinality | Dynamic, maintenance-free |

---

## 6. Ensuring Data Security and Compliance (8%)

### ACLs Least-Privilege on UC Securables

**What is it / when to use:**
- UC = fine-grained access control on catalog/schema/table/volume/connection/external-location.
- Least-privilege = grant only necessary permissions, deny by default.

**Heuristic:**
- Default: DENY (no one has access).
- Granular grants: SELECT for analysts, MODIFY for ETL, MANAGE for admins.
- Group-based: easier to maintain.

**Syntax:**
```sql
-- Create catalog
CREATE CATALOG my_catalog;

-- Grant SELECT (read-only)
GRANT SELECT ON CATALOG my_catalog TO analysts_group;

-- Grant MODIFY (write)
GRANT MODIFY ON TABLE my_catalog.schema.table TO etl_group;

-- Grant MANAGE (ownership, permissions)
GRANT MANAGE ON CATALOG my_catalog TO admin_group;

-- Deny (explicitly block)
DENY MODIFY ON TABLE my_catalog.schema.pii_table FROM marketing_group;
```

**Gotcha:**
- Inheritance: grant on catalog = inherits to future schemas/tables (but not retroactive).
- OWNER has all permissions (cannot be removed).

---

### NET-NEW: ABAC Policies with Governed Tags → Row/Column Filters

**What is it / when to use:**
- ABAC (Attribute-Based Access Control) = governance based on tags instead of group membership.
- Governed tags = centrally defined tags, applied to objects and users.
- Row/column filters generated automatically based on tag match.

**Heuristic:**
- ABAC for scale (hundreds of groups, complex queries).
- Example: tag `region=US` on table, user tag `region=US` → automatically only sees US rows.

**Syntax (verify in the docs):**
```sql
-- Create governed tag (namespace:value)
CREATE TAG TYPE region_tag;
ALTER TAG TYPE region_tag CHANGE IF EXISTS ADD POSSIBLE_VALUES ['US', 'EU', 'APAC'];

-- Apply tag to table
ALTER TABLE my_catalog.schema.sales
SET TAG region_tag = 'US';

-- Apply tag to user
ALTER USER alice SET TAG region_tag = 'US';

-- Automatic row filter (applied to query):
-- Alice only sees rows where region_tag = 'US'
```

---

### Anonymization / Pseudonymization — Hashing, Tokenization, Suppression, Generalization

**What is it / when to use:**
- **Hashing**: SHA256(PII) = one-way, deterministic, join-friendly.
- **Tokenization**: PII → random token, reversible with mapping (e.g., SSN → TKN-1234).
- **Suppression**: remove column entirely.
- **Generalization**: coarsen granularity (age 28 → age_bucket 20-30).

**Heuristic:**
- Hashing for join, no reversal.
- Tokenization for future reversal.
- Suppression when data not needed.
- Generalization for analysis without exposing granularity.

**Syntax:**
```python
from pyspark.sql.functions import sha2, col, when, floor

df = spark.createDataFrame(
    [(1, 'alice@example.com', 28), (2, 'bob@example.com', 35)],
    ['id', 'email', 'age']
)

# Hashing
anonymized = df.withColumn('email_hash', sha2(col('email'), 256))

# Generalization (age bucket)
anonymized = df.withColumn('age_bucket', floor(col('age') / 10) * 10)

# Suppression (drop email)
anonymized = df.drop('email')
```

**UC column masks (declarative):**
```sql
-- Create column mask
CREATE OR REPLACE FUNCTION my_catalog.masks.mask_email(e STRING)
RETURNS STRING
LANGUAGE SQL
AS 'SELECT CONCAT(SUBSTRING(e, 1, 3), "***@", SUBSTRING(e, -10))';

-- Apply mask
ALTER TABLE my_catalog.schema.users
ALTER COLUMN email SET MASK my_catalog.masks.mask_email
USING (email);
```

---

### Compliant Pipeline Batch + Streaming with PII Detection/Masking

**What is it / when to use:**
- PII detection = regex/pattern matching or ML model for email, SSN, phone, etc.
- Mask on-the-fly in Lakeflow Declarative Pipeline.

**Syntax:**
```python
import dlt
from pyspark.sql.functions import *

@dlt.table
def pii_detection_and_mask():
    return dlt.read_stream("raw_data") \
        .withColumn('has_email', col('text').rlike(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')) \
        .withColumn('text_masked', when(col('has_email'), 
            regexp_replace(col('text'), r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '[EMAIL]')
        ).otherwise(col('text')))
```

---

### Data Purging for Retention — GDPR Right-to-Erasure

**What is it / when to use:**
- Purging = delete PII based on retention policy.
- GDPR: right to deletion ("right to be forgotten").

**Heuristic:**
- Implement soft-delete (flag `is_deleted = true`) for audit.
- Hard-delete (vacuum) after retention period.
- Use Deletion Vectors to avoid full rewrite.

**Syntax:**
```python
# Soft delete
spark.sql("""
    UPDATE my_catalog.schema.users
    SET is_deleted = true
    WHERE user_id = 'alice' OR retention_date < CURRENT_DATE()
""")

# Hard delete (VACUUM)
spark.sql("""
    DELETE FROM my_catalog.schema.users
    WHERE is_deleted = true AND retention_date < CURRENT_DATE() - INTERVAL 90 DAYS
""")

# Vacuum (cleanup old versions)
spark.sql("""
    VACUUM my_catalog.schema.users RETAIN 30 DAYS
""")
```

---

## 7. Data Governance (5%)

### UC Tags and Comments for Discoverability

**What is it / when to use:**
- Tags = key-value metadata, searchable.
- Comments = human-readable descriptions.
- Essential for cataloging, lineage, compliance tagging.

**Syntax:**
```sql
-- Add comment
ALTER TABLE my_catalog.schema.my_table
COMMENT 'Customer transactions, PII masked, retention 7 years';

-- Add tags
ALTER TABLE my_catalog.schema.my_table
SET TAG environment = 'prod', pii_level = 'high', owner = 'finance';

-- Search by tag
SELECT * FROM system.lakeflow.objects
WHERE tag_name = 'pii_level' AND tag_value = 'high';
```

---

### Permission Inheritance in UC — Grant Inheritance

**What is it / when to use:**
- UC permissions inherit cascade: catalog → schema → table/volume.
- Grant at higher level = automatic on child objects (current and future).

**Heuristic:**
- Grant `SELECT` on catalog = all analysts read everything in that catalog (default).
- More granular at table/column level.

**Syntax:**
```sql
-- Grant SELECT on catalog (inherits to schemas, future tables)
GRANT SELECT ON CATALOG my_catalog TO analysts_group;

-- Override on schema (deny specific schema)
DENY SELECT ON SCHEMA my_catalog.sensitive_schema TO analysts_group;

-- Grant again on specific table (re-allow)
GRANT SELECT ON TABLE my_catalog.sensitive_schema.public_data TO analysts_group;
```

**Gotcha:**
- DENY does not inherit; if you want to block an entire schema, apply at schema level.
- Inheritance is not retroactive to objects created before grant (verify in docs v8.x).

---

## 8. Debugging and Deploying (10%)

### Diagnosis: Spark UI, Cluster Logs, System Tables, Query Profile

**What is it / when to use:**
- Spark UI (http://driver:4040): DAG visualization, executors, shuffle, memory.
- Cluster logs (S3 or ADLS): STDOUT, STDERR, driver logs.
- System tables: `system.compute.cluster_events`, `system.queries`.
- Query profile: SQL warehouse query history.

**Heuristic:**
- Spark UI for batch jobs, task-level breakdown.
- Cluster logs for startup failures, dependency errors.
- System tables for trend analysis, cost.
- Query profile for SQL performance.

**Syntax:**
```python
# Access cluster logs (via CLI)
# databricks cluster get-log --cluster-id <id>

# System tables
spark.sql("""
    SELECT cluster_id, event_type, timestamp, details
    FROM system.compute.cluster_events
    WHERE cluster_id = 'my_cluster' AND event_type IN ('DRIVER_NOT_RUNNING', 'EXECUTOR_DEAD')
    ORDER BY timestamp DESC LIMIT 20
""").show()

# Query history
spark.sql("""
    SELECT query_id, user_name, duration_ms, execution_status
    FROM system.queries.query_history
    WHERE warehouse_id = 'my_warehouse' AND execution_status = 'FAILED'
    ORDER BY start_time DESC LIMIT 10
""").show()
```

---

### Job Repairs + Parameter Overrides

**What is it / when to use:**
- Job repair = re-run failed job with same context (debug intermittent issues).
- Parameter overrides = alter parameters on re-run without changing job definition.

**Syntax:**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Repair (restart) run
w.jobs.repair_run(run_id=456)

# Submit run with parameter override
run = w.jobs.submit_run(
    job_id=123,
    notebook_params={
        'environment': 'debug',
        'sample_pct': '0.1'
    },
    tags={'debug': 'true'}
)

# Monitor repair
repair_run = w.jobs.get_run(run_id=run.run_id)
print(repair_run.state)
```

---

### Event Logs + Spark UI to Debug Pipelines

**What is it / when to use:**
- Event logs = detailed timeline of pipeline execution (Lakeflow Declarative Pipelines).
- Spark UI = low-level task execution.

**Syntax:**
```python
# Event logs
w = WorkspaceClient()
events = w.pipelines.get_event_logs(pipeline_id='my_pipeline')
for event in events:
    if event.level == 'ERROR':
        print(f"{event.timestamp}: {event.message}")

# Spark UI via cluster logs
# http://driver:4040/ (during run) or log file post-run
```

---

### Deploy with Declarative Automation Bundles

**What is it / when to use:**
- Bundles = IaC, Git-based, multi-target deployment.
- `databricks bundle deploy -t <target>` = reproducible, versioned.

**Syntax:**
```yaml
# databricks.yml
bundle:
  name: my-pipeline
  targets:
    dev:
      workspace:
        host: https://dev.cloud.databricks.com
      variables:
        job_max_concurrent_runs: 1
    prod:
      workspace:
        host: https://prod.cloud.databricks.com
      variables:
        job_max_concurrent_runs: 2

resources:
  jobs:
    my_job:
      name: etl-job
      max_concurrent_runs: ${var.job_max_concurrent_runs}
      tasks:
        - task_key: process
          notebook_task:
            notebook_path: ./src/process
          new_cluster:
            spark_version: "14.3.x-scala2.12"
```

**Deploy:**
```bash
cd /path/to/project
databricks bundle deploy -t prod
databricks bundle run -t prod --job my_job
```

---

### Git Folders (ex-Repos) for CI/CD Git-Based

**What is it / when to use:**
- Git folders = workspace folder linked to Git repo (push/pull sync).
- CI/CD integration: webhook on push → trigger job → run pipeline.

**Syntax:**
```python
# Create Git folder via CLI
# databricks workspace import-dir <local-dir> /Repos/my-repo

# Or via API
w = WorkspaceClient()
w.repos.create(
    url="https://github.com/myorg/myrepo.git",
    provider="gitHub",
    path="/Repos/production/myrepo"
)

# Trigger on push (webhook)
# GitHub Actions / GitLab CI / Jenkins → databricks jobs submit-run
```

---

## 9. Data Modeling (5%)

### Delta / Iceberg Table Layout — Partition-to-Grain, Clustering, Compaction

**What is it / when to use:**
- Partitioning = divide table by column (date, region); reduces scan area.
- Clustering (Liquid) = data locality per microbatch, dynamic.
- Grain = atomic level (e.g., transaction_date + order_id = grain of fact table).
- Compaction = merge small files, reduces metadata overhead.

**Heuristic:**
- Partition if < 1000 unique values and primary filter (date, region).
- Liquid Clustering if high-cardinality, evolving filters.
- Grain should be as fine as analysis needs (not necessarily atomic if only running rollups).

**Syntax:**
```python
# Partition + Clustering
spark.sql("""
    CREATE TABLE my_catalog.schema.fact_orders (
        order_id INT,
        customer_id INT,
        order_date DATE,
        amount DOUBLE
    )
    USING DELTA
    PARTITIONED BY (order_date)
    CLUSTER BY LIQUID (customer_id, amount)
""")

# Compaction (manual)
spark.sql("""
    OPTIMIZE my_catalog.schema.fact_orders
    ZORDER BY (customer_id)
""")
```

---

### Dimensional Modeling with Materialized Views + UC Metric Views

**What is it / when to use:**
- Dimensional modeling = star schema (fact + dims). Materialized views for pre-aggregation.
- **UC Metric Views** = governed, reusable metric definitions (NET-NEW).

**Syntax:**
```sql
-- Fact table (granular)
CREATE TABLE my_catalog.schema.fact_sales (
    sale_id INT,
    date_id INT,
    customer_id INT,
    product_id INT,
    amount DOUBLE,
    quantity INT
);

-- Dimension tables
CREATE TABLE my_catalog.schema.dim_customer (
    customer_id INT,
    name STRING,
    region STRING
);

-- Materialized view (pre-aggregation)
CREATE MATERIALIZED VIEW my_catalog.schema.sales_by_region_daily AS
SELECT
    c.region,
    f.date_id,
    SUM(f.amount) AS total_sales,
    COUNT(*) AS transaction_count
FROM my_catalog.schema.fact_sales f
JOIN my_catalog.schema.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.region, f.date_id;

-- UC Metric View (NET-NEW)
CREATE METRIC VIEW my_catalog.schema.metric_sales_by_region AS
SELECT
    c.region,
    SUM(f.amount) AS revenue
FROM my_catalog.schema.fact_sales f
JOIN my_catalog.schema.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.region;

-- Query metric view (reusable, governed)
SELECT * FROM my_catalog.schema.metric_sales_by_region WHERE region = 'US';
```

---

## Glossary of Renamings

| Old Term | New Term | Context |
|---|---|---|
| Delta Live Tables (DLT) | Lakeflow Declarative Pipelines | Declarative ETL pipelines |
| Declarative Automation Bundles (DABs) | Bundles | IaC / deploy infrastructure |
| APPLY CHANGES | AUTO CDC APIs | CDC in pipelines |
| Repos | Git folders | Version-controlled workspaces |
| Workflows | Lakeflow Jobs | Job orchestration |

---

## Quick Heuristics for Questions

### Streaming Table vs Materialized View
- **ST**: "Need append-only, exactly-once, low latency?" → **Streaming Table**
- **MV**: "Need full history, late updates/deletes, batch refresh?" → **Materialized View**

### Serverless vs Classic Compute
- **Serverless**: < 30min task, stateless, auto-scaling OK → **Serverless**
- **Classic**: long-running, state-heavy, debugging, complex UDF → **Classic**

### Delta vs Iceberg
- **Delta**: OLAP, fast data skipping, low cost → **Delta (default)**
- **Iceberg**: column-oriented, cross-workspace sharing, complex schema evolution → **Iceberg**

### Which cost optimization?
1. **CDF** for incremental (don't reprocess history).
2. **Liquid Clustering** for evolving queries (no manual OPTIMIZE).
3. **Deletion Vectors** for frequent deletes (no rewrite).
4. **Delta Cache** for read-heavy < 10x RAM.

### Which security technique?
- **PII in pipeline**: UC column masks + UC ABAC tags.
- **Row-level access**: row filters via UC + governed tags.
- **Compliance purging**: soft-delete + VACUUM after retention period.

### Job failed, how to debug?
1. Check **Spark UI** DAG (task-level bottleneck).
2. Check **cluster logs** (startup, dependency).
3. Check **system tables** (cluster events, query history).
4. Check **event logs** (pipeline stage breakdown).
5. **Repair run** (re-run with same context).

### Deploy: bundle vs Git folder?
- **Bundle**: IaC, versioned, multi-target overrides → **Bundles**
- **Git folder**: workspace sync with Git, CI/CD webhook → **Git folders** (+ bundles for resources)

---

## Dedicated Recap: APPLY CHANGES → AUTO CDC + SCD (Section 1)

### Renaming (vocabulary gotcha)
| Legacy (DLT) | Current (Lakeflow) |
|---|---|
| SQL `APPLY CHANGES INTO` | `AUTO CDC INTO` |
| Python `dlt.apply_changes()` | `dp.create_auto_cdc_flow()` |
| Python `dlt.apply_changes_from_snapshot()` | `dp.create_auto_cdc_from_snapshot_flow()` |

Processes feed of CDC (insert/update/delete) into a **pre-created streaming table target**; resolves order/late data; maintains SCD Type 1 or 2.

### Syntax
```sql
CREATE OR REFRESH STREAMING TABLE users;
CREATE FLOW user_flow AS AUTO CDC INTO users
FROM STREAM(user_changes)            -- table/view only, NOT subquery
KEYS (user_id)
APPLY AS DELETE WHEN operation = 'DELETE'   -- before SEQUENCE BY
SEQUENCE BY event_ts
STORED AS SCD TYPE 2;               -- default TYPE 1
```
```python
dp.create_streaming_table(name="users")
dp.create_auto_cdc_flow(
    target="users", source="user_changes", keys=["user_id"],
    sequence_by="event_ts",          # str, col("ts") or struct("ts","id")
    stored_as_scd_type=2,            # 1 (default) or 2
    apply_as_deletes=expr("operation = 'DELETE'"),
    ignore_null_updates=True,
    track_history_column_list=["balance","status"],  # SCD2: only these generate new version
)
```
Snapshot (Python only): `dp.create_auto_cdc_from_snapshot_flow(...)` compares consecutive complete snapshots.

### SCD Type 1 vs Type 2
| | Type 1 (default) | Type 2 |
|---|---|---|
| History | overwrites | keeps versions |
| System columns | — | `__START_AT` / `__END_AT` (sequence_by type) |
| Current row | the row | `WHERE __END_AT IS NULL` |
| `APPLY AS TRUNCATE` | ✅ | ❌ Type 1 only |
| `TRACK HISTORY ON` | n/a | only listed cols generate new version; rest = update in-place |

### Key Parameters
- `KEYS`/`keys` (PK, required); `SEQUENCE BY`/`sequence_by` (ordering; use high-precision timestamp or `STRUCT(ts,tiebreaker)`); `APPLY AS DELETE`/`apply_as_deletes` (on Type 2, closes version by setting `__END_AT`); `IGNORE NULL UPDATES`/`ignore_null_updates`; `COLUMNS ... EXCEPT`/`column_list`/`except_column_list`.

### Querying SCD Type 2
- Current: `WHERE __END_AT IS NULL` (materialize in MV `dim_*_current` if queried frequently).
- Point-in-time (inclusive/exclusive): `__START_AT <= D AND (__END_AT > D OR __END_AT IS NULL)`. `>=` at end = double-count at edge.
- Join fact × historical dim (revenue-correct): `s.event_date >= p.__START_AT AND (s.event_date < p.__END_AT OR p.__END_AT IS NULL)`.

### Gotchas
- `FROM STREAM(...)` accepts table/view only (not subquery) → pre-filter with temporary view.
- SQL: `APPLY AS DELETE/TRUNCATE` before `SEQUENCE BY`.
- Columns `__START_AT`/`__END_AT` with double underscore.
- `create_auto_cdc_flow()` returns no value; target already created with `create_streaming_table()`.

Source: databricks-pipelines skill (auto-cdc + scd-2-querying), aligned to official docs.

---

**Updated for new exam (Oct/2026). Verify features at docs.databricks.com before the exam.**
