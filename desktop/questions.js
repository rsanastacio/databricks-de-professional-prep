window.SIMULADOS = {
 "simulado1": {
  "title": "Practice Exam 1",
  "questions": [
   {
    "id": 1,
    "domain": "Developing Code",
    "topic": "Structured Streaming Stateful",
    "text": "You are implementing a real-time event aggregator with Structured Streaming that counts events by user_id in 10-minute windows. The pipeline receives events with up to 2 hours of delay. Which combination of configurations ensures that late-arriving events are captured without duplication upon resumption?",
    "options": {
     "A": "Output mode `complete` with `watermark = 2 hours` and remote checkpoint",
     "B": "Output mode `append` with `watermark = 10 minutes` and state checkpoint",
     "C": "Output mode `update` with `watermark = 2 hours` without checkpoint",
     "D": "Output mode `append` with `state_retention = 2 hours` and remote checkpoint"
    },
    "answer": "A",
    "explanation": "Watermark of 2 hours allows late-arriving events up to 2h. Remote checkpoint ensures resumption without duplication (output mode `complete` is correct for aggregations). Mode `append` with 10 minutes watermark would lose events outside the window; `update` without checkpoint loses state on crash.",
    "optExpl": {
     "A": "Correct: output mode `complete` returns all aggregated data and watermark of 2h captures events arriving up to 2 hours later.",
     "B": "Incorrect: append mode only writes new lines and would lose events arriving after the 10-minute window passes.",
     "C": "Incorrect: without checkpoint, state is lost upon resumption after failure and can generate duplication.",
     "D": "Incorrect: `state_retention` is not a valid Structured Streaming parameter; retention is controlled by watermark."
    }
   },
   {
    "id": 2,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Lakeflow Connect",
    "text": "A client needs to replicate inserts, updates, and deletes from a PostgreSQL table to Databricks in near-real time while maintaining a change history by date. Which solution requires the LEAST custom code?",
    "options": {
     "A": "Apache Kafka + Python Delta Writer",
     "B": "Lakeflow Connect managed connector with automatic CDC",
     "C": "DMS (AWS) + ADLS → Lakeflow API",
     "D": "Airflow + JDBC source + Spark batch"
    },
    "answer": "B",
    "explanation": "Lakeflow Connect managed connector for PostgreSQL CDC is officially supported and reduces code vs Kafka + custom writer. DMS and Airflow require more configuration; both are valid but B has \"less code\".",
    "optExpl": {
     "A": "Incorrect: Kafka + Delta Writer requires custom code for CDC and state management.",
     "B": "Correct: Lakeflow Connect managed connector provides automatic CDC with minimal custom code.",
     "C": "Incorrect: DMS + ADLS requires additional AWS configuration and custom pipeline.",
     "D": "Incorrect: Airflow + batch is a batch solution, not near-real time, and requires complex orchestration."
    }
   },
   {
    "id": 3,
    "domain": "Data Manipulation",
    "topic": "VARIANT",
    "text": "A JSON API returns a `metadata` field that can have dynamic structure (an array with varying objects or a simple string). You need to extract the first element if it's an array, or the full value if it's a string. Which function allows this without a type error?",
    "options": {
     "A": "`get_json_object(metadata, '$[0]')` with try-catch",
     "B": "`from_json(metadata, 'array<string>')` after schema validation",
     "C": "`variant_get(parse_json(metadata), ':0')` or `variant_get(parse_json(metadata), '')` depending on type",
     "D": "`json_extract_array(metadata)[0]` with `coalesce`"
    },
    "answer": "C",
    "explanation": "`variant_get(parse_json(...), ':0')` and variants handle VARIANT without pre-existing type error. `from_json` would force fixed schema (throw error on mismatch). `json_extract_array` fails if not an array.",
    "optExpl": {
     "A": "Incorrect: `get_json_object` fails with unexpected type and does not handle union of array/string.",
     "B": "Incorrect: `from_json` would force fixed schema and fail if field is pure string (type mismatch).",
     "C": "Correct: `variant_get` with `parse_json` handles dynamic types (array or string) without pre-existing error.",
     "D": "Incorrect: `json_extract_array` fails if metadata is not an array (does not handle union type)."
    }
   },
   {
    "id": 4,
    "domain": "Monitoring and Alerting",
    "topic": "Query Performance",
    "text": "A SQL Warehouse has slow queries (P99 increasing). You suspect data freshness or ineffective caching. Which metric from Query History and Warehouse Stats do you check FIRST to rule out stale data?",
    "options": {
     "A": "`scan_bytes` and `bytes_spilled` from the query",
     "B": "`cache_hit_ratio` of the warehouse + `total_rows_scanned` per table",
     "C": "`query_start_time` vs `table_updated_at` + verify Delta Cache status",
     "D": "`remote_IO_bytes` and `local_IO_bytes` from the Photon profile"
    },
    "answer": "C",
    "explanation": "`query_start_time` vs `table_updated_at` reveals whether data is stale. Delta Cache status shows hit rate. Both metrics are critical; C is most practical for ruling out stale data issue.",
    "optExpl": {
     "A": "Incorrect: `scan_bytes` and `bytes_spilled` indicate current performance, not whether data is stale.",
     "B": "Incorrect: `cache_hit_ratio` does not indicate whether cached data is out of date.",
     "C": "Correct: comparing `query_start_time` vs `table_updated_at` reveals stale data and Delta Cache status confirms.",
     "D": "Incorrect: `remote_IO_bytes` indicates I/O patterns, not data freshness."
    }
   },
   {
    "id": 5,
    "domain": "Cost & Performance Optimization",
    "topic": "CLUSTER BY AUTO",
    "text": "You have a fact table (`sales`) with 2B rows that is queried by `region` and `date_id` 80% of the time. The current job reads 50% of the table per query. Which strategy reduces cost most significantly?",
    "options": {
     "A": "Partitioning by `region` + `date_id`",
     "B": "`CLUSTER BY region, date_id` (manual, without automatic reordering)",
     "C": "`CLUSTER BY AUTO region, date_id` with Predictive Optimization",
     "D": "Liquid clustering without Predictive Optimization"
    },
    "answer": "C",
    "explanation": "`CLUSTER BY AUTO` with Predictive Optimization reorders automatically based on access patterns, reducing cost more than fixed partitioning (which does not scale to dynamic query patterns).",
    "optExpl": {
     "A": "Incorrect: fixed partitioning does not scale well for multiple dynamic query pattern combinations.",
     "B": "Incorrect: CLUSTER BY manual without automatic reordering does not adapt to access pattern changes.",
     "C": "Correct: `CLUSTER BY AUTO` with Predictive Optimization reorders automatically, significantly reducing cost.",
     "D": "Incorrect: liquid clustering without Predictive Optimization does not reorder automatically and benefit is limited."
    }
   },
   {
    "id": 6,
    "domain": "Data Security and Compliance",
    "topic": "ABAC with Governed Tags",
    "text": "You need to apply row filters and column masks to 50+ tables based on user department. Doing this table by table is unsustainable. Which approach scales better?",
    "options": {
     "A": "Create a parameterized view by `CURRENT_USER()` for each table",
     "B": "ABAC with managed tags (governed tags) + declarative row filters/column masks",
     "C": "Replicate data by department across separate catalogs with GRANT per catalog",
     "D": "UDFs that filter by `CURRENT_ROLE()` in each query"
    },
    "answer": "B",
    "explanation": "ABAC with governed tags enables row filters and column masks declaratively in bulk. Parameterized views do not scale well (50+ tables × multiple groups = combinatorial explosion).",
    "optExpl": {
     "A": "Incorrect: creating parameterized views for 50+ tables × multiple groups generates unsustainable combinatorics.",
     "B": "Correct: ABAC with governed tags enables declarative application of row filters and column masks in bulk.",
     "C": "Incorrect: replicating data across separate catalogs causes duplication and maintenance complexity.",
     "D": "Incorrect: UDFs in each query do not scale well; difficult to maintain and audit."
    }
   },
   {
    "id": 7,
    "domain": "Data Governance",
    "topic": "UC Metric Views",
    "text": "An analyst needs to monitor data delivery SLA: \"how many tables in the catalog were updated in the last 24 hours?\". Which object allows declaring this declaratively, auditing who accessed it, and being optimized by the system?",
    "options": {
     "A": "A SQL view that queries `information_schema.table_updates`",
     "B": "UC Metric View that aggregates count and last_modified_time per table",
     "C": "A Lakeview dashboard that queries UC lineage logs",
     "D": "A Delta table that writes UPDATE timestamps via a trigger"
    },
    "answer": "B",
    "explanation": "UC Metric Views are managed SQL objects that aggregate metadata and can audit access. Information schema is read-only; dashboards are passive; Delta has no built-in triggers.",
    "optExpl": {
     "A": "Incorrect: SQL view is read-only and not a managed object optimizable by system.",
     "B": "Correct: UC Metric View is managed SQL object that aggregates, audits access, and is optimized by planner.",
     "C": "Incorrect: Lakeview dashboard is passive; does not audit who accessed the table.",
     "D": "Incorrect: Delta has no built-in triggers; would be complex custom solution."
    }
   },
   {
    "id": 8,
    "domain": "Debugging and Deploying",
    "topic": "Git Folders",
    "text": "You have a workspace that syncs SQL queries from a Git repo. You modified a query that was deployed to production. What is the risk and how to mitigate it?",
    "options": {
     "A": "Query will run with repo version — use Git tags + branch protection to control release",
     "B": "Query can become orphaned if repo is deleted — version backups in /Workspace/archive",
     "C": "Change syncs immediately to all running the query — use Declarative Automation Bundles with change review",
     "D": "Workspace stays out-of-sync if repo changes — use read-only Git Folders and promote via pull request"
    },
    "answer": "D",
    "explanation": "Git Folders sync workspace with repo. Changes in repo sync to workspace. Risk: accidental changes propagate. Mitigation: read-only Git Folder + promote via PR + branch protection.",
    "optExpl": {
     "A": "Incorrect: Git Folder does not sync queries so they run with specific repo version.",
     "B": "Incorrect: not the primary risk of Git Folder; backups in /Workspace/archive do not prevent accidental changes.",
     "C": "Incorrect: Declarative Automation Bundles is for deployments, not Git Folder mitigation.",
     "D": "Correct: Git Folder syncs bidirectionally; using read-only + PR review mitigates accidental production changes."
    }
   },
   {
    "id": 9,
    "domain": "Data Modeling",
    "topic": "Slowly Changing Dimension",
    "text": "A customer dimension has history (new email, new address, status change). Which pattern with Databricks requires FEWER I/O operations when doing an update?",
    "options": {
     "A": "Simple SCD Type 1 (overwrite); retain history in backup table",
     "B": "SCD Type 2 with `MERGE` + insert new row with `is_current` flag",
     "C": "Automatic SCD Type 2 with `stored_as_scd_type(2)` in MERGE",
     "D": "Immutable event log + view that materializes current snapshot in MV"
    },
    "answer": "C",
    "explanation": "`stored_as_scd_type(2)` in MERGE automates SCD Type 2, reducing I/O vs manual operations + compaction.",
    "optExpl": {
     "A": "Incorrect: simple SCD Type 1 requires overwrite + backup; not automated and requires separate operations.",
     "B": "Incorrect: manual MERGE with new row insertion requires custom code; not automatic.",
     "C": "Correct: `stored_as_scd_type(2)` in MERGE automates SCD Type 2, reducing I/O vs manual operations.",
     "D": "Incorrect: immutable event log with MV is overkill and requires manual refresh; more complex than automatic MERGE."
    }
   },
   {
    "id": 10,
    "domain": "Developing Code",
    "topic": "Python UDF",
    "text": "A Python UDF `process_text` uses the `spacy` library that is not pre-installed on the cluster. For production use where the UDF must be reused across multiple jobs, which approach is most scalable?",
    "options": {
     "A": "Install `spacy` via `pip install` in each notebook before defining the UDF (only for that session)",
     "B": "Define the UDF in a shared notebook and rely on dynamic library loading",
     "C": "Manually manage dependencies via `spark.jars.packages` in each job configuration",
     "D": "Use a cluster init script to install `spacy` at startup (one-time setup, automatically available to all jobs)"
    },
    "answer": "D",
    "explanation": "Init script (D) is most scalable: one-time installation at cluster startup, automatically reused across all jobs. Notebook-level pip (A) is session-only; manual dependency management (C) requires per-job configuration.",
    "optExpl": {
     "A": "Incorrect: notebook-level pip install is session-only and requires reinstalling for each new notebook or cluster restart.",
     "B": "Incorrect: sharing UDFs via notebook does not solve the library installation problem.",
     "C": "Incorrect: `spark.jars.packages` is for Java/Scala packages, not Python libraries; requires manual per-job configuration.",
     "D": "Correct: init script installs spacy once at cluster startup and is automatically available to all jobs without per-job setup."
    }
   },
   {
    "id": 11,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Iceberg Format Target",
    "text": "You are ingesting data from an S3 data lake that is queried by multiple workspaces (shared via OpenSharing). The schema changes frequently. Which target format ensures schema enforcement and shared versioning?",
    "options": {
     "A": "Delta with `schema_evolution = true`",
     "B": "Iceberg with default catalog registered in the workspace",
     "C": "Parquet with schema inferred by Glue/Hive metastore",
     "D": "Iceberg with UC catalog and shared warehouse access"
    },
    "answer": "D",
    "explanation": "Iceberg with UC catalog ensures schema enforcement, shared versioning, and OpenSharing integration. Delta + OpenSharing works, but Iceberg is more robust for multi-workspace.",
    "optExpl": {
     "A": "Incorrect: Delta with `schema_evolution` does not ensure shared schema enforcement across workspaces.",
     "B": "Incorrect: Iceberg without UC does not ensure shared versioning across workspaces (OpenSharing).",
     "C": "Incorrect: Parquet lacks native schema enforcement; metastore does not scale well for OpenSharing.",
     "D": "Correct: Iceberg + UC catalog ensures schema enforcement, shared versioning, and OpenSharing integration."
    }
   },
   {
    "id": 12,
    "domain": "Data Manipulation",
    "topic": "Deletion Vectors",
    "text": "A Delta table has 10B records. You need to delete 100M rows (1% of total). Without deletion vectors, what would be the impact on a query after the DELETE?",
    "options": {
     "A": "Query reads only non-deleted rows (automatic rewrite)",
     "B": "Query reads all 10B rows and filters in the Photon layer",
     "C": "Query reads 10B rows and evaluates deletion bitmap for each row",
     "D": "Immediate rewrite rewrites all files in seconds"
    },
    "answer": "C",
    "explanation": "Without deletion vectors, Spark maintains a bitmap of deleted rows and evaluates each of the 10B rows (reading all 10B lines with filter bitmap). With DV, skips deleted files physically.",
    "optExpl": {
     "A": "Incorrect: without deletion vectors, automatic rewrite does not occur when deleting rows.",
     "B": "Incorrect: Photon does not filter deletion bitmap; Spark maintains bitmap for filtering.",
     "C": "Correct: without deletion vectors, Spark evaluates deletion bitmap for each of the 10B rows read.",
     "D": "Incorrect: DELETE without deletion vectors does not immediately rewrite all files."
    }
   },
   {
    "id": 13,
    "domain": "Monitoring and Alerting",
    "topic": "Predictive Optimization",
    "text": "Your warehouse receives queries that vary greatly in pattern (sometimes OLTP, sometimes OLAP). Which Predictive Optimization metric indicates that you need to adjust compaction hint or liquid clustering?",
    "options": {
     "A": "Query latency degradation > 20% vs baseline",
     "B": "Scan bytes vs bytes skipped ratio below 5%",
     "C": "Liquid clustering overhead (shuffle bytes) exceeding pruning benefit",
     "D": "Prediction of query time degradation based on historical access patterns"
    },
    "answer": "D",
    "explanation": "Predictive Optimization predicts query latency degradation based on historical access patterns, indicating when reordering/compaction is needed.",
    "optExpl": {
     "A": "Incorrect: latency degradation is observed symptom, not predictive Predictive Optimization metric.",
     "B": "Incorrect: is reactive pruning inefficiency metric, not predictive recommendation for reordering.",
     "C": "Incorrect: is specific overhead symptom, not general Predictive Optimization metric.",
     "D": "Correct: Predictive Optimization predicts query latency degradation based on historical access patterns."
    }
   },
   {
    "id": 14,
    "domain": "Cost & Performance Optimization",
    "topic": "Delta Cache",
    "text": "A dimension table (`dim_product`) is queried in 95% of warehouse queries. Which configuration maximizes Delta Cache hit rate and reduces cost?",
    "options": {
     "A": "`spark.databricks.io.cache.enabled = true` for all clusters",
     "B": "SQL Warehouse with cache enabled + `spark.databricks.io.cache.type = MEMORY` in warehouse settings",
     "C": "Force table in broadcast with `BROADCAST(dim_product)` in each query",
     "D": "Replicate dimension in MEMORY-ONLY table and reference via alias"
    },
    "answer": "B",
    "explanation": "SQL Warehouse with cache enabled + `MEMORY` type maximizes hit rate. Broadcast is for specific joins, not general dimension caching.",
    "optExpl": {
     "A": "Incorrect: only enables cache globally; does not optimize hit rate specifically for SQL Warehouse.",
     "B": "Correct: SQL Warehouse with cache enabled + MEMORY type specifically optimized for dimension caching.",
     "C": "Incorrect: broadcast is for specific joins; not continuous dimension caching mechanism.",
     "D": "Incorrect: MEMORY-ONLY tables do not persist data between queries (cache is per-session)."
    }
   },
   {
    "id": 15,
    "domain": "Data Security and Compliance",
    "topic": "Data Redaction at Scale",
    "text": "You need to redact PII (email, CPF) in 200+ tables in a catalog accessed by multiple groups. Which approach is more sustainable?",
    "options": {
     "A": "Create views with masked PII for each combination of table × group",
     "B": "Use UC column masks declaratively + ABAC to apply in bulk",
     "C": "Apply schema evolution with inline masking UDFs",
     "D": "Separate ETL that creates \"safe\" tables without PII for external groups"
    },
    "answer": "B",
    "explanation": "UC column masks declaratively + ABAC scale better than manual views. Views = N tables × M groups.",
    "optExpl": {
     "A": "Incorrect: 200+ tables × multiple groups creates combinatorially unsustainable setup.",
     "B": "Correct: UC column masks + ABAC scale with declarative policies applied in bulk.",
     "C": "Incorrect: data replication causes duplication and exponential maintenance complexity.",
     "D": "Incorrect: UDFs in each query do not scale well and are hard to maintain consistently."
    }
   },
   {
    "id": 16,
    "domain": "Debugging and Deploying",
    "topic": "Declarative Automation Bundles",
    "text": "You are migrating multiple SQL jobs from Data Factory to Databricks. Which approach reduces manual toil and enables version control + automatic retry?",
    "options": {
     "A": "Register jobs via Databricks REST API with Python loop",
     "B": "Use Declarative Automation Bundles (DAB) in YAML with Git sync",
     "C": "Copy SQL notebook to workspace and trigger via webhook",
     "D": "Use Azure Data Factory linked service with Databricks notebook activity"
    },
    "answer": "B",
    "explanation": "Declarative Automation Bundles (DAB) in YAML + Git sync is modern standard for version control + automatic deployment.",
    "optExpl": {
     "A": "Incorrect: REST API loop provides no version control and requires repeated manual work.",
     "B": "Correct: Declarative Automation Bundles (DAB) in YAML + Git sync enables version control and automatic deployment.",
     "C": "Incorrect: copying notebooks + webhooks does not provide adequate version control.",
     "D": "Incorrect: maintaining Azure Data Factory dependency does not significantly reduce toil."
    }
   },
   {
    "id": 17,
    "domain": "Debugging and Deploying",
    "topic": "Cluster Init Script Error",
    "text": "An init script installing package `xgboost` is failing silently. The cluster starts, but jobs fail with ImportError. What is the MOST likely cause and fix?",
    "options": {
     "A": "`pip` is not in PATH — use `/usr/bin/python3 -m pip install`",
     "B": "Init script returns error but cluster `exit 0` anyway — add `set -e` and fail-fast",
     "C": "XGBoost installed by user, not root — add `sudo` before pip",
     "D": "Package incompatible with DBR version — check pypi and test locally"
    },
    "answer": "B",
    "explanation": "Init script needs `set -e` to fail-fast. Without it, cluster starts even with error.",
    "optExpl": {
     "A": "Incorrect: if pip were not in PATH, command would fail immediately, not silently.",
     "B": "Correct: init script without `set -e` returns error but cluster starts anyway (implicit exit 0).",
     "C": "Incorrect: root vs user permissions are secondary; main problem is fail-fast.",
     "D": "Incorrect: package incompatibility would cause error during installation, not silent failure at startup."
    }
   },
   {
    "id": 18,
    "domain": "Data Modeling",
    "topic": "Fact Table Grain",
    "text": "A fact table (`events`) has grain `[timestamp, event_type, user_id]` but receives late-arriving corrections for events from 48 hours ago with field `is_correction = true`. Which design avoids aggregation duplication?",
    "options": {
     "A": "Insert corrections as new rows; MV groups `is_correction = false` only",
     "B": "MERGE with `WHEN MATCHED AND is_correction = true THEN UPDATE` based on composite key",
     "C": "Separate fact tables: `events_live` + `events_corrections` with UNION in view",
     "D": "Add `_dbt_valid_from` + `_dbt_valid_to` and materialize versioned snapshot"
    },
    "answer": "B",
    "explanation": "MERGE with composite key and `is_correction` flag prevents duplication. Inserting as new row does not aggregate easily.",
    "optExpl": {
     "A": "Incorrect: inserting corrections as new rows causes duplication and complex aggregation.",
     "B": "Correct: MERGE with composite key and `is_correction` flag updates existing row, avoiding duplication.",
     "C": "Incorrect: separate tables + UNION more complex and hard to aggregate without duplication.",
     "D": "Incorrect: versioning with timestamps is overkill for simple late correction handling."
    }
   },
   {
    "id": 19,
    "domain": "Developing Code",
    "topic": "AI Functions",
    "text": "You need to classify hundreds of millions of texts (`description`) into 5 categories using an LLM. Which approach combines performance and cost?",
    "options": {
     "A": "`ai_query('gpt-4o', 'Categorize: {description}')` in Python application with batch API",
     "B": "`ai_query()` UDF with `api_key` stored in UC secret, applied via Spark SQL batched",
     "C": "API call via `requests` in Python UDF with embedding cache",
     "D": "Fine-tuned local model in Databricks Model Registry, inference via model serving endpoint"
    },
    "answer": "B",
    "explanation": "`ai_query()` UDF with secret + batch processing scales. Direct GPT-4 would cost more; local model is alternative but LLM is more flexible.",
    "optExpl": {
     "A": "Incorrect: Python batch API calls do not scale to hundreds of millions (API latency).",
     "B": "Correct: Spark SQL UDF with batching + UC secrets scales to hundreds of millions with optimized performance.",
     "C": "Incorrect: Python UDF without Arrow vectorization; embedding cache requires additional setup.",
     "D": "Incorrect: fine-tuned local model is overkill for simple 5-category classification."
    }
   },
   {
    "id": 20,
    "domain": "Data Ingestion & Acquisition",
    "topic": "AUTO CDC + SCD",
    "text": "You have a `customer_snapshot` table that is overwritten daily with full extract from an ERP. Which setup captures changes (insert, update, delete) automatically without complex CDC?",
    "options": {
     "A": "MERGE with `WHEN NOT MATCHED BY SOURCE THEN DELETE` + `stored_as_scd_type(1)`",
     "B": "CDF (Change Data Feed) + job that compares snapshots and generates delta",
     "C": "`AUTO` MERGE with SCD logic: `stored_as_scd_type(2)` captures insert/update/delete",
     "D": "Lakeflow Connect auto-CDC with snapshot compare and DELETE tracking"
    },
    "answer": "C",
    "explanation": "`AUTO` MERGE with `stored_as_scd_type` automates CDC without external tools.",
    "optExpl": {
     "A": "Incorrect: MERGE with `WHEN NOT MATCHED BY SOURCE` does not automate CDC from full snapshot.",
     "B": "Incorrect: CDF + custom snapshot comparison job is not automatic; requires orchestration.",
     "C": "Correct: `AUTO` MERGE with `stored_as_scd_type(2)` automates CDC for snapshot comparison.",
     "D": "Incorrect: Lakeflow Connect is for ingestion; not a transformation SCD mechanism."
    }
   },
   {
    "id": 21,
    "domain": "Data Manipulation",
    "topic": "CDF + Streaming",
    "text": "A Delta table has CDF enabled. You want to consume deletes via Structured Streaming `readStream` to maintain cache invalidation. Which option works?",
    "options": {
     "A": "`spark.readStream.format('delta').option('withChangeDataFeed', true)`",
     "B": "`spark.readStream.format('delta').table('my_table').withColumn('_change_type')`",
     "C": "`spark.readStream.format('delta').option('startVersion', 0).load()` with CDF enabled in catalog",
     "D": "Option A, but requires CDF to be enabled beforehand via `ALTER TABLE ... SET TBLPROPERTIES`"
    },
    "answer": "D",
    "explanation": "CDF must be enabled via `ALTER TABLE ... SET TBLPROPERTIES`. Then `readStream` with CDF option works.",
    "optExpl": {
     "A": "Incorrect: option A works but requires CDF enabled beforehand (does not mention prerequisite).",
     "B": "Incorrect: column addition does not read CDF; only creates artificial column.",
     "C": "Incorrect: incorrect syntax for consuming CDF via readStream.",
     "D": "Correct: option A is correct but requires `ALTER TABLE ... SET TBLPROPERTIES` to enable CDF beforehand."
    }
   },
   {
    "id": 22,
    "domain": "Monitoring and Alerting",
    "topic": "Data Freshness SLA",
    "text": "A table that should be updated every 4 hours was not updated for 6 hours. Which alert fires?",
    "options": {
     "A": "Lakeview dashboard with `CASE WHEN age_minutes > 240 THEN 'ALERT'`",
     "B": "UC Metric View that monitors `table_modified_time` + Databricks Alert SQL",
     "C": "Background refresh on Streaming Table with timeout",
     "D": "Both A and B; B is more intelligent"
    },
    "answer": "B",
    "explanation": "UC Metric Views + Alert SQL is native Databricks form. Dashboard is passive; B is more intelligent.",
    "optExpl": {
     "A": "Incorrect: Lakeview dashboard is passive; does not automatically trigger alert on SLA violation.",
     "B": "Correct: UC Metric View + Databricks Alert SQL automatically triggers alert on SLA violation.",
     "C": "Incorrect: Streaming Table background refresh with timeout is not a feature; does not trigger alert.",
     "D": "Incorrect: A is passive; D is incorrect because A does not trigger alert."
    }
   },
   {
    "id": 23,
    "domain": "Cost & Performance Optimization",
    "topic": "Liquid Clustering vs Partitioning",
    "text": "A `transactions` table has 100B rows, queried by `user_id`, `date`, and `amount` in different combinations. Partitioning by `date` left many date folders empty. Which is the best alternative?",
    "options": {
     "A": "`CLUSTER BY user_id, date` with manual recompaction weekly",
     "B": "Liquid clustering: `CLUSTER BY user_id, date, amount`; Predictive Optimization reorders automatically",
     "C": "Dynamic partitioning with `PARTITION BY (user_id, date)`",
     "D": "Hash bucketing: `BUCKETED BY (user_id) INTO 1024 BUCKETS`"
    },
    "answer": "B",
    "explanation": "Liquid clustering + Predictive Optimization scales better than fixed partitioning for varied queries.",
    "optExpl": {
     "A": "Incorrect: manual weekly recompaction is tedious and does not scale for multiple patterns.",
     "B": "Correct: liquid clustering + Predictive Optimization reorders automatically, reducing cost.",
     "C": "Incorrect: dynamic partitioning does not solve empty folders problem.",
     "D": "Incorrect: hash bucketing is legacy and does not work well with modern Databricks."
    }
   },
   {
    "id": 24,
    "domain": "Data Security and Compliance",
    "topic": "GDPR / Right to Erasure",
    "text": "A GDPR erasure request is handled with `DELETE FROM customers WHERE customer_id = ?` on a Delta table that has deletion vectors enabled. What must the team do so the customer's data is physically removed, including from older table versions, without breaking concurrent readers?",
    "options": {
     "A": "Nothing else: the `DELETE` removes the rows from the data files immediately",
     "B": "Wait for time travel to expire; files of old versions are deleted automatically after the retention period",
     "C": "Set `delta.deletedFileRetentionDuration = '0 hours'`, disable the retention check, and run `VACUUM` right after the `DELETE`",
     "D": "Run `REORG TABLE customers APPLY (PURGE)` to rewrite files that carry deletion vectors, then run `VACUUM` once the retention window has passed"
    },
    "answer": "D",
    "explanation": "With deletion vectors, `DELETE` only marks rows as deleted; the bytes stay in the existing files. `REORG TABLE ... APPLY (PURGE)` rewrites those files without the deleted rows, and `VACUUM` (after the retention window) removes the old files that older versions still reference. Forcing a zero-hour retention is unsafe for concurrent readers and long-running queries.",
    "optExpl": {
     "A": "Incorrect: with deletion vectors enabled, DELETE only marks rows as deleted; the data stays in the files and in older versions.",
     "B": "Incorrect: old data files are never removed automatically by time travel expiry; only VACUUM deletes unreferenced files.",
     "C": "Incorrect: zero-hour retention disables a safety check and can break concurrent readers, and without a REORG the soft-deleted rows still live in current files.",
     "D": "Correct: REORG ... APPLY (PURGE) physically rewrites files to drop soft-deleted rows, and VACUUM after the retention window removes the old files from previous versions."
    }
   },
   {
    "id": 25,
    "domain": "Developing Code",
    "topic": "foreachBatch + State Management",
    "text": "You are using `foreachBatch` in Structured Streaming to write to Delta. You need to ensure that a restarted microbatch (after failure) does not cause duplication in the table. How?",
    "options": {
     "A": "Use `mergeSchema = true` and rely on automatic transactions",
     "B": "Idempotent sink with `MERGE` based on composite key + checkpoint recovery",
     "C": "Disable retry in batch sink",
     "D": "Register batch ID before writing, verify after"
    },
    "answer": "B",
    "explanation": "Idempotent sink with MERGE on composite key + checkpoint recovery prevents duplication on retry.",
    "optExpl": {
     "A": "Incorrect: `mergeSchema` and automatic transactions do not prevent duplication on microbatch retry.",
     "B": "Correct: idempotent sink with MERGE on composite key + checkpoint recovery prevents duplication on retry.",
     "C": "Incorrect: disabling retry is not a solution; can lose data on sink failure.",
     "D": "Incorrect: check-after is race condition; not atomically safe on retry."
    }
   },
   {
    "id": 26,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Streaming Table Maintenance",
    "text": "A Streaming Table (ST) in Unity Catalog that ingests from Kafka has a checkpoint that grew to 100 GB in 30 days. What is the cause and solution?",
    "options": {
     "A": "State accumulation without watermark; add watermark + state retention time",
     "B": "Checkpoint not cleaned automatically; run `ALTER TABLE ... RESET CHECKPOINT`",
     "C": "ST recompilation overhead; redesign query",
     "D": "Iceberg metadata accumulates; run `VACUUM` regularly"
    },
    "answer": "A",
    "explanation": "Watermark + state retention time control checkpoint growth.",
    "optExpl": {
     "A": "Correct: state accumulation without watermark causes checkpoint growth; add watermark + state retention.",
     "B": "Incorrect: `RESET CHECKPOINT` is not a solution; would lose state and cause reprocessing.",
     "C": "Incorrect: recompilation overhead is not cause of 100 GB checkpoint growth.",
     "D": "Incorrect: Iceberg VACUUM does not affect Streaming Table checkpoint size."
    }
   },
   {
    "id": 27,
    "domain": "Data Manipulation",
    "topic": "MERGE vs INSERT OVERWRITE Performance",
    "text": "You need to do SCD Type 1 (overwrite customer.email) on 500M rows using 100K update records. Which is more efficient?",
    "options": {
     "A": "`INSERT OVERWRITE` the entire table after join with updates",
     "B": "`MERGE INTO customer WHEN MATCHED THEN UPDATE SET email` limited to 100K affected rows",
     "C": "DELETE updates, then INSERT new; VACUUM",
     "D": "Batch insert with window function and row_number repartitioning"
    },
    "answer": "B",
    "explanation": "MERGE limited to 100K updates is more efficient than rewriting 500M rows.",
    "optExpl": {
     "A": "Incorrect: `INSERT OVERWRITE` rewrites all 500M rows; inefficient for 100K updates.",
     "B": "Correct: MERGE focused on 100K rows is more efficient than rewriting entire table.",
     "C": "Incorrect: DELETE + INSERT + VACUUM = 3 operations; more I/O than single MERGE.",
     "D": "Incorrect: repartitioning with window function is unnecessary overhead."
    }
   },
   {
    "id": 28,
    "domain": "Monitoring and Alerting",
    "topic": "SQL Warehouse Autoscaling",
    "text": "A SQL Warehouse has autoscaling of 2–10 clusters. During off-peak (2 clusters active), queries are slow. Which metric directly indicates insufficient cluster capacity rather than slow query execution?",
    "options": {
     "A": "Query execution time; if all queries take >5 seconds individually, the issue is compute",
     "B": "Spark executor memory usage from Spark UI; high memory indicates compute bottleneck",
     "C": "`query_queue_time` in warehouse stats; sustained queue time > 5 minutes indicates insufficient clusters",
     "D": "Query cache hit ratio; low cache hits indicate inefficient caching"
    },
    "answer": "C",
    "explanation": "Queue time directly indicates cluster capacity: sustained queueing > 5 minutes proves insufficient clusters. Execution time (A) does not distinguish between queue time and actual compute. Memory (B) and cache (D) are compute/efficiency metrics, not capacity indicators.",
    "optExpl": {
     "A": "Incorrect: execution time does not distinguish between queue time and actual compute time.",
     "B": "Incorrect: memory usage indicates compute efficiency, not whether queries are waiting in queue.",
     "C": "Correct: queue_time directly measures queue wait; sustained queue time > 5 minutes confirms insufficient cluster capacity.",
     "D": "Incorrect: cache hit ratio measures caching efficiency, not whether lack of clusters causes slow performance."
    }
   },
   {
    "id": 29,
    "domain": "Cost & Performance Optimization",
    "topic": "Serverless SQL Warehouse",
    "text": "You have a dashboard that runs 20 queries/minute during business hours. Which option reduces cost for \"pay-per-query\"?",
    "options": {
     "A": "Serverless SQL Warehouse with `spot_instances = true`",
     "B": "Classic cluster with autoscaling disabled and 1 single-node",
     "C": "Serverless SQL Warehouse (managed compute, no cluster management)",
     "D": "Photon-enabled classic warehouse with `spark.databricks.photon.ml.enabled = true`"
    },
    "answer": "C",
    "explanation": "Serverless SQL Warehouse is \"pay-per-query\" with automatically managed compute. Spot instances do not exist in serverless.",
    "optExpl": {
     "A": "Incorrect: serverless has no `spot_instances` option; compute is automatically managed.",
     "B": "Incorrect: classic cluster is not pay-per-query; pays continuously for active node.",
     "C": "Correct: serverless SQL Warehouse is pay-per-query with automatically managed compute, no management required.",
     "D": "Incorrect: classic warehouse requires continuous management; is not pay-per-query."
    }
   },
   {
    "id": 30,
    "domain": "Debugging and Deploying",
    "topic": "Spark SQL Query Plan",
    "text": "An `EXPLAIN PLAN` shows `BroadcastHashJoin` on a dimension of 5B rows, and queries fail with out-of-memory errors. What should you do?",
    "options": {
     "A": "Add `/*+ BROADCAST(dim) */` hint to force broadcast (explicitly request broadcast)",
     "B": "Check only `spark.sql.autoBroadcastJoinThreshold`; if it is < 5GB, increase it to fit the table",
     "C": "Reorder the join so the larger table is on the left",
     "D": "Disable automatic broadcast via `spark.sql.autoBroadcastJoinThreshold = 0` and use SortMergeJoin with skew hints"
    },
    "answer": "D",
    "explanation": "A 5B row table cannot be broadcast. The fix is SortMergeJoin (set `autoBroadcastJoinThreshold = 0` to disable broadcast) combined with skew hints to handle data skew that may occur in sort-merge joins. Join reordering (A) and increasing memory (C) do not solve the fundamental issue of table size.",
    "optExpl": {
     "A": "Incorrect: explicit BROADCAST hint would make the out-of-memory error worse, not solve it.",
     "B": "Incorrect: increasing `autoBroadcastJoinThreshold` to fit 5B rows is impossible and would cause OOM.",
     "C": "Incorrect: join order does not change the fact that 5B rows cannot fit in executor memory for broadcast.",
     "D": "Correct: disable broadcast via threshold = 0 to force SortMergeJoin, and use skew hints to handle data distribution skew in the merge phase."
    }
   },
   {
    "id": 31,
    "domain": "Data Security and Compliance",
    "topic": "Clean Rooms",
    "text": "You want to share data with an external partner but without exposing individual identities — only aggregations. Which Databricks solution enables this?",
    "options": {
     "A": "Export CSV to partner",
     "B": "UC schema shared with row filters applying aggregation",
     "C": "Databricks Clean Rooms: shared SQL analytics without exposing raw data",
     "D": "Delta Share with external recipient (OpenSharing)"
    },
    "answer": "C",
    "explanation": "Clean Rooms enable shared SQL analytics without exposing raw data.",
    "optExpl": {
     "A": "Incorrect: CSV export exposes raw data; unsafe for shared analytics.",
     "B": "Incorrect: row filters do not automatically aggregate; still expose granular data.",
     "C": "Correct: Databricks Clean Rooms enable shared SQL analytics without exposing raw data.",
     "D": "Incorrect: Delta Share with external recipient exposes raw data; is not a clean room."
    }
   },
   {
    "id": 32,
    "domain": "Developing Code",
    "topic": "Window Function + Partitioning",
    "text": "A query calculates `ROW_NUMBER()` OVER `(PARTITION BY user_id ORDER BY timestamp)`. Imbalanced partitions exist: one user_id has 1B rows, others have few. What risks emerge?",
    "options": {
     "A": "Window function logic breaks; results are non-deterministic across imbalanced partitions",
     "B": "A single task stalls processing 1B rows while other tasks finish, and subsequent stages wait for the straggler",
     "C": "Performance impact is minimal; Spark parallelizes each partition independently",
     "D": "Both straggler delay and latency increase; consider repartitioning or skew hints to redistribute load"
    },
    "answer": "D",
    "explanation": "Imbalanced partitions cause both straggler delay (B—one task processing 1B rows blocks stage completion) and overall latency increase (D). Repartitioning or skew hints redistribute data to even out partition sizes and improve parallelism.",
    "optExpl": {
     "A": "Incorrect: window functions produce correct results on imbalanced partitions; logic does not depend on partition size.",
     "B": "Incorrect: while straggler delay occurs, this alone does not account for the overall latency pattern (incomplete answer).",
     "C": "Incorrect: performance impact is not minimal; data skew causes significant straggler delays and slows the entire job.",
     "D": "Correct: imbalanced partitions cause straggler tasks (1B rows on one task) and overall latency degradation; repartitioning or skew hints mitigate by redistributing data."
    }
   },
   {
    "id": 33,
    "domain": "Developing Code",
    "topic": "Schema Drift Detection",
    "text": "Kafka ingestion with Structured Streaming detects new field `new_field` in JSON events. You want to fail the pipeline if any schema drift occurs. Which approach(es) will catch the new field?",
    "options": {
     "A": "`failOnNewColumnViolation = true` in Spark readStream",
     "B": "Just set `mergeSchema = false` (default is already false)",
     "C": "Pre-ingestion JSON schema validation that rejects events with unexpected fields",
     "D": "A and C together; B alone does not prevent new fields from entering as nulls"
    },
    "answer": "D",
    "explanation": "Option A (`failOnNewColumnViolation`) and Option C (external schema validation) both work for catching schema drift. Option B (`mergeSchema = false`) is already the default but does not prevent new fields—they are added as null columns (does not fail the pipeline).",
    "optExpl": {
     "A": "Incomplete: `failOnNewColumnViolation = true` catches schema drift in Spark but only at Spark read stage.",
     "B": "Incorrect: `mergeSchema = false` is already the default; new fields are still added as null columns and pipeline continues (does not fail).",
     "C": "Incomplete: external validation catches drift before Spark but requires external tooling and may have latency.",
     "D": "Correct: combining A (Spark-side failure) and C (external validation before ingestion) provides defense-in-depth schema enforcement."
    }
   },
   {
    "id": 34,
    "domain": "Data Manipulation",
    "topic": "VARIANT Type Performance",
    "text": "Query `WHERE variant_get(col, ':field') = 'value'` on a 1B row VARIANT column is slow. After profiling, you find Spark performs a full table scan with no predicate push-down. What is the best permanent solution?",
    "options": {
     "A": "Add index on the VARIANT column via `CREATE INDEX col (field)`",
     "B": "Rewrite the query using `get_json_object(to_json(col), '$field')` instead of `variant_get()`",
     "C": "Increase executor memory and parallelize with more partitions",
     "D": "Extract the field as a separate normal column during ingestion; apply predicate to the extracted column"
    },
    "answer": "D",
    "explanation": "VARIANT does not support indexes (A) or efficient predicate push-down (B). Query rewrites (C) provide no permanent benefit. The best solution is to extract the field as a separate normal column at ingestion (D), enabling efficient filtering on the extracted column.",
    "optExpl": {
     "A": "Incorrect: VARIANT columns do not support creating indexes; indexing has no effect on performance.",
     "B": "Incorrect: query rewrites do not address the root cause; Spark still cannot push predicates down through VARIANT operations.",
     "C": "Incorrect: increasing executor memory or parallelism does not change the fact that Spark cannot push predicates on VARIANT efficiently.",
     "D": "Correct: extract the field as a separate normal column during ingestion; filtering on normal columns enables predicate push-down and partition pruning."
    }
   },
   {
    "id": 35,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Iceberg Table Evolution",
    "text": "An Iceberg table has existing data with schema `{id: int, name: string}`. You add `created_date: timestamp` but need to ensure old rows (which have no value for this column) can still be read without schema conflicts. Which is correct?",
    "options": {
     "A": "Use `ALTER TABLE ... ADD COLUMN created_date TIMESTAMP` with no default (let Iceberg handle missing values)",
     "B": "Use `ALTER TABLE ... ADD COLUMN created_date TIMESTAMP NOT NULL` to enforce consistency",
     "C": "Use `ALTER TABLE ... ADD COLUMN created_date TIMESTAMP DEFAULT NULL` so old rows resolve to NULL",
     "D": "Schema evolution in Iceberg does not support optional columns; must rewrite all data"
    },
    "answer": "C",
    "explanation": "Option C uses DEFAULT NULL, allowing old rows to safely read as NULL for the new column. Option B (NOT NULL without default) fails because old rows lack a value for a NOT NULL column. Option A without explicit default can cause type conflicts on reads.",
    "optExpl": {
     "A": "Incorrect: without explicit DEFAULT NULL, old rows may produce schema type conflicts when reading the missing column.",
     "B": "Incorrect: NOT NULL without default value fails on old rows that lack a value for this column.",
     "C": "Correct: DEFAULT NULL allows old rows to safely read the new column as NULL, enabling seamless schema evolution.",
     "D": "Incorrect: Iceberg fully supports optional column evolution; no rewrite is required for backward-compatible adds."
    }
   },
   {
    "id": 36,
    "domain": "Developing Code",
    "topic": "Watermark in Multi-Stream Join",
    "text": "You are joining two streams (events and actions) in Structured Streaming. How to ensure late-arriving events do not cause incorrect join?",
    "options": {
     "A": "Add watermark to both streams with same delay (e.g., 1 hour)",
     "B": "State timeout + recheckpoint; do not use watermark in join",
     "C": "Output mode `append` enforces correct join",
     "D": "Join is not possible in streams; use micro-batch MERGE instead"
    },
    "answer": "A",
    "explanation": "Watermark on both streams with same delay ensures correct join.",
    "optExpl": {
     "A": "Correct: adding watermark to both streams with same delay ensures correct join.",
     "B": "Incorrect: state timeout without watermark is insufficient for correct join.",
     "C": "Incorrect: append mode does not ensure correct join; requires watermark.",
     "D": "Incorrect: join is possible in streams with watermark; does not need micro-batch MERGE."
    }
   },
   {
    "id": 37,
    "domain": "Developing Code",
    "topic": "Streaming Performance Degradation",
    "text": "A Structured Streaming job ingesting 100K events/second from Kafka slowed to 50K/second after 3 days. Latency also increased from 10 seconds to 5 minutes. What are the likely causes and how would you diagnose?",
    "options": {
     "A": "Kafka topic under-partitioned; increase partition count",
     "B": "Ineffective watermark causing state to grow unbounded",
     "C": "Checkpoint files accumulating due to state size",
     "D": "Both B and C cause degradation; monitor state_bytes and checkpoint size in Spark Streaming UI to confirm"
    },
    "answer": "D",
    "explanation": "State explosion without watermark (B) and checkpoint growth (C) together cause the observed throughput drop and latency increase after 3 days. Monitoring state_bytes and checkpoint size in Spark UI confirms the diagnosis. Kafka rebalancing (A) would show immediate impact, not gradual 3-day degradation.",
    "optExpl": {
     "A": "Incorrect: partition rebalancing would cause immediate throughput drop, not gradual degradation over 3 days.",
     "B": "Incomplete: state explosion alone explains slowdown but not the full picture of checkpoint growth.",
     "C": "Incomplete: checkpoint growth is a symptom of state explosion, not an independent cause.",
     "D": "Correct: unbounded state growth (B) causes both state explosion and checkpoint bloat (C); diagnosis via state_bytes and checkpoint size metrics in Spark UI."
    }
   },
   {
    "id": 38,
    "domain": "Cost & Performance Optimization",
    "topic": "Compaction Strategy",
    "text": "A table receives 100K inserts/day via streaming. Small files accumulate. Your goal: minimize operational overhead while maintaining performance. Which strategy works best?",
    "options": {
     "A": "Manual daily `OPTIMIZE TABLE` + `VACUUM` (requires scheduled jobs)",
     "B": "Manual weekly bucketing reconfiguration to fix file size",
     "C": "Classic partitioning by date (does not prevent small file problem)",
     "D": "Liquid clustering + Predictive Optimization for automatic, continuous compaction (minimal overhead)"
    },
    "answer": "D",
    "explanation": "Liquid clustering + Predictive Optimization provides automatic, hands-off compaction without requiring manual OPTIMIZE jobs or static bucketing configuration. Manual OPTIMIZE (A) requires operational overhead; bucketing (B) is static and inflexible.",
    "optExpl": {
     "A": "Incorrect: manual daily OPTIMIZE requires operational overhead (scheduling, monitoring); not minimal overhead.",
     "B": "Incorrect: manual bucketing reconfiguration is tedious and inflexible; does not prevent small files from ongoing inserts.",
     "C": "Incorrect: date partitioning addresses time-based queries but does not solve the small file accumulation problem.",
     "D": "Correct: Liquid clustering + Predictive Optimization enables fully automatic compaction with minimal manual intervention."
    }
   },
   {
    "id": 39,
    "domain": "Data Security and Compliance",
    "topic": "UC Secret Management",
    "text": "A job needs a password to access an external database. Where to store and retrieve it?",
    "options": {
     "A": "Hardcode in notebook (NEVER)",
     "B": "UC Secrets via `dbutils.secrets.get(scope='db_scope', key='db_password')`",
     "C": "Store in `spark.conf` via cluster environment var",
     "D": "Both B and C work; B is more secure (audit trail)"
    },
    "answer": "B",
    "explanation": "UC Secrets with audit trail is more secure than env vars.",
    "optExpl": {
     "A": "Incorrect: hardcoding in notebook is insecure; NEVER use for credentials.",
     "B": "Correct: UC Secrets via `dbutils.secrets.get()` is secure with audit trail.",
     "C": "Incorrect: environment vars lack audit trail; less secure than UC Secrets.",
     "D": "Incorrect: C not recommended; B is the secure approach."
    }
   },
   {
    "id": 40,
    "domain": "Debugging and Deploying",
    "topic": "Notebook Parameter Injection",
    "text": "A parameterized notebook receives `{{date}}` that should be 2024-01-15. Job executes with literal value `{{date}}`. What is the problem?",
    "options": {
     "A": "Databricks does not interpolate literals in jobs; use `dbutils.widgets` instead",
     "B": "Job config passing wrong value; check `job_parameters` in job definition",
     "C": "Notebook created via Git Folder; Git Folder does not interpolate parameters",
     "D": "Both A and B; prefer `dbutils.widgets.get()` with default"
    },
    "answer": "B",
    "explanation": "Job config must pass parameter correctly. Parameterized notebook uses `dbutils.widgets.get()` to receive value.",
    "optExpl": {
     "A": "Incorrect: interpolation can work if job config is correct.",
     "B": "Correct: problem is job config passing literal value instead of dynamic parameter.",
     "C": "Incorrect: Git Folder does not affect job parameter interpolation.",
     "D": "Incorrect: A is incorrect; only B is correct."
    }
   },
   {
    "id": 41,
    "domain": "Data Governance",
    "topic": "Permission Inheritance in UC",
    "text": "You granted `USAGE` on a schema. Does it make a difference if you later grant `SELECT` on a specific table? (inheritance perspective)",
    "options": {
     "A": "Both require grant; schema USAGE is prerequisite for table access",
     "B": "Table grant inherits automatically from schema grant; redundant",
     "C": "Schema grant is necessary but not sufficient; table grant is also required",
     "D": "Table grant ignores schema grant; only table level matters"
    },
    "answer": "C",
    "explanation": "Schema USAGE is necessary prerequisite; table grant also required (does not inherit automatically).",
    "optExpl": {
     "A": "Incorrect: not exactly a prerequisite in strict binary sense.",
     "B": "Incorrect: table grant does not inherit automatically from schema grant.",
     "C": "Correct: schema USAGE is necessary prerequisite; table grant also required (does not inherit).",
     "D": "Incorrect: schema grant still necessary even with table grant."
    }
   },
   {
    "id": 42,
    "domain": "Data Manipulation",
    "topic": "Time Travel + Rollback",
    "text": "You accidentally executed `DELETE FROM sales` without WHERE. Last backup is 2 hours old. Which Databricks feature enables rollback?",
    "options": {
     "A": "`SELECT * FROM sales@0` (version 0)",
     "B": "`RESTORE TABLE sales TO VERSION x` where x = version from 2 hours ago",
     "C": "Delta time travel: `SELECT * FROM sales TIMESTAMP AS OF '...'` but how to restore?",
     "D": "Run `RESTORE` + recycle bin (28 days), but may be too late if VACUUM was run"
    },
    "answer": "B",
    "explanation": "`RESTORE TABLE ... TO VERSION x` or `SELECT ... TIMESTAMP AS OF` for time-travel. RESTORE writes new version; time-travel is read-only.",
    "optExpl": {
     "A": "Incorrect: correct syntax is `SELECT * FROM sales VERSION AS OF 0`; `@0` is not standard.",
     "B": "Correct: `RESTORE TABLE sales TO VERSION x` is correct rollback form.",
     "C": "Incorrect: `TIMESTAMP AS OF` is read-only; does not restore (does not modify state).",
     "D": "Incorrect: if VACUUM was run with low retention, data may be permanently lost."
    }
   },
   {
    "id": 43,
    "domain": "Cost & Performance Optimization",
    "topic": "Table Statistics and Query Planning",
    "text": "An EXPLAIN shows `HashAgg` with high spill instead of the more efficient `SortAgg`. You suspect Catalyst made the wrong aggregation choice. What must you do first to help Catalyst choose correctly?",
    "options": {
     "A": "Enable `spark.sql.statistics.histogramEnabled = true` in cluster config",
     "B": "Manually rewrite the query to use `GROUP BY ... WITH SORT AGGREGATE`",
     "C": "Run `ANALYZE TABLE ... COMPUTE STATISTICS` to populate rowCount/totalSize stats in metastore",
     "D": "Increase `spark.sql.shuffle.partitions` to reduce per-partition data size"
    },
    "answer": "C",
    "explanation": "Catalyst uses table statistics (rowCount, totalSize) to decide between HashAgg and SortAgg. Running `ANALYZE TABLE ... COMPUTE STATISTICS` populates these stats in the metastore. Without stats, Catalyst uses heuristics which may be wrong.",
    "optExpl": {
     "A": "Incorrect: histogram is optional and does not directly control aggregation strategy choice.",
     "B": "Incorrect: while metastore stats are used, you must first populate them with ANALYZE; metastore alone does not help without data.",
     "C": "Correct: ANALYZE TABLE ... COMPUTE STATISTICS is the command that populates rowCount/totalSize into the metastore, which Catalyst then uses for aggregation strategy selection.",
     "D": "Incorrect: without explicit stats, fallback heuristics are indeed suboptimal, but this does not solve the problem."
    }
   },
   {
    "id": 44,
    "domain": "Data Manipulation",
    "topic": "Aggregate Functions + Null Handling",
    "text": "Query: `SELECT SUM(amount) FROM sales WHERE amount IS NOT NULL`. Alternative: `SELECT SUM(COALESCE(amount, 0)) FROM sales`. Which is correct?",
    "options": {
     "A": "First is correct; second sums 0s as fake values",
     "B": "Second is correct; does not change result if no nulls",
     "C": "Both correct if schema guarantees NO NULL; first is more readable",
     "D": "Depends on business: first ignores nulls, second treats as 0"
    },
    "answer": "D",
    "explanation": "First ignores nulls; second treats as 0. Business decides; first is more typical.",
    "optExpl": {
     "A": "Incorrect: depends on business; not simply \"first is correct\".",
     "B": "Incorrect: first is more typical for SUM; second treats NULL as 0 (different semantics).",
     "C": "Incorrect: D is more correct as it recognizes semantic difference.",
     "D": "Correct: first ignores NULLs; second treats as 0; business decides correct semantics."
    }
   },
   {
    "id": 45,
    "domain": "Monitoring and Alerting",
    "topic": "Job Failure Alerting",
    "text": "A critical job running MERGE fails silently; you discover it only when business data is missing. You need to alert the team within 1 minute of failure with audit trail. What is the recommended approach?",
    "options": {
     "A": "Configure job-level notifications to Slack; fires immediately upon failure",
     "B": "Databricks Alerts feature querying `system.jobs.runs` and filtering `state = FAILED`; integrates with alerts infrastructure",
     "C": "Custom Python script polling `system.jobs.runs` every minute and sending manual alerts",
     "D": "Combine job notifications (A) with Databricks Alerts (B) for redundant alerting with full audit trail"
    },
    "answer": "D",
    "explanation": "Combining job notifications (A) for immediate alert with Databricks Alerts (B) for managed, audited alerting provides both rapid notification and compliance audit trail. Job notifications alone may be unreliable; custom polling (C) is not recommended.",
    "optExpl": {
     "A": "Incomplete: job notifications provide immediate alert but lack audit trail and may not integrate with compliance systems.",
     "B": "Incomplete: Databricks Alerts provide managed, audited alerting but may not have the same immediate notification guarantee as job webhooks.",
     "C": "Incorrect: custom polling is fragile, requires maintenance, and lacks audit trail compared to managed solutions.",
     "D": "Correct: combining job notifications (A) for speed with Databricks Alerts (B) for audit trail provides both rapid notification and compliance logging."
    }
   },
   {
    "id": 46,
    "domain": "Cost & Performance Optimization",
    "topic": "Column Mask Performance Impact",
    "text": "You need to mask SSN in 500+ queries. You want to avoid both: (1) runtime masking overhead on every query, and (2) duplication of data. What is the best strategy?",
    "options": {
     "A": "Pre-compute `AES_ENCRYPT()` at ingress to create encrypted column alongside raw data",
     "B": "Apply UC column masks directly; Databricks handles with negligible overhead if predicate push-down works",
     "C": "Create materialized view pre-masked for all sensitive columns; users query the MV instead",
     "D": "Use UC column masks (B) for most queries plus materialized views (C) for critical high-frequency queries to avoid repeated masking cost"
    },
    "answer": "D",
    "explanation": "UC column masks (B) provide centralized, auditable masking for most queries with negligible overhead. Materialized views (C) add pre-masked copies for critical high-frequency queries to eliminate repeated masking cost. Combining both optimizes both coverage and performance.",
    "optExpl": {
     "A": "Incorrect: pre-encrypting stores both encrypted and raw data (duplication) and loses Databricks' UC masking audit trail.",
     "B": "Incomplete: UC column masks provide centralized masking but add runtime overhead on every query.",
     "C": "Incomplete: materialized pre-masked views eliminate runtime overhead but duplicate data across hundreds of columns (not scalable).",
     "D": "Correct: UC column masks (B) for centralized audited masking plus materialized views (C) for critical high-frequency queries balances coverage and performance."
    }
   },
   {
    "id": 47,
    "domain": "Developing Code",
    "topic": "Job Cluster vs Attached Cluster",
    "text": "A production job runs on an all-purpose cluster that also hosts interactive analyst notebooks. The job's runtime is unpredictable and it occasionally fails when analysts restart the cluster. What is the most appropriate change?",
    "options": {
     "A": "Keep the shared cluster; Databricks isolates scheduled jobs from notebooks automatically",
     "B": "Raise the shared cluster's maximum autoscaling workers so both workloads have enough capacity",
     "C": "Ask analysts not to restart the cluster during the job's schedule window",
     "D": "Run the job on its own job compute (a job cluster or serverless jobs compute) so it gets isolated resources and a lifecycle independent of interactive users"
    },
    "answer": "D",
    "explanation": "A shared all-purpose cluster exposes the job to resource contention, cluster restarts and shared Spark state from interactive users. Dedicated job compute starts for the run, has its own resources and terminates afterwards, which also makes it cheaper than keeping an all-purpose cluster up for scheduled work.",
    "optExpl": {
     "A": "Incorrect: jobs and notebooks on the same all-purpose cluster share the driver and executors; there is no automatic isolation.",
     "B": "Incorrect: more workers can ease contention but do not protect the job from restarts or shared Spark state, and they raise cost.",
     "C": "Incorrect: a process agreement is fragile and does not remove contention or shared state; it is not an engineering fix.",
     "D": "Correct: job compute isolates resources and lifecycle from interactive users, removing contention and restart risk at lower cost."
    }
   },
   {
    "id": 48,
    "domain": "Developing Code",
    "topic": "PySpark vs Pandas Performance",
    "text": "An operation on 100M rows: standard PySpark UDF takes 30 seconds; rewriting it as a Pandas UDF takes 5 seconds (6x faster). What is the root cause of the dramatic improvement?",
    "options": {
     "A": "PySpark serializes data to Python pickle; Pandas UDF uses Arrow columnar format (no JVM overhead)",
     "B": "PySpark only runs on one executor; Pandas UDF automatically parallelizes",
     "C": "Pandas UDF avoids Python execution entirely; runs compiled machine code",
     "D": "Combination: Pandas UDF reduces serialization overhead (A) AND Catalyst optimizes it as columnar operation (B)"
    },
    "answer": "D",
    "explanation": "Pandas UDF's 6x speedup comes from two factors: (A) Arrow columnar format eliminates JVM-to-Python row serialization overhead, and (B) Catalyst planner treats it as a columnar operation, enabling vectorized execution and optimization. Both are necessary for the performance gain.",
    "optExpl": {
     "A": "Incomplete: serialization overhead reduction is one factor but does not account for Catalyst optimization.",
     "B": "Incomplete: Catalyst optimization contributes but arrow serialization improvement is equally important.",
     "C": "Incorrect: both PySpark and Pandas UDF parallelize; the difference is execution model, not parallelism.",
     "D": "Correct: both serialization efficiency (A) and Catalyst columnar optimization (B) together achieve the 6x speedup in Pandas UDF."
    }
   },
   {
    "id": 49,
    "domain": "Developing Code",
    "topic": "SQL Dynamic Query Generation",
    "text": "A report needs to group by a user-chosen column. You receive `group_by = user_input`. Which approach prevents SQL injection?",
    "options": {
     "A": "Direct string concatenation: `f\"SELECT {group_by}, SUM(amount) FROM sales GROUP BY {group_by}\"`",
     "B": "Use parameterized binding: `spark.sql(f\"SELECT ? AS col, SUM(amount) FROM sales GROUP BY ?\", [user_input, user_input])`",
     "C": "Validate user_input against a hardcoded allowlist of permitted column names; throw error if not in list",
     "D": "Combination: validate against allowlist (C) and use parameterized queries (B) for defense in depth"
    },
    "answer": "D",
    "explanation": "Defense in depth: validate user input against a hardcoded allowlist (C) and use parameterized queries (B). Allowlist ensures only known columns are allowed; parameterized queries provide additional protection. Direct concatenation (A) is vulnerable to SQL injection.",
    "optExpl": {
     "A": "Incorrect: direct string concatenation with user input is vulnerable to SQL injection attacks.",
     "B": "Incorrect: parameterized binding alone cannot handle column names (only values); GROUP BY column name still requires validation.",
     "C": "Incomplete: allowlist validation is necessary but parameterized queries provide defense in depth.",
     "D": "Correct: validate against allowlist (C) to ensure only known column names, and use parameterized queries (B) where possible for defense in depth."
    }
   },
   {
    "id": 50,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Streaming Ingestion Performance",
    "text": "A stream of 1M events/second is ingested into Delta via Structured Streaming. Latency is increasing. What is the MOST likely cause?",
    "options": {
     "A": "Spark executor memory overflow",
     "B": "State accumulation without watermark; checkpointing grows indefinitely",
     "C": "Partition count low; data accumulates in partitions",
     "D": "Delta schema evolution on each micro-batch"
    },
    "answer": "B",
    "explanation": "Without watermark, state grows indefinitely; checkpoints grow with state.",
    "optExpl": {
     "A": "Incorrect: executor memory overflow would cause immediate crash, not gradual degradation.",
     "B": "Correct: state accumulation without watermark; checkpoints grow indefinitely causing degradation.",
     "C": "Incorrect: low partition count does not cause degradation; partitions scale.",
     "D": "Incorrect: schema evolution does not cause consistent throughput degradation."
    }
   },
   {
    "id": 51,
    "domain": "Cost & Performance Optimization",
    "topic": "Index-free Pruning via Iceberg",
    "text": "A 500B event record table is frequently queried by high-cardinality date ranges (e.g., \"last 6 hours\", \"yesterday\"). Which approach optimizes pruning without building explicit B-tree indexes?",
    "options": {
     "A": "Partition by month: `PARTITION BY MONTH(event_timestamp)` (creates one folder per month)",
     "B": "Rely on Iceberg's manifest statistics: each file stores min/max event_timestamp; Iceberg prunes files with non-matching ranges",
     "C": "Liquid clustering via `CLUSTER BY event_timestamp` only; do not use Predictive Optimization",
     "D": "Combine fixed partitioning (A) for coarse filtering with Iceberg manifest statistics (B) for fine-grained pruning (most efficient)"
    },
    "answer": "D",
    "explanation": "Combining fixed partitioning (A) for coarse directory pruning with Iceberg manifest statistics (B) for file-level pruning provides the most efficient approach. Manifest statistics prune individual Parquet files based on min/max; fixed partitioning is coarser but reduces directory scans.",
    "optExpl": {
     "A": "Incomplete: fixed monthly partitioning provides coarse pruning but does not optimize for high-cardinality intra-month range queries.",
     "B": "Incomplete: manifest statistics alone work but combining with partitioning provides better performance.",
     "C": "Incorrect: liquid clustering alone lacks the coarse directory-level pruning that partitioning provides.",
     "D": "Correct: combine fixed partitioning (A) for directory pruning with Iceberg manifest statistics (B) for file-level pruning; most efficient for high-cardinality ranges."
    }
   },
   {
    "id": 52,
    "domain": "Developing Code",
    "topic": "Encryption Key Management in Spark",
    "text": "A Spark job needs to retrieve an encryption key. Which approach provides both security and audit logging?",
    "options": {
     "A": "Hardcode the key in the job source code",
     "B": "Pass the key via cluster environment variable; rotate manually",
     "C": "Embed the key in the `spark.conf` configuration (user-visible in job UI)",
     "D": "Retrieve via `dbutils.secrets.get(scope, key)` from UC Secrets (audit trail + no logging exposure)"
    },
    "answer": "D",
    "explanation": "UC Secrets via `dbutils.secrets.get()` provides both security (never logged in Spark UI or configs) and audit trail. Hardcoding (A), environment vars (B), and spark.conf (C) all expose secrets in logs, UI, or configs.",
    "optExpl": {
     "A": "Incorrect: hardcoding secrets in source code is visible to anyone with code access; no audit trail.",
     "B": "Incorrect: environment variables are visible in cluster logs and lack audit trail on access.",
     "C": "Incorrect: secrets in spark.conf are visible in Spark UI job configs and logs.",
     "D": "Correct: UC Secrets with `dbutils.secrets.get()` hides keys from logs, provides automatic audit trail on each access."
    }
   },
   {
    "id": 53,
    "domain": "Developing Code",
    "topic": "Type Casting Errors",
    "text": "During ingestion, `CAST(date_string AS DATE)` encounters 1 invalid value in 1B rows and fails the entire job. You want to handle the error gracefully and continue processing. What is the best approach?",
    "options": {
     "A": "Replace with `TRY_CAST(date_string AS DATE)`, which returns NULL for invalid values and allows the job to continue",
     "B": "Add a CASE statement with regex pre-validation: `CASE WHEN date_string MATCHES regex THEN CAST(...) ELSE NULL END`",
     "C": "Use `CAST(...FORMAT 'yyyy-MM-dd')` to be strict about format and fail fast",
     "D": "Skip invalid rows via `WHERE REGEXP_LIKE(date_string, '^\\\\d{4}-\\\\d{2}-\\\\d{2}$')`"
    },
    "answer": "A",
    "explanation": "`TRY_CAST` returns NULL for invalid values and allows the job to continue without error. Pre-validation (B, D) adds complexity; strict format checking (C) fails fast rather than handling gracefully.",
    "optExpl": {
     "A": "Correct: `TRY_CAST` is the idiomatic approach; returns NULL for invalid values and job continues without error.",
     "B": "Incorrect: CASE with regex pre-validation is unnecessarily verbose compared to TRY_CAST.",
     "C": "Incorrect: strict format casting fails fast on invalid rows instead of handling gracefully.",
     "D": "Incorrect: skipping invalid rows silently loses data; TRY_CAST is better (transforms to NULL, preserving row count)."
    }
   },
   {
    "id": 54,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Multiformat Ingestion with Lakeflow",
    "text": "You ingest Parquet, JSON, and CSV from S3 into Delta. Schema evolves with each format. What is the most robust solution?",
    "options": {
     "A": "Spark auto schema inference with `mergeSchema = true` (requires manual job per format)",
     "B": "Unity Catalog table specifications (does not auto-detect format differences)",
     "C": "Lakeflow Connect with automatic per-format schema detection and compatibility checks",
     "D": "A and C combined: Spark baseline plus Lakeflow for format-specific handling (most robust)"
    },
    "answer": "D",
    "explanation": "Combining Spark auto-schema (A) with Lakeflow Connect (C) provides the most robust multi-format ingestion: Spark handles basic schema inference while Lakeflow detects format-specific issues and validates compatibility across Parquet, JSON, and CSV.",
    "optExpl": {
     "A": "Incomplete: raw Spark auto-schema inference works but does not handle format-specific schema conflicts (e.g., JSON array vs CSV single value).",
     "B": "Incorrect: UC table specs do not automatically detect schema differences across format types.",
     "C": "Incomplete: Lakeflow Connect alone without Spark baseline requires more manual configuration.",
     "D": "Correct: combining Spark auto-schema (A) with Lakeflow Connect (C) auto-detection provides format-aware schema handling and validation."
    }
   },
   {
    "id": 55,
    "domain": "Monitoring and Alerting",
    "topic": "Predictive Optimization Metrics",
    "text": "Selective queries on a large Delta table are slow. Which signal in the query profile best indicates that the table's data layout should be clustered (for example with `CLUSTER BY AUTO`)?",
    "options": {
     "A": "Shuffle write bytes are high in a join stage",
     "B": "Almost all files and bytes are read for a selective filter, with very few pruned",
     "C": "An \"optimizer recommendation score\" below 80%",
     "D": "A high number of active executors in the Executors tab"
    },
    "answer": "B",
    "explanation": "If a selective filter still reads nearly every file, data skipping is not working for that column, which is exactly what clustering on the filtered columns (or letting `CLUSTER BY AUTO` choose them) fixes. Shuffle volume and executor count describe joins and compute sizing, not data layout.",
    "optExpl": {
     "A": "Incorrect: high shuffle writes point to join or aggregation strategy, not to the table's data layout.",
     "B": "Correct: reading almost all files for a selective filter means poor data skipping, which clustering on the filtered columns addresses.",
     "C": "Incorrect: there is no such standard metric in the query profile.",
     "D": "Incorrect: executor count reflects compute sizing, not how well the table's files are pruned."
    }
   },
   {
    "id": 56,
    "domain": "Debugging and Deploying",
    "topic": "Incremental Pipeline Issues",
    "text": "A daily incremental job: `SELECT * FROM src WHERE updated_at > '{{yesterday}}'` runs daily, picking only yesterday's updates. One day, a data quality fix retroactively updated records from 30 days ago with updated_at = today. What happens to the pipeline and how to fix?",
    "options": {
     "A": "Idempotent: the job picks the 30 days and merges cleanly with existing data (no issue)",
     "B": "Breaking: the job now picks 30 days of data today; aggregate metrics are duplicated/wrong (late-arriving updates break simple incremental)",
     "C": "Job skips early; triggers alert (fails fast due to data anomaly)",
     "D": "Late-arriving updates break simple timestamp incremental (B); use idempotent MERGE with watermark to handle late updates correctly"
    },
    "answer": "D",
    "explanation": "Simple timestamp-based incremental breaks when historical data is retroactively updated (scenario B). MERGE with idempotent logic and watermark handles late updates by matching on composite key and updating existing records instead of duplicating.",
    "optExpl": {
     "A": "Incorrect: late-arriving updates break simple incremental; the job does pick all 30 days, causing duplication.",
     "B": "Incomplete: correctly describes what happened (all 30 days picked) but does not address the problem or solution.",
     "C": "Incorrect: the job does not fail; it succeeds but produces wrong results (duplicated aggregates).",
     "D": "Correct: simple timestamp-based incremental is vulnerable to retroactive updates; idempotent MERGE with watermark prevents duplication."
    }
   },
   {
    "id": 57,
    "domain": "Data Modeling",
    "topic": "Conformed Dimensions",
    "text": "Multiple fact tables inconsistently reference `customer` (customer_id vs customer_pk, different attributes). You want to consolidate without breaking history or existing queries. Which approach works?",
    "options": {
     "A": "Create new conformed `dim_customer`; both old and new fact tables reference it (parallel operation during transition)",
     "B": "Modify existing fact table schemas directly with `ALTER TABLE ... DROP/ADD` columns",
     "C": "Recreate fact tables with new dimension; archive old versions (clean but risky cutover)",
     "D": "Both A and C work together: create new dimension, gradually migrate each fact table (A), eventually recreate final ones (C)"
    },
    "answer": "D",
    "explanation": "Combining approach A (create conformed dimension + gradual fact table migration) with approach C (eventual fact table recreation) provides low-risk consolidation. Fact tables can reference the new dimension incrementally while old schemas remain intact, then eventually clean up via recreation.",
    "optExpl": {
     "A": "Incomplete: creating conformed dimension works but needs a migration strategy to avoid breaking existing queries.",
     "B": "Incorrect: direct ALTER TABLE DROP/ADD on historical fact tables is risky and can break existing dependencies.",
     "C": "Incomplete: recreating fact tables works but is disruptive; needs gradual migration strategy.",
     "D": "Correct: combine gradual migration via new dimension (A) with eventual fact table recreation (C) for low-risk consolidation that preserves history."
    }
   },
   {
    "id": 58,
    "domain": "Developing Code",
    "topic": "Broadcast Join Optimization",
    "text": "A query joins `sales` (100B rows) and `dim_product` (10K rows). Spark chooses BroadcastNestedLoopJoin (slow). How to force BroadcastHashJoin?",
    "options": {
     "A": "`/*+ BROADCAST(dim_product) */` hint",
     "B": "Increase `spark.sql.autoBroadcastJoinThreshold` to > 10K MB",
     "C": "Reorder join: `SELECT * FROM dim_product JOIN sales`",
     "D": "Both A and B; A is more direct"
    },
    "answer": "A",
    "explanation": "`/*+ BROADCAST(dim_product) */` hint forces BroadcastHashJoin; B auto-increases threshold.",
    "optExpl": {
     "A": "Correct: `/*+ BROADCAST(dim_product) */` hint forces BroadcastHashJoin.",
     "B": "Incorrect: increasing `autoBroadcastJoinThreshold` does not force broadcast of 10K (small table).",
     "C": "Incorrect: join reordering does not guarantee small table broadcast.",
     "D": "Incorrect: B not effective; only A is correct."
    }
   },
   {
    "id": 59,
    "domain": "Cost & Performance Optimization",
    "topic": "Predictive Optimization Fine-Tuning",
    "text": "A table with Predictive Optimization enabled runs automatic reordering every 24h, consuming 2h of compute. It is excessive. Which option reduces frequency?",
    "options": {
     "A": "Disable Predictive Optimization completely",
     "B": "Adjust `OPTIMIZE` frequency via `CREATE TABLE ... TBLPROPERTIES (...)`",
     "C": "Configure `spark.databricks.predictiveOptimization.maxFrequency = weekly`",
     "D": "Both B and C; C is more effective"
    },
    "answer": "C",
    "explanation": "Configure `spark.databricks.predictiveOptimization.maxFrequency` to reduce excessive recompaction.",
    "optExpl": {
     "A": "Incorrect: disabling Predictive Optimization loses automatic optimization benefit.",
     "B": "Incorrect: no standard `OPTIMIZE frequency` property in CREATE TABLE.",
     "C": "Correct: `spark.databricks.predictiveOptimization.maxFrequency = weekly` reduces frequency.",
     "D": "Incorrect: B not correct; only C solves this."
    }
   },
   {
    "id": 60,
    "domain": "Data Governance",
    "topic": "UC Asset Inventory",
    "text": "An internal audit requires a comprehensive, queryable list of all tables in the catalog with: owner, last modification timestamp, and data classification tags. Which source provides this as queryable records (not UI)?",
    "options": {
     "A": "Query Unity Catalog system table `system.information_schema.tables` joined with UC tags; export to audit report",
     "B": "Glean search API to list tables and retrieve metadata",
     "C": "Databricks metadata REST API (beta) without tag information",
     "D": "Combination of multiple sources (A + metadata API) because no single source has all fields"
    },
    "answer": "A",
    "explanation": "Querying `system.information_schema.tables` directly provides table names, owners, and timestamps in a queryable SQL table. UC tags are joinable for classification. This is the native, audit-ready source. Glean (B) is search-based, not queryable; metadata API (C) lacks tag information.",
    "optExpl": {
     "A": "Correct: `system.information_schema.tables` is a queryable system table with owner, timestamps, and UC tags; provides structured, auditable inventory.",
     "B": "Incorrect: Glean is a search tool, not a queryable data source; returns ranked results, not structured exports.",
     "C": "Incorrect: Databricks metadata API does not include UC tag information; incomplete for classification requirement.",
     "D": "Incorrect: a single query on system tables (A) is simpler and more direct than combining multiple sources."
    }
   }
  ]
 },
 "simulado2": {
  "title": "Practice Exam 2",
  "questions": [
   {
    "id": 1,
    "domain": "Developing Code",
    "topic": "Python/SQL",
    "text": "A Structured Streaming pipeline receives data from Kafka in 30-second batches. Processing performs an aggregation with 5-minute windows on `event_timestamp`. Which configuration ensures that late-arriving events (arriving up to 10 minutes after the window closes) are reprocessed in the correct window?",
    "options": {
     "A": "Increase `spark.sql.streaming.forceDeleteTempCheckpointLocation` and set `outputMode=\"complete\"`",
     "B": "Configure `watermark(\"event_timestamp\", \"10 minutes\")` before aggregation and use `outputMode=\"append\"` with CDF enabled",
     "C": "Set `chkpointLocation` with `update` mode and `microBatchMs=10000`",
     "D": "Use `.option(\"mergeSchema\", \"true\")` and replicate the query across 10 executors"
    },
    "answer": "B",
    "explanation": "Watermark of 10 minutes allows late-arriving events (up to 10 min) to be reprocessed in correct window. `outputMode=\"append\"` is recommended for streaming. CDF is redundant here.",
    "optExpl": {
     "A": "Incorrect: forceDeleteTempCheckpointLocation clears old checkpoints, not delay tolerance — watermark requires explicit configuration.",
     "B": "Correct: watermark('event_timestamp', '10 minutes') defines delay tolerance for late event reprocessing; outputMode='append' is recommended for streaming with state.",
     "C": "Incorrect: microBatchMs and chkpointLocation set timing and location, not delay tolerance — lacks explicit watermark definition.",
     "D": "Incorrect: mergeSchema addresses schema evolution, not watermark — replicating query across executors does not affect delay tolerance."
    }
   },
   {
    "id": 2,
    "domain": "Data Ingestion & Acquisition",
    "topic": "PostgreSQL Logical Replication",
    "text": "Your client has a PostgreSQL database with ~10GB of data. You want to sync changes (inserts/updates/deletes) in near real-time via Lakeflow Connect. What is the main database prerequisite (besides credentials)?",
    "options": {
     "A": "Physical replication enabled and all tables with `replica identity full`",
     "B": "Logical Decoding activated, WAL level set to `logical`, and a permanent replication slot",
     "C": "Audit trigger on each table plus external transaction log",
     "D": "Foreign Data Wrapper (FDW) plus `postgres_fdw` extension"
    },
    "answer": "B",
    "explanation": "Lakeflow Connect requires Logical Decoding enabled on PostgreSQL, WAL level set to `logical`, and a permanent replication slot. Replica identity full is useful but not critical prerequisite.",
    "optExpl": {
     "A": "Incorrect: Physical replication is for disaster recovery standby; Logical Decoding (not physical) is native CDC mechanism.",
     "B": "Correct: Logical Decoding, WAL level logical, and permanent replication slot are critical prerequisites for change capture in Lakeflow Connect.",
     "C": "Incorrect: Audit triggers have high overhead and lack transactional consistency — Logical Decoding is native CDC mechanism.",
     "D": "Incorrect: FDW (Foreign Data Wrapper) enables remote queries, not change capture — CDC requires Logical Decoding on PostgreSQL."
    }
   },
   {
    "id": 3,
    "domain": "Data Manipulation",
    "topic": "SQL",
    "text": "You have a Delta table with column `data: VARIANT` containing JSON with variable structure. You need to extract the field `user.email` present in ~80% of records; in the other 20%, the field may not exist. Which is the most efficient and safe approach?",
    "options": {
     "A": "`SELECT get_json_object(data, '$.user.email') AS email FROM table` with NULL handling",
     "B": "`SELECT data['user']['email'] AS email FROM table` followed by `WHERE email IS NOT NULL`",
     "C": "`SELECT variant_get(data, 'user.email', 'string') AS email FROM table`",
     "D": "Convert VARIANT to string via `to_json()`, then use regex"
    },
    "answer": "C",
    "explanation": "`variant_get(data, 'user.email', 'string')` is safe and efficient for extracting from VARIANT with fallback to NULL. More explicit than bracket syntax.",
    "optExpl": {
     "A": "Incorrect: get_json_object() operates on JSON string, not VARIANT — requires prior cast and is less efficient.",
     "B": "Incorrect: Bracket syntax data['user']['email'] is valid but less explicit than variant_get with defined type.",
     "C": "Correct: variant_get(data, 'user.email', 'string') is native VARIANT constructor with NULL fallback and explicit type — idiomatic in Databricks.",
     "D": "Incorrect: to_json() plus regex is costly (serialization plus pattern matching) versus native variant_get."
    }
   },
   {
    "id": 4,
    "domain": "Monitoring and Alerting",
    "topic": "Streaming Latency",
    "text": "Your Structured Streaming pipeline processes 1M events/second. Checkpoint indicated 50ms end-to-end latency, but it suddenly jumped to 2 seconds. Where would you look FIRST to diagnose?",
    "options": {
     "A": "Spark driver logs on the cluster",
     "B": "Throughput metrics and batch duration in Spark UI (streaming tab) plus verify event ingestion rate",
     "C": "Increase `spark.sql.shuffle.partitions` and enable `spark.streaming.backpressure.enabled`",
     "D": "Check disk I/O of checkpoint storage and Kafka broker status"
    },
    "answer": "B",
    "explanation": "Throughput metrics (input rate vs. processing rate) on Spark Streaming tab show degradation immediately. If processing rate drops, there is a bottleneck.",
    "optExpl": {
     "A": "Incorrect: Driver logs are useful for deep debugging, but Spark UI streaming tab shows aggregated metrics faster.",
     "B": "Correct: Input rate vs. processing rate on Spark Streaming tab immediately shows back-pressure — direct diagnosis.",
     "C": "Incorrect: Increasing partitions without diagnosis is 'turning screws' — back-pressure may come from I/O, not partitions.",
     "D": "Incorrect: Full checklist (Kafka lag, checkpoint I/O) is valid but less direct than throughput metrics in Spark UI."
    }
   },
   {
    "id": 5,
    "domain": "Cost & Performance Optimization",
    "topic": "",
    "text": "A 500GB Delta table receives frequent small writes (10–50 rows per write). Reads are slow. Which combination best reduces cost and improves performance?",
    "options": {
     "A": "Enable deletion vectors, CLUSTER BY auto, and Delta cache",
     "B": "Only run `OPTIMIZE` daily plus increase worker nodes",
     "C": "Disable multiversion concurrency control (MVCC) and partition by date",
     "D": "Convert to native Parquet (disable Delta) and use S3 Select"
    },
    "answer": "A",
    "explanation": "Deletion vectors avoid rewriting entire files. CLUSTER BY auto optimizes layout. Delta cache reduces I/O. Together, they solve frequent small writes and slow reads.",
    "optExpl": {
     "A": "Correct: Deletion vectors avoid rewriting files on deletes — CLUSTER BY auto optimizes layout — Delta cache stores hot data.",
     "B": "Incorrect: Daily OPTIMIZE is fixed overhead, not continuous — more workers do not reduce small write latency.",
     "C": "Incorrect: MVCC is essential for Databricks concurrency — date partitioning does not solve small write fragmentation.",
     "D": "Incorrect: Native Parquet loses ACID transactions — S3 Select does not apply to writes (read-only)."
    }
   },
   {
    "id": 6,
    "domain": "Data Security and Compliance",
    "topic": "Central Masking Policy",
    "text": "Your client wants a central masking policy that automatically applies to any column tagged with governed tag `pii` — including tables created in the future by different users. What is the native Databricks solution?",
    "options": {
     "A": "Create a masking stored procedure and associate via `ALTER TABLE ... SET CLUSTER BY`",
     "B": "ABAC (Attribute-Based Access Control) policies in Unified Catalog with governed tags",
     "C": "Row and column security with masked views plus nightly audit job",
     "D": "Create a masking model in AI functions and apply as `CHECK` constraint"
    },
    "answer": "B",
    "explanation": "ABAC (Attribute-Based Access Control) in UC with governed tags allows central policy automatically applying to any column with tag `pii`.",
    "optExpl": {
     "A": "Incorrect: CLUSTER BY is for layout optimization (Z-order), not masking policy.",
     "B": "Correct: ABAC in UC with governed tags applies policy automatically to new columns marked with tag pii — native Databricks solution.",
     "C": "Incorrect: Masked views must be created manually; do not auto-apply to future-created tables.",
     "D": "Incorrect: AI functions are transformation functions, not policy engine — CHECK constraint is validation, not masking."
    }
   },
   {
    "id": 7,
    "domain": "Data Modeling",
    "topic": "Centralized Metrics",
    "text": "An organization has 150 tables in UC with different data quality levels. It wants to create a central metric \"data_quality_score\" that appears in governance and is reusable by multiple dashboards. Which approach is recommended?",
    "options": {
     "A": "UC Metric View (centralized definition) plus Materialized View (pre-aggregation)",
     "B": "Delta Live Tables with `@quality_expectation` plus Dashboard reading the expectations table",
     "C": "Compute the metric in a Python job nightly and write to a table, then use `CREATE VIEW`",
     "D": "Use `GET_METRIC` via SQL Warehouse in each dashboard"
    },
    "answer": "A",
    "explanation": "UC Metric View provides centralized governed definition. Materialized View pre-aggregates for performance. Combined, offer governance plus reusability.",
    "optExpl": {
     "A": "Correct: UC Metric Views offer centralized governed definition — Materialized Views pre-aggregate for performance — complement each other.",
     "B": "Incorrect: DLT orchestrates pipelines with quality expectations, not centralized governance metrics.",
     "C": "Incorrect: Nightly Python job is batch, not continuous — not centralized solution (each dashboard copies query).",
     "D": "Incorrect: GET_METRIC is not native Databricks function — each dashboard writes separate query."
    }
   },
   {
    "id": 8,
    "domain": "Developing Code",
    "topic": "Python/SQL",
    "text": "An iterative development uses a notebook with SQL query that filters data by date using `WHERE date > CURRENT_DATE() - 7`. After converting to a scheduled daily job, the 7-day rolling window does not work correctly in some runs. What is the best practice?",
    "options": {
     "A": "Use `WHERE date > cast(current_timestamp() as date) - interval 7 days` and add retry logic",
     "B": "Pass the date as a task parameter via `spark.conf` in ISO 8601 format",
     "C": "Use Databricks Workflows with parametrization (`{{task.run_id}}`) and run with `dbutils.notebook.run()`",
     "D": "Store the last run checkpoint in UC and read the date from checkpoint on the next run"
    },
    "answer": "B",
    "explanation": "Passing date as parameter via configuration ensures consistency in rolling window between runs. More reliable than `CURRENT_DATE()` in jobs.",
    "optExpl": {
     "A": "Incorrect: CURRENT_DATE() changes daily — in scheduled job, does not guarantee consistent rolling window between runs.",
     "B": "Correct: Passing date as parameter via spark.conf ensures fixed value for the run — 7-day window is consistent.",
     "C": "Incorrect: {{task.run_id}} is run ID, not date — dbutils.notebook.run() passes base_parameters but run_id is inappropriate.",
     "D": "Incorrect: Checkpoint tracks streaming state, not window date — not appropriate parametrization mechanism."
    }
   },
   {
    "id": 9,
    "domain": "Data Ingestion & Acquisition",
    "topic": "REST API",
    "text": "You receive data from a REST API that returns paginated JSON. The endpoint supports range queries by timestamp. Which ingestion strategy is most robust for ingesting ~100M records with 3 years of history?",
    "options": {
     "A": "Call the API hour by hour (parallel) using `parallel_requests`, save raw to Delta, then process",
     "B": "Ingest everything in 1 call (1 giant JSON), store in `BLOB` column, then parse",
     "C": "Use Lakeflow Connect configured for CDC (if supported) or external Apache NiFi",
     "D": "Ingest by day (parallel) in Workflows tasks, save with `mergeSchema=true`"
    },
    "answer": "A",
    "explanation": "Calling API hour by hour in parallel, saving raw to Delta, then processing is robust for large historical volume.",
    "optExpl": {
     "A": "Correct: Hour-by-hour parallelization over 3 years (~26k hours) is scalable — saving raw to Delta before processing is robust (retry-friendly).",
     "B": "Incorrect: Single call for 3 years is serial with timeout risk — BLOB column is inefficient for later parsing.",
     "C": "Incorrect: Lakeflow Connect is for database sources (PostgreSQL, Teradata), not REST API.",
     "D": "Incorrect: Daily granularity is less parallelizable than hourly — mergeSchema without necessity is overhead."
    }
   },
   {
    "id": 10,
    "domain": "Data Manipulation",
    "topic": "Incremental Sync",
    "text": "You have two tables: `orders` (1M rows, updated daily) and `shipments` (500K rows, updated in real-time via CDF). You want to keep a fact table `fact_order_shipment` synchronized combining both. Which strategy is most efficient?",
    "options": {
     "A": "Merge incrementally via CDF, using `MERGE` with CDC subqueries",
     "B": "Run `DELETE FROM fact_order_shipment` plus `INSERT` full join (daily batch)",
     "C": "Create a Materialized View `AS SELECT ... FROM orders FULL OUTER JOIN shipments` and use `REFRESH`",
     "D": "Use Structured Streaming to consume `shipments` CDF plus join with `orders` snapshot"
    },
    "answer": "A",
    "explanation": "`MERGE` with CDF consumes only incremental changes from `shipments`. More efficient than full refresh.",
    "optExpl": {
     "A": "Correct: CDF captures only incremental changes from shipments — MERGE combines with orders snapshot — efficient.",
     "B": "Incorrect: DELETE plus INSERT full join is complete refresh — inefficient for large volume with small changes.",
     "C": "Incorrect: MV REFRESH rescans both tables — does not leverage CDF incremental changes.",
     "D": "Incorrect: Streaming for join with batch snapshot is more complex architecture than simple incremental MERGE."
    }
   },
   {
    "id": 11,
    "domain": "Cost & Performance Optimization",
    "topic": "Warehouse Sizing",
    "text": "A SQL Warehouse has 100 workers, costs $500/hour when idle. Query log analysis shows 30% are ad-hoc queries (< 1 min each) and 70% are dashboards with predictable access patterns. Which optimization reduces cost while maintaining SLA?",
    "options": {
     "A": "Migrate ad-hoc queries to serverless compute plus keep dashboards on dedicated warehouse",
     "B": "Use Delta cache for all queries plus reduce worker count to 50",
     "C": "Partition all tables by date plus enable `CLUSTER BY` auto",
     "D": "Convert slow queries to Materialized Views plus disable cache"
    },
    "answer": "A",
    "explanation": "Serverless compute for ad-hoc (pay-as-you-go) plus dedicated warehouse for dashboards (predictable, reserved capacity) reduces total cost.",
    "optExpl": {
     "A": "Correct: Serverless compute charges per use (ad-hoc is pay-as-you-go) — dedicated warehouse is hourly, optimized for dashboards — combo reduces idle cost.",
     "B": "Incorrect: Delta cache is complementary, not base-cost reduction — reducing workers hurts predictable dashboard performance.",
     "C": "Incorrect: Partitioning plus CLUSTER BY improve performance, not idle cost of 100-worker warehouse.",
     "D": "Incorrect: Converting to MV and disabling cache improves only refresh, not base warehouse idle cost."
    }
   },
   {
    "id": 12,
    "domain": "Developing Code",
    "topic": "Python",
    "text": "In a PySpark job, you want to process VARIANT columns containing nested arrays. Which approach ensures better performance in complex transformations?",
    "options": {
     "A": "`df.selectExpr(\"explode_outer(variant_col) as item\")` then iterate in Python RDD",
     "B": "Use `sql(\"SELECT ... FROM delta.`path` WHERE ...\")` plus native SQL to manipulate VARIANT",
     "C": "Convert VARIANT to JSON string in Python, parse with `json.loads()`, then reassemble",
     "D": "Use `pyspark.sql.functions.col()` with `.getItem()` chaining in SQL expressions"
    },
    "answer": "B",
    "explanation": "Native SQL with VARIANT manipulation is more efficient than Python/RDD. Spark SQL optimizer optimizes VARIANT operations.",
    "optExpl": {
     "A": "Incorrect: Python RDD plus loop forfeit Spark SQL optimization — explode plus Python iteration very slow for complex transformations.",
     "B": "Correct: Native SQL with VARIANT (variant_get, etc) optimized by Catalyst optimizer — better performance.",
     "C": "Incorrect: VARIANT → JSON string → Python parse → reassemble is multiple serializations — high overhead.",
     "D": "Incorrect: getItem() is valid Column API, but SQL native with explode/lateral/FLATTEN is more idiomatic and optimized."
    }
   },
   {
    "id": 13,
    "domain": "Monitoring and Alerting",
    "topic": "Materialized View Staleness",
    "text": "A critical dashboard is fed by a Materialized View. Suddenly, data becomes stale (hours outdated). What is the fastest way to diagnose whether the problem is automatic refresh or refresh cost?",
    "options": {
     "A": "Check `system.views.materialized_views` metadata table plus refresh job logs",
     "B": "Query Delta Lake statistics (`DESCRIBE DETAIL`) and verify last modification timestamp",
     "C": "Run `SHOW TBLPROPERTIES` on the Materialized View and search for `last_refresh_time`",
     "D": "Review billable cluster costs over the last 3 days plus connect to Predictive Optimization"
    },
    "answer": "C",
    "explanation": "`SHOW TBLPROPERTIES` on Materialized View shows `last_refresh_time` and refresh status. Most direct.",
    "optExpl": {
     "A": "Incorrect: system.views.materialized_views may have metadata, but TBLPROPERTIES is more direct for last_refresh_time.",
     "B": "Incorrect: DESCRIBE DETAIL shows base table changes, not MV refresh status.",
     "C": "Correct: SHOW TBLPROPERTIES on MV returns last_refresh_time — most direct staleness diagnosis.",
     "D": "Incorrect: Cluster cost does not indicate if refresh was running or staleness is from expired refresh."
    }
   },
   {
    "id": 14,
    "domain": "Data Governance",
    "topic": "Cross-Workspace UC Sharing",
    "text": "You work with UC across 5 workspaces. A project requires multiple workspaces to read the same table with different permissions per workspace. What is the correct approach using UC?",
    "options": {
     "A": "Replicate the table in each workspace with workspace-specific ABAC policies",
     "B": "Use Open Sharing or D2D (Data to Data) to share the table with permission granularity per workspace",
     "C": "Create delegated views in each workspace calling UDF for workspace_id validation",
     "D": "Use `ALTER TABLE ... OWNER TO` to transfer permission plus recreate the table in each workspace"
    },
    "answer": "B",
    "explanation": "Open Sharing (between Databricks workspaces) or D2D allows sharing with granular permission per workspace. Native UC.",
    "optExpl": {
     "A": "Incorrect: Replicating tables across workspaces is overhead — loses centralized sharing benefit.",
     "B": "Correct: Open Sharing (UC-native share across Databricks workspaces) plus D2D allows permission granularity per workspace.",
     "C": "Incorrect: Delegated views plus workspace_id UDF is custom, not native — Open Sharing is native solution.",
     "D": "Incorrect: ALTER TABLE OWNER does not share table — recreating in each workspace replicates data."
    }
   },
   {
    "id": 15,
    "domain": "Debugging and Deploying",
    "topic": "Deserialization Error",
    "text": "A scheduled job set to run daily at 8 AM begins failing after 2 weeks of operation. Logs show `SparkException: Task deserialization error`. What is the most likely cause and fix?",
    "options": {
     "A": "Old JAR version cache — clear cluster cache and reimport libraries",
     "B": "Dependency mismatch or Python class change — review if lib was upgraded plus increase `spark.driver.maxResultSize`",
     "C": "Corrupted checkpoint file — remove checkpoint and restart",
     "D": "DB connection timeout — increase `spark.sql.connect.timeout`"
    },
    "answer": "B",
    "explanation": "`SparkException: Task deserialization error` after 2 weeks typically means class/lib incompatibility. Clean and reimport libraries resolves.",
    "optExpl": {
     "A": "Incorrect: Old JAR cache is possible, but 'reimporting libraries' is true fix — deserialization is class mismatch.",
     "B": "Correct: Dependency mismatch (lib upgraded on cluster but job ran with old version) causes task deserialization error — fix is review libs.",
     "C": "Incorrect: Checkpoint corruption gives checkpoint read/write error, not task deserialization error.",
     "D": "Incorrect: DB timeout causes network/connection error, not task deserialization error."
    }
   },
   {
    "id": 16,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Lakebridge Teradata LDAP",
    "text": "You ingest data from a legacy data warehouse (Teradata) via Lakebridge. The connection uses LDAP. Which configuration is necessary ON THE TERADATA SIDE for Lakebridge to work?",
    "options": {
     "A": "Only enable remote access plus create user with `GRANT CONNECT` privilege",
     "B": "Enable LDAP LogMech on Teradata, open ODBC port, and ensure LDAP user has permission",
     "C": "Create a Foreign Data Wrapper (FDW) on Teradata plus open port 1025",
     "D": "Configure Teradata viewpoints with `GRANT SELECT` for Databricks users"
    },
    "answer": "B",
    "explanation": "Lakebridge to Teradata with LDAP requires LDAP LogMech enabled, ODBC port (1025/TCP default) open, and LDAP user with permission. Setup on Teradata side is essential.",
    "optExpl": {
     "A": "Incorrect: GRANT CONNECT necessary, but LDAP LogMech on Teradata is critical missing prerequisite.",
     "B": "Correct: LDAP LogMech enabled on Teradata plus port ODBC (1025/TCP default) plus LDAP user permission = complete setup.",
     "C": "Incorrect: FDW is PostgreSQL feature, not Teradata — Teradata uses ODBC/TDDSQL for connection.",
     "D": "Incorrect: Teradata viewpoints is legacy — modern Lakebridge uses direct access via LDAP."
    }
   },
   {
    "id": 17,
    "domain": "Developing Code",
    "topic": "SQL",
    "text": "You want to run a series of data quality tests in SQL during ingestion via DLT. What is the best way to express \"if > 5% of records have `price < 0`, fail the pipeline\"?",
    "options": {
     "A": "Use `@quality_expectation` or `EXPECT` statement in DLT with action `fail`",
     "B": "`IF (SELECT COUNT(*) FROM data WHERE price < 0) > (SELECT COUNT(*) * 0.05 FROM data) THEN RAISE`",
     "C": "Create a stored procedure running `SELECT COUNT(*) ... FILTER (price < 0)` and call `raise_error()`",
     "D": "Filter records with `WHERE price >= 0` plus log discarded count"
    },
    "answer": "A",
    "explanation": "DLT natively supports `@quality_expectation` or `EXPECT` statements with `action fail`. Idiomatic approach.",
    "optExpl": {
     "A": "Correct: @quality_expectation or EXPECT statement with action fail is native DLT — pipeline fails if violated.",
     "B": "Incorrect: IF/THEN/RAISE is non-standard procedural SQL — RAISE is not native Databricks SQL function.",
     "C": "Incorrect: Stored procedure with manual raise_error is workaround; not idiomatic DLT expectation.",
     "D": "Incorrect: Filtering silently does not fail pipeline — requirement is 'fail if > 5%' records invalid."
    }
   },
   {
    "id": 18,
    "domain": "Cost & Performance Optimization",
    "topic": "Iceberg Optimization",
    "text": "An Iceberg table grows 50GB/day. Performance of `SELECT * WHERE date > CURRENT_DATE()` is degrading. Which optimization is recommended IN ICEBERG?",
    "options": {
     "A": "Enable `CLUSTER BY` auto on Iceberg plus compact old snapshots",
     "B": "Run daily `OPTIMIZE` plus enable metadata caching with Z-order",
     "C": "Partition by `date` plus keep only last 90 days via `EXPIRE_SNAPSHOTS`",
     "D": "Convert to Delta and use deletion vectors"
    },
    "answer": "C",
    "explanation": "Partition by date in Iceberg plus `EXPIRE_SNAPSHOTS` removes old snapshots, reducing file count and metadata.",
    "optExpl": {
     "A": "Incorrect: CLUSTER BY complements but does not solve 'file count exploding' — EXPIRE_SNAPSHOTS is essential.",
     "B": "Incorrect: OPTIMIZE is Delta (not Iceberg native) — Iceberg uses EXPIRE_SNAPSHOTS for snapshot cleanup.",
     "C": "Correct: Partition by date in Iceberg plus EXPIRE_SNAPSHOTS removes old snapshots, reducing metadata.",
     "D": "Incorrect: Converting to Delta does not solve Iceberg performance — question asks Iceberg optimization."
    }
   },
   {
    "id": 19,
    "domain": "Data Modeling",
    "topic": "SCD Type 2 Maintenance",
    "text": "You have an SCD Type 2 table where each update creates a new row with `effective_date` and `end_date`. Your job updates records in batch daily. Which SQL is most efficient for maintaining SCD Type 2?",
    "options": {
     "A": "Use `MERGE` with `WHEN MATCHED ... UPDATE ... SET end_date = CURRENT_DATE()` and `WHEN NOT MATCHED ... INSERT`",
     "B": "`DELETE ... WHERE status = 'active'` plus `INSERT` with `INSERT OVERWRITE TABLE` scd_table",
     "C": "Use AUTO CDC with configuration `stored_as_scd_type = 2` in DLT",
     "D": "Create a Materialized View calculating `max(effective_date)` per key plus outer join"
    },
    "answer": "C",
    "explanation": "AUTO CDC with `stored_as_scd_type = 2` in DLT maintains SCD Type 2 automatically. More efficient than manual MERGE.",
    "optExpl": {
     "A": "Incorrect: MERGE with UPDATE/INSERT is manual requiring explicit SCD Type 2 logic — not auto-optimized.",
     "B": "Incorrect: DELETE plus INSERT OVERWRITE is destructive — data loss risk if error occurs.",
     "C": "Correct: AUTO CDC in DLT with stored_as_scd_type=2 maintains SCD Type 2 automatically — idiomatic DLT.",
     "D": "Incorrect: MV plus outer join is manual — less efficient than native AUTO CDC."
    }
   },
   {
    "id": 20,
    "domain": "Monitoring and Alerting",
    "topic": "Kafka Back-pressure",
    "text": "A streaming job consuming from Kafka is experiencing back-pressure. Which metric in Spark UI would you check to confirm if Kafka is experiencing growing lag?",
    "options": {
     "A": "Input rate vs. Processing rate on the Streaming tab graph",
     "B": "Executor memory usage plus GC time in the stage explorer",
     "C": "Databricks Job Runs log with `kafka_consumer_lag` metric",
     "D": "Task duration breakdown in the SQL tab"
    },
    "answer": "A",
    "explanation": "Spark Streaming tab shows \"Input rate\" vs. \"Processing rate\" graphs. If processing rate < input rate, there is lag.",
    "optExpl": {
     "A": "Correct: Spark Streaming tab 'Input rate' vs. 'Processing rate' graph immediately shows back-pressure.",
     "B": "Incorrect: Executor memory plus GC time shows GC pause, not consumer lag — lag is unprocessed events.",
     "C": "Incorrect: kafka_consumer_lag is Kafka native metric, not exposed as standard Databricks Streaming tab metric.",
     "D": "Incorrect: SQL tab is for SQL queries, not streaming lag — Streaming tab is correct location."
    }
   },
   {
    "id": 21,
    "domain": "Data Security and Compliance",
    "topic": "Column Masking",
    "text": "You want a sensitive column (ssn) to be ALWAYS masked ONLY for non-admin users on a table. Masking cannot be bypassed via direct SQL. What is the solution?",
    "options": {
     "A": "Create a view with `CASE WHEN is_admin() THEN ssn ELSE NULL END` plus revoke access to the base table",
     "B": "Use Row and Column Security (RCS) with a masking policy per governed tag",
     "C": "Use Dynamic Data Masking (DDM) with SQL rule plus ensure only admin can create SQL UDF",
     "D": "Replicate the table with blank ssn column, keep original in private schema plus use access role"
    },
    "answer": "B",
    "explanation": "Row and Column Security (RCS) with masking policy per tag is native in UC and cannot be bypassed.",
    "optExpl": {
     "A": "Incorrect: CASE view can be bypassed by direct base table access — revoking access possible but less robust.",
     "B": "Correct: RCS (Row and Column Security) in UC with policy on governed tag is native — applied in engine, cannot be bypassed.",
     "C": "Incorrect: 'Dynamic Data Masking' is marketing term — UC implements via RCS, not separate feature.",
     "D": "Incorrect: Replicating table plus roles is overhead — RCS is native masking policy solution."
    }
   },
   {
    "id": 22,
    "domain": "Developing Code",
    "topic": "Python/SQL",
    "text": "Your notebook does `spark.read.parquet(\"s3://bucket/path\")` each time it runs. There is 100GB of data. Which optimization ensures that re-reading the same path uses cache without rewriting?",
    "options": {
     "A": "Use `spark.sql.parquet.cacheMetadata=true` plus enable Delta cache",
     "B": "Save result to Delta after first read, then read from Delta",
     "C": "Configure `spark.sql.shuffle.partitions` and use `cache()` DataFrame plus `persist()`",
     "D": "Use Apache Iceberg instead of Parquet plus enable metadata caching"
    },
    "answer": "B",
    "explanation": "Save result to Delta after first read, then read from Delta, ensures cached snapshot and compacted structure.",
    "optExpl": {
     "A": "Incorrect: parquet.cacheMetadata caches metadata, but Parquet re-read re-does schema inference.",
     "B": "Correct: Save to Delta (first run) plus read from Delta (later runs) = persistent optimized snapshot.",
     "C": "Incorrect: cache()/persist() are in-memory (executor memory, not disk); does not persist between job runs.",
     "D": "Incorrect: Iceberg is valid alternative but overhead — Delta write is more practical solution."
    }
   },
   {
    "id": 23,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Low-Latency Webhook",
    "text": "You have a webhook sending events every second. You want to ingest to Delta with latency < 1 second end-to-end. Which setup is most appropriate?",
    "options": {
     "A": "Kafka topic → Structured Streaming → Delta with 500ms micro-batch",
     "B": "Webhook → Kinesis stream → Delta via Lakeflow Connect",
     "C": "Webhook → HTTP listener app → direct append to Delta (Python loop)",
     "D": "Webhook → Auto Loader in `STREAMING` mode with `trigger(once=False)` and latestFirst"
    },
    "answer": "A",
    "explanation": "Kafka → Structured Streaming with 500ms micro-batch → Delta guarantees < 1 sec end-to-end latency.",
    "optExpl": {
     "A": "Correct: Kafka → Structured Streaming with 500ms micro-batch offers low latency, fault tolerance, native checkpointing.",
     "B": "Incorrect: Lakeflow Connect is for database sources (CDC), not event streams.",
     "C": "Incorrect: Python HTTP loop direct = no ACID, no checkpointing, no fault tolerance — high risk.",
     "D": "Incorrect: Auto Loader is for files (S3, GCS), not webhooks — latestFirst is non-existent option."
    }
   },
   {
    "id": 24,
    "domain": "Data Manipulation",
    "topic": "SQL",
    "text": "A table has column `tags: ARRAY<STRUCT<name: STRING, value: VARIANT>>`. You need to count records where some tag has `name = 'category'` and `value.id` > 100. Which query is correct?",
    "options": {
     "A": "`SELECT COUNT(*) FROM table WHERE EXISTS (SELECT 1 FROM tags WHERE tags.name = 'category' AND tags.value:id > 100)`",
     "B": "`SELECT COUNT(DISTINCT id) FROM table, LATERAL FLATTEN(tags) t WHERE t.value:name = 'category' AND t.value:value:id > 100`",
     "C": "`SELECT COUNT(*) FROM table WHERE ANY(tags, t -> t.name = 'category' AND t.value:id > 100)`",
     "D": "`SELECT COUNT(*) FROM table WHERE array_contains(tags, map('name', 'category', 'id', '>100'))`"
    },
    "answer": "C",
    "explanation": "`ANY(tags, t -> t.name = 'category' AND t.value:id > 100)` uses `ANY` predicate on array with lambda for nested struct testing.",
    "optExpl": {
     "A": "Incorrect: EXISTS does not apply to arrays — would be scalar subquery, not array predicate.",
     "B": "Incorrect: LATERAL FLATTEN disaggregates array to rows, but t.value:name is wrong syntax (should be t.name).",
     "C": "Correct: ANY(array, lambda) is idiomatic array predicate — lambda applies AND logic on fields.",
     "D": "Incorrect: array_contains checks membership; '> 100' as string is literal, not operator — cannot do comparison."
    }
   },
   {
    "id": 25,
    "domain": "Cost & Performance Optimization",
    "topic": "Query Shuffle",
    "text": "A query on SQL Warehouse takes 5 minutes. Profiling shows 80% of time in shuffle. Your selectivity index is 2% (filters 2% of data). Which optimization is most efficient?",
    "options": {
     "A": "Use `CLUSTER BY` auto on the table plus reorder columns in select",
     "B": "Partitioning plus Z-order on filter column plus increase worker count",
     "C": "Enable Adaptive Query Execution (AQE) plus `broadcast_join_threshold`",
     "D": "Create a B-tree index and use hint `USE INDEX`"
    },
    "answer": "B",
    "explanation": "80% in shuffle with low selectivity → partitioning plus Z-order reduces volume. Largest gain over increasing workers.",
    "optExpl": {
     "A": "Incorrect: CLUSTER BY auto is complementary; reordering SELECT columns does not change shuffle.",
     "B": "Correct: Partitioning (prune partitions) plus Z-order (cluster rows) reduce shuffled data volume — efficient.",
     "C": "Incorrect: AQE is complementary — broadcast for small join sides, not 80% shuffle.",
     "D": "Incorrect: B-tree indexes not native in Databricks — Z-order is effective index used."
    }
   },
   {
    "id": 26,
    "domain": "Debugging and Deploying",
    "topic": "Workflow Retries",
    "text": "A Databricks Workflow runs via Declarative Automation (databricks.yml). A step fails. What is the native way to integrate retry logic without modifying the notebook?",
    "options": {
     "A": "Add `max_retries: 3` and `retry_on_timeout: true` to the task definition in databricks.yml",
     "B": "Wrap the notebook with Python script doing retry via `dbutils.notebook.run()` with try-except",
     "C": "Use `tasks: [{name: ..., job_cluster_config: ..., max_concurrent_runs: 1}]` plus job scheduling",
     "D": "Run each task as a separate `run_now` with manual validation"
    },
    "answer": "A",
    "explanation": "Declarative Automation natively supports `max_retries` and `retry_on_timeout` in task definition.",
    "optExpl": {
     "A": "Correct: max_retries plus retry_on_timeout in databricks.yml task definition is native Declarative Automation support.",
     "B": "Incorrect: Python wrapper with try-except is possible but not native declarative — wrapper overhead.",
     "C": "Incorrect: max_concurrent_runs controls concurrency, not retry — does not address task failure.",
     "D": "Incorrect: Manual run_now is manual, not automated — not integrated in workflow."
    }
   },
   {
    "id": 27,
    "domain": "Data Governance",
    "topic": "ABAC Policy Exception",
    "text": "Your UC has a table with PII column (email). You created a governed tag `pii` and applied ABAC masking policy. A new user NEEDS TO SEE THE REAL EMAIL (unmasked). What is the correct process?",
    "options": {
     "A": "Remove from `analysts` group, add to `data_officers` group with policy override",
     "B": "Request admin to create exception in policy using `ALTER POLICY ... ADD EXCEPTION`",
     "C": "User runs `USE UNMASKED_COPY` before selecting (does not exist, trick question)",
     "D": "Create a dedicated view with `SELECT email FROM table WHERE current_user() IN ('user@email.com')`"
    },
    "answer": "B",
    "explanation": "ABAC policy allows exceptions via `ALTER POLICY ... ADD EXCEPTION` for specific users.",
    "optExpl": {
     "A": "Incorrect: Adding user to data_officers group may work if group has permission, but not 'policy override'.",
     "B": "Correct: ALTER POLICY ADD EXCEPTION allows granular exceptions for specific users — native ABAC solution.",
     "C": "Incorrect: USE UNMASKED_COPY does not exist — trick question confirmed.",
     "D": "Incorrect: Dedicated view is workaround; does not leverage native ABAC policy exception framework."
    }
   },
   {
    "id": 28,
    "domain": "Developing Code",
    "topic": "Python",
    "text": "In a Spark job, you need to process data in smaller batches to avoid OOM. Which is the most idiomatic way in PySpark?",
    "options": {
     "A": "Use `repartition()` plus `groupByKey()` plus loop on `collect()`",
     "B": "`foreachPartition()` or `foreachBatch()` with batch size limits",
     "C": "`take(n)` in loop plus reprocess",
     "D": "Increase `spark.executor.memory` and let Spark manage partitions"
    },
    "answer": "B",
    "explanation": "`foreachPartition()` or `foreachBatch()` with batch size limit is idiomatic for processing in batches and avoiding OOM.",
    "optExpl": {
     "A": "Incorrect: collect() brings data to driver memory (OOM risk) — repartition plus groupByKey plus collect is antipattern.",
     "B": "Correct: foreachPartition() (RDD) or foreachBatch() (DataFrame) process in batches — batch size limit prevents OOM.",
     "C": "Incorrect: take(n) is selection function, not processing — not idiomatic for transformations.",
     "D": "Incorrect: Increasing memory is fallback; true solution is distributed batch processing."
    }
   },
   {
    "id": 29,
    "domain": "Monitoring and Alerting",
    "topic": "Data Freshness SLA",
    "text": "A pipeline has SLA \"data must be current by 6 AM\". The table feeding the dashboard has no alerts. What is the best native Databricks configuration?",
    "options": {
     "A": "Use `ALTER TABLE ... ADD CONSTRAINT freshness_check`",
     "B": "Configure alerts in SQL Warehouse query profiling",
     "C": "Create a validation job running at 5:50 AM, check `DESCRIBE DETAIL` timestamp and trigger Alert (webhook/Slack)",
     "D": "Use native SQL Warehouse Query Alert set to run SLA check queries"
    },
    "answer": "C",
    "explanation": "Validation job running before SLA, checking timestamp via `DESCRIBE DETAIL`, triggering webhook/Slack alert. Common practice.",
    "optExpl": {
     "A": "Incorrect: Constraints validate values (NOT NULL, UNIQUE), not time-based freshness.",
     "B": "Incorrect: SQL Warehouse query profiling is for performance, not table freshness monitoring.",
     "C": "Correct: Validation job at 5:50 AM checking DESCRIBE DETAIL last_modified timestamp and emitting alert — common practice.",
     "D": "Incorrect: Query Alert is for query result content, not table freshness metadata."
    }
   },
   {
    "id": 30,
    "domain": "Data Security and Compliance",
    "topic": "External Partner Sharing",
    "text": "You need to provide read access to a UC table for an external partner. The table contains sensitive data. What is the recommended approach using native UC?",
    "options": {
     "A": "Export data to CSV, share via S3 pre-signed URL",
     "B": "Use Open Sharing (if partner has Databricks) or Clean Rooms (private sharing)",
     "C": "Create a view with data subset plus grant `SELECT` via shared role",
     "D": "Replicate table to partner's workspace plus manage permissions per-workspace"
    },
    "answer": "B",
    "explanation": "Open Sharing (partner with Databricks) or Clean Rooms (private sharing) is native UC for external partners.",
    "optExpl": {
     "A": "Incorrect: CSV plus S3 pre-signed URL is outside governance — sensitive data without audit trail.",
     "B": "Correct: Open Sharing (partner with Databricks account) plus Clean Rooms (managed sharing) are native UC.",
     "C": "Incorrect: View with SELECT via shared role is workaround — Open Sharing is native.",
     "D": "Incorrect: Replicating to workspace is overhead — sharing is native solution."
    }
   },
   {
    "id": 31,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Incremental JDBC",
    "text": "You ingest from a legacy database via JDBC. The table has 500M rows and grows 10M/day. Which strategy ensures efficient incremental ingestion?",
    "options": {
     "A": "Full query (`SELECT *`) daily with `mergeSchema=true`",
     "B": "Use `READ_FROM` with hint on monotonic sequence column plus `WHERE col > last_value`",
     "C": "Lakeflow Connect in CDC mode (if supported) or JDBC with parameterized increment query",
     "D": "Use Apache Sqoop plus convert to Parquet after"
    },
    "answer": "C",
    "explanation": "Lakeflow Connect CDC (if DB supports) or JDBC with parameterized sequence ensures efficient incremental ingestion.",
    "optExpl": {
     "A": "Incorrect: Daily SELECT * plus mergeSchema is full refresh — inefficient for 500M plus 10M/day growth.",
     "B": "Incorrect: READ_FROM with sequence column plus WHERE > last_value is standard, but Lakeflow CDC is more robust.",
     "C": "Correct: Lakeflow Connect CDC (if DB supports) captures native changes — JDBC parameterized sequence is fallback.",
     "D": "Incorrect: Apache Sqoop is legacy; Lakeflow Connect is modern native Databricks solution."
    }
   },
   {
    "id": 32,
    "domain": "Developing Code",
    "topic": "SQL",
    "text": "In a SQL notebook, you do `CREATE TEMP VIEW v AS SELECT ...`. Then you want to run in parallel two commands: `INSERT INTO tab1 SELECT * FROM v` and `INSERT INTO tab2 SELECT * FROM v`. What is the risk and how to avoid?",
    "options": {
     "A": "TEMP VIEW only lives in the session — use `CREATE VIEW` (persistent) or `GLOBAL TEMP VIEW`",
     "B": "No risk, TEMP VIEW is accessible across the session",
     "C": "Must recreate view in each parallelism — use `spark.parallelize()`",
     "D": "Problem is the insert, not the view — add `OVERWRITE` clause"
    },
    "answer": "A",
    "explanation": "TEMP VIEW lives only in SQL session. Parallelism (dbutils.notebook.run, tasks) = new session = no access. Use `CREATE VIEW` or `GLOBAL TEMP VIEW`.",
    "optExpl": {
     "A": "Correct: TEMP VIEW lives only in SQL session — parallelism (dbutils.notebook.run, tasks) = new session = no access. Use `CREATE VIEW` or `GLOBAL TEMP VIEW`.",
     "B": "Incorrect: TEMP VIEW is session-local; parallelism = new session = TEMP VIEW invisible — risk exists.",
     "C": "Incorrect: spark.parallelize() creates RDD, does not reuse view — not correct solution.",
     "D": "Incorrect: OVERWRITE is write mode, does not solve TEMP VIEW scope across sessions."
    }
   },
   {
    "id": 33,
    "domain": "Cost & Performance Optimization",
    "topic": "Materialized View Strategy",
    "text": "A BI report is fed by 3 Materialized Views combining data from 10 Delta tables. Refresh is scheduled hourly. Additional ad-hoc queries hit the MVs. Which strategy reduces cost?",
    "options": {
     "A": "Convert MVs to normal (non-materialized) views plus enable Delta cache",
     "B": "Use UC Metric Views for aggregations plus keep only 1 MV base for raw data",
     "C": "Combine refresh of 3 MVs in 1 job plus use scheduled warehouses that auto start/stop",
     "D": "Disable automatic refresh plus run manual refresh on-demand"
    },
    "answer": "C",
    "explanation": "Combine 3 refreshes into 1 job, use scheduled warehouses on/off auto. Amortizes startup overhead, pay only during refresh period.",
    "optExpl": {
     "A": "Incorrect: Normal (non-materialized) views re-scan each query — worse than MV.",
     "B": "Incorrect: Metric Views plus 1 MV is more complex — does not reduce base cost of 3 MV refreshes.",
     "C": "Correct: Combine 3 refreshes into 1 job, use scheduled warehouses on/off auto. Amortizes startup overhead, pay only during refresh period.",
     "D": "Incorrect: Manual refresh not scalable; automatic refresh necessary for standard BI."
    }
   },
   {
    "id": 34,
    "domain": "Monitoring and Alerting",
    "topic": "Ingestion Failure",
    "text": "An ingestion job starts failing with `FileNotFoundError` after a week running. The code did not change. What is the most likely cause?",
    "options": {
     "A": "S3 path expired or bucket was deleted",
     "B": "IAM permission was revoked or credentials expired",
     "C": "Cluster was terminated, new cluster does not have access to same bucket",
     "D": "All above are possible — check driver logs, validate permissions, test path"
    },
    "answer": "D",
    "explanation": "All are possible causes. Validate logs, test path S3, check IAM credentials.",
    "optExpl": {
     "A": "Incorrect: S3 path expiration or bucket deletion is possible cause of FileNotFoundError.",
     "B": "Incorrect: IAM permission revocation or credential expiration is possible cause.",
     "C": "Incorrect: New cluster may have different IAM role or missing mount config — possible cause.",
     "D": "Correct: All above are possible. Validate logs, test path S3, check IAM credentials."
    }
   },
   {
    "id": 35,
    "domain": "Data Modeling",
    "topic": "Event Type Metrics",
    "text": "You have an events table with `event_type: STRING` and want to calculate metrics by type. Which is the most performant using VARIANT/STRUCT?",
    "options": {
     "A": "`SELECT event_type, COUNT(*) FROM events GROUP BY event_type` plus Python loop on result",
     "B": "Use `CASE` statements for each type plus dynamic aggregation",
     "C": "Transform to VARIANT with structure `{type: X, metrics: {...}}` then parse",
     "D": "Partition logically (views per type) plus run separate query for each"
    },
    "answer": "A",
    "explanation": "`SELECT event_type, COUNT(*) FROM events GROUP BY event_type` is native SQL optimized by Catalyst. Idiomatic, efficient.",
    "optExpl": {
     "A": "Correct: `SELECT event_type, COUNT(*) FROM events GROUP BY event_type` is optimized by Catalyst. Idiomatic, efficient.",
     "B": "Incorrect: CASE statements for each type is manual — less clean than native GROUP BY.",
     "C": "Incorrect: Transform to VARIANT plus parse is unnecessary overhead.",
     "D": "Incorrect: Views per type plus separate query is multiple passes — single GROUP BY efficient."
    }
   },
   {
    "id": 36,
    "domain": "Data Manipulation",
    "topic": "Exactly-Once Streaming",
    "text": "You consume Kafka messages with Structured Streaming. Producer sends events with distinct timestamps. You want to guarantee exactly-once processing even with failures. Which configuration is critical?",
    "options": {
     "A": "Use `outputMode=\"append\"` without checkpoint (does not guarantee)",
     "B": "Checkpoint enabled plus idempotent writer (save with idempotency key) plus `completionTrigger`",
     "C": "`outputMode=\"complete\"` with `trigger(once=True)` and checkpoint",
     "D": "Checkpoint alone is sufficient (false, also needs idempotent sink)"
    },
    "answer": "B",
    "explanation": "Exactly-once requires checkpoint plus idempotent writer plus sink that avoids duplicates. Checkpoint alone insufficient.",
    "optExpl": {
     "A": "Incorrect: outputMode='append' without checkpoint = no state tracking = at-least-once (duplicates possible).",
     "B": "Correct: Checkpoint tracks offsets — idempotent writer (idempotency key) avoids duplicates — exactly-once guaranteed.",
     "C": "Incorrect: Complete mode retains all aggregated data (memory overhead) — trigger(once=True) is single batch.",
     "D": "Incorrect: Checkpoint alone is necessary but insufficient — also needs idempotent sink."
    }
   },
   {
    "id": 37,
    "domain": "Cost & Performance Optimization",
    "topic": "Multi-Stage Query",
    "text": "A query takes 10 minutes. Profiling shows: 40% network (shuffle), 30% JSON parse, 20% sort, 10% I/O. Which optimization brings highest gain?",
    "options": {
     "A": "Increase partitions plus use Z-order",
     "B": "Pre-parse JSON to VARIANT at ingestion plus enable Delta cache",
     "C": "Convert JSON to native Parquet before query",
     "D": "Increase worker count and `spark.sql.shuffle.partitions`"
    },
    "answer": "B",
    "explanation": "JSON parsing is 30% of time. Pre-parsing to VARIANT at ingestion plus Delta cache avoids re-parse. Largest gain.",
    "optExpl": {
     "A": "Incorrect: Increasing partitions plus Z-order helps shuffle (40%), but parsing is 30% — does not address issue.",
     "B": "Correct: Pre-parse JSON to VARIANT at ingestion = parse once — Delta cache avoids re-read — reduces 30%.",
     "C": "Incorrect: Converting to Parquet does not help; Parquet is compression, not parse overhead.",
     "D": "Incorrect: Increasing workers/partitions does not reduce parse operation — parallelizing does not reduce parsing if dataset small."
    }
   },
   {
    "id": 38,
    "domain": "Developing Code",
    "topic": "SQL",
    "text": "You create a stored procedure doing `INSERT INTO table SELECT ...` with date parameter. For testing, you run with fixed date. In production (scheduled job), date should be dynamic (today). What is the correct practice?",
    "options": {
     "A": "Store date in a metadata table, stored proc reads it",
     "B": "Use `DEFAULT CURRENT_DATE()` as parameter with null-check plus override in job",
     "C": "Use `COALESCE(parameter_date, CURRENT_DATE())` in procedure",
     "D": "Hardcode `CURRENT_DATE()` directly in procedure (no parameter)"
    },
    "answer": "C",
    "explanation": "`COALESCE(parameter_date, CURRENT_DATE())` is idiomatic — null parameter falls to dynamic date.",
    "optExpl": {
     "A": "Incorrect: Metadata table is extra indirection — less clean than standard COALESCE.",
     "B": "Incorrect: DEFAULT parameter plus null-check in job is verbose — COALESCE is idiomatic.",
     "C": "Correct: COALESCE(parameter_date, CURRENT_DATE()) = if parameter NULL, use CURRENT_DATE() — idiomatic.",
     "D": "Incorrect: Hardcoding CURRENT_DATE() without parameter = no way to test with fixed date — poor design."
    }
   },
   {
    "id": 39,
    "domain": "Debugging and Deploying",
    "topic": "Notebook Parameters",
    "text": "A notebook exports parameters via `dbutils.widgets.get()`. A scheduled job passes parameters via `%run ../config`. Suddenly, everything fails with \"widget not found\". Why?",
    "options": {
     "A": "`%run` does not pass widgets — use `dbutils.notebook.run()` with `base_parameters` dict",
     "B": "Config notebook must be in same workspace (relative path does not work across workspaces)",
     "C": "`dbutils.widgets.get()` does not work in jobs — use environment variables via `spark.conf`",
     "D": "Missing `%render` before using widgets"
    },
    "answer": "A",
    "explanation": "`%run` is magic command (no context passing) — use `dbutils.notebook.run()` with `base_parameters` dict passes variables.",
    "optExpl": {
     "A": "Correct: %run is magic command (no context passing) — dbutils.notebook.run() with base_parameters dict passes variables.",
     "B": "Incorrect: Relative path works within workspace — issue is %run does not pass context, not about path.",
     "C": "Incorrect: dbutils.widgets.get() works in jobs if notebook called via dbutils.notebook.run() with base_parameters.",
     "D": "Incorrect: %render does not exist (or unnecessary) — issue is %run vs dbutils.notebook.run()."
    }
   },
   {
    "id": 40,
    "domain": "Monitoring and Alerting",
    "topic": "Dashboard Inconsistency",
    "text": "A dashboard shows inconsistency: two identical reports return different values. Dashboard uses same Materialized View. What is the most likely cause?",
    "options": {
     "A": "MV was refreshing while one report ran — one saw older version, other saw newer",
     "B": "One report uses browser-local cache, other does not",
     "C": "Delta version history — one query sees old snapshot, other sees new snapshot",
     "D": "Bug in dashboard query, not MV fault"
    },
    "answer": "A",
    "explanation": "MV refresh atomically replaces version — one query sees v1, other sees v2 = refresh happened between them. Transient inconsistency.",
    "optExpl": {
     "A": "Correct: MV refresh atomically replaces version — one query sees v1, other sees v2 = refresh occurred between them.",
     "B": "Incorrect: Browser-local cache does not affect server-side query results — does not cause data inconsistency.",
     "C": "Incorrect: Delta version history is possible with multiple versions, but MV refresh is likely cause.",
     "D": "Incorrect: Data inconsistency not query bug — MV refresh timing is cause."
    }
   },
   {
    "id": 41,
    "domain": "Data Manipulation",
    "topic": "Masking Policy Logic",
    "text": "You have a table with columns `customer_id`, `email`, `age`. You want a policy masking `email` for all EXCEPT `data_engineers`. Which is correct configuration in UC?",
    "options": {
     "A": "Use ABAC policy with `principal != \"data_engineers\"` → apply masking (inverted logic not supported)",
     "B": "Use \"positive\" rule: `principal == \"data_engineers\"` → NO masking, `else` → masking",
     "C": "Use Row-Level Security instead of Column-Level (not applicable here)",
     "D": "Create 2 views: 1 with email for data_engineers, 1 without for others"
    },
    "answer": "B",
    "explanation": "ABAC in UC supports \"positive\" logic: `principal == \"data_engineers\"` → NO mask; default → mask.",
    "optExpl": {
     "A": "Incorrect: Inverted logic (principal != X) not direct mode in ABAC — ABAC uses positive logic.",
     "B": "Correct: ABAC allows 'positive rule': principal == 'data_engineers' → NO mask; else → mask.",
     "C": "Incorrect: RLS for row filtering, not column masking — column masking is Column Security responsibility.",
     "D": "Incorrect: Dual views is manual workaround — ABAC is native solution."
    }
   },
   {
    "id": 42,
    "domain": "Developing Code",
    "topic": "Python",
    "text": "In a PySpark job, you need to apply costly transformation on RDD. Which is the most efficient?",
    "options": {
     "A": "`rdd.map(expensive_func)` without persistence",
     "B": "`rdd.map(expensive_func).cache().count()` to force computation plus then use",
     "C": "Use DataFrame with SQL UDF instead of Python RDD",
     "D": "`rdd.persist(StorageLevel.DISK_ONLY)` if memory is limited"
    },
    "answer": "B",
    "explanation": "`cache().count()` forces immediate computation and caching. Subsequent uses read from cache. With limited memory, consider `DISK_ONLY` storage level.",
    "optExpl": {
     "A": "Incorrect: map() without cache re-computes on each action — inefficient for costly transformation.",
     "B": "Correct: cache().count() forces immediate computation and storage — later uses read from cache.",
     "C": "Incorrect: SQL UDF is better than Python RDD, but question is how to cache efficiently.",
     "D": "Incorrect: persist(DISK_ONLY) without count() is lazy — needs action to force computation."
    }
   },
   {
    "id": 43,
    "domain": "Data Security and Compliance",
    "topic": "Column Access Audit",
    "text": "You want to audit WHO accessed WHICH column of a UC table. Native Databricks audit logs show \"SELECT * FROM table\" but not column granularity. Which is the solution?",
    "options": {
     "A": "Use UC Column-Level Lineage with masking policies (shows block, not access)",
     "B": "Create view per user plus column-level security plus monitor differentiated access logs",
     "C": "Use Predictive Optimization to track column access (not its function)",
     "D": "Implement custom logging in UDF recording column accessed plus audit via external DB"
    },
    "answer": "D",
    "explanation": "UC audit logs do not track individual column access. Custom logging in UDF recording \"user X accessed column Y\" plus external audit DB is real solution.",
    "optExpl": {
     "A": "Incorrect: UC lineage shows data transformations, not access audit — does not distinguish which column accessed.",
     "B": "Incorrect: UC access logs do not track column granularity — all logs show 'SELECT * FROM table'.",
     "C": "Incorrect: Predictive Optimization is for query optimizer recommendations, not audit.",
     "D": "Correct: Custom logging in UDF recording 'user X accessed column Y' plus external DB audit = real solution."
    }
   },
   {
    "id": 44,
    "domain": "Data Manipulation",
    "topic": "MERGE vs UPDATE",
    "text": "You have a large table (1TB) and want to update ~1% of rows. `MERGE` vs `UPDATE`: which is better?",
    "options": {
     "A": "`UPDATE` is more direct, always better",
     "B": "`MERGE` always better for large volumes, even small changes",
     "C": "Use `MERGE` for many changes; `UPDATE` if < 5% (different performance in Delta)",
     "D": "Both equivalent in Delta — choose for code clarity"
    },
    "answer": "C",
    "explanation": "Delta: `MERGE` for large-scale changes — `UPDATE` for < 5% rows (targeted, efficient). Different performance profiles.",
    "optExpl": {
     "A": "Incorrect: UPDATE direct for targeted updates, but MERGE offers more semantics — not 'always better'.",
     "B": "Incorrect: MERGE always better for large-scale changes, but Delta optimizes UPDATE for < 5%.",
     "C": "Correct: Delta: MERGE for large-scale changes — UPDATE for < 5% rows (targeted efficient).",
     "D": "Incorrect: Not equivalent in Delta — UPDATE optimized for small changes, MERGE for large."
    }
   },
   {
    "id": 45,
    "domain": "Cost & Performance Optimization",
    "topic": "Query Regression",
    "text": "A query doing full table scan of 100GB ran in 2 min, now takes 10 min. Nothing changed in code. What to check FIRST?",
    "options": {
     "A": "Table changes: new partitioning / compaction failing / old Delta version",
     "B": "Cluster: change in worker count / worker type / available memory",
     "C": "Query: change in hints / join order / predicate pushdown",
     "D": "Cache: Delta cache disabled or LRU expired history"
    },
    "answer": "A",
    "explanation": "Full table scan degrading — table quality changed (compaction degrading, old Delta version).",
    "optExpl": {
     "A": "Correct: If code unchanged, table quality changed (compaction degrading, old Delta version).",
     "B": "Incorrect: If cluster unchanged (worker count, type, memory), not cluster fault.",
     "C": "Incorrect: Code unchanged — query hints unchanged.",
     "D": "Incorrect: Delta cache expiration does not cause full scan degradation."
    }
   },
   {
    "id": 46,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Variable-Encoding CSV",
    "text": "You receive CSV files daily in S3. File has variable encoding (UTF-8, Latin-1). What is the robust way to ingest with Auto Loader?",
    "options": {
     "A": "`spark.read.option(\"encoding\", \"UTF-8\").csv(path)`",
     "B": "Use Auto Loader with `cloudFiles.schemaInference=true` and native encoding detector",
     "C": "Pre-process files with `file_modify_time` plus use `multiLine=true`, `charToEscapeQuoteEscaping=true`",
     "D": "Auto Loader does not support variable encoding — convert externally to UTF-8 first"
    },
    "answer": "B",
    "explanation": "Auto Loader with `cloudFiles.schemaInference=true` plus native encoding detection supports variable-encoding CSV. Fallback: pre-process with iconv.",
    "optExpl": {
     "A": "Incorrect: spark.read.csv does not support native 'encoding' option — Spark assumes UTF-8.",
     "B": "Correct: Auto Loader with cloudFiles.schemaInference=true plus native encoding detection supports variable CSV.",
     "C": "Incorrect: file_modify_time is file filter — multiLine/charToEscapeQuoteEscaping are CSV format options.",
     "D": "Incorrect: Auto Loader is flexible — supports variable encoding with schemaInference=true."
    }
   },
   {
    "id": 47,
    "domain": "Developing Code",
    "topic": "Python",
    "text": "In a notebook combining SQL and PySpark: SQL query returns 1M rows. You do `df = spark.sql(\"SELECT ...\")` then `df.collect()`. What is the risk?",
    "options": {
     "A": "`collect()` brings everything to driver memory — can cause OOM",
     "B": "None, PySpark manages memory automatically",
     "C": "`df` stays in RDD, not driver memory",
     "D": "Only fails if partition count < worker count"
    },
    "answer": "A",
    "explanation": "`collect()` brings 1M rows to driver memory — real OOM risk. Use `count()` or `write` instead.",
    "optExpl": {
     "A": "Correct: collect() brings 1M rows to driver JVM heap — real OOM risk = real problem.",
     "B": "Incorrect: PySpark does not auto-manage collect() — collect() is eager action materializing.",
     "C": "Incorrect: df stays in executor memory until collect() — collect() materializes to driver.",
     "D": "Incorrect: collect() failure does not depend on partition count — always brings to driver."
    }
   },
   {
    "id": 48,
    "domain": "Debugging and Deploying",
    "topic": "Automated Tag Application",
    "text": "Your client wants a governed tag `sensitive` applied automatically to any column with `@pii` annotation or containing \"ssn\". What is the solution?",
    "options": {
     "A": "Use governance rules in UC with automatic pattern matching",
     "B": "Create job scanning schema and running `ALTER TABLE ... SET TBLPROPERTIES (sensitive=true)` based on name pattern",
     "C": "Use Predictive Optimization to suggest tags automatically",
     "D": "Configure ABAC policy that auto-applies tag on column creation"
    },
    "answer": "B",
    "explanation": "Job scanning schema, identifying columns with \"ssn\"/\"pii\" in name, then `ALTER TABLE ... SET TAG` is common workaround.",
    "optExpl": {
     "A": "Incorrect: 'governance rules' with 'automatic pattern matching' not standard native phrasing.",
     "B": "Correct: Job scanning schema, identifying columns with 'ssn'/'pii' in name, then ALTER TABLE SET TAG is common workaround.",
     "C": "Incorrect: Predictive Optimization is for performance recommendation, not tag suggestion.",
     "D": "Incorrect: ABAC policy does not create tags — policy consumes applied tags."
    }
   },
   {
    "id": 49,
    "domain": "Cost & Performance Optimization",
    "topic": "OOM at Scale",
    "text": "A job runs fine in testing (10M data) but fails in production (10B data) with OutOfMemory. You increase `spark.executor.memory`. Failure persists, not in driver, in executor. What is the real cause?",
    "options": {
     "A": "Not executor memory — may be full disk spill or partition imbalance",
     "B": "Just increasing memory should resolve",
     "C": "Job is not scalable — needs rewrite",
     "D": "Increasing memory does not help spill — use `repartition()` or compact data"
    },
    "answer": "D",
    "explanation": "If increasing executor memory does not resolve OOM and not driver OOM, issue is partition imbalance or spill. `repartition()` resolves.",
    "optExpl": {
     "A": "Incorrect: Partition imbalance plus spill are possible, but does not offer solution (only diagnosis).",
     "B": "Incorrect: Increasing memory may not resolve if problem is spill (full disk) or imbalance.",
     "C": "Incorrect: Job may be scalable with repartition — does not need rewrite.",
     "D": "Correct: If increasing executor memory does not resolve OOM and not driver OOM, issue is spill or partition imbalance — repartition() resolves."
    }
   },
   {
    "id": 50,
    "domain": "Developing Code",
    "topic": "Python",
    "text": "In PySpark job, you do exploratory analysis on Delta table. You want to apply costly transformation without OOM risk. Which is safest?",
    "options": {
     "A": "Use `repartition()` before `map()` plus apply on small partitions",
     "B": "`df.map(func).cache().count()` to force immediate computation",
     "C": "Use `foreachPartition()` with batch size limit",
     "D": "Increase `spark.driver.memory` and let Spark manage"
    },
    "answer": "B",
    "explanation": "`df.map(func).cache().count()` forces immediate computation and caching. Safe for exploratory analysis. With limited memory, consider `DISK_ONLY` storage level.",
    "optExpl": {
     "A": "Incorrect: repartition() may help parallelization, but cache().count() better for exploration.",
     "B": "Correct: df.map(func).cache().count() forces computation plus caches result — safe for exploratory analysis.",
     "C": "Incorrect: foreachPartition() is for batch processing, not interactive exploration.",
     "D": "Incorrect: Increasing driver memory is fallback, not solution for batch processing."
    }
   },
   {
    "id": 51,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Webhook Deduplication",
    "text": "You have a webhook sending events every second. Webhook can resend same event multiple times (retry). You want to deduplicate. What is the approach?",
    "options": {
     "A": "Add `UNIQUE` constraint on ID column (fails if ID already exists)",
     "B": "Use `INSERT IGNORE` (does not exist in Delta/Databricks)",
     "C": "Use `MERGE` with `WHEN NOT MATCHED INSERT` based on dedup key plus `DELETE FROM staging WHERE ...`",
     "D": "Insert to staging, then `INSERT INTO target SELECT DISTINCT * FROM staging`"
    },
    "answer": "C",
    "explanation": "`MERGE` with `WHEN NOT MATCHED INSERT` based on ID ensures no duplicates in webhook deduplication.",
    "optExpl": {
     "A": "Incorrect: UNIQUE constraint on ID = fails if ID exists (duplicate webhook) — error on INSERT.",
     "B": "Incorrect: INSERT IGNORE does not exist in Delta — Delta has no 'ignore' native semantics.",
     "C": "Correct: MERGE with WHEN NOT MATCHED INSERT = inserts only if ID not exists — efficient deduplication.",
     "D": "Incorrect: INSERT DISTINCT = removes duplicates, but DISTINCT on JSON/complex types may not be deterministic."
    }
   },
   {
    "id": 52,
    "domain": "Data Manipulation",
    "topic": "Partitioned Table Performance",
    "text": "You have sales table partitioned by `date`. Daily INSERT now takes 3x longer. Cause: many small partitions. Which solution is better?",
    "options": {
     "A": "Run `OPTIMIZE` with Z-order on full table (costly, full scan)",
     "B": "Run `OPTIMIZE` only on today's partition (`OPTIMIZE table WHERE date = ...`)",
     "C": "Use incremental compaction (does not exist built-in — use `REPARTITION` before INSERT)",
     "D": "Use auto-compaction via automatic `OPTIMIZE` in Databricks (available, needs config)"
    },
    "answer": "B",
    "explanation": "`OPTIMIZE` only on today's partition (WHERE date = TODAY) is efficient — compacts only new partition.",
    "optExpl": {
     "A": "Incorrect: OPTIMIZE full table with Z-order = full scan all data (costly for daily growth).",
     "B": "Correct: OPTIMIZE on specific partition (WHERE date = TODAY) = compacts only new partition.",
     "C": "Incorrect: Incremental compaction not built-in; repartition is workaround.",
     "D": "Incorrect: Auto OPTIMIZE possible in Databricks (Predictive Optimization), but requires additional config."
    }
   },
   {
    "id": 53,
    "domain": "Debugging and Deploying",
    "topic": "Intermittent Failure",
    "text": "Scheduled job fails intermittently (50% of time). Logs are identical. What probably happens?",
    "options": {
     "A": "Race condition with parallel job or Delta table locking",
     "B": "Random network timeout",
     "C": "Cluster scaling down between jobs, sometimes yes sometimes no",
     "D": "Without more info, diagnosis is impossible — need more telemetry (cluster metrics, warehouse logs)"
    },
    "answer": "D",
    "explanation": "50% intermittent failure — race condition, random timeout, or cluster scale-down. Need telemetry.",
    "optExpl": {
     "A": "Incorrect: Race condition plus Delta locking = possible cause of intermittent failure.",
     "B": "Incorrect: Random network timeout = possible cause of intermittent failure.",
     "C": "Incorrect: Cluster scale-down intermittent = possible cause of intermittent failure.",
     "D": "Correct: 50% intermittent = multiple possible causes — need cluster metrics plus warehouse logs."
    }
   },
   {
    "id": 54,
    "domain": "Data Governance",
    "topic": "Permission Management",
    "text": "You have UC table used by 3 groups: `analysts` (SELECT), `engineers` (SELECT + MODIFY), `admins` (full). What is most efficient permission management?",
    "options": {
     "A": "Create 3 roles plus make specific GRANT on each",
     "B": "Use inheritance: create `editors` role with MODIFY, `readers` role with SELECT, then `admins EXTEND editors`",
     "C": "Make GRANT directly per user (not scalable)",
     "D": "Run job doing `ALTER TABLE OWNER TO` on each group change"
    },
    "answer": "A",
    "explanation": "Create 3 roles plus specific GRANT on each is standard scalable UC approach.",
    "optExpl": {
     "A": "Correct: 3 roles (analysts, engineers, admins) plus specific GRANT = standard UC scalable approach.",
     "B": "Incorrect: Role inheritance possible in UC, but 'EXTEND' keyword not standard in GRANT.",
     "C": "Incorrect: GRANT per individual user = not scalable (user change = manual update).",
     "D": "Incorrect: ALTER TABLE OWNER periodically = not GRANT mechanism — ownership does not substitute GRANT."
    }
   },
   {
    "id": 55,
    "domain": "Debugging and Deploying",
    "topic": "Streaming Latency After 30 Days",
    "text": "A streaming job consuming from Kafka has growing back-pressure. After 30 days running, latency suddenly jumps. What is most likely cause?",
    "options": {
     "A": "Kafka has lag — check partition size and consumer group",
     "B": "Checkpoint state exploded — clean old checkpoint and restart stream",
     "C": "Shuffle memory grew — executor memory limit hit",
     "D": "Aggregation window accumulating state — watermark may be stuck or state not expiring"
    },
    "answer": "D",
    "explanation": "Latency growing after 30 days streaming — window state accumulating. Watermark stuck or state not expiring.",
    "optExpl": {
     "A": "Incorrect: Kafka lag = possible, but '30 days' pattern points to state accumulation.",
     "B": "Incorrect: Checkpoint exploding = possible, but '30 days' points to state old (watermark not expiring).",
     "C": "Incorrect: Shuffle memory growing = possible, but less likely after '30 days'.",
     "D": "Correct: Latency in streaming grows after 30 days — window state accumulating. Watermark stuck (not advancing) = state not expiring."
    }
   },
   {
    "id": 56,
    "domain": "Cost & Performance Optimization",
    "topic": "Stakeholder Metrics",
    "text": "A Warehouse has 1PB data. Users complain queries are slow. You need metric to convince leadership to invest in optimization. Which metric is most convincing?",
    "options": {
     "A": "Query count (does not show user impact)",
     "B": "Cost per query (shows waste)",
     "C": "P95 query latency plus total cost (shows poor UX + $ waste together)",
     "D": "Warehouse size (not directly related to performance)"
    },
    "answer": "C",
    "explanation": "P95 latency shows poor UX. Cost per query shows waste. Together, convince leadership.",
    "optExpl": {
     "A": "Incorrect: Query count = volume, not impact — does not convince on performance.",
     "B": "Incorrect: Cost per query = waste, but does not show UX impact (users waiting long).",
     "C": "Correct: P95 latency = '95% queries > X seconds' (poor UX) plus cost total = waste plus user impact.",
     "D": "Incorrect: Warehouse size = capacity, not latency — no direct correlation with performance."
    }
   },
   {
    "id": 57,
    "domain": "Data Security and Compliance",
    "topic": "PII Data Locality",
    "text": "Your organization has regulatory requirement: PII data must reside only on dedicated clusters. What is native Databricks approach?",
    "options": {
     "A": "Use `spark.conf` to limit read path to specific cluster via access control",
     "B": "UC policies plus compute-level ACL (if supported) or implement at storage level",
     "C": "Replicate PII in UC schema, grant access only from managed compute",
     "D": "Use UC Governance with table locality restriction (not native feature — manual)"
    },
    "answer": "C",
    "explanation": "Replicate PII in UC schema, grant access only from managed compute is closest native solution.",
    "optExpl": {
     "A": "Incorrect: spark.conf does not control table locality — not native mechanism.",
     "B": "Incorrect: Compute-level ACL not native UC feature ('if supported' = maybe not).",
     "C": "Correct: Replicate PII in UC schema plus dedicated compute = physical plus logical isolation (closest native).",
     "D": "Incorrect: 'table locality restriction' not native feature — manual implementation."
    }
   },
   {
    "id": 58,
    "domain": "Developing Code",
    "topic": "Python",
    "text": "In PySpark job, you want to process VARIANT columns in SQL transformation. Which approach ensures better performance?",
    "options": {
     "A": "Use `df.selectExpr(\"explode_outer(variant_col) as item\")` then iterate in Python",
     "B": "Use `sql(\"SELECT ... FROM delta.`path` WHERE ...\")` plus native SQL to manipulate VARIANT",
     "C": "Convert VARIANT to JSON string in Python, parse with `json.loads()`, then reassemble",
     "D": "Use `pyspark.sql.functions.col()` with `.getItem()` chaining"
    },
    "answer": "B",
    "explanation": "Native SQL with VARIANT is more efficient than Python/RDD. Optimizer optimizes VARIANT operations.",
    "optExpl": {
     "A": "Incorrect: explode_outer plus Python RDD = no Spark SQL optimization.",
     "B": "Correct: Native SQL with VARIANT = optimized by Catalyst optimizer.",
     "C": "Incorrect: VARIANT → JSON → Python = multiple serializations overhead.",
     "D": "Incorrect: getItem() is Column API — less direct than native SQL with VARIANT."
    }
   },
   {
    "id": 59,
    "domain": "Developing Code",
    "topic": "SQL/Python",
    "text": "You have nested JSON and want to extract multiple fields. Which is more performant: `parse_json()` once plus multiple `variant_get()`, or parse N times?",
    "options": {
     "A": "Parse N times is inefficient — use `parse_json()` once stored in VARIANT",
     "B": "Both equivalent in Spark SQL (optimizer is smart)",
     "C": "Parse once in Python, then in SQL (hybrid)",
     "D": "Use `get_json_object()` N times — Spark optimizes internally"
    },
    "answer": "A",
    "explanation": "Parse JSON once to VARIANT, then multiple `variant_get()` calls read cached VARIANT. Avoids re-parse.",
    "optExpl": {
     "A": "Correct: parse_json() once to VARIANT = 'memoized' — multiple variant_get() read cached VARIANT.",
     "B": "Incorrect: Spark SQL not 'smart' enough to deduplicate parse_json calls — N calls = N parses.",
     "C": "Incorrect: Python parse plus SQL parse = hybrid overhead.",
     "D": "Incorrect: get_json_object() N times = N parses — Spark does not deduplicate (unlike VARIANT memoization)."
    }
   },
   {
    "id": 60,
    "domain": "Developing Code",
    "topic": "Python/SQL",
    "text": "An iterative notebook with Structured Streaming processes data via `foreachBatch()`. After converting to scheduled job, latency degrades with 30 days runtime without restart. What is the real problem?",
    "options": {
     "A": "Checkpoint file gets too large, needs periodic cleanup",
     "B": "Window state accumulating in memory — watermark expired or offset tracking fails",
     "C": "Kubernetes pod memory leak in long duration",
     "D": "Python garbage collection degrading performance"
    },
    "answer": "B",
    "explanation": "Latency degrading after 30 days streaming — window state accumulating. Watermark expired or offset tracking fails.",
    "optExpl": {
     "A": "Incorrect: Large checkpoint possible, but '30 days' points to state accumulation (watermark).",
     "B": "Correct: Window state accumulating after 30 days — watermark expired (not advancing) or offset tracking fails.",
     "C": "Incorrect: Pod memory leak = Databricks not natively Kubernetes (clusters are cloud VMs).",
     "D": "Incorrect: Python GC degradation = unlikely cause of latency jump after '30 days'."
    }
   }
  ]
 }
};
