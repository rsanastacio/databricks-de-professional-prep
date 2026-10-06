> Material de estudo original (não são questões/conteúdo oficiais). Fonte de verdade: docs.databricks.com e o guia oficial de out/2026 (coluna New Exam). Reconfirme features antes da prova.

---

# Certificação Databricks Certified Data Engineer Professional — Novo Exame

## 1. Developing Code for Data Processing using Python and SQL (23%)

### Declarative Automation Bundles (DABs) — Estrutura e CI/CD

**O que é / quando usar:**
- Bundles (anteriormente DABs) = IaC declarativo com `databricks.yml`, Python/SQL templates renderizados via Jinja2.
- Define jobs, pipelines, clusters, dashboards como código. Suporta múltiplos *targets* (dev/staging/prod) com overrides per-target.
- Essencial para deploy reproduzível, versionamento em Git e CI/CD.

**Heurística:**
- Use bundles sempre que precisar de automação reproduzível. Repo pattern (Git folders) + bundles = default moderno.
- Targets permitem mesma definição, parametrizada para cada ambiente.

**Sintaxe-chave:**
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
- `databricks bundle deploy -t prod` = deploya target prod com overrides.

**Pegadinha:**
- Secrets em bundles: use `{{env "MY_SECRET"}}` ou UC secret scopes, não hardcode.
- Variable references com `${var.varname}` precisam estar em `variables:` ou CLI.

---

### Troubleshooting de Dependências

**O que é / quando usar:**
- PyPI packages, local wheels, source archives podem falhar em serverless compute (restrito, sandbox).
- Lakeflow Declarative Pipelines, Jobs, bundles — cada um tem contexto diferente de dependency resolution.

**Heurística:**
- Serverless compute: apenas PyPI packages estáveis e pré-built (wheels). Packages C-extension-heavy falham se não há driver compilado.
- Clusters classic: podem usar source archives, mas aumenta startup time.
- **Preferir clusters com alta-memory para tasks que precisam de muito RAM** durante instalação.

**Sintaxe-chave:**
```python
# Lakeflow Declarative Pipeline: pip_requirements
# dlt.yml:
{%- if target == 'prod' %}
pip_requirements: "requirements-prod.txt"
{%- else %}
pip_requirements: "requirements-dev.txt"
{%- endif %}

# PySpark UDF com dependencies:
# Usa --py-files ou job clusters config.
spark.conf.set("spark.pyspark.python", "/usr/bin/python3")
```
- Job cluster config: `python_wheel_distributions` ou `pip_packages` em cluster spec.

**Pegadinha:**
- Serverless não suporta `--py-files` com source .py diretamente; precisa de .zip ou importar via package.
- Versão de Python mismatch entre local build e serverless runtime (verificar DBR).

---

### UDFs — Pandas, Python, SQL + Unity Catalog

**O que é / quando usar:**
- **Pandas UDF**: vetorizado, rápido (Apache Arrow), recomendado p/ transformações por coluna.
- **Python UDF**: lento, serialização row-by-row, evitar em streaming.
- **SQL UDF**: determinístico, integrado em catálogo, reutilizável cross-workspace (UC).

**Heurística:**
- Pandas UDF p/ lógica complexa em batch.
- SQL UDF p/ transformações simples, reutilizáveis, sem state externo.
- Python UDF último recurso (call-out a Python, I/O externo).

**Sintaxe-chave:**
```python
# Pandas UDF
import pandas as pd
from pyspark.sql.functions import pandas_udf
@pandas_udf("double")
def multiply_by_two(s: pd.Series) -> pd.Series:
    return s * 2

# UC SQL UDF (armazenado, versionado)
CREATE OR REPLACE FUNCTION my_catalog.my_schema.my_func(x INT)
RETURNS INT
LANGUAGE SQL
AS 'SELECT x * 2'

# Chamar UC function
SELECT my_catalog.my_schema.my_func(col) FROM table
```

**Pegadinha:**
- Pandas UDF com imports dentro da função = overhead. Mova imports p/ top-level.
- UC SQL UDFs: não podem ter lógica arbitrária SQL, só SELECT simples.

---

### Lakeflow Declarative Pipelines (ex-DLT) + Auto Loader

**O que é / quando usar:**
- Declarative SQL/Python p/ ETL modular com dependency tracking automático.
- Auto Loader = connector gerenciado para cloud storage (S3, ADLS, GCS) com schema inference e CDC.
- Streaming tables vs materialized views = trade-off latência/custo/história.

**Estrutura mínima:**
```python
# pipeline.py (ou .sql)
import dlt
from pyspark.sql.functions import *

# Source: Auto Loader do S3
@dlt.table(comment="Raw events from S3")
def raw_events():
    return spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", "/tmp/schema") \
        .load("s3://bucket/path")

# Streaming table: append-only, exatamente uma vez
@dlt.table(
    name="events",
    path="/mnt/events",
    comment="Cleaned events"
)
def events():
    return dlt.read_stream("raw_events") \
        .filter(col("event_id").isNotNull())

# Materialized view: history completa, refrescável
@dlt.view
def daily_summary():
    return dlt.read("events") \
        .groupBy(date_trunc("day", "event_time")) \
        .agg(count("*"))
```

**Heurística de streaming table vs MV:**
| Aspecto | Streaming Table | Materialized View |
|--------|-----------------|-------------------|
| Modo | Append-only, exactly-once | Recalculado cada refresh |
| Latência | Baixa (incremental) | Batch (refresh period) |
| Custo | Menor (apenas novos dados) | Maior (recomputa tudo) |
| Usos | CDC, event streams | Agregações, histórico completo |
| Downtime | Sem backfill se falhar | Backfill automático no refresh |

**Pegadinha:**
- Auto Loader com `mode="FAILONMALFORMEDDATA"` (default) aborta pipeline se arquivo ruins existe. Use `"PERMISSIVE"` + filter nulls.
- Streaming table com late-arriving data: checkpoint pode estar muito além; considerar `initialCheckpointLocation`.

---

### CDC com AUTO CDC APIs (APPLY CHANGES)

**O que é / quando usar:**
- Captura mudanças (inserts, updates, deletes) em source incremental.
- `APPLY CHANGES` = sintaxe Lakeflow Declarative Pipelines p/ SCD Type 1 ou Type 2.
- **NET-NEW**: stored_as_scd_type = armazena histórico full ou apenas última versão.

**Sintaxe-chave:**
```sql
-- Lakeflow Declarative Pipeline (APPLY CHANGES)
CREATE OR REFRESH STREAMING TABLE customer_scd AS
  APPLY CHANGES INTO live.customer_scd
  FROM raw_customer_changes
  KEYS (customer_id)
  SEQUENCE BY change_time
  COLUMNS * EXCEPT (operation, _rescued_data)
  STORED AS SCD TYPE 2
  -- TYPE 2: mantém histórico (start_version, end_version, is_current)
  -- TYPE 1: apenas versão current (sobrescreve)
  
-- Depois, usar via TIME TRAVEL ou is_current filter:
SELECT * FROM live.customer_scd WHERE is_current
```

**Heurística:**
- SCD Type 1 quando só importa estado atual (ex: endereço).
- SCD Type 2 quando precisa rastrear quando cada dado mudou (ex: plano de subscription).

**Pegadinha:**
- `SEQUENCE BY` order DEVE estar em ordem causal, senão order de aplicação fica errada.
- Deletes: marcar com operation='DELETE' e source garantirá remoção.

---

### Spark Structured Streaming vs Lakeflow Declarative Pipelines

**Heurística:**
- Lakeflow Declarative Pipelines: default recomendado, auto-scaling, restart automático, UI nativa.
- Spark Structured Streaming: quando precisa de lógica customizada stateful (state store management, complex windowing).

**NET-NEW Structured Streaming: Stateful Operations**

Watermarks, output modes, foreachBatch, checkpoints, exactly-once recovery.

```python
from pyspark.sql.functions import window, col

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .load()

# Watermark: late data chegando até N minutes depois do event_time
query = df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "5 minutes")) \
    .count() \
    .writeStream \
    .outputMode("update")  # update = apenas rows que mudaram \
    .option("checkpointLocation", "/mnt/checkpoint") \
    .foreachBatch(process_batch)  # Custom logic per micro-batch \
    .start()

def process_batch(batch_df, batch_id):
    batch_df.write.mode("append").saveAsTable("output_table")

query.awaitTermination()
```

**Output modes:**
- `append`: só dados novos (default, recomendado).
- `update`: mudanças em aggregate (rows que mudaram).
- `complete`: estado completo do aggregate (pode crescer muito).

**Checkpoint = idempotência:**
- Restart automático retoma de last checkpoint, garante exactly-once.
- Dica: separar checkpoint por DAG (não compartilhar).

---

### Lakeflow Jobs com Control Flow (If/Else, For Each)

**O que é / quando usar:**
- Lakeflow Jobs (ex-Workflows) = job orchestration com dependency tracking, retry, alerts.
- Control flow = branching (IF/ELSE), loops (FOR EACH), dynamic task generation.

**Sintaxe-chave (YAML):**
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
          run_if: "ALL_DONE"  # ou "AT_LEAST_ONE_SUCCESS"
          
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

**Pegadinha:**
- Job parameters vs notebook parameters: use `dbutils.widgets.get()` p/ ler from job.
- Retries default = 1; configurar via `max_retries` no task.

---

### Serverless Compute — Environments, Dependency Mgmt, Performance Mode

**O que é / quando usar:**
- Serverless compute = managed, autoscaling, sem cluster overhead. Ideal p/ batch jobs e pipelines.
- **Environments** = pre-built Docker images com dependências pre-installed, reutilizáveis.
- Performance mode = dedicated resources, previsível, mais caro.

**Sintaxe-chave:**
```python
# Configurar environment (Docker)
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

# Usar em job
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

**Heurística:**
- Serverless p/ tasks < 30min, não-stateful.
- Classic clusters p/ long-running, state-heavy, ou debugging.

**Pegadinha:**
- Serverless = no driver local storage (~512MB). Usar /tmp com limite.
- No `dbutils.fs.rm()` de volumes grandes em serverless (timeout).

---

### High-Memory Notebook Tasks

**O que é / quando usar:**
- Notebook tasks em jobs podem ser alocar compute separado (não reusa driver do cluster).
- Alta memória p/ large DataFrame transformações.

**Sintaxe:**
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

### Testes: assertDataFrameEqual, assertSchemaEqual, DataFrame.transform

**O que é / quando usar:**
- Unit testing p/ transformações PySpark.
- `assertDataFrameEqual` = compara dados + schema.
- `assertSchemaEqual` = compara apenas schema.
- `DataFrame.transform()` = chaining de transformações, testável.

**Sintaxe:**
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

**Pegadinha:**
- assertDataFrameEqual é sensível a order e tipos. Use `checkRowOrder=False` se não importa.

---

### Auto-Optimization e Control de Retries

**O que é / quando usar:**
- Auto-optimization = Delta Lab feature que reescreve pequenos arquivos, otimiza data skipping automático.
- Disable retries p/ jobs que precisam falhar rápido (fail-fast debugging).

**Sintaxe:**
```python
# Enable auto-optimization na tabela
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

### Formatos e Fontes — Ingresso Multi-Source

**O que é / quando usar:**
- Delta (v1/v2) = default; Iceberg = coluna-oriented, time-travel, evolution. Parquet = storage comprimido. JSON, CSV = semi-structured. Binary = logs raw.
- Message buses = Kafka, Kinesis, Pub/Sub p/ streaming ingress.
- Cloud storage = S3, ADLS, GCS p/ batch.

**Heurística:**
- Delta = default p/ OLAP lakehouse, melhor data skipping.
- Iceberg = quando precisa cross-workspaces sharing, schema evolution complexa, ou performance queries em colunas sparse.
- Kafka/Kinesis = event-driven ingress, volume alto, latência baixa.

**Sintaxe:**
```python
# Auto Loader (cloudFiles) com múltiplos formatos
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

### CDC Incremental com Lakeflow + Delta/Iceberg

**O que é / quando usar:**
- Capture source deltas (novo/mudado/deletado), aplicar downstream.
- Delta CDF (Change Data Feed) = histórico de mudanças row-level.

**Sintaxe:**
```sql
-- Enable CDF na tabela source
ALTER TABLE source_table
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Ler mudanças
SELECT * FROM table_changes('source_table', 0)
WHERE _change_type IN ('insert', 'update_postimage', 'delete');

-- No Lakeflow Declarative Pipeline
CREATE STREAMING TABLE sink AS
  SELECT * FROM stream_changes('source_table')
```

---

### NET-NEW: Lakeflow Connect — Conectores CDC Gerenciados

**O que é / quando usar:**
- Lakeflow Connect = conectores pre-built, managed CDC p/ SQL Server, MySQL, PostgreSQL.
- Não precisa script custom, scheduler, ou gerenciar offset.

**Heurística:**
- Usar Lakeflow Connect quando source é SQL Server/MySQL/PostgreSQL.
- Fallback p/ custom Spark job se source exótico ou lógica CDC very custom.

**Sintaxe (confirmar na doc):**
```yaml
# Em Lakeflow Declarative Pipeline config
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

### NET-NEW: OpenSharing (D2D e Databricks-to-Open) e Clean Rooms

**O que é / quando usar:**
- **Databricks-to-Databricks (D2D) Sharing**: compartilhar tabelas between workspaces, managed credentials.
- **Databricks-to-Open**: compartilhar p/ Snowflake, BigQuery, etc. via protocol aberto.
- **Clean Rooms**: workspace isolado p/ colaboração confidencial.

**Heurística:**
- D2D p/ compartilhamento intra-org.
- Databricks-to-Open p/ partners externos.
- Clean Rooms p/ análises sensíveis (revenue-share, joint analytics).

**Sintaxe:**
```python
# Share table via D2D (requisitor-side)
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

### Lakehouse Federation com Governança

**O que é / quando usar:**
- Query federated data (Delta, Iceberg, External Services) como tabelas locais.
- UC permissions = control access a federated objects.
- Connection-level credentials = secret management integrado.

**Sintaxe:**
```python
# Criar connection p/ external warehouse
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

### Transformações Avançadas — Window, Join, Aggregation

**O que é / quando usar:**
- Window functions = ranking, lead/lag, aggregates over partitions sem colapsar linhas.
- Joins = cartesian, left/right/inner/outer; reordena por heurística Catalyst.
- Aggregations = groupBy+agg, distinct counts, percentiles.

**Sintaxe:**
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

# Lead/Lag: acesso to dados anteriores/próximos
w_time = Window.partitionBy('dept_id').orderBy('salary')
result = df.withColumn('prev_salary', lag('salary').over(w_time))

# Aggregate
result = df.groupBy('dept_id').agg(
    spark_sum('salary').alias('total_salary'),
    count_distinct('name').alias('emp_count')
)
```

**Pegadinha:**
- Window sem ORDER BY particiona mas não ordena = resultado imprevisível.
- Join sem hint: Spark reordena; use hint p/ broadcast small side: `/*+ BROADCAST(t2) */`.

---

### NET-NEW: VARIANT — parse_json, variant_get, Colon-Path

**O que é / quando usar:**
- VARIANT = semi-structured data type em SQL p/ JSON without parsing.
- Lazy parsing = performance melhor p/ queries que tocam poucos campos.

**Sintaxe:**
```sql
-- Parse JSON to VARIANT (lazy)
SELECT parse_json('{"a": 1, "b": {"c": 2}}') AS v;

-- Extract fields via colon-path
SELECT v:a, v:b:c FROM table;

-- variant_get p/ explicit extraction
SELECT variant_get(v, 'a', 'int') AS a_int FROM table;

-- Converter VARIANT to JSON string
SELECT to_json(v) FROM table;
```

**Pegadinha:**
- `v:a` retorna VARIANT; use `v:a::int` para casting.
- Colon-path é case-sensitive p/ keys.

---

### NET-NEW: AI Functions — ai_query p/ Inferência em Pipeline

**O que é / quando usar:**
- `ai_query` = chamar LLM (Claude, GPT, etc.) como UDF dentro SQL/streaming.
- Use p/ enrichment, classification, NER em data pipeline.

**Sintaxe (confirmar na doc):**
```sql
-- ai_query: classification example
SELECT 
    text,
    ai_query('SELECT classify_sentiment(@text)', MAP(ARRAY['text'], ARRAY[text])) AS sentiment
FROM text_table;

-- Com model specification
SELECT ai_query(
    'my-model:endpoint',
    'Summarize this: @text',
    MAP(ARRAY['text'], ARRAY[description])
) AS summary
FROM documents;
```

**Pegadinha:**
- Cost = por token sent; monitore usage.
- Latency = call externo; considerar batch vs row-by-row trade-off.

---

### Data Quality Expectations em Lakeflow Declarative Pipelines

**O que é / quando usar:**
- DQ expectations = assertions inline no pipeline (quarantine/drop/fail).
- `expect` = validação inline; `assert` = falha pipeline.
- Invalids podem ir p/ quarantine zone (separate table) p/ debug later.

**Sintaxe:**
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

**Heurística:**
- Use `expect_or_drop` p/ bad rows (logging automático).
- Use `expect_or_fail` p/ crítico (deve falhar).
- Quarantine table p/ audit trail.

---

## 4. Monitoring and Alerting (10%)

### System Tables — Billing, Compute, Access, Lakeflow

**O que é / quando usar:**
- System tables = tables governadas no `system` catalog, acesso read-only.
- `system.billing.list_prices` = preços.
- `system.compute.clusters` = cluster inventory.
- `system.access.audit_logs` = audit trail.
- `system.lakeflow.*` = pipeline runs, datasets, etc.

**Sintaxe:**
```sql
-- Custo por warehouse
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

### REST APIs / CLI / SDK p/ Monitorar Jobs e Pipelines

**O que é / quando usar:**
- Databricks REST API v2.1 = jobs, runs, pipelines endpoints.
- CLI = `databricks jobs get-run <run-id>`.
- Python SDK (`databricks.sdk.WorkspaceClient`) = programmatic access.

**Sintaxe:**
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

### Event Logs de Pipelines

**O que é / quando usar:**
- Event logs = detailed timestamped log de cada etapa pipeline.
- Acessíveis via REST API `/events` endpoint.
- Inclui: task start, data flow, lineage, errors, durations.

**Sintaxe:**
```python
# Fetch event logs
w = WorkspaceClient()
events = w.pipelines.get_event_logs(pipeline_id='my_pipeline')
for event in events:
    print(f"{event.timestamp}: {event.event_type} - {event.details}")
```

---

### Databricks Lakehouse Alerts

**O que é / quando usar:**
- Alerts = trigger automático em métricas governadas, qualidade, custo, SQL warehouse health, audit, AI agent quality, Lakeflow Job branching.
- Múltiplos canais: Slack, email, webhook.

**Heurística:**
- Alerta custo quando gasto ultrapassa threshold mensal.
- Alerta DQ quando rejeição % sobe.
- Alerta warehouse health quando query latency degrada.

**Sintaxe (confirmar na doc):**
```python
# Create alert via UI ou API
# databricks alerts create \
#   --name "High warehouse cost" \
#   --metric "warehouse_cost" \
#   --condition ">" \
#   --threshold 10000 \
#   --notification_channel "slack"
```

---

### Lakeflow Jobs UI/API

**O que é / quando usar:**
- Lakeflow Jobs = orchestration engine (ex-Workflows).
- UI = visual DAG, run history, retry.
- API = programmatic job management.

**Sintaxe:**
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

**O que é / quando usar:**
- **Managed tables** = ownership by UC, automatic optimization.
- **Predictive Optimization** = Delta Lab automatically analyzes e recommends optimization (z-ordering, compaction, clustering).
- **Liquid Clustering** = dinâmic rebinning, melhor p/ evolving filter patterns que CLUSTER BY.

**Heurística:**
- Managed tables default (UC governance + optimization).
- Predictive Optimization para tabelas > 1GB, acesso irregular.
- Liquid Clustering p/ queries com múltiplos filters em evolução.

**Sintaxe:**
```python
# Managed table com UC
spark.sql("""
    CREATE TABLE my_catalog.my_schema.my_table (
        id INT,
        customer_id INT,
        date DATE
    )
    USING DELTA
    CLUSTER BY (customer_id)
""")

# Enable Predictive Optimization (na tabela)
spark.sql("""
    ALTER TABLE my_catalog.my_schema.my_table
    SET TBLPROPERTIES ('delta.predictiveOptimization.enabled' = 'true')
""")

# Liquid Clustering (NET-NEW syntax, confirmar)
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

**O que é / quando usar:**
- **Deletion Vectors (DV)** = logical delete markers, não reescreve arquivos. Rápido, overhead mínimo.
- **Liquid Clustering** = rebinning dinâmico. Melhor p/ skewed, evolving queries.
- **CLUSTER BY AUTO** = seleção automática de clustering baseado em workload (confirmar na doc).

**Heurística:**
| Técnica | Overhead | Uso | 
|---------|----------|-----|
| Deletion Vectors | Mínimo | Deletes frequentes, sem reescrita |
| Liquid Clustering | Médio | Múltiplos filters em evolução |
| CLUSTER BY AUTO | Médio | Workload adaptativo, hands-off |

**Sintaxe:**
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

### Delta Cache p/ Leituras Repetidas

**O que é / quando usar:**
- Delta cache = in-memory cache de Parquet files em cluster nodes, survives query restarts.
- Reutilizável across queries, não invalidado se data não mudou.

**Heurística:**
- Ativar p/ tabelas read-heavy, tamanho < 10x cluster RAM.
- Desativar p/ tabelas em constant update (cache churn overhead).

**Sintaxe:**
```python
# Enable cache numa cluster
# Cluster config ou notebook:
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "10gb")

# Força cache de tabela específica
spark.sql("""
    CACHE TABLE my_catalog.my_schema.my_table
""")

# Check cache status
spark.sql("SELECT * FROM system.cacheinfo")
```

---

### CDF (Change Data Feed) p/ Expor Mudanças Row-Level

**O que é / quando usar:**
- CDF = histórico de mudanças (insert, update_preimage, update_postimage, delete).
- Processamento incremental downstream sem reprocessar tudo.

**Sintaxe:**
```python
# Enable CDF na tabela
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

**Pegadinha:**
- CDF storage cost = extra copies de dados mudados; monitore.

---

### Query Profile p/ Identificar Gargalos

**O que é / quando usar:**
- Query profile = breakdown de execution time por stage (data skipping, join strategy, shuffle).
- Acesso via Spark UI ou SQL warehouses Query History.

**Heurística:**
- Stage com shuffle > 90% tempo = problema. Considerar reparticionar, aumentar cluster RAM, ou usar Liquid Clustering.
- Data skipping stats < 10% = index mal feito. Considerar ZORDER ou Liquid Clustering.

**Sintaxe:**
```python
# Query profile no SQL warehouse (histórico automático)
# Ou, no notebook:
spark.sql("EXPLAIN FORMATTED SELECT ...").show()

# Detailed Spark UI: http://driver:4040/SQL/
```

---

### Liquid Clustering vs Partitioning / ZORDER

**Heurística:**
| Técnica | Incremental cost | Best for | Setup |
|---------|------------------|----------|-------|
| Partitioning | Alto (rewrite) | Low-cardinality (year, region) | Static, well-known |
| ZORDER | Alto (rewrite) | Static, known access pattern | Periodic optimize command |
| Liquid Clustering | Médio (incremental) | Evolving, high-cardinality | Dynamic, maintenance-free |

---

## 6. Ensuring Data Security and Compliance (8%)

### ACLs Least-Privilege em UC Securables

**O que é / quando usar:**
- UC = fine-grained access control em catalog/schema/table/volume/connection/external-location.
- Least-privilege = grant apenas permissions necessárias, deny default.

**Heurística:**
- Default: DENY (ninguém tem acesso).
- Granular grants: SELECT p/ analysts, MODIFY p/ ETL, MANAGE p/ admins.
- Group-based: mais fácil manter.

**Sintaxe:**
```sql
-- Criar catalog
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

**Pegadinha:**
- Inheritance: grant no catalog = herda p/ futuros schemas/tables (mas não retroativo).
- OWNER tiene todos os permissions (não pode ser removido).

---

### NET-NEW: ABAC Policies com Governed Tags → Row/Column Filters

**O que é / quando usar:**
- ABAC (Attribute-Based Access Control) = governança baseada em tags ao invés de group membership.
- Governed tags = tags definidas centralmente, aplicadas a objetos e users.
- Row/column filters gerados automaticamente baseado em tag match.

**Heurística:**
- ABAC p/ escala (centenas de grupos, queries complexas).
- Exemplo: tag `region=US` em tabela, user tag `region=US` → automaticamente só vê US rows.

**Sintaxe (confirmar na doc):**
```sql
-- Create governed tag (namespace:value)
CREATE TAG TYPE region_tag;
ALTER TAG TYPE region_tag CHANGE IF EXISTS ADD POSSIBLE_VALUES ['US', 'EU', 'APAC'];

-- Apply tag a tabela
ALTER TABLE my_catalog.schema.sales
SET TAG region_tag = 'US';

-- Apply tag a user
ALTER USER alice SET TAG region_tag = 'US';

-- Automatic row filter (aplicado ao query): 
-- Alice vê apenas rows onde region_tag = 'US'
```

---

### Anonimização / Pseudonimização — Hashing, Tokenization, Suppression, Generalization

**O que é / quando usar:**
- **Hashing**: SHA256(PII) = one-way, determinístico, join-friendly.
- **Tokenization**: PII → random token, reversível com mapping (ex.: SSN → TKN-1234).
- **Suppression**: remover coluna entirely.
- **Generalization**: coarsen granularidade (age 28 → age_bucket 20-30).

**Heurística:**
- Hashing p/ join, sem reversão.
- Tokenization p/ reversão futura.
- Suppression p/ não precisa dos dados.
- Generalization p/ análise sem expor granularidade.

**Sintaxe:**
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

**UC column masks (declarativo):**
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

### Pipeline Compliant Batch + Streaming com Detecção/Masking de PII

**O que é / quando usar:**
- PII detection = regex/pattern matching ou ML model p/ email, SSN, phone, etc.
- Mask on-the-fly em Lakeflow Declarative Pipeline.

**Sintaxe:**
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

### Data Purging p/ Retenção — GDPR Right-to-Erasure

**O que é / quando usar:**
- Purging = delete PII baseado em retention policy.
- GDPR: direito de deleção ("right to be forgotten").

**Heurística:**
- Implementar soft-delete (flag `is_deleted = true`) p/ auditoria.
- Hard-delete (vacuum) depois de retention period.
- Use Deletion Vectors p/ evitar reescrita completa.

**Sintaxe:**
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

### UC Tags e Comments p/ Discoverability

**O que é / quando usar:**
- Tags = key-value metadata, searchable.
- Comments = human-readable descriptions.
- Essencial p/ cataloging, lineage, compliance tagging.

**Sintaxe:**
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

### Herança de Permissões no UC — Grant Inheritance

**O que é / quando usar:**
- UC permissions herdam cascata: catalog → schema → table/volume.
- Grant em nível superior = automático em objetos filhos (atuais e futuros).

**Heurística:**
- Grant `SELECT` no catálogo = todos analysts legem tudo naquele catálogo (default).
- Mais granular no table/column level.

**Sintaxe:**
```sql
-- Grant SELECT no catálogo (herda para schemas, tables futuras)
GRANT SELECT ON CATALOG my_catalog TO analysts_group;

-- Override em schema (deny a schema específico)
DENY SELECT ON SCHEMA my_catalog.sensitive_schema TO analysts_group;

-- Grant novamente em table específica (re-allow)
GRANT SELECT ON TABLE my_catalog.sensitive_schema.public_data TO analysts_group;
```

**Pegadinha:**
- DENY não herda; se quiser bloquear um schema inteiro, apply no schema level.
- Herança não é retroativa a objetos criados antes do grant (confirmar na doc v8.x).

---

## 8. Debugging and Deploying (10%)

### Diagnóstico: Spark UI, Cluster Logs, System Tables, Query Profile

**O que é / quando usar:**
- Spark UI (http://driver:4040): DAG visualization, executors, shuffle, memory.
- Cluster logs (S3 ou ADLS): STDOUT, STDERR, driver logs.
- System tables: `system.compute.cluster_events`, `system.queries`.
- Query profile: SQL warehouse query history.

**Heurística:**
- Spark UI p/ batch jobs, task-level breakdown.
- Cluster logs p/ startup failures, dependency errors.
- System tables p/ trend analysis, custo.
- Query profile p/ SQL performance.

**Sintaxe:**
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

**O que é / quando usar:**
- Job repair = re-run failed job com mesmo context (debugar intermittent issues).
- Parameter overrides = alterar parâmetros no re-run sem alterar job definition.

**Sintaxe:**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Repair (restart) run
w.jobs.repair_run(run_id=456)

# Submit run com parameter override
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

### Event Logs + Spark UI p/ Debugar Pipelines

**O que é / quando usar:**
- Event logs = timeline detalhado de pipeline execution (Lakeflow Declarative Pipelines).
- Spark UI = low-level task execution.

**Sintaxe:**
```python
# Event logs
w = WorkspaceClient()
events = w.pipelines.get_event_logs(pipeline_id='my_pipeline')
for event in events:
    if event.level == 'ERROR':
        print(f"{event.timestamp}: {event.message}")

# Spark UI via cluster logs
# http://driver:4040/ (durante run) ou arquivo de log post-run
```

---

### Deploy com Declarative Automation Bundles

**O que é / quando usar:**
- Bundles = IaC, Git-based, multi-target deployment.
- `databricks bundle deploy -t <target>` = reproducible, versioned.

**Sintaxe:**
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

### Git Folders (ex-Repos) p/ CI/CD Git-Based

**O que é / quando usar:**
- Git folders = workspace folder linked a Git repo (push/pull sync).
- CI/CD integration: webhook on push → trigger job → run pipeline.

**Sintaxe:**
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

### Layout de Tabelas Delta / Iceberg — Partition-to-Grain, Clustering, Compaction

**O que é / quando usar:**
- Partitioning = divide table by column (date, region); reduz scan area.
- Clustering (Liquid) = data locality per microbatch, dinâmico.
- Grain = atomic level (ex: transaction_date + order_id = grain of fact table).
- Compaction = merge small files, reduz metadata overhead.

**Heurística:**
- Partition se < 1000 unique values e principal filter (date, region).
- Liquid Clustering se alta-cardinality, evolving filters.
- Grain deve ser tão fina quanto analysis needs (not precisa ser atomic se só rodam rollups).

**Sintaxe:**
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

### Dimensional Modeling com Materialized Views + UC Metric Views

**O que é / quando usar:**
- Dimensional modeling = star schema (fact + dims). Materialized views p/ pre-aggregation.
- **UC Metric Views** = governed, reusable metric definitions (NET-NEW).

**Sintaxe:**
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

-- Query metric view (reutilizável, governada)
SELECT * FROM my_catalog.schema.metric_sales_by_region WHERE region = 'US';
```

---

## Glossário de Renomeações

| Término Antigo | Termo Novo | Contexto |
|-----------------|-----------|---------|
| Delta Live Tables (DLT) | Lakeflow Declarative Pipelines | Pipelines ETL declarativas |
| Declarative Automation Bundles (DABs) | Bundles | IaC / deploy infrastructure |
| APPLY CHANGES | AUTO CDC APIs | CDC em pipelines |
| Repos | Git folders | Version-controlled workspaces |
| Workflows | Lakeflow Jobs | Job orchestration |

---

## Heurísticas Rápidas p/ Questões

### Streaming Table vs Materialized View
- **ST**: "Precisa append-only, exactly-once, latência baixa?" → **Streaming Table**
- **MV**: "Precisa histórico completo, updates/deletes tardios, refresh batch?" → **Materialized View**

### Serverless vs Classic Compute
- **Serverless**: < 30min task, stateless, auto-scaling OK → **Serverless**
- **Classic**: long-running, state-heavy, debugging, UDF complexa → **Classic**

### Delta vs Iceberg
- **Delta**: OLAP, data skipping rápido, custo baixo → **Delta (default)**
- **Iceberg**: coluna-oriented, cross-workspace sharing, evolução schema complexa → **Iceberg**

### Qual otimização de custo?
1. **CDF** p/ incremental (não reprocessa histórico).
2. **Liquid Clustering** p/ queries evoluindo (sem OPTIMIZE manual).
3. **Deletion Vectors** p/ deletes frequentes (sem reescrita).
4. **Delta Cache** p/ read-heavy < 10x RAM.

### Qual técnica de segurança?
- **PII em pipeline**: UC column masks + UC ABAC tags.
- **Row-level access**: row filters via UC + governed tags.
- **Compliance purging**: soft-delete + VACUUM after retention period.

### Job falhou, como debugar?
1. Check **Spark UI** DAG (task-level bottleneck).
2. Check **cluster logs** (startup, dependency).
3. Check **system tables** (cluster events, query history).
4. Check **event logs** (pipeline stage breakdown).
5. **Repair run** (re-run com same context).

### Deploy: bundle vs Git folder?
- **Bundle**: IaC, versionado, multi-target overrides → **Bundles**
- **Git folder**: workspace sync com Git, CI/CD webhook → **Git folders** (+ bundles p/ resources)

---

## Recap dedicado: APPLY CHANGES → AUTO CDC + SCD (Section 1)

### Rename (pegadinha de vocabulário)
| Legado (DLT) | Atual (Lakeflow) |
|---|---|
| SQL `APPLY CHANGES INTO` | `AUTO CDC INTO` |
| Python `dlt.apply_changes()` | `dp.create_auto_cdc_flow()` |
| Python `dlt.apply_changes_from_snapshot()` | `dp.create_auto_cdc_from_snapshot_flow()` |

Processa feed de CDC (insert/update/delete) numa **streaming table alvo pré-criada**; resolve ordem/late data; mantém SCD Type 1 ou 2.

### Sintaxe
```sql
CREATE OR REFRESH STREAMING TABLE users;
CREATE FLOW user_flow AS AUTO CDC INTO users
FROM STREAM(user_changes)            -- só table/view, NÃO subquery
KEYS (user_id)
APPLY AS DELETE WHEN operation = 'DELETE'   -- antes do SEQUENCE BY
SEQUENCE BY event_ts
STORED AS SCD TYPE 2;               -- default TYPE 1
```
```python
dp.create_streaming_table(name="users")
dp.create_auto_cdc_flow(
    target="users", source="user_changes", keys=["user_id"],
    sequence_by="event_ts",          # str, col("ts") ou struct("ts","id")
    stored_as_scd_type=2,            # 1 (default) ou 2
    apply_as_deletes=expr("operation = 'DELETE'"),
    ignore_null_updates=True,
    track_history_column_list=["balance","status"],  # SCD2: só estas geram nova versão
)
```
Snapshot (Python only): `dp.create_auto_cdc_from_snapshot_flow(...)` compara snapshots completos consecutivos.

### SCD Type 1 vs Type 2
| | Type 1 (default) | Type 2 |
|---|---|---|
| Histórico | sobrescreve | mantém versões |
| Colunas sistema | — | `__START_AT` / `__END_AT` (tipo do sequence_by) |
| Linha atual | a linha | `WHERE __END_AT IS NULL` |
| `APPLY AS TRUNCATE` | ✅ | ❌ só Type 1 |
| `TRACK HISTORY ON` | n/a | só cols listadas geram nova versão; resto = update in-place |

### Parâmetros-chave
- `KEYS`/`keys` (PK, obrigatório); `SEQUENCE BY`/`sequence_by` (ordenação; use ts alta precisão ou `STRUCT(ts,tiebreaker)`); `APPLY AS DELETE`/`apply_as_deletes` (no Type 2, fecha a versão setando `__END_AT`); `IGNORE NULL UPDATES`/`ignore_null_updates`; `COLUMNS ... EXCEPT`/`column_list`/`except_column_list`.

### Consultar SCD Type 2
- Atual: `WHERE __END_AT IS NULL` (materialize num MV `dim_*_current` se consulta muito).
- Point-in-time (inclusiva/exclusiva): `__START_AT <= D AND (__END_AT > D OR __END_AT IS NULL)`. `>=` no fim = double-count na borda.
- Join fato × dim histórica (revenue-correct): `s.event_date >= p.__START_AT AND (s.event_date < p.__END_AT OR p.__END_AT IS NULL)`.

### Pegadinhas
- `FROM STREAM(...)` aceita só table/view (não subquery) → pré-filtre com temporary view.
- SQL: `APPLY AS DELETE/TRUNCATE` antes de `SEQUENCE BY`.
- Colunas `__START_AT`/`__END_AT` com duplo underscore.
- `create_auto_cdc_flow()` não retorna valor; target já criada com `create_streaming_table()`.

Fonte: skill `databricks-pipelines` (auto-cdc + scd-2-querying), alinhado à doc oficial.

---

**Atualizado p/ exame novo (out/2026). Reconfirme features em docs.databricks.com antes da prova.**
