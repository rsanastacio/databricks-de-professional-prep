# Simulado — Databricks Certified Data Engineer Professional (Exame Novo)

> **Aviso:** Questões ORIGINAIS de prática, não são questões oficiais do exame. Gabaritos baseados na documentação oficial Databricks; sempre confirme na documentação quando tiver dúvidas. **Tempo recomendado: 120 minutos** (2 minutos por questão).

---

## Questões

### 1. [Developing Code — Structured Streaming Stateful]
Você está implementando um agregador de eventos em tempo real com Structured Streaming que conta eventos por user_id em janelas de 10 minutos. O pipeline recebe eventos com até 2 horas de atraso. Qual combinação de configurações garante que late-arriving events sejam capturados sem duplicação ao retomar?

A) Output mode `complete` com `watermark = 2 hours` e checkpoint remoto  
B) Output mode `append` com `watermark = 10 minutes` e state checkpoint  
C) Output mode `update` com `watermark = 2 hours` sem checkpoint  
D) Output mode `append` com `state_retention = 2 hours` e checkpoint remoto

---

### 2. [Data Ingestion & Acquisition — Lakeflow Connect]
Um cliente precisa replicar inserts, updates e deletes de uma tabela PostgreSQL para Databricks em quase-real time, mantendo histórico de mudanças por data. Qual solução requer MENOS código customizado?

A) Apache Kafka + Python Delta Writer  
B) Lakeflow Connect managed connector com CDC automático  
C) DMS (AWS) + ADLS → Lakeflow API  
D) Airflow + JDBC source + Spark batch

---

### 3. [Data Manipulation — VARIANT]
Uma API JSON retorna um campo `metadata` que pode ter estrutura dinâmica (um array com objetos variados ou um string simples). Você precisa extrair o primeiro elemento se for array, ou o valor completo se for string. Qual função permite isso sem erro de tipo?

A) `get_json_object(metadata, '$[0]')` com try-catch  
B) `from_json(metadata, 'array<string>')` após validação de schema  
C) `variant_get(parse_json(metadata), ':0')` ou `variant_get(parse_json(metadata), '')` conforme tipo  
D) `json_extract_array(metadata)[0]` com `coalesce`

---

### 4. [Monitoring and Alerting — Query Performance]
Um SQL Warehouse está com queries lentas (P99 aumentando). Você suspeita de data freshness ou cache ineficaz. Qual métrica do Query History e Warehouse Stats você verifica PRIMEIRO para descartar stale data?

A) `scan_bytes` e `bytes_spilled` do query  
B) `cache_hit_ratio` do warehouse + `total_rows_scanned` por tabela  
C) `query_start_time` vs `table_updated_at` + verificar Delta Cache status  
D) `remote_IO_bytes` e `local_IO_bytes` do Photon profile

---

### 5. [Cost & Performance Optimization — CLUSTER BY AUTO]
Você tem uma fact table (`sales`) com 2B de linhas que é queried por `region` e `date_id` 80% das vezes. O job atual lê 50% da tabela por query. Qual estratégia reduz custo mais significativamente?

A) Particionamento por `region` + `date_id`  
B) `CLUSTER BY region, date_id` (manual, sem reordenação automática)  
C) `CLUSTER BY AUTO region, date_id` com Predictive Optimization  
D) Liquid clustering sem Predictive Optimization

---

### 6. [Data Security and Compliance — ABAC com Governed Tags]
Você precisa aplicar row filters e column masks em 50+ tabelas baseado no departamento do usuário. Fazer isso tabela por tabela é insustentável. Qual abordagem escala melhor?

A) Criar uma view parametrizada por `CURRENT_USER()` para cada tabela  
B) ABAC com tags gerenciadas (governed tags) + row filters/column masks declarativas  
C) Replicar dados por departamento em catálogos separados com GRANT por catálogo  
D) UDFs que filtram conforme `CURRENT_ROLE()` em cada query

---

### 7. [Data Governance — UC Metric Views]
Um analista precisa monitorar SLA de entrega de dados: "quantas tabelas do catálogo foram atualizadas nos últimos 24h?". Qual objeto permite contar isso de forma declarativa, auditar quem acessou, e ser otimizado pelo sistema?

A) Uma SQL view que consulta `information_schema.table_updates`  
B) UC Metric View que agrega contagem e last_modified_time por tabela  
C) Um dashboard Lakeview que query logs de lineage do UC  
D) Uma tabela Delta que grava timestamp de UPDATE em um trigger

---

### 8. [Debugging and Deploying — Git Folders]
Você tem um workspace que sincroniza SQL queries de um repo Git. Você alterou uma query que estava deployada em produção. Qual é o risco e como mitigá-lo?

A) Query vai rodar com versão do repo — use Git tags + branch protection para controlar release  
B) Query pode ficar orphaned se o repo for deletado — versione backups em /Workspace/archive  
C) Mudança sincroniza imediatamente para todos que rodam a query — use Declarative Automation Bundles com review de mudança  
D) Workspace fica out-of-sync se repo mudar — use Git folders read-only e promova via pull request

---

### 9. [Data Modeling — Slowly Changing Dimension]
Uma dimensão de clientes tem histórico (novo email, novo endereço, mudança de status). Qual padrão com Databricks requer MENOS operações de I/O ao fazer update?

A) SCD Type 1 simples (sobrescrever); reter histórico em backup table  
B) SCD Type 2 com `MERGE` + inserir nova linha com flag `is_current`  
C) SCD Type 2 automática com `stored_as_scd_type(1)` ou `(2)` no MERGE  
D) Event log imutável + view que materializa current snapshot em MV

---

### 10. [Developing Code — Python UDF]
Uma Python UDF `process_text` usa biblioteca `spacy` que não é pre-installed no cluster. Qual abordagem permite usar a UDF sem erro ImportError?

A) Instalar `spacy` via `pip install` no notebook antes de definir a UDF  
B) Usar init script no cluster para instalar `spacy` na startup  
C) Registrar a UDF como Spark SQL UDF com `spark.udf.register` e library passada via requirements  
D) Todas as anteriores, mas init script é mais escalável para múltiplos jobs

---

### 11. [Data Ingestion & Acquisition — Iceberg Format Target]
Você está ingerindo dados de um data lake S3 que é queried por múltiplos workspaces (compartilhado via OpenSharing). O schema muda frequentemente. Qual formato alvo garante schema enforcement e versionamento compartilhado?

A) Delta com `schema_evolution = true`  
B) Iceberg com default catalog registrado no workspace  
C) Parquet com schema inferido por Glue/Hive metastore  
D) Iceberg com UC catalog e shared warehouse access

---

### 12. [Data Manipulation — Deletion Vectors]
Uma tabela Delta tem 10B de registros. Você precisa deletar 100M de linhas (1% do total). Sem deletion vectors, qual seria o impacto de uma query após o DELETE?

A) Query lê apenas as linhas não-deletadas (rewrite automático)  
B) Query lê todas as 10B linhas e filtra no Photon layer  
C) Query lê 10B linhas e avalia deletion bitmap para cada linha  
D) Rewrite imediato reescreve todos os arquivos em segundos

---

### 13. [Monitoring and Alerting — Predictive Optimization]
Seu warehouse recebe queries que variam bastante em padrão (sometimes OLTP, sometimes OLAP). Qual métrica de Predictive Optimization indica que é necessário ajustar hint de compaction ou liquid clustering?

A) Query latency degradation > 20% vs baseline  
B) Scan bytes vs bytes skipped ratio abaixo de 5%  
C) Liquid clustering overhead (shuffle bytes) superando benefício de pruning  
D) Previsão de query time degradation baseado em access patterns históricos

---

### 14. [Cost & Performance Optimization — Delta Cache]
Uma tabela dimension (`dim_product`) é consultada em 95% das queries do warehouse. Qual configuração maximiza Delta Cache hit rate e reduz custo?

A) `spark.databricks.io.cache.enabled = true` para todos os clusters  
B) SQL Warehouse com cache enabled + `spark.databricks.io.cache.type = MEMORY` no warehouse settings  
C) Forçar tabela em broadcast com `BROADCAST(dim_product)` em cada query  
D) Replicar dimension em MEMORY-ONLY table e referenciar via alias

---

### 15. [Data Security and Compliance — Data Redaction at Scale]
Você precisa redactar PII (email, CPF) em 200+ tabelas de um catálogo que é acessado por múltiplos grupos. Qual abordagem é mais sustentável?

A) Criar views com PII mascaradas para cada combinação de tabela × grupo  
B) Usar UC column masks declarativas + ABAC para aplicar em bulk  
C) Aplicar schema evolution com UDFs de mascaramento inline  
D) ETL separado que cria tabelas "safe" sem PII para grupos externos

---

### 16. [Debugging and Deploying — Declarative Automation Bundles]
Você está migrando múltiplos jobs SQL de Data Factory para Databricks. Qual abordagem reduz manual toil e permite version control + retry automático?

A) Registrar jobs via Databricks REST API com Python loop  
B) Usar Declarative Automation Bundles (DAB) em YAML com Git sync  
C) Copiar notebook SQL para workspace e triggerar via webhook  
D) Usar Azure Data Factory linked service com Databricks notebook activity

---

### 17. [Debugging and Deploying — Cluster Init Script Error]
Um init script que instala package `xgboost` está falhando silenciosamente. O cluster inicia, mas jobs falham com ImportError. Qual é a causa MAIS provável e fix?

A) `pip` não está no PATH — use `/usr/bin/python3 -m pip install`  
B) Init script retorna erro mas cluster `exit 0` mesmo assim — adicione `set -e` e fail-fast  
C) XGBoost instalado por user, não por root — adicione `sudo` antes de pip  
D) Package não compatível com DBR version — verificar pypi e testar localmente

---

### 18. [Data Modeling — Fact Table Grain]
Uma fact table (`events`) tem grain `[timestamp, event_type, user_id]`, mas recebe late-arriving corrections para eventos de 48h atrás com campo `is_correction = true`. Qual design evita duplicação na agregação?

A) Inserir corrections como novas linhas; MV agrupa `is_correction = false` apenas  
B) MERGE com `WHEN MATCHED AND is_correction = true THEN UPDATE` baseado em composite key  
C) Separate fact tables: `events_live` + `events_corrections` com UNION em view  
D) Adicionar `_dbt_valid_from` + `_dbt_valid_to` e matrixialize versioned snapshot

---

### 19. [Developing Code — AI Functions]
Você precisa classificar centenas de milhões de textos (`description`) entre 5 categorias usando um LLM. Qual abordagem combina performance e custo?

A) `ai_query('gpt-4o', 'Categorize: {description}')` em aplicação Python com batch API  
B) `ai_query()` UDF com `api_key` armazenada em UC secret, aplicado via Spark SQL batched  
C) API chamada via `requests` em Python UDF com cache de embedding  
D) Fine-tuned model local em Databricks Model Registry, inferência via model serving endpoint

---

### 20. [Data Ingestion & Acquisition — AUTO CDC + SCD]
Você tem uma tabela `customer_snapshot` que é sobrescrita diariamente com full extract de um ERP. Qual setup captura automáticamente mudanças (insert, update, delete) sem CDC complexo?

A) MERGE com `WHEN NOT MATCHED BY SOURCE THEN DELETE` + `stored_as_scd_type(1)`  
B) CDF (Change Data Feed) + job que compara snapshots e gera delta  
C) `AUTO` MERGE com SCD logic: `stored_as_scd_type(2)` captura insert/update/delete  
D) Lakeflow Connect auto-CDC com snapshot compare e DELETE tracking

---

### 21. [Data Manipulation — CDF + Streaming]
Uma tabela Delta tem CDF habilitado. Você quer consumir deletes via Structured Streaming `readStream` para manter cache invalidation. Qual opção funciona?

A) `spark.readStream.format('delta').option('withChangeDataFeed', true)`  
B) `spark.readStream.format('delta').table('my_table').withColumn('_change_type')`  
C) `spark.readStream.format('delta').option('startVersion', 0).load()` com CDF enabled no catalog  
D) Opção A, mas requer CDC to be enabled beforehand via `ALTER TABLE ... SET TBLPROPERTIES`

---

### 22. [Monitoring and Alerting — Data Freshness SLA]
Uma tabela que deveria ser atualizada a cada 4 horas não foi atualizada em 6 horas. Qual alerta dispara?

A) Lakeview dashboard com `CASE WHEN age_minutes > 240 THEN 'ALERT'`  
B) UC Metric View que monitora `table_modified_time` + Databricks Alert SQL  
C) Background refresh on Streaming Table com timeout  
D) Ambas A e B; B é mais inteligente

---

### 23. [Cost & Performance Optimization — Liquid Clustering vs Partitioning]
Uma tabela `transactions` tem 100B linhas, querida por `user_id`, `date`, e `amount` em diferentes combinações. Particionamento por `date` deixava muitos date folders vazios. Qual é a melhor alternativa?

A) `CLUSTER BY user_id, date` com recompactação manual semanal  
B) Liquid clustering: `CLUSTER BY user_id, date, amount`; Predictive Optimization reordena automaticamente  
C) Particionamento dinâmico com `PARTITION BY (user_id, date)`  
D) Hash bucketing: `BUCKETED BY (user_id) INTO 1024 BUCKETS`

---

### 24. [Data Security and Compliance — GDPR / Right to Erasure]
Uma query `DELETE FROM customers WHERE customer_id = ?` é executada para atender solicitação de erasure. Qual Databricks feature garante que cópias antigas (backups, versioning) também respeitem a deleção?

A) Deletion vectors apenas marcam como deleted em versão atual  
B) UC recycle bin (28 dias) retém dados deletados; usar `PURGE` + permanente delete com data retention policy  
C) Configurar `delta.deletedFileRetentionDuration = 0` e executar `VACUUM` imediatamente  
D) Todas as anteriores em combinação: PURGE + VACUUM(0) + disable recycle bin

---

### 25. [Developing Code — foreachBatch + State Management]
Você está usando `foreachBatch` em Structured Streaming para escrever em Delta. Você precisa garantir que microbatch reiniciado (após falha) não cause duplicação na tabela. Como?

A) Usar `mergeSchema = true` e confiar em transactions auto  
B) Idempotent sink com `MERGE` baseado em composite key + checkpoint recovery  
C) Disabilitar retry no batch sink  
D) Registrar batch ID antes de escrever, verificar depois

---

### 26. [Data Ingestion & Acquisition — Streaming Table Maintenance]
Um Streaming Table (ST) em Unity Catalog que ingere de Kafka tem checkpoint que cresceu para 100 GB em 30 dias. Qual é causa e solução?

A) State accumulation sem watermark; adicionar watermark + state retention time  
B) Checkpoint não é limpo automaticamente; rodar `ALTER TABLE ... RESET CHECKPOINT`  
C) ST recompilation overhead; redesenhar query  
D) Iceberg metadata acumula; rodar `VACUUM` regularmente

---

### 27. [Data Manipulation — MERGE vs INSERT OVERWRITE Performance]
Você precisa fazer SCD Type 1 (sobrescrever customer.email) em 500M linhas usando 100K registros de updates. Qual é mais eficiente?

A) `INSERT OVERWRITE` a tabela inteira após join com updates  
B) `MERGE INTO customer WHEN MATCHED THEN UPDATE SET email` limitado a 100K linhas afetadas  
C) DELETE updates, depois INSERT novos; VACUUM  
D) Batch insert com window function e row_number reparticionamento

---

### 28. [Monitoring and Alerting — SQL Warehouse Autoscaling]
Um SQL Warehouse tem autoscaling de 2–10 clusters. Queries levam 2 minutos mas durante off-peak ficam lentas (30 segundos, 2 clusters). Como diagnosticar se é queueing vs compute?

A) Verificar `query_queue_time` vs `query_duration` nas warehouse stats  
B) Olhar `execution_status = QUEUED` vs `RUNNING` no query history  
C) Ambas, mas especialmente o Queue time de 5+ minutos indica falta de clusters  
D) Revisar autoscaling policy (`spark.databricks.cluster.profile = singleNode` está errado)

---

### 29. [Cost & Performance Optimization — Serverless SQL Warehouse]
Você tem uma dashboard que roda 20 queries/minuto durante business hours. Qual opção reduz custo para "pay-per-query"?

A) Serverless SQL Warehouse com `spot_instances = true`  
B) Classic cluster com autoscaling desabilitado e 1 single-node  
C) Serverless SQL Warehouse (managed compute, sem gerenciar cluster)  
D) Photon-enabled classic warehouse com `spark.databricks.photon.ml.enabled = true`

---

### 30. [Debugging and Deploying — Spark SQL Query Plan]
Um `EXPLAIN PLAN` mostra `BroadcastHashJoin` em uma dimensão de 5B linhas. Qual é o problema e fix?

A) Join order está errado; usar `/*+ BROADCAST(dim) */` hint  
B) Broadcast falha em tabelas > 1 GB; usar `SortMergeJoin` em vez disso  
C) Broadcast precisa caber em executor memory; aumentar `spark.sql.autoBroadcastJoinThreshold`  
D) Ambas B e C; desabilitar broadcast automático e usar SortMergeJoin + skew hints

---

### 31. [Data Security and Compliance — Clean Rooms]
Você quer compartilhar dados com parceiro externo, mas sem expor identidades individuais — apenas agregações. Qual solução Databricks permite isso?

A) Exportar CSV para partner  
B) UC schema compartilhado com row filters aplicando agregação  
C) Databricks Clean Rooms: SQL analytics compartilhado sem dados raw expostos  
D) Delta share com external recipient (OpenSharing)

---

### 32. [Developing Code — Window Function + Partitioning]
Uma query calcula `ROW_NUMBER()` OVER `(PARTITION BY user_id ORDER BY timestamp)`. Qual risco existe se partições forem muito desbalanceadas?

A) Window function não funciona com partições  
B) Data skew causa straggler task; uma partição com 1B rows paralisa job  
C) Resultado está correto, mas latency aumenta  
D) Ambas B e C; considerar reparticionamento ou approx_percentile para estimativa

---

### 33. [Developing Code — Schema Drift Detection]
Uma ingestão de Kafka com Structured Streaming detecta novo campo `new_field` em evento JSON. Default behavior: coluna é adicionada como null. Você quer falhar ao invés. Qual opção?

A) `failOnNewColumnViolation = true`  
B) `mergeSchema = false` (default)  
C) Validar schema com JSON schema validator antes de ingerir  
D) A e C; B está errado

---

### 34. [Data Manipulation — VARIANT Type Performance]
Você tem coluna VARIANT com 1B de registros. Query `WHERE variant_get(col, ':field') = 'value'` está lento. Qual é likely cause?

A) Variant indexing não é suportado; criar coluna extracted normal  
B) Full table scan; Variant predicate push-down não funciona eficientemente  
C) Spark não otimiza variant_get() ; reescrever com `get_json_object(to_json(col), '$field')`  
D) Todas as anteriores; melhor extrair campo como coluna durante ingestão

---

### 35. [Data Ingestion & Acquisition — Iceberg Table Evolution]
Uma tabela Iceberg foi criada com schema `{id: int, name: string}`. Você precisa adicionar `created_date: timestamp`. Como garantir que dados antigos não quebrem?

A) `ALTER TABLE ... ADD COLUMN created_date TIMESTAMP DEFAULT NULL`  
B) `ALTER TABLE ... ADD COLUMNS (created_date TIMESTAMP NOT NULL)`  
C) Opção A; B falharia em reads de dados antigos (schema mismatch)  
D) Ambas funcionam em Iceberg (schema evolution é suportada)

---

### 36. [Developing Code — Watermark em Multi-Stream Join]
Você está fazendo join de dois streams (events e actions) em Structured Streaming. Como garantir que late-arriving events não causem incorrect join?

A) Adicionar watermark em ambos streams com mesmo delay (ex: 1 hour)  
B) State timeout + recheckpoint; não usar watermark em join  
C) Output mode `append` força join correto  
D) Join não é possível em streams; usar micro-batch MERGE em vez

---

### 37. [Developing Code — Streaming Performance Degradation]
Um Streaming Table que ingeria 100K events/segundo começou a processar apenas 50K/segundo após 3 dias. Qual é provável gargalo?

A) Kafka consumer lag aumentando — verificar topic partition rebalancing  
B) State size crescendo; watermark ineficaz causando state explosion  
C) Checkpoint overhead; checkpointing cresce com estado não-gerenciado  
D) Ambas B e C; monitorar state bytes no Spark UI

---

### 38. [Cost & Performance Optimization — Compaction Strategy]
Uma tabela recebe 100K inserts/dia via streaming. Sem manual compaction, small files acumulam. Qual abordagem balanceia performance vs cost?

A) `OPTIMIZE TABLE` diária + `VACUUM`  
B) Liquid clustering + autom compaction via Predictive Optimization  
C) Iceberg auto-compaction com target file size  
D) Ambas A e B; Predictive Optimization é mais eficiente

---

### 39. [Data Security and Compliance — UC Secret Management]
Um job precisa de senha para acessar external database. Onde armazenar e recuperar?

A) Hardcode na notebook (NUNCA)  
B) UC Secrets via `dbutils.secrets.get(scope='db_scope', key='db_password')`  
C) Armazenar em `spark.conf` via cluster environment var  
D) Ambas B e C funcionam; B é mais seguro (audit trail)

---

### 40. [Debugging and Deploying — Notebook Parameter Injection]
Um notebook parametrizado recebe `{{date}}` que deveria ser 2024-01-15. Job executa com valor literal `{{date}}`. Qual é o problema?

A) Databricks não interpola literals em jobs; usar `dbutils.widgets` em vez  
B) Job config passando valor errado; verificar `job_parameters` no job definition  
C) Notebook criado via Git Folder; Git Folder não interpola parameters  
D) Ambas A e B; prefira `dbutils.widgets.get()` com default

---

### 41. [Data Governance — Permission Inheritance in UC]
Você grantou `USAGE` em um schema. Faz diferença se você depois grantar `SELECT` em table específica? (inheritance perspective)

A) Ambas precisam de grant; schema USAGE é prerequisite para table access  
B) Table grant herda automáticamente de schema grant; redundante  
C) Schema grant é necessário mas não suficiente; table grant é obrigatório também  
D) Table grant ignora schema grant; apenas table level importa

---

### 42. [Data Manipulation — Time Travel + Rollback]
Você executou acidentalmente `DELETE FROM sales` sem WHERE. Última backup é de 2 horas atrás. Qual Databricks feature permite rollback?

A) `SELECT * FROM sales@0` (version 0)  
B) `RESTORE TABLE sales TO VERSION x` onde x = versão 2 horas atrás  
C) Delta time travel: `SELECT * FROM sales TIMESTAMP AS OF '...'` mas como restore?  
D) Rodar `RESTORE` + recycle bin (28 dias), mas talvez seja tarde se VACUUM foi rodado

---

### 43. [Cost & Performance Optimization — Table Statistics and Query Planning]
Um EXPLAIN mostra `HashAgg` em lugar de `SortAgg`, causando spill. Qual estatística Databricks usa para escolher?

A) `spark.sql.statistics.histogramEnabled = true` com histogram accuracy  
B) `table_stats` (rowCount, totalSize) armazenado em metastore  
C) Ambas; `ANALYZE TABLE ... COMPUTE STATISTICS` popula rowCount/totalSize  
D) Sem stats explícito, Catalyst usa fallback heuristics (pode ser subóptimo)

---

### 44. [Data Manipulation — Aggregate Functions + Null Handling]
Query: `SELECT SUM(amount) FROM sales WHERE amount IS NOT NULL`. Versão alternativa: `SELECT SUM(COALESCE(amount, 0)) FROM sales`. Qual é correta?

A) Primeira está correta; segunda soma 0s como fake values  
B) Segunda está correta; não muda resultado se não há nulls  
C) Ambas corretas se schema garante NO NULL; primeira é mais legível  
D) Depende de negócio: primeira ignora nulls, segunda trata como 0

---

### 45. [Monitoring and Alerting — Job Failure Alerting]
Um job SQL que executa MERGE em tabela crítica falha silenciosamente. Você só descobre quando BI relata falta de dados. Como alertar pro-ativamente?

A) Job notification settings + Slack webhook  
B) Databricks Alerts feature (SQL workspace ou job completion check)  
C) Query no `system.jobs.runs` que monitora `state = FAILED` e envia alerta  
D) Todas; B é mais robusta (managed)

---

### 46. [Cost & Performance Optimization — Column Mask Performance Impact]
Dados sensíveis (SSN) precisam ser mascarados. Aplicar column masks em 500+ queries impacta performance? Como otimizar?

A) Usar `AES_ENCRYPT()` UDF no ingress (pré-computado, sem overhead)  
B) UC column masks com statistics + pruning; negligible overhead se predicate push-down funciona  
C) Materialized view com dados pre-masked para queries críticas + column masks para o resto  
D) Ambas B e C; B para performance, C para query acceleration

---

### 47. [Developing Code — Job Cluster vs Attached Cluster]
Um job está anexado a um cluster all-purpose que também roda notebooks interativas. Qual risco existe?

A) Job pode ser cancelado se usuário interromper notebook  
B) Job e notebook competem por recursos  
C) Job pode afetar Spark context do notebook (UI limpa, cache)  
D) Todas as anteriores; usar job cluster (isolated, dedicated) para reliability

---

### 48. [Developing Code — PySpark vs Pandas Performance]
Uma operação `df.groupBy('category').agg(sum('amount')).collect()` em 100M linhas. PySpark mostra 30 segundos; reescrever em Pandas UDF: 5 segundos. Por quê?

A) PySpark tem overhead de serialização Python; Pandas UDF usa Arrow + vectorized execution  
B) Pandas UDF é otimizado pelo Catalyst planner  
C) PySpark não paralela; Pandas UDF sim  
D) Ambas A e B; prefira Pandas UDF para operações Python-heavy

---

### 49. [Developing Code — SQL Dynamic Query Generation]
Um relatório precisa agrupar dinamicamente por coluna escolhida pelo usuário (ex: `group_by = 'region'`). Qual abordagem é segura vs SQL injection?

A) Concatenar string: `SELECT {group_by}, SUM(amount) FROM sales GROUP BY {group_by}`  
B) Usar parameterized query: `CONCAT('GROUP BY ', group_by)` em prepared statement  
C) SQL validation + allowlist de colunas; ou usar framework que valida  
D) Ambas B e C; C é mais robusta; A é perigosa (SQL injection risk)

---

### 50. [Data Ingestion & Acquisition — Streaming Ingestion Performance]
Um stream de 1M eventos/segundo é ingerido em Delta via Structured Streaming. Latency está aumentando. Qual é a causa MAIS provável?

A) Spark executor memory overflow  
B) State accumulation sem watermark; checkpointing cresce indefinidamente  
C) Partition count baixo; dados acumulam em partições  
D) Delta schema evolution a cada micro-batch

---

### 51. [Cost & Performance Optimization — Index-free Pruning via Iceberg]
Uma tabela de eventos com 500B registros é queried por `event_timestamp` (range filters frequentes). Qual abordagem em Iceberg otimiza pruning sem criar índices explícitos?

A) Particionamento por event_timestamp com `PARTITION BY MONTH(event_timestamp)`  
B) Iceberg manifest prune + statistics (min/max) por arquivo Parquet  
C) Liquid clustering `CLUSTER BY event_timestamp` com Predictive Optimization  
D) Ambas A e B; B é mais eficiente para range queries em high-cardinality

---

### 52. [Developing Code — Encryption Key Management in Spark]
Qual é o fluxo correto para usar chaves de criptografia em jobs Spark dentro Databricks?

A) Hardcoded em `spark.conf`; rotacionadas manualmente  
B) UC Secrets recuperado via `dbutils.secrets.get()` dentro PySpark job  
C) Passar via environment var do cluster (mais inseguro)  
D) Ambas A e B funcionam, mas B é seguro com audit trail

---

### 53. [Developing Code — Type Casting Errors]
Um cast explícito `CAST(date_string AS DATE)` falha com valor inválido em 1 linha de 1B. Job inteiro falha. Como contornar?

A) `TRY_CAST(date_string AS DATE)` retorna NULL se inválido, continua job  
B) `CAST(date_string AS DATE FORMAT 'yyyy-MM-dd')` com format hint  
C) `IF(REGEXP_LIKE(...), CAST(...), NULL)` com pré-validação  
D) Ambas A e C funcionam; A é mais legível

---

### 54. [Data Ingestion & Acquisition — Multiformat Ingestion with Lakeflow]
Você ingere Parquet, JSON, e CSV de S3 em uma tabela Delta. Schema evolui em cada formato. Qual ferramenta simplifica isso?

A) Spark auto schema inference + `mergeSchema = true`  
B) Lakeflow Connect com auto-schema detection por source format  
C) Databricks Unity Catalog auto-schema propagation  
D) Ambas A e B; B é mais robusto para multi-format

---

### 55. [Monitoring and Alerting — Predictive Optimization Metrics]
Qual métrica no Spark UI + Predictive Optimization UI indica que reordenação automática via CLUSTER BY AUTO é necessária?

A) `shuffle_write_bytes` > 50% dos dados  
B) `scan_bytes` vs `bytes_skipped` ratio indicando poor pruning  
C) Optimizer recommendation score < 80%  
D) Todas as anteriores; B é mais direta

---

### 56. [Debugging and Deploying — Incremental Pipeline Issues]
Um job que roda `SELECT * FROM src WHERE updated_at > '{{yesterday}}'` falha consistentemente. Você descobre que um cliente atualizou 30 dias de histórico ontem. Qual foi o impacto?

A) Job só pegou 1 dia de dados; faltam 29 dias  
B) Job pegou todos os 30 dias (correto para late-arriving updates)  
C) Job não retomou; precisa reprocessar tudo  
D) Ambas A e B; incrementalidade não é automática; use MERGE incremental + watermark

---

### 57. [Data Modeling — Conformed Dimensions]
Múltiplas fact tables referenciam `customer` de forma inconsistente (customer_id vs customer_pk, diferentes atributos). Como consolidar sem quebrar histórico?

A) Criar dimensão conformed `dim_customer` nova; fact tables old e new apontam para ambas  
B) `ALTER TABLE fact_old DROP COLUMN customer_pk; ADD COLUMN customer_id` com MERGE histórico  
C) Recriar fact tables com nova dimensão; versionar old como archive  
D) Ambas A e C; gradualmente migrar via incremental pipeline

---

### 58. [Developing Code — Broadcast Join Optimization]
Uma query faz join entre `sales` (100B rows) e `dim_product` (10K rows). Spark escolhe BroadcastNestedLoopJoin (lento). Como forçar BroadcastHashJoin?

A) `/*+ BROADCAST(dim_product) */` hint  
B) Aumentar `spark.sql.autoBroadcastJoinThreshold` para > 10K MB  
C) Reordenar join: `SELECT * FROM dim_product JOIN sales`  
D) Ambas A e B; A é mais direto

---

### 59. [Cost & Performance Optimization — Predictive Optimization Fine-Tuning]
Uma tabela com Predictive Optimization habilitado tem reordenação automática roda a cada 24h, causando 2h de compute. É excessivo. Qual opção reduz frequência?

A) Desabilitar Predictive Optimization completamente  
B) Ajustar `OPTIMIZE` frequency via `CREATE TABLE ... TBLPROPERTIES (...)`  
C) Configurar `spark.databricks.predictiveOptimization.maxFrequency = weekly`  
D) Ambas B e C; C é mais eficaz

---

### 60. [Data Governance — UC Asset Inventory]
Um auditoria interna precisa listar todas as tables no catálogo, proprietários, última modificação, e data classification (public/private/sensitive). Qual ferramenta fornece isso natively?

A) Unity Catalog system tables (`system.information_schema.tables`) com tags de classificação  
B) Databricks metadata API + UC tags  
C) Glean search de sistema  
D) Ambas A e B; A é system table query, B é API call com metadados

---

---

## Gabarito e Explicações

**1. Resposta: A**  
Watermark de 2 horas permite late-arriving events até 2h. Checkpoint remoto garante retomada sem duplicação (output mode `complete` é correto para agregações). Mode `append` com 10 minutos watermark perderia eventos fora da janela; `update` sem checkpoint perde estado em crash.

**2. Resposta: B**  
Lakeflow Connect managed connector para PostgreSQL CDC é oficialmente suportado e reduz código vs Kafka + custom writer. DMS e Airflow requerem mais configuração; ambas são válidas mas B é "menos código".

**3. Resposta: C**  
`variant_get(parse_json(...), ':0')` e variantes tratam VARIANT sem type error pré-existente. `from_json` forçaria schema fixo (throw error em mismatch). `json_extract_array` falha se não for array.

**4. Resposta: C**  
`query_start_time` vs `table_updated_at` revela se dados estão stale. Delta Cache status mostra hit rate. Ambas métricas são críticas; C é mais prático para descartar stale data issue.

**5. Resposta: C**  
`CLUSTER BY AUTO` com Predictive Optimization reordena automaticamente baseado em access patterns, reduzindo custo mais que particionamento fixo (que não escala para query patterns dinâmicos).

**6. Resposta: B**  
ABAC com governed tags permite aplicar row filters e column masks declarativamente em bulk. Views parametrizadas não escalam bem (50+ tabelas × múltiplos grupos = combinatória).

**7. Resposta: B**  
UC Metric Views são objetos SQL gerenciados que agregam metadados e podem auditar acesso. Information schema é read-only; dashboard é passivo; triggers Delta não são built-in.

**8. Resposta: D**  
Git Folders sincronizam workspace com repo. Mudança no repo sincroniza para workspace. Risk: mudanças acidentais propagam. Mitigação: Git Folder read-only + promote via PR + branch protection.

**9. Resposta: C**  
`stored_as_scd_type(2)` em MERGE automatiza SCD Type 2, reduzindo I/O comparado a operações manuais + compaction.

**10. Resposta: D**  
Todas funcionam; init script é mais escalável para múltiplos jobs (uma vez, reutiliza).

**11. Resposta: D**  
Iceberg com UC catalog garante schema enforcement, versionamento compartilhado, e integração com OpenSharing. Delta + OpenSharing funciona, mas Iceberg é mais robusto para multi-workspace.

**12. Resposta: C**  
Sem deletion vectors, Spark mantém bitmap de deletados e avalia cada linha (quer dizer, reading 10B linhas com filter bitmap). Com DV, skips deleted files fisicamente.

**13. Resposta: D**  
Predictive Optimization prevê degradação de query latency baseado em histórico, indicando quando reordenação/compaction é necessária.

**14. Resposta: B**  
SQL Warehouse com cache enabled + `MEMORY` type maximiza hit rate. Broadcast é para joins específicos, não para dimension caching geral.

**15. Resposta: B**  
UC column masks declarativas + ABAC escalam melhor que views manuais. Views = N tabelas × M grupos.

**16. Resposta: B**  
Declarative Automation Bundles (DAB) em YAML + Git sync é o padrão moderno para version control + deployment automático.

**17. Resposta: B**  
Init script precisa ter `set -e` para falhar-fast. Sem isso, cluster inicia mesmo com erro.

**18. Resposta: B**  
MERGE com composite key e `is_correction` flag evita duplicação. Insert como nova linha não é agregável facilmente.

**19. Resposta: B**  
`ai_query()` UDF com secret + batch processing escala. GPT-4 direct seria mais caro; local model é alternativa mas LLM é mais flexible.

**20. Resposta: C**  
`AUTO` MERGE com `stored_as_scd_type` automatiza CDC sem external tools.

**21. Resposta: D**  
CDF deve estar habilitado via `ALTER TABLE ... SET TBLPROPERTIES`. Depois, `readStream` com CDF option funciona.

**22. Resposta: B**  
UC Metric Views + Alert SQL é forma native Databricks. Dashboard é passivo; B é mais inteligente.

**23. Resposta: B**  
Liquid clustering + Predictive Optimization escala melhor que particionamento fixo para queries variadas.

**24. Resposta: D**  
Todas são combinadas: PURGE (table), VACUUM(0 days), disable recycle bin para erasure compliance.

**25. Resposta: B**  
Idempotent sink com MERGE baseado em composite key + checkpoint recovery previne duplicação em retry.

**26. Resposta: A**  
Watermark + state retention time controla checkpoint growth.

**27. Resposta: B**  
MERGE limitado a 100K atualizações é mais eficiente que reescrever 500M linhas.

**28. Resposta: C**  
Ambas métricas são necessárias; queue time > 5 minutos indica falta de clusters.

**29. Resposta: C**  
Serverless SQL Warehouse é "pay-per-query" com compute managed automaticamente. Spot instances não existem em serverless.

**30. Resposta: D**  
Broadcast em tabelas > 1 GB falha. SortMergeJoin + skew hints é alternativa.

**31. Resposta: C**  
Clean Rooms permite SQL analytics compartilhado sem exposição de dados raw.

**32. Resposta: D**  
Data skew causa straggler + latency aumenta (task com 1B rows paralisa job).

**33. Resposta: D**  
A e C funcionam; A com `failOnNewColumnViolation` ou C com validação pre-ingestion.

**34. Resposta: D**  
Variant predicate push-down é limitado; melhor extrair campo em ingestão como coluna normal.

**35. Resposta: C**  
`ADD COLUMN ... DEFAULT NULL` permite schema evolution sem quebrar old data reads.

**36. Resposta: A**  
Watermark em ambos streams com mesmo delay garante join correto.

**37. Resposta: D**  
Ambas B e C causam degradação; monitorar state bytes no Spark UI via Streaming UI.

**38. Resposta: D**  
Ambas funcionam; Predictive Optimization é mais automatizada.

**39. Resposta: B**  
UC Secrets com audit trail é mais seguro que env vars.

**40. Resposta: B**  
Job config precisa passar parameter corretamente. Notebook parametrizado usa `dbutils.widgets.get()` para receber valor.

**41. Resposta: C**  
Schema USAGE é pré-requisito; table grant é obrigatório também (não herda automaticamente).

**42. Resposta: B**  
`RESTORE TABLE ... TO VERSION x` ou `SELECT ... TIMESTAMP AS OF` para time-travel. RESTORE escreve versão nova; time-travel é read-only.

**43. Resposta: C**  
`ANALYZE TABLE` popula stats; Catalyst usa stats para escolher agregação strategy.

**44. Resposta: D**  
Primeira ignora nulls; segunda trata como 0. Negócio decide; primeira é mais típica.

**45. Resposta: D**  
Todas; B (Alerts) é mais robusta.

**46. Resposta: D**  
Column masks têm overhead negligível com predicate push-down; MVs materialized fornecem query acceleration quando crítico.

**47. Resposta: D**  
Todas as anteriores; job cluster é isolado e dedicado.

**48. Resposta: D**  
Arrow serialization + vectorized execution torna Pandas UDF 6x mais rápida em operações Python.

**49. Resposta: D**  
Ambas; C (allowlist) é mais robusto que concatenação direta (SQL injection risk).

**50. Resposta: B**  
Sem watermark, estado cresce indefinidamente; checkpoints crescem com estado.

**51. Resposta: D**  
Ambas A e B; B (Iceberg statistics + manifest prune) é mais eficiente que particionamento fixo para range queries.

**52. Resposta: D**  
UC Secrets em job Spark é a forma segura com audit trail; environment vars são menos seguras.

**53. Resposta: A**  
`TRY_CAST` continua job; A é mais legível.

**54. Resposta: D**  
Ambas; B (Lakeflow) é mais robusto para multi-format.

**55. Resposta: B**  
Scan vs skipped ratio < 5% indica poor pruning; Predictive Opt recomenda reordenação.

**56. Resposta: D**  
Incrementalidade requer MERGE com late-arriving support. Simple incremental com timestamp é vulnerable.

**57. Resposta: D**  
Ambas; A permite gradual migration sem perder histórico.

**58. Resposta: A**  
`/*+ BROADCAST(dim_product) */` hint força BroadcastHashJoin; B aumenta threshold automaticamente.

**59. Resposta: C**  
Configurar `spark.databricks.predictiveOptimization.maxFrequency` reduz recompactação excessiva.

**60. Resposta: A**  
UC system tables (`information_schema.tables`) + tags de classificação fornece inventory nativo.
