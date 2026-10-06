# Simulado 2 — Databricks Certified Data Engineer Professional (Exame Novo)

> **Questões ORIGINAIS de prática, não são questões oficiais. Gabaritos baseados na doc Databricks; confirme sempre na documentação oficial. Cronometre 120 minutos. Nível: intermediário-avançado.**

---

## Questões

**1. [Developing Code — Python/SQL]**  
Um pipeline de Structured Streaming recebe dados de Kafka em batches de 30 segundos. O processamento realiza uma agregação com janelas de 5 minutos sobre `event_timestamp`. Qual configuração garante que eventos atrasados (chegando até 10 minutos após a janela fechar) sejam reprocessados na janela correta?

A) Aumentar `spark.sql.streaming.forceDeleteTempCheckpointLocation` e definir `outputMode="complete"`  
B) Configurar `watermark("event_timestamp", "10 minutes")` antes da agregação e usar `outputMode="append"` com CDF habilitado  
C) Definir `chkpointLocation` com modo `update` e `microBatchMs=10000`  
D) Usar `.option("mergeSchema", "true")` e replicar a query em 10 executores  

---

**2. [Data Ingestion & Acquisition]**  
Seu cliente tem um banco de dados PostgreSQL com ~10GB de dados. Você quer sincronizar alterações (inserts/updates/deletes) em tempo quase-real via Lakeflow Connect. Qual é o principal pré-requisito de banco de dados (além de credenciais)?

A) Replicação física habilitada e todas as tabelas com `replica identity full`  
B) Logical Decoding ativado, WAL level configurado como `logical`, e uma slot de replicação permanente  
C) Trigger de auditoria em cada tabela + log de transações externo  
D) Foreign Data Wrapper (FDW) + extensão `postgres_fdw`  

---

**3. [Data Manipulation — SQL]**  
Você tem uma tabela Delta com coluna `data: VARIANT` contendo JSON com estrutura variável. Precisa extrair o campo `user.email` presente em ~80% dos registros; nos outros 20%, o campo pode não existir. Qual é a forma mais eficiente e segura?

A) `SELECT get_json_object(data, '$.user.email') AS email FROM tabela` com tratamento de NULL  
B) `SELECT data['user']['email'] AS email FROM tabela` seguido por `WHERE email IS NOT NULL`  
C) `SELECT variant_get(data, 'user.email', 'string') AS email FROM tabela`  
D) Converter VARIANT para string via `to_json()`, depois usar regex  

---

**4. [Monitoring and Alerting]**  
Seu pipeline Structured Streaming processa 1M eventos/segundo. O checkpoint indicava 50ms de latência end-to-end, mas de repente subiu para 2 segundos. Onde você olharia PRIMEIRO para diagnosticar?

A) Logs de driver do Spark no cluster  
B) Métricas de throughput e batch duration no Spark UI (streaming tab) + verificar taxa de eventos recebidos  
C) Aumentar `spark.sql.shuffle.partitions` e `spark.streaming.backpressure.enabled`  
D) Verificar disk I/O do checkpoint storage e estado do broker de Kafka  

---

**5. [Cost & Performance Optimization]**  
Uma tabela Delta de 500GB recebe escritas pequenas frequentes (10–50 linhas por escrita). Leituras estão lentas. Qual combinação melhor reduz custo e melhora performance?

A) Habilitar deletion vectors, CLUSTER BY auto, e Delta cache  
B) Apenas compactar com `OPTIMIZE` diariamente + aumentar worker nodes  
C) Desabilitar multiversion concurrency control (MVCC) e usar particionamento por data  
D) Converter para Parquet nativo (desabilitar Delta) e usar S3 Select  

---

**6. [Data Security and Compliance]**  
Seu cliente quer uma política de mascaramento central que aplique-se automaticamente a qualquer coluna marcada com tag governada `pii` — inclusive tabelas criadas no futuro por diferentes usuários. Qual é a solução nativa do Databricks?

A) Criar uma stored procedure de mascaramento e associá-la via `ALTER TABLE ... SET CLUSTER BY`  
B) Políticas ABAC (Attribute-Based Access Control) em Unified Catalog com governed tags  
C) Row and column security com views mascaradas + job de auditoria nightly  
D) Criar um modelo de mascaramento em AI functions e aplicar como `CHECK` constraint  

---

**7. [Data Modeling]**  
Uma organização tem 150 tabelas em UC com diferentes níveis de qualidade de dados. Quer criar uma métrica central "data_quality_score" que apareça em governança e seja reutilizável por múltiplos dashboards. Qual abordagem é recomendada?

A) UC Metric View (definição centralizada) + Materialized View (pré-agregação)  
B) Delta Live Tables com `@quality_expectation` + Dashboard que lê a tabela de expectativas  
C) Computar a métrica num job Python nightly e escrever numa tabela, depois usar `CREATE VIEW`  
D) Usar `GET_METRIC` via SQL Warehouse em cada dashboard  

---

**8. [Developing Code — Python/SQL]**  
Um desenvolvimento iterativo usa um notebook com query SQL que filtra dados por data usando `WHERE date > CURRENT_DATE() - 7`. Após converter para um job agendado diariamente, percebeu que a janela móvel de 7 dias não funciona corretamente em alguns runs. Qual é a melhor prática?

A) Usar `WHERE date > cast(current_timestamp() as date) - interval 7 days` e adicionar retry logic  
B) Passar a data como parâmetro da task via `spark.conf` + usar formato ISO 8601  
C) Usar Databricks Workflows com parametrização (`{{task.run_id}}`) e executar com `dbutils.notebook.run()`  
D) Armazenar o checkpoint da última run em UC + ler a data do checkpoint na próxima run  

---

**9. [Data Ingestion & Acquisition]**  
Você recebe dados de uma API REST que retorna JSON pagado. O endpoint suporta range queries por timestamp. Qual estratégia de ingestion é mais robusta para ingeri ~100M registros com histórico de 3 anos?

A) Chamar a API de 1 hora em 1 hora (paralelo) usando `parallel_requests`, salvar bruto em Delta, depois processar  
B) Ingeri tudo em 1 call (1 JSON gigante), armazenar em `BLOB` coluna, depois fazer parsing  
C) Usar Lakeflow Connect configurado para CDC (se suportado) ou Apache NiFi externo  
D) Ingeri por dia (paralelo) em tasks de Workflows, salvar com `mergeSchema=true`  

---

**10. [Data Manipulation]**  
Você tem duas tabelas: `orders` (1M de linhas, atualizada diariamente) e `shipments` (500K linhas, atualizada em tempo real via CDF). Quer manter um fato `fact_order_shipment` sincronizado que combina ambas. Qual estratégia é mais eficiente?

A) Mergear incrementalmente via CDF, usando `MERGE` com subqueries de CDC  
B) Rodar `DELETE FROM fact_order_shipment` + `INSERT` full join (daily batch)  
C) Criar uma Materialized View `AS SELECT ... FROM orders FULL OUTER JOIN shipments` e usar `REFRESH`  
D) Usar Structured Streaming para consumir CDF de `shipments` + join com snapshot de `orders`  

---

**11. [Cost & Performance Optimization]**  
Um cluster SQL Warehouse tem 100 workers, custa $500/hora parado. Análise de query logs mostra que 30% das queries são ad-hoc (< 1 min cada) e 70% são dashboards com padrão de acesso previsível. Qual otimização reduz custo mantendo SLA?

A) Migrar ad-hoc queries para serverless compute + manter dashboards no warehouse dedicado  
B) Usar Delta cache para todas as queries + reduzir worker count para 50  
C) Particionaretodas as tabelas por data + habilitar `CLUSTER BY` automático  
D) Converter queries lentas para Materialized Views + desativar cache  

---

**12. [Developing Code — Python]**  
Em um job PySpark, você quer processar VARIANT colunas que contêm arrays aninhados. Qual abordagem garante melhor performance em transformações complexas?

A) `df.selectExpr("explode_outer(variant_col) as item")` depois iterar em Python RDD  
B) Usar `sql("SELECT ... FROM delta.`path` WHERE ...")` + SQL nativo para manipular VARIANT  
C) Converter VARIANT para string JSON em Python, parsear com `json.loads()`, depois remontar  
D) Usar `pyspark.sql.functions.col()` com `.getItem()` chaining em SQL expressions  

---

**13. [Monitoring and Alerting]**  
Um dashboard crítico é alimentado por uma Materialized View. De repente, os dados ficam "stale" (defasados há horas). Qual é o meio mais rápido de diagnosticar se o problema é refresh automático ou custo de refresh?

A) Verificar `system.views.materialized_views` na tabela de metadata + log de jobs de refresh  
B) Consultar Delta Lake statistics (`DESCRIBE DETAIL`) e verificar última timestamp de modificação  
C) Executar `SHOW TBLPROPERTIES` na Materialized View e procurar por `last_refresh_time`  
D) Revisar o custo de billable clusters nos últimos 3 dias + conectar ao Predictive Optimization  

---

**14. [Data Governance]**  
Você trabalha com UC em 5 workspaces. Um projeto requer que múltiplos workspaces leiam da mesma tabela com permissões diferentes por workspace. Qual é a abordagem correta usando UC?

A) Replicar a tabela em cada workspace com políticas ABAC específicas  
B) Usar Open Sharing ou D2D (Data to Data) para compartilhar a tabela com granularity de permissão  
C) Criar views delegadas em cada workspace que chamam UDF de validação de workspace_id  
D) Usar `ALTER TABLE ... OWNER TO` para transferir permissão + recriar a tabela em cada workspace  

---

**15. [Debugging and Deploying]**  
Um job agendado para rodar diariamente às 8 AM começa a falhar após 2 semanas de funcionamento. Logs mostram `SparkException: Task deserialization error`. Qual é a causa mais provável e correção?

A) Cache de versão JAR antiga — limpar cache do cluster e reimport libraries  
B) Dependency mismatch ou mudança na classe Python — revisar se houve upgrade de lib + aumentar `spark.driver.maxResultSize`  
C) Ficheiro de checkpoint corrompido — remover checkpoint e reiniciar  
D) Timeout de conexão de DB — aumentar `spark.sql.connect.timeout`  

---

**16. [Data Ingestion & Acquisition]**  
Você ingere dados de um data warehouse legado (Teradata) via Lakebridge. A conexão é LDAP. Qual configuração é necessária NO LADO DO TERADATA para que Lakebridge funcione?

A) Apenas habilitar acesso remoto + criar usuário com `GRANT CONNECT` privilégio  
B) Habilitar LDAP LogMech no Teradata, liberar porta ODBC, e garantir que o usuário LDAP tem permissão  
C) Criar um Foreign Data Wrapper (FDW) no Teradata + abrir port 1025  
D) Configurar Teradata viewpoints com `GRANT SELECT` para usuários Databricks  

---

**17. [Developing Code — SQL]**  
Você quer rodar uma série de testes de data quality no SQL durante a ingestão via DLT. Qual é a melhor forma de expressar "se > 5% de registros têm `price < 0`, falhar a pipeline"?

A) Usar `@quality_expectation` ou `EXPECT` statement em DLT com action `fail`  
B) `IF (SELECT COUNT(*) FROM data WHERE price < 0) > (SELECT COUNT(*) * 0.05 FROM data) THEN RAISE`  
C) Criar uma stored procedure que roda `SELECT COUNT(*) ... FILTER (price < 0)` e chama `raise_error()`  
D) Filtrar registros com `WHERE price >= 0` + usar logging de descartados  

---

**18. [Cost & Performance Optimization]**  
Uma tabela Iceberg cresce 50GB/dia. Performance de `SELECT * WHERE date > CURRENT_DATE()` está degradando. Qual é a otimização recomendada NO ICEBERG?

A) Habilitar `CLUSTER BY` automático no Iceberg + compactar snapshots antigos  
B) Usar `OPTIMIZE` diário + habilitar metadata caching em Z-order  
C) Particionar por `date` + manter apenas últimos 90 dias via `EXPIRE_SNAPSHOTS`  
D) Converter para Delta e usar deletion vectors  

---

**19. [Data Modeling]**  
Você tem uma tabela SCD Type 2 onde cada atualização cria nova linha com `effective_date` e `end_date`. Seu job atualiza registros em batch diariamente. Qual SQL é mais eficiente para manter SCD Type 2?

A) Usar `MERGE` com `WHEN MATCHED ... UPDATE ... SET end_date = CURRENT_DATE()` e `WHEN NOT MATCHED ... INSERT`  
B) `DELETE ... WHERE status = 'active'` + `INSERT` com `INSERT OVERWRITE TABLE` scd_table  
C) Usar AUTO CDC com configuração `stored_as_scd_type = 2` em DLT  
D) Criar uma Materialized View que calcula `max(effective_date)` por chave + fazer outer join  

---

**20. [Monitoring and Alerting]**  
Um job de streaming que consuma de Kafka está com back-pressure. Qual métrica no Spark UI você confirma para validar se Kafka está com lag crescente?

A) Input rate vs. Processing rate no gráfico de Streaming tab  
B) Executor memory usage + GC time no stage explorer  
C) Databricks Job Runs log com `kafka_consumer_lag` metric  
D) Task duration breakdown no SQL tab  

---

**21. [Data Security and Compliance]**  
Você quer que uma coluna sensível (ssn) seja sempre mascarada APENAS para usuários não-admins em uma tabela. O mascaramento não pode ser contornado via SQL direto. Qual é a solução?

A) Criar uma view com `CASE WHEN is_admin() THEN ssn ELSE NULL END` + revogar acesso à tabela base  
B) Usar Row and Column Security (RCS) com uma política de mascaramento por tag governada  
C) Usar Dynamic Data Masking (DDM) com regra SQL + garantir que apenas admin pode criar SQL UDF  
D) Replicar a tabela com coluna ssn em branco, manter original em schema privado + usar role de acesso  

---

**22. [Developing Code — Python/SQL]**  
Seu notebook faz `spark.read.parquet("s3://bucket/path")` cada vez que é executado. Há 100GB de dados. Qual optimization garante que relectura do mesmo path use cache sem reescrever?

A) Usar `spark.sql.parquet.cacheMetadata=true` + habilitar Delta cache  
B) Salvar resultado em Delta após primeira leitura, depois ler de Delta  
C) Configurar `spark.sql.shuffle.partitions` e usar `cache()` DataFrame + `persist()`  
D) Usar Apache Iceberg ao invés de Parquet + habilitar metadata caching  

---

**23. [Data Ingestion & Acquisition]**  
Você tem um webhook que envia eventos a cada segundo. Quer ingerir para Delta com latência < 1 segundo de ponta-a-ponta. Qual setup é mais apropriado?

A) Kafka topic → Structured Streaming → Delta com micro-batch 500ms  
B) Webhook → Kinesis stream → Delta via Lakeflow Connect  
C) Webhook → HTTP listener app → append direto em Delta (Python loop)  
D) Webhook → Auto Loader em `STREAMING` mode com `trigger(once=False)` e latestFirst  

---

**24. [Data Manipulation — SQL]**  
Uma tabela tem coluna `tags: ARRAY<STRUCT<name: STRING, value: VARIANT>>`. Você precisa contar registros onde alguma tag tem `name = 'category'` e `value.id` > 100. Qual query é correta?

A) `SELECT COUNT(*) FROM table WHERE EXISTS (SELECT 1 FROM tags WHERE tags.name = 'category' AND tags.value:id > 100)`  
B) `SELECT COUNT(DISTINCT id) FROM table, LATERAL FLATTEN(tags) t WHERE t.value:name = 'category' AND t.value:value:id > 100`  
C) `SELECT COUNT(*) FROM table WHERE ANY(tags, t -> t.name = 'category' AND t.value:id > 100)`  
D) `SELECT COUNT(*) FROM table WHERE array_contains(tags, map('name', 'category', 'id', '>100'))`  

---

**25. [Cost & Performance Optimization]**  
Uma query em SQL Warehouse demora 5 minutos. Profiling mostra 80% do tempo em shuffle. Seu índice de seletividade é 2% (filtra 2% dos dados). Qual é a otimização mais eficiente?

A) Usar `CLUSTER BY` automático na tabela + reordenar colunas no select  
B) Particionamento + Z-order em coluna de filtro + aumentar worker count  
C) Habilitar Adaptive Query Execution (AQE) + `broadcast_join_threshold`  
D) Criar um índice B-tree e usar hint `USE INDEX`  

---

**26. [Debugging and Deploying]**  
Um Databricks Workflow roda via Declarative Automation (databricks.yml). Um step falha. Qual é a forma de integrar retry logic NATIVA sem modificar o notebook?

A) Adicionar `max_retries: 3` e `retry_on_timeout: true` na task definition do databricks.yml  
B) Wrappear o notebook com script Python que faz retry via `dbutils.notebook.run()` com try-except  
C) Usar `tasks: [{name: ..., job_cluster_config: ..., max_concurrent_runs: 1}]` + job scheduling  
D) Rodar cada task como um `run_now` separado com validação manual  

---

**27. [Data Governance]**  
Seu UC tem uma tabela com coluna de PII (email). Você criou uma governed tag `pii` e aplicou política ABAC de mascaramento. Um novo usuário PRECISA VER O EMAIL REAL (sem mascara). Qual é o processo correto?

A) Remove do grupo `analysts`, adiciona ao grupo `data_officers` com override no Policy  
B) Solicita ao admin que cria exceção na policy usando `ALTER POLICY ... ADD EXCEPTION`  
C) O usuário executa `USE UNMASKED_COPY` antes de selecionar (não existe, pegadinha)  
D) Criar uma view dedicada com `SELECT email FROM table WHERE current_user() IN ('user@email.com')`  

---

**28. [Developing Code — Python/SQL]**  
Em um job Spark, você precisa processar dados em lotes menores para evitar OOM. Qual é a forma mais idiomática em PySpark?

A) Usar `repartition()` + `groupByKey()` + loop em `collect()`  
B) `foreachPartition()` ou `foreachBatch()` com limites de batch size  
C) `take(n)` em loop + reprocessar  
D) Aumentar `spark.executor.memory` e deixar Spark gerenciar partições  

---

**29. [Monitoring and Alerting]**  
Um pipeline tem SLA de "dados devem estar atualizados até 6 AM". A tabela que alimenta o dashboard não tem alertas. Qual é a melhor configuração no Databricks nativo?

A) Usar `ALTER TABLE ... ADD CONSTRAINT freshness_check`  
B) Configurar alertas no SQL Warehouse query profiling  
C) Criar um job de validação que roda às 5:50 AM, verifica `DESCRIBE DETAIL` timestamp e dispara Alert (webhook/Slack)  
D) Usar Query Alert nativo do SQL Warehouse configurado para rodar queries de SLA check  

---

**30. [Data Security and Compliance]**  
Você precisa fornecer acesso de leitura a uma tabela UC para um parceiro externo. A tabela contém dados sensíveis. Qual é a abordagem recomendada usando UC nativo?

A) Exportar dados para CSV, compartilhar via S3 pre-signed URL  
B) Usar Open Sharing (se parceiro tem Databricks) ou Clean Rooms (compartilhamento privado)  
C) Criar uma view com subset de dados + conceder `SELECT` via role compartilhado  
D) Replicar tabela para workspace do parceiro + gerenciar permissões per-workspace  

---

**31. [Data Ingestion & Acquisition]**  
Você ingere de um banco de dados legado via JDBC. A tabela tem 500M de linhas e cresce 10M/dia. Qual estratégia garante ingestão incremental eficiente?

A) Query completa (`SELECT *`) diariamente com `mergeSchema=true`  
B) Usar `READ_FROM` com hint de coluna de sequência monotônica + `WHERE col > last_value`  
C) Lakeflow Connect em CDC mode (se suportado) ou JDBC com query parametrizada de incremento  
D) Usar Apache Sqoop + converter para Parquet depois  

---

**32. [Developing Code — SQL]**  
Em um notebook SQL, você faz `CREATE TEMP VIEW v AS SELECT ...`. Depois quer rodar em paralelo dois comandos: `INSERT INTO tab1 SELECT * FROM v` e `INSERT INTO tab2 SELECT * FROM v`. Qual é o risco e como evitar?

A) TEMP VIEW só vive na sessão — usar `CREATE VIEW` (persistente) ou `GLOBAL TEMP VIEW`  
B) Não há risco, TEMP VIEW é acessível em toda a sessão  
C) Necessário re-criar a view em cada paralelismo — usar `spark.parallelize()`  
D) O problema é inserção, não view — adicionar `OVERWRITE` clause  

---

**33. [Cost & Performance Optimization]**  
Um relatório de BI é alimentado por 3 Materialized Views que combinam dados de 10 tabelas Delta. Refresh é agendado de 1 em 1 hora. Queries adicionais ad-hoc consultam as MV. Qual é a estratégia para reduzir custo?

A) Converter MV para views normais (não-materializadas) + habilitar Delta cache  
B) Usar UC Metric Views para agregações + manter apenas 1 MV base para dados brutos  
C) Combinar refresh das 3 MVs em 1 job + usar scheduled warehouses que iniciam/param automaticamente  
D) Desabilitar refresh automático + rodar refresh manual sob demanda  

---

**34. [Monitoring and Alerting]**  
Um job de ingestão começa a falhar com `FileNotFoundError` após semana funcionando. O código não mudou. Qual é a causa mais provável?

A) Caminho S3 expirou ou bucket foi apagado  
B) Permissão de IAM foi revogada / credenciais venceram  
C) Cluster foi terminado, novo cluster não tem acesso ao mesmo bucket  
D) Todas as acima são possíveis — verificar logs de driver + validar permissões + testar caminho  

---

**35. [Data Modeling]**  
Você tem uma tabela de eventos com `event_type: STRING` e quer calcular métricas separadas por tipo. Qual é a forma mais performática usando VARIANT/STRUCT?

A) `SELECT event_type, COUNT(*) FROM events GROUP BY event_type` + loop Python sobre resultado  
B) Usar `CASE` statements para cada tipo + aggregação dinâmica  
C) Transformar para VARIANT com estrutura `{type: X, metrics: {...}}` e depois fazer parse  
D) Particionar logicamente (views por tipo) + rodar query separada para cada  

---

**36. [Data Manipulation]**  
Você consome mensagens de Kafka com Structured Streaming. O producer envia eventos com timestamps distintos. Você quer garantir exactly-once processing mesmo com falhas. Qual configuração é crítica?

A) Usar `outputMode="append"` sem checkpoint (não garante)  
B) Checkpoint habilitado + idempotent writer (salvar com idempotency key) + `completionTrigger`  
C) `outputMode="complete"` com `trigger(once=True)` e checkpoint  
D) Apenas checkpoint está suficiente (falso, precisa também de sink idempotente)  

---

**37. [Cost & Performance Optimization]**  
Uma query demora 10 minutos. Profiling mostra: 40% em rede (shuffle), 30% em parse JSON, 20% em sort, 10% I/O. Qual otimização traz maior ganho?

A) Aumentar partições + usar Z-order  
B) Pré-parsear JSON em VARIANT na ingestão + habilitar Delta cache  
C) Converter JSON para Parquet nativo antes de query  
D) Aumentar worker count e `spark.sql.shuffle.partitions`  

---

**38. [Developing Code — SQL]**  
Você cria uma stored procedure que faz `INSERT INTO table SELECT ...` com parametro de data. Para testar, você roda com data fixa. Em produção (job agendado), a data deve ser dinâmica (hoje). Qual é a prática correta?

A) Armazenar a data em uma tabela de metadados, stored proc lê dela  
B) Usar `DEFAULT CURRENT_DATE()` como parâmetro com null-check + override no job  
C) Usar `COALESCE(parameter_date, CURRENT_DATE())` na procedure  
D) Hardcode `CURRENT_DATE()` direto na procedure (sem parâmetro)  

---

**39. [Debugging and Deploying]**  
Um notebook exporta parâmetros via `dbutils.widgets.get()`. O job agendado passa parametros via `%run ../config`. De repente, tudo falha com "widget not found". Por quê?

A) `%run` não passa widgets — usar `dbutils.notebook.run()` com `base_parameters` dict  
B) Notebook de config deve estar no mesmo workspace (caminho relativo não funciona entre workspaces)  
C) `dbutils.widgets.get()` não funciona em jobs — usar environment variables via `spark.conf`  
D) Falta `%render` antes de usar os widgets  

---

**40. [Monitoring and Alerting]**  
Um dashboard apresenta inconsistência: dois relatórios idênticos retornam valores diferentes. O dashboard usa a mesma Materialized View. Qual é a causa mais provável?

A) MV estava em refresh enquanto um relatório rodava — um viu versão anterior, outro versão nova  
B) Um relatório usa cache local do browser, outro não  
C) Delta version history — um query vê snapshot antigo, outro snapshot novo  
D) Bug na query do dashboard, não culpa da MV  

---

**41. [Data Manipulation]**  
Você tem uma tabela com colunas `customer_id`, `email`, `age`. Quer uma política que mascare `email` para todos EXCETO `data_engineers`. Qual é a configuração correta em UC?

A) Usar ABAC policy com `principal != "data_engineers"` → aplicar mascaramento (lógica invertida, não suportada)  
B) Usar "positive" rule: `principal == "data_engineers"` → SEM mascaramento, `else` → mascaramento  
C) Usar Row-Level Security em lugar de Column-Level (não é aplicável aqui)  
D) Criar 2 views: 1 com email para data_engineers, 1 sem email para outros  

---

**42. [Developing Code — Python]**  
Em um PySpark job, você precisa aplicar transformação custosa em um RDD. Qual é a forma mais eficiente?

A) `rdd.map(expensive_func)` sem persistência  
B) `rdd.map(expensive_func).cache().count()` para forçar computação + depois usar  
C) Usar DataFrame com SQL UDF em lugar de Python RDD  
D) `rdd.persist(StorageLevel.DISK_ONLY)` se memory é limitada  

---

**43. [Data Security and Compliance]**  
Você quer auditar QUEM acessou QUAL coluna de uma tabela UC. O rastreamento nativo do Databricks (audit logs) mostra "SELECT * FROM table" mas não granularidade de coluna. Qual é a solução?

A) Usar UC Column-Level Lineage com políticas de mascaramento (mostra o bloqueio, não o acesso)  
B) Criar view por usuário + column-level security + monitorar access logs diferenciados  
C) Usar Predictive Optimization para rastrear acessos por coluna (não é sua função)  
D) Implementar logging customizado em UDF que registra coluna acessada + auditar via externa DB  

---

**44. [Data Manipulation]**  
Você tem uma tabela large (1TB) e quer atualizar ~1% das linhas. `MERGE` vs `UPDATE`: qual é melhor?

A) `UPDATE` é mais direto, sempre melhor  
B) `MERGE` é sempre melhor para grandes volumes, mesmo com pequenas mudanças  
C) Usar `MERGE` quando muitas linhas mudam; `UPDATE` se < 5% (performance diferente em Delta)  
D) Ambos são equivalentes em Delta — escolha por clareza de código  

---

**45. [Cost & Performance Optimization]**  
Um query que fazia full table scan de 100GB em 2 min agora faz em 10 min. Nada mudou no código. Qual é a primeira coisa a verificar?

A) Alterações na tabela: nova particionamento / compactação falhando / versão Delta antiga  
B) Cluster: mudança de worker count / worker type / memory disponível  
C) Query: mudança de hints / join order / predicate pushdown  
D) Cache: Delta cache foi desabilitado ou LRU expirou histórico  

---

**46. [Data Ingestion & Acquisition]**  
Você recebe arquivos CSV diariamente em um bucket S3. Arquivo tem encoding variável (UTF-8, Latin-1). Qual é a forma robusta de ingerir com Auto Loader?

A) `spark.read.option("encoding", "UTF-8").csv(path)`  
B) Usar Auto Loader com `cloudFiles.schemaInference=true` e detector de encoding nativo  
C) Pre-processar arquivos com `file_modify_time` + usar `multiLine=true`, `charToEscapeQuoteEscaping=true`  
D) Auto Loader não suporta encoding variável — converter externamente para UTF-8 antes  

---

**47. [Developing Code — Python]**  
Em um notebook, você combina SQL e PySpark. SQL query retorna 1M de linhas. Você faz `df = spark.sql("SELECT ...")` depois `df.collect()`. Qual é o risco?

A) `collect()` traz tudo para memory do driver — pode causar OOM  
B) Nenhum, PySpark gerencia memoria automaticamente  
C) `df` permanece em RDD, não em memória do driver  
D) Só falha se número de partições < worker count  

---

**48. [Debugging and Deploying]**  
Seu cliente quer que uma tag governada `sensitive` seja aplicada automaticamente em qualquer coluna com `@pii` ou contendo "ssn". Qual é a solução?

A) Usar governance rules em UC com pattern matching automático  
B) Criar um job que roda `ALTER TABLE ... SET TBLPROPERTIES (sensitive=true)` em base a scan de schema  
C) Usar Predictive Optimization para sugerir tags automaticamente  
D) Configurar ABAC policy que auto-aplica tag na coluna criada  

---

**49. [Cost & Performance Optimization]**  
Um job roda bem em testing (10M de dados), mas em produção (10B de dados) falha com OutOfMemory. Você aumenta `spark.executor.memory`. Falha persiste, mas não no driver, no executor. Qual é a verdadeira causa?

A) Não é executor memory — pode ser spill disco cheio ou partição desbalanceada  
B) Apenas aumentar memory já deveria resolver  
C) O job não é scala-able — precisa ser reescrito  
D) Aumentar memory não ajuda em spill — usar `repartition()` ou compactar dados  

---

**50. [Developing Code — Python]**  
Em um PySpark job, você faz uma análise exploratória sobre tabela Delta. Você quer aplicar transformação custosa sem risco de OOM. Qual é a forma mais segura?

A) Usar `repartition()` antes de `map()` + aplicar em partições pequenas  
B) `df.map(func).cache().count()` para forçar computação imediata  
C) Usar `foreachPartition()` com limite de batch size  
D) Aumentar `spark.driver.memory` e deixar Spark gerenciar  

---

**51. [Data Ingestion & Acquisition]**  
Você tem um webhook que envia eventos a cada segundo. Um webhook pode reenviar o mesmo evento múltiplas vezes (retry). Você quer desduplicar. Qual é a abordagem?

A) Adicionar `UNIQUE` constraint na coluna de ID (pode falhar se ID já existe)  
B) Usar `INSERT IGNORE` (não existe em Delta/Databricks)  
C) Usar `MERGE` com `WHEN NOT MATCHED INSERT` baseado em chave de dedup + `DELETE FROM staging WHERE ...`  
D) Inserir em staging, depois `INSERT INTO target SELECT DISTINCT * FROM staging`  

---

**52. [Data Manipulation]**  
Você tem tabela de vendas particionada por `date`. Um INSERT diário agora demora 3x mais. Causa: muitas pequenas partições. Qual solução é melhor?

A) `OPTIMIZE` com Z-order na tabela completa (custoso, scan completo)  
B) `OPTIMIZE` apenas na partição do dia (`OPTIMIZE table WHERE date = ...`)  
C) Usar compactação incremental (não existe built-in — usar `REPARTITION` antes de INSERT)  
D) Auto-compactação via `OPTIMIZE` automático em Databricks (disponível, mas exige config)  

---

**53. [Debugging and Deploying]**  
Um job agendado falha intermitentemente (50% das vezes). Logs são idênticos. O que provavelmente acontece?

A) Race condition com job paralelo / locking de tabela Delta  
B) Timeout aleatório de rede  
C) Cluster scala-down entre jobs, vez sim vez não  
D) Sem mais info, é impossível — precisa de mais telemetria (cluster metrics, warehouse logs)  

---

**54. [Data Governance]**  
Você tem uma tabela em UC que será usada por 3 grupos: `analysts` (SELECT), `engineers` (SELECT + MODIFY), `admins` (full). Qual é a forma mais eficiente de gerenciar permissões?

A) Criar 3 roles + fazer GRANT específico em cada uma  
B) Usar inheritance: criar role `editors` com MODIFY, role `readers` com SELECT, depois `admins EXTEND editors`  
C) Fazer GRANT direto por usuário (não escalável)  
D) Usar um job que roda `ALTER TABLE OWNER TO` a cada mudança de grupo  

---

**55. [Debugging and Deploying]**  
Um job de streaming que consuma de Kafka está com back-pressure crescente. De repente, latência salta. Qual é a causa mais provável após 30 dias de execução?

A) Kafka tem lag — verificar partition size e consumer group  
B) Checkpoint estado explodiu — limpar checkpoint antigo e reiniciar stream  
C) Shuffle memory cresceu — executor memory limit atingido  
D) Janela de agregação está acumulando — watermark pode estar fixo / estado não está sendo limpado  

---

**56. [Cost & Performance Optimization]**  
Uma tabela Warehouse com 1PB de dados. Usuários reclamam que queries demoram muito. Você precisa de métrica para convencer liderança a investir em otimização. Qual métrica é mais convincente?

A) Query count (não mostra impacto em usuário)  
B) Custo por query (mostra desperdício)  
C) P95 query latency + custo total (mostra UX ruim + $ waste juntos)  
D) Warehouse size (não diretamente relacionado com performance)  

---

**57. [Data Security and Compliance]**  
Sua organização tem compliance regulatória que exige: dados PII devem residir apenas em clusters dedicados. Qual é a abordagem nativa em Databricks?

A) Usar `spark.conf` para limitar read path a específico cluster via access control  
B) UC políticas + compute-level ACL (se suportado) ou implementar em nível de storage  
C) Replicar dados PII em UC schema específico, conceder acesso apenas de cluster gerenciado  
D) Usar Unity Catalog Governance com restriction de table locality (não é feature nativa — manual)  

---

**58. [Developing Code — Python]**  
Em um PySpark job, você quer processar VARIANT colunas em uma transformação SQL. Qual abordagem garante melhor performance?

A) Usar `df.selectExpr("explode_outer(variant_col) as item")` depois iterar em Python  
B) Usar `sql("SELECT ... FROM delta.`path` WHERE ...")` + SQL nativo para manipular VARIANT  
C) Converter VARIANT para string JSON em Python, parsear com `json.loads()`, depois remontar  
D) Usar `pyspark.sql.functions.col()` com `.getItem()` chaining  

---

**59. [Developing Code — SQL/Python]**  
Você tem JSON aninhado e quer extrair múltiplos campos. Qual é mais performático: `parse_json()` uma vez + múltiplos `variant_get()`, ou parsear N vezes?

A) Parsear N vezes é ineficiente — usar `parse_json()` uma vez armazenado em VARIANT  
B) Ambos equivalentes em Spark SQL (optimizer é inteligente)  
C) Parse uma vez em Python, depois em SQL (hybrid)  
D) Usar `get_json_object()` N vezes — Spark otimiza internamente  

---

**60. [Developing Code — Python/SQL]**  
Um desenvolvimento iterativo usa um notebook com Structured Streaming que processa dados via `foreachBatch()`. Após converter para um job agendado, a latência degrada com 30 dias de execução sem reinício. Qual é o verdadeiro problema?

A) Checkpoint arquivo ficar muito grande, precisa limpeza periódica  
B) Estado da janela acumulando em memória — watermark expirado ou offset tracking não funciona  
C) Kubernetes pod memory leak em longa duração  
D) Python garbage collection degrading performance  

---

## Gabarito e Explicações

**1. Resposta: B**  
Watermark de 10 minutos permite que eventos atrasados (até 10 min) sejam reprocessados na janela correta. `outputMode="append"` é recomendado para streaming. CDF é redundante aqui.

**2. Resposta: B**  
Lakeflow Connect precisa de Logical Decoding ativado no PostgreSQL, WAL level `logical`, e uma slot de replicação permanente. Replica identity full é útil mas não pré-requisito crítico.

**3. Resposta: C**  
`variant_get(data, 'user.email', 'string')` é seguro e eficiente para extrair de VARIANT com fallback para NULL. Mais explícito que sintaxe de colchete.

**4. Resposta: B**  
Métricas de throughput (input rate vs. processing rate) no Spark Streaming tab mostram imediatamente degradação. Se processing rate caiu, há gargalo.

**5. Resposta: A**  
Deletion vectors evitam reescrever arquivos inteiros. CLUSTER BY auto otimiza layout. Delta cache reduz I/O. Juntos, resolvem escritas frequentes e leituras lentas.

**6. Resposta: B**  
ABAC (Attribute-Based Access Control) em UC com governed tags permite política central que aplica-se automaticamente a qualquer coluna com tag `pii`.

**7. Resposta: A**  
UC Metric View fornece definição centralizada governada. Materialized View pré-agrega para performance. Combinadas, oferecem governança + reutilização.

**8. Resposta: B**  
Passar data como parâmetro via configuração garante consistência na janela móvel entre runs. Mais confiável que `CURRENT_DATE()` em jobs.

**9. Resposta: A**  
Chamar API por 1 hora em paralelo, salvar bruto em Delta, depois processar, é robusto para grande volume histórico.

**10. Resposta: A**  
`MERGE` com CDF consume apenas mudanças incrementais de `shipments`. Mais eficiente que full refresh.

**11. Resposta: A**  
Serverless compute para ad-hoc (pay-as-you-go) + warehouse dedicado para dashboards (previsível, reserved capacity) reduz custo total.

**12. Resposta: B**  
SQL nativo com manipulação VARIANT é mais eficiente que Python/RDD. Spark SQL optimizer otimiza VARIANT operações.

**13. Resposta: C**  
`SHOW TBLPROPERTIES` em Materialized View mostra `last_refresh_time` e status de refresh. Mais direto.

**14. Resposta: B**  
Open Sharing (entre workspaces Databricks) ou D2D permite compartilhamento com granularidade diferente por workspace. UC nativo.

**15. Resposta: B**  
`SparkException: Task deserialization error` após 2 semanas é tipicamente mudança de classe/lib incompatibilidade. Limpar e reimportar libraries resolve.

**16. Resposta: B**  
Lakebridge em Teradata com LDAP requer LDAP LogMech ativado, porta ODBC liberada, usuário LDAP com permissão. Setup no Teradata é essencial.

**17. Resposta: A**  
DLT nativo suporta `@quality_expectation` ou `EXPECT` statements com `action fail`. Forma idiomática.

**18. Resposta: C**  
Particionar por date em Iceberg + `EXPIRE_SNAPSHOTS` elimina snapshots antigos, reduzindo file count e metadados.

**19. Resposta: C**  
AUTO CDC com `stored_as_scd_type = 2` em DLT mantém SCD Type 2 automaticamente. Mais eficiente que MERGE manual.

**20. Resposta: A**  
Spark Streaming tab mostra gráficos "Input rate" vs. "Processing rate". Se processing rate < input rate, há lag.

**21. Resposta: B**  
Row and Column Security (RCS) com política de mascaramento por tag é nativo em UC e não pode ser contornado.

**22. Resposta: B**  
Salvar resultado em Delta após primeira leitura, depois ler de Delta, garante snapshot cacheado e estrutura compactada.

**23. Resposta: A**  
Kafka → Structured Streaming com micro-batch 500ms → Delta garante < 1 seg latência ponta-a-ponta.

**24. Resposta: C**  
`ANY(tags, t -> t.name = 'category' AND t.value:id > 100)` usa `ANY` predicate em array com lambda para testar struct aninhado.

**25. Resposta: B**  
80% em shuffle com seletividade baixa → particionamento + Z-order reduz volume. Maior ganho que aumentar workers.

**26. Resposta: A**  
Declarative Automation suporta `max_retries` e `retry_on_timeout` nativa em task definition.

**27. Resposta: B**  
ABAC policy permite exceções via `ALTER POLICY ... ADD EXCEPTION` para usuários específicos.

**28. Resposta: B**  
`foreachPartition()` ou `foreachBatch()` com batch size limit é idiomático para processar em lotes e evitar OOM.

**29. Resposta: C**  
Job de validação que roda antes do SLA, verifica timestamp via `DESCRIBE DETAIL`, dispara alerta webhook/Slack. Prática comum.

**30. Resposta: B**  
Open Sharing (parceiro com Databricks) ou Clean Rooms (compartilhamento privado) é solução nativa UC para parceiros externos.

**31. Resposta: C**  
Lakeflow Connect com CDC ou JDBC parametrizada em coluna de sequência garante ingestão incremental eficiente.

**32. Resposta: A**  
TEMP VIEW vive na sessão. Usar `CREATE VIEW` (persistente) ou `GLOBAL TEMP VIEW` em paralelismo.

**33. Resposta: C**  
Combinar refresh das 3 MVs + scheduled warehouses on/off automático reduz overhead e custo ocioso.

**34. Resposta: D**  
Todas são causas possíveis. Validar logs, testar caminho S3, verificar credenciais IAM.

**35. Resposta: A**  
`SELECT event_type, COUNT(*) FROM events GROUP BY event_type` é forma mais performática — SQL nativo.

**36. Resposta: B**  
Exactly-once requer checkpoint + idempotent writer + sink que não insere duplicatas. Checkpoint sozinho não basta.

**37. Resposta: B**  
JSON parsing é 30% do tempo. Pré-parsear em VARIANT na ingestão + Delta cache evita re-parse. Maior ganho.

**38. Resposta: C**  
`COALESCE(parameter_date, CURRENT_DATE())` é idiomático — parâmetro nulo cai para data dinâmica.

**39. Resposta: A**  
`%run` não passa widgets. Usar `dbutils.notebook.run()` com `base_parameters` dict passa variáveis.

**40. Resposta: A**  
MV em refresh durante query — um viu snapshot anterior, outro posterior. Refresh causa inconsistência transitória.

**41. Resposta: B**  
ABAC policy em UC suporta lógica positiva: `principal == "data_engineers"` → sem mask; default → mask.

**42. Resposta: B**  
`cache().count()` força computação + caching. Sem `count()`, cache é lazy. Com memory limitada, considerar `DISK_ONLY`.

**43. Resposta: D**  
UC audit logs não rastreiam coluna individual. Logging customizado em UDF + auditoria externa DB é solução real.

**44. Resposta: C**  
Delta: `MERGE` com matching pequeno é otimizado; `UPDATE` é direto. Se < 5% linhas mudam, `UPDATE` pode ser mais eficiente.

**45. Resposta: A**  
Query de full table scan degradando — alteração em tabela (compactação falhando, versão Delta antiga).

**46. Resposta: B**  
Auto Loader com `schemaInference=true` detecta encoding automaticamente. Fallback: pre-processar com iconv.

**47. Resposta: A**  
`collect()` traz 1M linhas para memory do driver — risco de OOM. Usar `count()` ou `write` para evitar.

**48. Resposta: B**  
Job que escaneia schema e aplica tags baseado em padrão (nome contém "ssn" ou "pii") é workaround comum.

**49. Resposta: D**  
Se aumentar executor memory não resolve e não é driver OOM, é partição desbalanceada ou spill. `repartition()` resolve.

**50. Resposta: B**  
`df.map(func).cache().count()` força computação imediata e caching. Com memory limitada, considerar `DISK_ONLY` storage level.

**51. Resposta: C**  
`MERGE` com `WHEN NOT MATCHED INSERT` baseado em ID dedup garante no duplicatas em webhook deduplicação.

**52. Resposta: B**  
`OPTIMIZE` apenas em partição do dia é eficiente — compacta apenas partição nova.

**53. Resposta: D**  
Falhas intermitentes 50% — race condition, timeout aleatório, ou cluster scala-down. Precisa telemetria.

**54. Resposta: A**  
Criar 3 roles + fazer GRANT específico é approach padrão e escalável em UC.

**55. Resposta: D**  
Latência em streaming cresce após 30 dias — watermark acumulando estado old + janela não expirando.

**56. Resposta: C**  
P95 latência mostra UX ruim. Custo por query mostra desperdício. Juntos, convence liderança.

**57. Resposta: C**  
Replicar PII em UC schema específico, conceder acesso de compute dedicado é closest solução nativa.

**58. Resposta: B**  
SQL nativo com VARIANT é mais eficiente que Python/RDD. Optimizer otimiza VARIANT operações.

**59. Resposta: A**  
Parsear JSON uma vez em VARIANT, depois múltiplos `variant_get()`, evita re-parse.

**60. Resposta: B**  
Latência em streaming cresce após 30 dias — estado de janela acumulando. Watermark expirado ou offset tracking falha.

---

**Fim do Simulado 2**

> 60 questões, distribuição corrigida: DC 14, DIA 7, DM 7, Monitoring 6, CostPerf 9, Security 5, Data Governance 3, Debugging 6, Data Modeling 3. Todos os tópicos NET-NEW cobertos. Gabarito baseado na doc oficial Databricks.
