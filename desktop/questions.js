window.SIMULADOS = {
 "simulado1": {
  "title": "Simulado 1",
  "questions": [
   {
    "id": 1,
    "domain": "Developing Code",
    "topic": "Structured Streaming Stateful",
    "text": "Você está implementando um agregador de eventos em tempo real com Structured Streaming que conta eventos por user_id em janelas de 10 minutos. O pipeline recebe eventos com até 2 horas de atraso. Qual combinação de configurações garante que late-arriving events sejam capturados sem duplicação ao retomar?",
    "options": {
     "A": "Output mode `complete` com `watermark = 2 hours` e checkpoint remoto",
     "B": "Output mode `append` com `watermark = 10 minutes` e state checkpoint",
     "C": "Output mode `update` com `watermark = 2 hours` sem checkpoint",
     "D": "Output mode `append` com `state_retention = 2 hours` e checkpoint remoto"
    },
    "answer": "A",
    "explanation": "Watermark de 2 horas permite late-arriving events até 2h. Checkpoint remoto garante retomada sem duplicação (output mode `complete` é correto para agregações). Mode `append` com 10 minutos watermark perderia eventos fora da janela; `update` sem checkpoint perde estado em crash.",
    "optExpl": {
     "A": "Correto: output mode `complete` retorna todos os dados agregados e watermark de 2h captura eventos que chegam até 2 horas depois.",
     "B": "Errado: append mode apenas escreve novas linhas e perderia eventos que chegam após a janela de 10 minutos passar.",
     "C": "Errado: sem checkpoint, o estado é perdido ao retomar após falha e pode gerar duplicação.",
     "D": "Errado: `state_retention` não é parâmetro válido de Structured Streaming; a retenção é controlada por watermark."
    }
   },
   {
    "id": 2,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Lakeflow Connect",
    "text": "Um cliente precisa replicar inserts, updates e deletes de uma tabela PostgreSQL para Databricks em quase-real time, mantendo histórico de mudanças por data. Qual solução requer MENOS código customizado?",
    "options": {
     "A": "Apache Kafka + Python Delta Writer",
     "B": "Lakeflow Connect managed connector com CDC automático",
     "C": "DMS (AWS) + ADLS → Lakeflow API",
     "D": "Airflow + JDBC source + Spark batch"
    },
    "answer": "B",
    "explanation": "Lakeflow Connect managed connector para PostgreSQL CDC é oficialmente suportado e reduz código vs Kafka + custom writer. DMS e Airflow requerem mais configuração; ambas são válidas mas B é \"menos código\".",
    "optExpl": {
     "A": "Errado: Kafka + Delta Writer requer código customizado para CDC e gerenciamento de state.",
     "B": "Correto: Lakeflow Connect managed connector fornece CDC automático e requer mínimo código customizado.",
     "C": "Errado: DMS + ADLS requer configuração adicional AWS e pipeline customizado.",
     "D": "Errado: Airflow + batch é solução batch, não quase-real time, e requer orquestração complexa."
    }
   },
   {
    "id": 3,
    "domain": "Data Manipulation",
    "topic": "VARIANT",
    "text": "Uma API JSON retorna um campo `metadata` que pode ter estrutura dinâmica (um array com objetos variados ou um string simples). Você precisa extrair o primeiro elemento se for array, ou o valor completo se for string. Qual função permite isso sem erro de tipo?",
    "options": {
     "A": "`get_json_object(metadata, '$[0]')` com try-catch",
     "B": "`from_json(metadata, 'array<string>')` após validação de schema",
     "C": "`variant_get(parse_json(metadata), ':0')` ou `variant_get(parse_json(metadata), '')` conforme tipo",
     "D": "`json_extract_array(metadata)[0]` com `coalesce`"
    },
    "answer": "C",
    "explanation": "`variant_get(parse_json(...), ':0')` e variantes tratam VARIANT sem type error pré-existente. `from_json` forçaria schema fixo (throw error em mismatch). `json_extract_array` falha se não for array.",
    "optExpl": {
     "A": "Errado: `get_json_object` falha com tipo inesperado e não trata union de array/string.",
     "B": "Errado: `from_json` forçaria schema fixo e falharia se campo for string pura (type mismatch).",
     "C": "Correto: `variant_get` com `parse_json` trata tipos dinâmicos (array ou string) sem erro pré-existente.",
     "D": "Errado: `json_extract_array` falha se metadata não for array (não trata union type)."
    }
   },
   {
    "id": 4,
    "domain": "Monitoring and Alerting",
    "topic": "Query Performance",
    "text": "Um SQL Warehouse está com queries lentas (P99 aumentando). Você suspeita de data freshness ou cache ineficaz. Qual métrica do Query History e Warehouse Stats você verifica PRIMEIRO para descartar stale data?",
    "options": {
     "A": "`scan_bytes` e `bytes_spilled` do query",
     "B": "`cache_hit_ratio` do warehouse + `total_rows_scanned` por tabela",
     "C": "`query_start_time` vs `table_updated_at` + verificar Delta Cache status",
     "D": "`remote_IO_bytes` e `local_IO_bytes` do Photon profile"
    },
    "answer": "C",
    "explanation": "`query_start_time` vs `table_updated_at` revela se dados estão stale. Delta Cache status mostra hit rate. Ambas métricas são críticas; C é mais prático para descartar stale data issue.",
    "optExpl": {
     "A": "Errado: `scan_bytes` e `bytes_spilled` indicam performance atual, não revelam se dados estão stale.",
     "B": "Errado: `cache_hit_ratio` não indica se os dados em cache estão desatualizados.",
     "C": "Correto: comparar `query_start_time` vs `table_updated_at` revela se dados estão stale e Delta Cache status confirma.",
     "D": "Errado: `remote_IO_bytes` indica I/O patterns, não data freshness."
    }
   },
   {
    "id": 5,
    "domain": "Cost & Performance Optimization",
    "topic": "CLUSTER BY AUTO",
    "text": "Você tem uma fact table (`sales`) com 2B de linhas que é queried por `region` e `date_id` 80% das vezes. O job atual lê 50% da tabela por query. Qual estratégia reduz custo mais significativamente?",
    "options": {
     "A": "Particionamento por `region` + `date_id`",
     "B": "`CLUSTER BY region, date_id` (manual, sem reordenação automática)",
     "C": "`CLUSTER BY AUTO region, date_id` com Predictive Optimization",
     "D": "Liquid clustering sem Predictive Optimization"
    },
    "answer": "C",
    "explanation": "`CLUSTER BY AUTO` com Predictive Optimization reordena automaticamente baseado em access patterns, reduzindo custo mais que particionamento fixo (que não escala para query patterns dinâmicos).",
    "optExpl": {
     "A": "Errado: particionamento fixo não escala bem para múltiplas combinações de query patterns dinâmicos.",
     "B": "Errado: CLUSTER BY manual sem reordenação automática não adapta a mudanças de access patterns.",
     "C": "Correto: `CLUSTER BY AUTO` com Predictive Optimization reordena automaticamente, reduzindo custo significativamente.",
     "D": "Errado: liquid clustering sem Predictive Optimization não reordena automaticamente e benefício é limitado."
    }
   },
   {
    "id": 6,
    "domain": "Data Security and Compliance",
    "topic": "ABAC com Governed Tags",
    "text": "Você precisa aplicar row filters e column masks em 50+ tabelas baseado no departamento do usuário. Fazer isso tabela por tabela é insustentável. Qual abordagem escala melhor?",
    "options": {
     "A": "Criar uma view parametrizada por `CURRENT_USER()` para cada tabela",
     "B": "ABAC com tags gerenciadas (governed tags) + row filters/column masks declarativas",
     "C": "Replicar dados por departamento em catálogos separados com GRANT por catálogo",
     "D": "UDFs que filtram conforme `CURRENT_ROLE()` em cada query"
    },
    "answer": "B",
    "explanation": "ABAC com governed tags permite aplicar row filters e column masks declarativamente em bulk. Views parametrizadas não escalam bem (50+ tabelas × múltiplos grupos = combinatória).",
    "optExpl": {
     "A": "Errado: criar views parametrizadas para 50+ tabelas × múltiplos grupos gera combinatória insustentável.",
     "B": "Correto: ABAC com governed tags permite aplicar row filters e column masks declarativamente em bulk.",
     "C": "Errado: replicar dados em catálogos separados causa duplicação e complexidade de manutenção.",
     "D": "Errado: UDFs em cada query não escalam bem; difícil de manter e auditável."
    }
   },
   {
    "id": 7,
    "domain": "Data Governance",
    "topic": "UC Metric Views",
    "text": "Um analista precisa monitorar SLA de entrega de dados: \"quantas tabelas do catálogo foram atualizadas nos últimos 24h?\". Qual objeto permite contar isso de forma declarativa, auditar quem acessou, e ser otimizado pelo sistema?",
    "options": {
     "A": "Uma SQL view que consulta `information_schema.table_updates`",
     "B": "UC Metric View que agrega contagem e last_modified_time por tabela",
     "C": "Um dashboard Lakeview que query logs de lineage do UC",
     "D": "Uma tabela Delta que grava timestamp de UPDATE em um trigger"
    },
    "answer": "B",
    "explanation": "UC Metric Views são objetos SQL gerenciados que agregam metadados e podem auditar acesso. Information schema é read-only; dashboard é passivo; triggers Delta não são built-in.",
    "optExpl": {
     "A": "Errado: SQL view é read-only e não é objeto gerenciado otimizável pelo sistema.",
     "B": "Correto: UC Metric View é objeto SQL gerenciado que agrega, audita acesso e é otimizado pelo planner.",
     "C": "Errado: dashboard Lakeview é passive; não audita quem acessou a tabela.",
     "D": "Errado: Delta não possui triggers built-in; seria solução complexa e custom."
    }
   },
   {
    "id": 8,
    "domain": "Debugging and Deploying",
    "topic": "Git Folders",
    "text": "Você tem um workspace que sincroniza SQL queries de um repo Git. Você alterou uma query que estava deployada em produção. Qual é o risco e como mitigá-lo?",
    "options": {
     "A": "Query vai rodar com versão do repo — use Git tags + branch protection para controlar release",
     "B": "Query pode ficar orphaned se o repo for deletado — versione backups em /Workspace/archive",
     "C": "Mudança sincroniza imediatamente para todos que rodam a query — use Declarative Automation Bundles com review de mudança",
     "D": "Workspace fica out-of-sync se repo mudar — use Git folders read-only e promova via pull request"
    },
    "answer": "D",
    "explanation": "Git Folders sincronizam workspace com repo. Mudança no repo sincroniza para workspace. Risk: mudanças acidentais propagam. Mitigação: Git Folder read-only + promote via PR + branch protection.",
    "optExpl": {
     "A": "Errado: Git Folder não sincroniza queries de forma que rodem com versão específica do repo.",
     "B": "Errado: não é o principal risk de Git Folder; backups em /Workspace/archive não previnem mudanças acidentais.",
     "C": "Errado: Declarative Automation Bundles é para deployments, não para mitigação de Git Folder.",
     "D": "Correto: Git Folder sincroniza bidirecionalmente; usar read-only + PR review mitiga mudanças acidentais em produção."
    }
   },
   {
    "id": 9,
    "domain": "Data Modeling",
    "topic": "Slowly Changing Dimension",
    "text": "Uma dimensão de clientes tem histórico (novo email, novo endereço, mudança de status). Qual padrão com Databricks requer MENOS operações de I/O ao fazer update?",
    "options": {
     "A": "SCD Type 1 simples (sobrescrever); reter histórico em backup table",
     "B": "SCD Type 2 com `MERGE` + inserir nova linha com flag `is_current`",
     "C": "SCD Type 2 automática com `stored_as_scd_type(1)` ou `(2)` no MERGE",
     "D": "Event log imutável + view que materializa current snapshot em MV"
    },
    "answer": "C",
    "explanation": "`stored_as_scd_type(2)` em MERGE automatiza SCD Type 2, reduzindo I/O comparado a operações manuais + compaction.",
    "optExpl": {
     "A": "Errado: SCD Type 1 simples requer sobrescrever + backup; não automatiza e requer operações separadas.",
     "B": "Errado: MERGE manual com inserção de nova linha requer código customizado; não é automático.",
     "C": "Correto: `stored_as_scd_type(2)` em MERGE automatiza SCD Type 2, reduzindo I/O comparado a operações manuais.",
     "D": "Errado: event log imutável com MV é overkill e requer refresh manual; mais complexo que MERGE automático."
    }
   },
   {
    "id": 10,
    "domain": "Developing Code",
    "topic": "Python UDF",
    "text": "Uma Python UDF `process_text` usa biblioteca `spacy` que não é pre-installed no cluster. Qual abordagem permite usar a UDF sem erro ImportError?",
    "options": {
     "A": "Instalar `spacy` via `pip install` no notebook antes de definir a UDF",
     "B": "Usar init script no cluster para instalar `spacy` na startup",
     "C": "Registrar a UDF como Spark SQL UDF com `spark.udf.register` e library passada via requirements",
     "D": "Todas as anteriores, mas init script é mais escalável para múltiplos jobs"
    },
    "answer": "D",
    "explanation": "Todas funcionam; init script é mais escalável para múltiplos jobs (uma vez, reutiliza).",
    "optExpl": {
     "A": "Correto para sessão única, mas não escala para múltiplos jobs (requer pip em cada run).",
     "B": "Correto e mais escalável para múltiplos jobs (instalação única na startup do cluster).",
     "C": "Correto com gerenciamento de requirements, mas requer setup adicional de cluster config.",
     "D": "Correto: todas funcionam, mas init script é mais escalável para múltiplos jobs (reutilização)."
    }
   },
   {
    "id": 11,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Iceberg Format Target",
    "text": "Você está ingerindo dados de um data lake S3 que é queried por múltiplos workspaces (compartilhado via OpenSharing). O schema muda frequentemente. Qual formato alvo garante schema enforcement e versionamento compartilhado?",
    "options": {
     "A": "Delta com `schema_evolution = true`",
     "B": "Iceberg com default catalog registrado no workspace",
     "C": "Parquet com schema inferido por Glue/Hive metastore",
     "D": "Iceberg com UC catalog e shared warehouse access"
    },
    "answer": "D",
    "explanation": "Iceberg com UC catalog garante schema enforcement, versionamento compartilhado, e integração com OpenSharing. Delta + OpenSharing funciona, mas Iceberg é mais robusto para multi-workspace.",
    "optExpl": {
     "A": "Errado: Delta com `schema_evolution` não garante schema enforcement compartilhado entre workspaces.",
     "B": "Errado: Iceberg sem UC não garante versionamento compartilhado entre workspaces (OpenSharing).",
     "C": "Errado: Parquet não tem schema enforcement nativo; metastore não escala bem para OpenSharing.",
     "D": "Correto: Iceberg + UC catalog garante schema enforcement, versionamento compartilhado e integração OpenSharing."
    }
   },
   {
    "id": 12,
    "domain": "Data Manipulation",
    "topic": "Deletion Vectors",
    "text": "Uma tabela Delta tem 10B de registros. Você precisa deletar 100M de linhas (1% do total). Sem deletion vectors, qual seria o impacto de uma query após o DELETE?",
    "options": {
     "A": "Query lê apenas as linhas não-deletadas (rewrite automático)",
     "B": "Query lê todas as 10B linhas e filtra no Photon layer",
     "C": "Query lê 10B linhas e avalia deletion bitmap para cada linha",
     "D": "Rewrite imediato reescreve todos os arquivos em segundos"
    },
    "answer": "C",
    "explanation": "Sem deletion vectors, Spark mantém bitmap de deletados e avalia cada linha (quer dizer, reading 10B linhas com filter bitmap). Com DV, skips deleted files fisicamente.",
    "optExpl": {
     "A": "Errado: sem deletion vectors, rewrite automático não ocorre ao deletar linhas.",
     "B": "Errado: Photon não filtra bitmap de deletados; Spark mantém bitmap para filtrar.",
     "C": "Correto: sem deletion vectors, Spark avalia bitmap de deletados para cada linha das 10B linhas lidas.",
     "D": "Errado: DELETE sem deletion vectors não reescreve imediatamente todos os arquivos."
    }
   },
   {
    "id": 13,
    "domain": "Monitoring and Alerting",
    "topic": "Predictive Optimization",
    "text": "Seu warehouse recebe queries que variam bastante em padrão (sometimes OLTP, sometimes OLAP). Qual métrica de Predictive Optimization indica que é necessário ajustar hint de compaction ou liquid clustering?",
    "options": {
     "A": "Query latency degradation > 20% vs baseline",
     "B": "Scan bytes vs bytes skipped ratio abaixo de 5%",
     "C": "Liquid clustering overhead (shuffle bytes) superando benefício de pruning",
     "D": "Previsão de query time degradation baseado em access patterns históricos"
    },
    "answer": "D",
    "explanation": "Predictive Optimization prevê degradação de query latency baseado em histórico, indicando quando reordenação/compaction é necessária.",
    "optExpl": {
     "A": "Errado: degradação de latency é sintoma observado, não métrica preditiva de Predictive Optimization.",
     "B": "Errado: é métrica reativa de pruning ineficaz, não preditiva que recomenda reordenação.",
     "C": "Errado: é sintoma específico de overhead, não métrica geral de Predictive Optimization.",
     "D": "Correto: Predictive Optimization prevê degradação de query latency baseado em histórico de access patterns."
    }
   },
   {
    "id": 14,
    "domain": "Cost & Performance Optimization",
    "topic": "Delta Cache",
    "text": "Uma tabela dimension (`dim_product`) é consultada em 95% das queries do warehouse. Qual configuração maximiza Delta Cache hit rate e reduz custo?",
    "options": {
     "A": "`spark.databricks.io.cache.enabled = true` para todos os clusters",
     "B": "SQL Warehouse com cache enabled + `spark.databricks.io.cache.type = MEMORY` no warehouse settings",
     "C": "Forçar tabela em broadcast com `BROADCAST(dim_product)` em cada query",
     "D": "Replicar dimension em MEMORY-ONLY table e referenciar via alias"
    },
    "answer": "B",
    "explanation": "SQL Warehouse com cache enabled + `MEMORY` type maximiza hit rate. Broadcast é para joins específicos, não para dimension caching geral.",
    "optExpl": {
     "A": "Errado: apenas habilita cache globalmente; não otimiza hit rate especificamente para SQL Warehouse.",
     "B": "Correto: SQL Warehouse com cache enabled + tipo MEMORY é otimizado especificamente para dimension caching.",
     "C": "Errado: broadcast é para joins específicos; não é mecanismo de dimension caching contínuo.",
     "D": "Errado: MEMORY-ONLY tables não persistem dados entre queries (cache é por sessão)."
    }
   },
   {
    "id": 15,
    "domain": "Data Security and Compliance",
    "topic": "Data Redaction at Scale",
    "text": "Você precisa redactar PII (email, CPF) em 200+ tabelas de um catálogo que é acessado por múltiplos grupos. Qual abordagem é mais sustentável?",
    "options": {
     "A": "Criar views com PII mascaradas para cada combinação de tabela × grupo",
     "B": "Usar UC column masks declarativas + ABAC para aplicar em bulk",
     "C": "Aplicar schema evolution com UDFs de mascaramento inline",
     "D": "ETL separado que cria tabelas \"safe\" sem PII para grupos externos"
    },
    "answer": "B",
    "explanation": "UC column masks declarativas + ABAC escalam melhor que views manuais. Views = N tabelas × M grupos.",
    "optExpl": {
     "A": "Errado: combinatória de 200+ tabelas × múltiplos grupos é combinatorialmente insustentável.",
     "B": "Correto: UC column masks + ABAC escalam com policies declarativas aplicadas em bulk.",
     "C": "Errado: replicação de dados causa duplicação e complexidade exponencial de manutenção.",
     "D": "Errado: UDFs em cada query não escalam bem e são difíceis de manter consistentemente."
    }
   },
   {
    "id": 16,
    "domain": "Debugging and Deploying",
    "topic": "Declarative Automation Bundles",
    "text": "Você está migrando múltiplos jobs SQL de Data Factory para Databricks. Qual abordagem reduz manual toil e permite version control + retry automático?",
    "options": {
     "A": "Registrar jobs via Databricks REST API com Python loop",
     "B": "Usar Declarative Automation Bundles (DAB) em YAML com Git sync",
     "C": "Copiar notebook SQL para workspace e triggerar via webhook",
     "D": "Usar Azure Data Factory linked service com Databricks notebook activity"
    },
    "answer": "B",
    "explanation": "Declarative Automation Bundles (DAB) em YAML + Git sync é o padrão moderno para version control + deployment automático.",
    "optExpl": {
     "A": "Errado: REST API loop não fornece version control e requer manual toil repetido.",
     "B": "Correto: Declarative Automation Bundles (DAB) em YAML + Git sync permite version control e deployment automático.",
     "C": "Errado: copiar notebooks + webhooks não fornece version control adequado.",
     "D": "Errado: manter dependência de Azure Data Factory não reduz toil significativamente."
    }
   },
   {
    "id": 17,
    "domain": "Debugging and Deploying",
    "topic": "Cluster Init Script Error",
    "text": "Um init script que instala package `xgboost` está falhando silenciosamente. O cluster inicia, mas jobs falham com ImportError. Qual é a causa MAIS provável e fix?",
    "options": {
     "A": "`pip` não está no PATH — use `/usr/bin/python3 -m pip install`",
     "B": "Init script retorna erro mas cluster `exit 0` mesmo assim — adicione `set -e` e fail-fast",
     "C": "XGBoost instalado por user, não por root — adicione `sudo` antes de pip",
     "D": "Package não compatível com DBR version — verificar pypi e testar localmente"
    },
    "answer": "B",
    "explanation": "Init script precisa ter `set -e` para falhar-fast. Sem isso, cluster inicia mesmo com erro.",
    "optExpl": {
     "A": "Errado: se pip não estivesse no PATH, comando falharia imediatamente, não silenciosamente.",
     "B": "Correto: init script sem `set -e` retorna erro mas cluster inicia mesmo assim (exit 0 implícito).",
     "C": "Errado: permissões de root vs user são secundárias; problema principal é fail-fast.",
     "D": "Errado: incompatibilidade de package causaria erro durante instalação, não silenciosamente na startup."
    }
   },
   {
    "id": 18,
    "domain": "Data Modeling",
    "topic": "Fact Table Grain",
    "text": "Uma fact table (`events`) tem grain `[timestamp, event_type, user_id]`, mas recebe late-arriving corrections para eventos de 48h atrás com campo `is_correction = true`. Qual design evita duplicação na agregação?",
    "options": {
     "A": "Inserir corrections como novas linhas; MV agrupa `is_correction = false` apenas",
     "B": "MERGE com `WHEN MATCHED AND is_correction = true THEN UPDATE` baseado em composite key",
     "C": "Separate fact tables: `events_live` + `events_corrections` com UNION em view",
     "D": "Adicionar `_dbt_valid_from` + `_dbt_valid_to` e matrixialize versioned snapshot"
    },
    "answer": "B",
    "explanation": "MERGE com composite key e `is_correction` flag evita duplicação. Insert como nova linha não é agregável facilmente.",
    "optExpl": {
     "A": "Errado: inserir correções como novas linhas causa duplicação e agregação complexa.",
     "B": "Correto: MERGE com composite key e flag `is_correction` atualiza linha existente, evitando duplicação.",
     "C": "Errado: tabelas separadas + UNION é mais complexo e difícil de agregar sem duplicação.",
     "D": "Errado: versionamento com timestamps é overkill para tratamento de late corrections simples."
    }
   },
   {
    "id": 19,
    "domain": "Developing Code",
    "topic": "AI Functions",
    "text": "Você precisa classificar centenas de milhões de textos (`description`) entre 5 categorias usando um LLM. Qual abordagem combina performance e custo?",
    "options": {
     "A": "`ai_query('gpt-4o', 'Categorize: {description}')` em aplicação Python com batch API",
     "B": "`ai_query()` UDF com `api_key` armazenada em UC secret, aplicado via Spark SQL batched",
     "C": "API chamada via `requests` em Python UDF com cache de embedding",
     "D": "Fine-tuned model local em Databricks Model Registry, inferência via model serving endpoint"
    },
    "answer": "B",
    "explanation": "`ai_query()` UDF com secret + batch processing escala. GPT-4 direct seria mais caro; local model é alternativa mas LLM é mais flexible.",
    "optExpl": {
     "A": "Errado: chamadas Python batch API não escalam para centenas de milhões (latência por API).",
     "B": "Correto: UDF SQL Spark com batching + UC secrets escala para centenas de milhões com performance otimizada.",
     "C": "Errado: Python UDF sem Arrow vectorization; cache de embedding requer setup adicional.",
     "D": "Errado: fine-tuned model local é overkill para classificação em 5 categorias simples."
    }
   },
   {
    "id": 20,
    "domain": "Data Ingestion & Acquisition",
    "topic": "AUTO CDC + SCD",
    "text": "Você tem uma tabela `customer_snapshot` que é sobrescrita diariamente com full extract de um ERP. Qual setup captura automáticamente mudanças (insert, update, delete) sem CDC complexo?",
    "options": {
     "A": "MERGE com `WHEN NOT MATCHED BY SOURCE THEN DELETE` + `stored_as_scd_type(1)`",
     "B": "CDF (Change Data Feed) + job que compara snapshots e gera delta",
     "C": "`AUTO` MERGE com SCD logic: `stored_as_scd_type(2)` captura insert/update/delete",
     "D": "Lakeflow Connect auto-CDC com snapshot compare e DELETE tracking"
    },
    "answer": "C",
    "explanation": "`AUTO` MERGE com `stored_as_scd_type` automatiza CDC sem external tools.",
    "optExpl": {
     "A": "Errado: MERGE com `WHEN NOT MATCHED BY SOURCE` não automatiza CDC de snapshot completo.",
     "B": "Errado: CDF + job customizado que compara snapshots não é automático; requer orquestração.",
     "C": "Correto: `AUTO` MERGE com `stored_as_scd_type(2)` automatiza CDC para comparação de snapshot.",
     "D": "Errado: Lakeflow Connect é para ingestão; não é mecanismo de transformação SCD."
    }
   },
   {
    "id": 21,
    "domain": "Data Manipulation",
    "topic": "CDF + Streaming",
    "text": "Uma tabela Delta tem CDF habilitado. Você quer consumir deletes via Structured Streaming `readStream` para manter cache invalidation. Qual opção funciona?",
    "options": {
     "A": "`spark.readStream.format('delta').option('withChangeDataFeed', true)`",
     "B": "`spark.readStream.format('delta').table('my_table').withColumn('_change_type')`",
     "C": "`spark.readStream.format('delta').option('startVersion', 0).load()` com CDF enabled no catalog",
     "D": "Opção A, mas requer CDC to be enabled beforehand via `ALTER TABLE ... SET TBLPROPERTIES`"
    },
    "answer": "D",
    "explanation": "CDF deve estar habilitado via `ALTER TABLE ... SET TBLPROPERTIES`. Depois, `readStream` com CDF option funciona.",
    "optExpl": {
     "A": "Errado: opção A funciona mas requer CDF habilitado beforehand (não menciona pré-requisito).",
     "B": "Errado: adição de coluna não lê CDF; apenas cria coluna artificial.",
     "C": "Errado: sintaxe incorreta para consumir CDF via readStream.",
     "D": "Correto: opção A é correta mas requer `ALTER TABLE ... SET TBLPROPERTIES` para habilitar CDF beforehand."
    }
   },
   {
    "id": 22,
    "domain": "Monitoring and Alerting",
    "topic": "Data Freshness SLA",
    "text": "Uma tabela que deveria ser atualizada a cada 4 horas não foi atualizada em 6 horas. Qual alerta dispara?",
    "options": {
     "A": "Lakeview dashboard com `CASE WHEN age_minutes > 240 THEN 'ALERT'`",
     "B": "UC Metric View que monitora `table_modified_time` + Databricks Alert SQL",
     "C": "Background refresh on Streaming Table com timeout",
     "D": "Ambas A e B; B é mais inteligente"
    },
    "answer": "B",
    "explanation": "UC Metric Views + Alert SQL é forma native Databricks. Dashboard é passivo; B é mais inteligente.",
    "optExpl": {
     "A": "Errado: dashboard Lakeview é passive; não dispara alerta automático em violação de SLA.",
     "B": "Correto: UC Metric View + Databricks Alert SQL dispara alerta automático em violação de SLA.",
     "C": "Errado: ST background refresh com timeout não é feature; não dispara alerta.",
     "D": "Errado: A é passive; D está incorreto pois A não dispara alerta."
    }
   },
   {
    "id": 23,
    "domain": "Cost & Performance Optimization",
    "topic": "Liquid Clustering vs Partitioning",
    "text": "Uma tabela `transactions` tem 100B linhas, querida por `user_id`, `date`, e `amount` em diferentes combinações. Particionamento por `date` deixava muitos date folders vazios. Qual é a melhor alternativa?",
    "options": {
     "A": "`CLUSTER BY user_id, date` com recompactação manual semanal",
     "B": "Liquid clustering: `CLUSTER BY user_id, date, amount`; Predictive Optimization reordena automaticamente",
     "C": "Particionamento dinâmico com `PARTITION BY (user_id, date)`",
     "D": "Hash bucketing: `BUCKETED BY (user_id) INTO 1024 BUCKETS`"
    },
    "answer": "B",
    "explanation": "Liquid clustering + Predictive Optimization escala melhor que particionamento fixo para queries variadas.",
    "optExpl": {
     "A": "Errado: recompactação manual semanal é tedioso e não escala bem para múltiplos patterns.",
     "B": "Correto: liquid clustering + Predictive Optimization reordena automaticamente, reduzindo custo.",
     "C": "Errado: particionamento dinâmico não resolve problema de folders vazios.",
     "D": "Errado: hash bucketing é Legacy e não combina bem com Databricks moderna."
    }
   },
   {
    "id": 24,
    "domain": "Data Security and Compliance",
    "topic": "GDPR / Right to Erasure",
    "text": "Uma query `DELETE FROM customers WHERE customer_id = ?` é executada para atender solicitação de erasure. Qual Databricks feature garante que cópias antigas (backups, versioning) também respeitem a deleção?",
    "options": {
     "A": "Deletion vectors apenas marcam como deleted em versão atual",
     "B": "UC recycle bin (28 dias) retém dados deletados; usar `PURGE` + permanente delete com data retention policy",
     "C": "Configurar `delta.deletedFileRetentionDuration = 0` e executar `VACUUM` imediatamente",
     "D": "Todas as anteriores em combinação: PURGE + VACUUM(0) + disable recycle bin"
    },
    "answer": "D",
    "explanation": "Todas são combinadas: PURGE (table), VACUUM(0 days), disable recycle bin para erasure compliance.",
    "optExpl": {
     "A": "Errado: deletion vectors apenas marcam como deleted; não elimina dados antigos automaticamente.",
     "B": "Errado: recycle bin retém dados se não for desabilitado; não é suficiente sozinho.",
     "C": "Errado: `delta.deletedFileRetentionDuration = 0` + VACUUM sozinho não garante compliance (recycle bin retém).",
     "D": "Correto: combinação de PURGE + VACUUM(0) + desabilitar recycle bin garante deleção permanente para GDPR."
    }
   },
   {
    "id": 25,
    "domain": "Developing Code",
    "topic": "foreachBatch + State Management",
    "text": "Você está usando `foreachBatch` em Structured Streaming para escrever em Delta. Você precisa garantir que microbatch reiniciado (após falha) não cause duplicação na tabela. Como?",
    "options": {
     "A": "Usar `mergeSchema = true` e confiar em transactions auto",
     "B": "Idempotent sink com `MERGE` baseado em composite key + checkpoint recovery",
     "C": "Disabilitar retry no batch sink",
     "D": "Registrar batch ID antes de escrever, verificar depois"
    },
    "answer": "B",
    "explanation": "Idempotent sink com MERGE baseado em composite key + checkpoint recovery previne duplicação em retry.",
    "optExpl": {
     "A": "Errado: `mergeSchema` e transactions auto não previnem duplicação em retry de microbatch.",
     "B": "Correto: MERGE idempotente com composite key + checkpoint recovery previne duplicação em retry.",
     "C": "Errado: desabilitar retry não é solução; pode perder dados em falha de sink.",
     "D": "Errado: check-after é race condition; não é atomicamente seguro em retry."
    }
   },
   {
    "id": 26,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Streaming Table Maintenance",
    "text": "Um Streaming Table (ST) em Unity Catalog que ingere de Kafka tem checkpoint que cresceu para 100 GB em 30 dias. Qual é causa e solução?",
    "options": {
     "A": "State accumulation sem watermark; adicionar watermark + state retention time",
     "B": "Checkpoint não é limpo automaticamente; rodar `ALTER TABLE ... RESET CHECKPOINT`",
     "C": "ST recompilation overhead; redesenhar query",
     "D": "Iceberg metadata acumula; rodar `VACUUM` regularmente"
    },
    "answer": "A",
    "explanation": "Watermark + state retention time controla checkpoint growth.",
    "optExpl": {
     "A": "Correto: state accumulation sem watermark causa checkpoint crescer indefinidamente; adicionar watermark + state retention.",
     "B": "Errado: `RESET CHECKPOINT` não é solução; faria perder estado e causar reprocessamento.",
     "C": "Errado: recompilation overhead não é causa de checkpoint crescer 100 GB.",
     "D": "Errado: Iceberg VACUUM não afeta tamanho de ST checkpoint."
    }
   },
   {
    "id": 27,
    "domain": "Data Manipulation",
    "topic": "MERGE vs INSERT OVERWRITE Performance",
    "text": "Você precisa fazer SCD Type 1 (sobrescrever customer.email) em 500M linhas usando 100K registros de updates. Qual é mais eficiente?",
    "options": {
     "A": "`INSERT OVERWRITE` a tabela inteira após join com updates",
     "B": "`MERGE INTO customer WHEN MATCHED THEN UPDATE SET email` limitado a 100K linhas afetadas",
     "C": "DELETE updates, depois INSERT novos; VACUUM",
     "D": "Batch insert com window function e row_number reparticionamento"
    },
    "answer": "B",
    "explanation": "MERGE limitado a 100K atualizações é mais eficiente que reescrever 500M linhas.",
    "optExpl": {
     "A": "Errado: `INSERT OVERWRITE` reescreve 500M linhas; ineficiente para 100K updates.",
     "B": "Correto: MERGE focado apenas em 100K linhas é mais eficiente que reescrever tabela inteira.",
     "C": "Errado: DELETE + INSERT + VACUUM = 3 operações; mais I/O que MERGE único.",
     "D": "Errado: reparticionamento com window function é overhead desnecessário."
    }
   },
   {
    "id": 28,
    "domain": "Monitoring and Alerting",
    "topic": "SQL Warehouse Autoscaling",
    "text": "Um SQL Warehouse tem autoscaling de 2–10 clusters. Queries levam 2 minutos mas durante off-peak ficam lentas (30 segundos, 2 clusters). Como diagnosticar se é queueing vs compute?",
    "options": {
     "A": "Verificar `query_queue_time` vs `query_duration` nas warehouse stats",
     "B": "Olhar `execution_status = QUEUED` vs `RUNNING` no query history",
     "C": "Ambas, mas especialmente o Queue time de 5+ minutos indica falta de clusters",
     "D": "Revisar autoscaling policy (`spark.databricks.cluster.profile = singleNode` está errado)"
    },
    "answer": "C",
    "explanation": "Ambas métricas são necessárias; queue time > 5 minutos indica falta de clusters.",
    "optExpl": {
     "A": "Correto: `query_queue_time` vs `query_duration` revela se gargalo é queueing.",
     "B": "Correto: `execution_status` QUEUED vs RUNNING identifica queueing.",
     "C": "Correto: ambas métricas necessárias; queue time > 5 minutos indica falta de clusters.",
     "D": "Errado: `singleNode` é legítimo para workloads apropriados."
    }
   },
   {
    "id": 29,
    "domain": "Cost & Performance Optimization",
    "topic": "Serverless SQL Warehouse",
    "text": "Você tem uma dashboard que roda 20 queries/minuto durante business hours. Qual opção reduz custo para \"pay-per-query\"?",
    "options": {
     "A": "Serverless SQL Warehouse com `spot_instances = true`",
     "B": "Classic cluster com autoscaling desabilitado e 1 single-node",
     "C": "Serverless SQL Warehouse (managed compute, sem gerenciar cluster)",
     "D": "Photon-enabled classic warehouse com `spark.databricks.photon.ml.enabled = true`"
    },
    "answer": "C",
    "explanation": "Serverless SQL Warehouse é \"pay-per-query\" com compute managed automaticamente. Spot instances não existem em serverless.",
    "optExpl": {
     "A": "Errado: serverless não tem opção `spot_instances`; compute é managed automaticamente.",
     "B": "Errado: classic cluster não é pay-per-query; pagaria por node ativo contínuamente.",
     "C": "Correto: serverless SQL Warehouse é pay-per-query com compute managed e não requer gerenciamento.",
     "D": "Errado: classic warehouse requer gerenciamento contínuo; não é pay-per-query."
    }
   },
   {
    "id": 30,
    "domain": "Debugging and Deploying",
    "topic": "Spark SQL Query Plan",
    "text": "Um `EXPLAIN PLAN` mostra `BroadcastHashJoin` em uma dimensão de 5B linhas. Qual é o problema e fix?",
    "options": {
     "A": "Join order está errado; usar `/*+ BROADCAST(dim) */` hint",
     "B": "Broadcast falha em tabelas > 1 GB; usar `SortMergeJoin` em vez disso",
     "C": "Broadcast precisa caber em executor memory; aumentar `spark.sql.autoBroadcastJoinThreshold`",
     "D": "Ambas B e C; desabilitar broadcast automático e usar SortMergeJoin + skew hints"
    },
    "answer": "D",
    "explanation": "Broadcast em tabelas > 1 GB falha. SortMergeJoin + skew hints é alternativa.",
    "optExpl": {
     "A": "Errado: hint BROADCAST em 5B linhas falha; broadcast não resolve o problema.",
     "B": "Correto: broadcast falha em tabelas > ~1GB; alternativa é SortMergeJoin.",
     "C": "Correto: broadcast precisa caber em executor memory; aumentar threshold não garante cabe.",
     "D": "Correto: desabilitar broadcast automático + usar SortMergeJoin + skew hints é abordagem correta."
    }
   },
   {
    "id": 31,
    "domain": "Data Security and Compliance",
    "topic": "Clean Rooms",
    "text": "Você quer compartilhar dados com parceiro externo, mas sem expor identidades individuais — apenas agregações. Qual solução Databricks permite isso?",
    "options": {
     "A": "Exportar CSV para partner",
     "B": "UC schema compartilhado com row filters aplicando agregação",
     "C": "Databricks Clean Rooms: SQL analytics compartilhado sem dados raw expostos",
     "D": "Delta share com external recipient (OpenSharing)"
    },
    "answer": "C",
    "explanation": "Clean Rooms permite SQL analytics compartilhado sem exposição de dados raw.",
    "optExpl": {
     "A": "Errado: exportar CSV expõe dados raw; inseguro para analytics compartilhado.",
     "B": "Errado: row filters não agregam automaticamente; ainda expõe dados granulares.",
     "C": "Correto: Databricks Clean Rooms permite SQL analytics compartilhado sem exposição de dados raw.",
     "D": "Errado: Delta share com external recipient expõe dados raw; não é clean room."
    }
   },
   {
    "id": 32,
    "domain": "Developing Code",
    "topic": "Window Function + Partitioning",
    "text": "Uma query calcula `ROW_NUMBER()` OVER `(PARTITION BY user_id ORDER BY timestamp)`. Qual risco existe se partições forem muito desbalanceadas?",
    "options": {
     "A": "Window function não funciona com partições",
     "B": "Data skew causa straggler task; uma partição com 1B rows paralisa job",
     "C": "Resultado está correto, mas latency aumenta",
     "D": "Ambas B e C; considerar reparticionamento ou approx_percentile para estimativa"
    },
    "answer": "D",
    "explanation": "Data skew causa straggler + latency aumenta (task com 1B rows paralisa job).",
    "optExpl": {
     "A": "Errado: window functions funcionam com partições; é o propósito delas.",
     "B": "Correto: data skew causa straggler task; partição com 1B rows paralisa job.",
     "C": "Correto: resultado está correto mas latência aumenta devido a straggler.",
     "D": "Correto: ambas B e C; reparticionamento ou sampling mitiga impacto de skew."
    }
   },
   {
    "id": 33,
    "domain": "Developing Code",
    "topic": "Schema Drift Detection",
    "text": "Uma ingestão de Kafka com Structured Streaming detecta novo campo `new_field` em evento JSON. Default behavior: coluna é adicionada como null. Você quer falhar ao invés. Qual opção?",
    "options": {
     "A": "`failOnNewColumnViolation = true`",
     "B": "`mergeSchema = false` (default)",
     "C": "Validar schema com JSON schema validator antes de ingerir",
     "D": "A e C; B está errado"
    },
    "answer": "D",
    "explanation": "A e C funcionam; A com `failOnNewColumnViolation` ou C com validação pre-ingestion.",
    "optExpl": {
     "A": "Correto: `failOnNewColumnViolation = true` faz falhar ao detectar novo campo.",
     "B": "Errado: `mergeSchema = false` é default e permite novo campo como null; não faz falhar.",
     "C": "Correto: validação de JSON schema antes de ingestão faz falhar se schema não match.",
     "D": "Correto: A e C fazem falhar em schema drift; B está errado."
    }
   },
   {
    "id": 34,
    "domain": "Data Manipulation",
    "topic": "VARIANT Type Performance",
    "text": "Você tem coluna VARIANT com 1B de registros. Query `WHERE variant_get(col, ':field') = 'value'` está lento. Qual é likely cause?",
    "options": {
     "A": "Variant indexing não é suportado; criar coluna extracted normal",
     "B": "Full table scan; Variant predicate push-down não funciona eficientemente",
     "C": "Spark não otimiza variant_get() ; reescrever com `get_json_object(to_json(col), '$field')`",
     "D": "Todas as anteriores; melhor extrair campo como coluna durante ingestão"
    },
    "answer": "D",
    "explanation": "Variant predicate push-down é limitado; melhor extrair campo em ingestão como coluna normal.",
    "optExpl": {
     "A": "Correto: variant indexing não é suportado; indexação em VARIANT é ineficiente.",
     "B": "Correto: full table scan; predicate push-down em VARIANT é limitado.",
     "C": "Correto: Spark não otimiza `variant_get()` eficientemente; reescrever com `get_json_object` ajuda.",
     "D": "Correto: todas as causas são válidas; melhor extrair campo como coluna durante ingestão."
    }
   },
   {
    "id": 35,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Iceberg Table Evolution",
    "text": "Uma tabela Iceberg foi criada com schema `{id: int, name: string}`. Você precisa adicionar `created_date: timestamp`. Como garantir que dados antigos não quebrem?",
    "options": {
     "A": "`ALTER TABLE ... ADD COLUMN created_date TIMESTAMP DEFAULT NULL`",
     "B": "`ALTER TABLE ... ADD COLUMNS (created_date TIMESTAMP NOT NULL)`",
     "C": "Opção A; B falharia em reads de dados antigos (schema mismatch)",
     "D": "Ambas funcionam em Iceberg (schema evolution é suportada)"
    },
    "answer": "C",
    "explanation": "`ADD COLUMN ... DEFAULT NULL` permite schema evolution sem quebrar old data reads.",
    "optExpl": {
     "A": "Correto: `ADD COLUMN ... DEFAULT NULL` permite schema evolution sem quebrar reads de dados antigos.",
     "B": "Errado: `ADD COLUMN ... NOT NULL` falharia em reads de dados antigos (valor faltante).",
     "C": "Correto: A permite evolution; B falharia em schema mismatch com dados legados.",
     "D": "Errado: B não funciona em Iceberg; NOT NULL sem default quebra schema evolution."
    }
   },
   {
    "id": 36,
    "domain": "Developing Code",
    "topic": "Watermark em Multi-Stream Join",
    "text": "Você está fazendo join de dois streams (events e actions) em Structured Streaming. Como garantir que late-arriving events não causem incorrect join?",
    "options": {
     "A": "Adicionar watermark em ambos streams com mesmo delay (ex: 1 hour)",
     "B": "State timeout + recheckpoint; não usar watermark em join",
     "C": "Output mode `append` força join correto",
     "D": "Join não é possível em streams; usar micro-batch MERGE em vez"
    },
    "answer": "A",
    "explanation": "Watermark em ambos streams com mesmo delay garante join correto.",
    "optExpl": {
     "A": "Correto: adicionar watermark em ambos streams com mesmo delay garante join correto.",
     "B": "Errado: state timeout sem watermark é insuficiente para garantir join correto.",
     "C": "Errado: append mode não garante join correto; precisa de watermark.",
     "D": "Errado: join é possível em streams com watermark; não precisa de micro-batch MERGE."
    }
   },
   {
    "id": 37,
    "domain": "Developing Code",
    "topic": "Streaming Performance Degradation",
    "text": "Um Streaming Table que ingeria 100K events/segundo começou a processar apenas 50K/segundo após 3 dias. Qual é provável gargalo?",
    "options": {
     "A": "Kafka consumer lag aumentando — verificar topic partition rebalancing",
     "B": "State size crescendo; watermark ineficaz causando state explosion",
     "C": "Checkpoint overhead; checkpointing cresce com estado não-gerenciado",
     "D": "Ambas B e C; monitorar state bytes no Spark UI"
    },
    "answer": "D",
    "explanation": "Ambas B e C causam degradação; monitorar state bytes no Spark UI via Streaming UI.",
    "optExpl": {
     "A": "Errado: consumer lag seria sintoma visível em Spark UI antes de 3 dias.",
     "B": "Correto: state explosion sem watermark causa throughput degradation.",
     "C": "Correto: checkpoint overhead com estado crescente causa degradação.",
     "D": "Correto: ambas B e C causam degradação; monitorar state bytes no Spark Streaming UI."
    }
   },
   {
    "id": 38,
    "domain": "Cost & Performance Optimization",
    "topic": "Compaction Strategy",
    "text": "Uma tabela recebe 100K inserts/dia via streaming. Sem manual compaction, small files acumulam. Qual abordagem balanceia performance vs cost?",
    "options": {
     "A": "`OPTIMIZE TABLE` diária + `VACUUM`",
     "B": "Liquid clustering + autom compaction via Predictive Optimization",
     "C": "Iceberg auto-compaction com target file size",
     "D": "Ambas A e B; Predictive Optimization é mais eficiente"
    },
    "answer": "D",
    "explanation": "Ambas funcionam; Predictive Optimization é mais automatizada.",
    "optExpl": {
     "A": "Correto: `OPTIMIZE` diária + `VACUUM` balanceia performance vs cost.",
     "B": "Correto: Predictive Optimization automatiza compaction; mais eficiente que manual.",
     "C": "Correto: Iceberg auto-compaction com target file size é automático.",
     "D": "Correto: ambas A e B funcionam; Predictive Optimization é mais eficiente que manual."
    }
   },
   {
    "id": 39,
    "domain": "Data Security and Compliance",
    "topic": "UC Secret Management",
    "text": "Um job precisa de senha para acessar external database. Onde armazenar e recuperar?",
    "options": {
     "A": "Hardcode na notebook (NUNCA)",
     "B": "UC Secrets via `dbutils.secrets.get(scope='db_scope', key='db_password')`",
     "C": "Armazenar em `spark.conf` via cluster environment var",
     "D": "Ambas B e C funcionam; B é mais seguro (audit trail)"
    },
    "answer": "B",
    "explanation": "UC Secrets com audit trail é mais seguro que env vars.",
    "optExpl": {
     "A": "Errado: hardcode em notebook é inseguro; NUNCA usar para credenciais.",
     "B": "Correto: UC Secrets via `dbutils.secrets.get()` é seguro com audit trail.",
     "C": "Errado: environment vars não têm audit trail; menos seguro que UC Secrets.",
     "D": "Errado: C não é recomendado; B é a abordagem segura."
    }
   },
   {
    "id": 40,
    "domain": "Debugging and Deploying",
    "topic": "Notebook Parameter Injection",
    "text": "Um notebook parametrizado recebe `{{date}}` que deveria ser 2024-01-15. Job executa com valor literal `{{date}}`. Qual é o problema?",
    "options": {
     "A": "Databricks não interpola literals em jobs; usar `dbutils.widgets` em vez",
     "B": "Job config passando valor errado; verificar `job_parameters` no job definition",
     "C": "Notebook criado via Git Folder; Git Folder não interpola parameters",
     "D": "Ambas A e B; prefira `dbutils.widgets.get()` com default"
    },
    "answer": "B",
    "explanation": "Job config precisa passar parameter corretamente. Notebook parametrizado usa `dbutils.widgets.get()` para receber valor.",
    "optExpl": {
     "A": "Errado: interpolação pode funcionar se job config estiver correta.",
     "B": "Correto: problema é job config passando valor literal em vez de parâmetro dinâmico.",
     "C": "Errado: Git Folder não afeta interpolação de parâmetros de job.",
     "D": "Errado: A está errado; apenas B é correto."
    }
   },
   {
    "id": 41,
    "domain": "Data Governance",
    "topic": "Permission Inheritance in UC",
    "text": "Você grantou `USAGE` em um schema. Faz diferença se você depois grantar `SELECT` em table específica? (inheritance perspective)",
    "options": {
     "A": "Ambas precisam de grant; schema USAGE é prerequisite para table access",
     "B": "Table grant herda automáticamente de schema grant; redundante",
     "C": "Schema grant é necessário mas não suficiente; table grant é obrigatório também",
     "D": "Table grant ignora schema grant; apenas table level importa"
    },
    "answer": "C",
    "explanation": "Schema USAGE é pré-requisito; table grant é obrigatório também (não herda automaticamente).",
    "optExpl": {
     "A": "Errado: não exatamente pré-requisite em senso binário estrito.",
     "B": "Errado: table grant não herda automaticamente de schema grant.",
     "C": "Correto: schema USAGE é pré-requisite necessário; table grant também obrigatório (não herda).",
     "D": "Errado: schema grant ainda é necessário mesmo com table grant."
    }
   },
   {
    "id": 42,
    "domain": "Data Manipulation",
    "topic": "Time Travel + Rollback",
    "text": "Você executou acidentalmente `DELETE FROM sales` sem WHERE. Última backup é de 2 horas atrás. Qual Databricks feature permite rollback?",
    "options": {
     "A": "`SELECT * FROM sales@0` (version 0)",
     "B": "`RESTORE TABLE sales TO VERSION x` onde x = versão 2 horas atrás",
     "C": "Delta time travel: `SELECT * FROM sales TIMESTAMP AS OF '...'` mas como restore?",
     "D": "Rodar `RESTORE` + recycle bin (28 dias), mas talvez seja tarde se VACUUM foi rodado"
    },
    "answer": "B",
    "explanation": "`RESTORE TABLE ... TO VERSION x` ou `SELECT ... TIMESTAMP AS OF` para time-travel. RESTORE escreve versão nova; time-travel é read-only.",
    "optExpl": {
     "A": "Errado: sintaxe correta é `SELECT * FROM sales VERSION AS OF 0`; `@0` não é padrão.",
     "B": "Correto: `RESTORE TABLE sales TO VERSION x` é forma correta de rollback.",
     "C": "Errado: `TIMESTAMP AS OF` é read-only; não faz restore (não modifica estado).",
     "D": "Errado: se VACUUM foi rodado com retention baixa, dados podem estar permanentemente perdidos."
    }
   },
   {
    "id": 43,
    "domain": "Cost & Performance Optimization",
    "topic": "Table Statistics and Query Planning",
    "text": "Um EXPLAIN mostra `HashAgg` em lugar de `SortAgg`, causando spill. Qual estatística Databricks usa para escolher?",
    "options": {
     "A": "`spark.sql.statistics.histogramEnabled = true` com histogram accuracy",
     "B": "`table_stats` (rowCount, totalSize) armazenado em metastore",
     "C": "Ambas; `ANALYZE TABLE ... COMPUTE STATISTICS` popula rowCount/totalSize",
     "D": "Sem stats explícito, Catalyst usa fallback heuristics (pode ser subóptimo)"
    },
    "answer": "C",
    "explanation": "`ANALYZE TABLE` popula stats; Catalyst usa stats para escolher agregação strategy.",
    "optExpl": {
     "A": "Errado: histogram é adicional; não é suficiente sozinho para escolher agregação strategy.",
     "B": "Correto: `table_stats` (rowCount, totalSize) armazenado em metastore usado por Catalyst.",
     "C": "Correto: `ANALYZE TABLE` popula rowCount/totalSize; Catalyst usa para escolher estratégia.",
     "D": "Errado: fallback heuristics pode ser subóptimo; stats explícitas são melhores."
    }
   },
   {
    "id": 44,
    "domain": "Data Manipulation",
    "topic": "Aggregate Functions + Null Handling",
    "text": "Query: `SELECT SUM(amount) FROM sales WHERE amount IS NOT NULL`. Versão alternativa: `SELECT SUM(COALESCE(amount, 0)) FROM sales`. Qual é correta?",
    "options": {
     "A": "Primeira está correta; segunda soma 0s como fake values",
     "B": "Segunda está correta; não muda resultado se não há nulls",
     "C": "Ambas corretas se schema garante NO NULL; primeira é mais legível",
     "D": "Depende de negócio: primeira ignora nulls, segunda trata como 0"
    },
    "answer": "D",
    "explanation": "Primeira ignora nulls; segunda trata como 0. Negócio decide; primeira é mais típica.",
    "optExpl": {
     "A": "Errado: depende de negócio; não é simplesmente \"primeira está correta\".",
     "B": "Errado: primeira é mais típica para SUM; segunda trata NULL como 0 (semântica diferente).",
     "C": "Errado: D está mais correto pois reconhece diferença semântica.",
     "D": "Correto: primeira ignora NULLs; segunda trata como 0; negócio decide semântica correta."
    }
   },
   {
    "id": 45,
    "domain": "Monitoring and Alerting",
    "topic": "Job Failure Alerting",
    "text": "Um job SQL que executa MERGE em tabela crítica falha silenciosamente. Você só descobre quando BI relata falta de dados. Como alertar pro-ativamente?",
    "options": {
     "A": "Job notification settings + Slack webhook",
     "B": "Databricks Alerts feature (SQL workspace ou job completion check)",
     "C": "Query no `system.jobs.runs` que monitora `state = FAILED` e envia alerta",
     "D": "Todas; B é mais robusta (managed)"
    },
    "answer": "D",
    "explanation": "Todas; B (Alerts) é mais robusta.",
    "optExpl": {
     "A": "Correto: job notification + Slack webhook funciona para alertas.",
     "B": "Correto: Databricks Alerts feature é mais robusta e gerenciada.",
     "C": "Correto: query em `system.jobs.runs` funciona manualmente.",
     "D": "Correto: todas funcionam; B (Alerts managed) é mais robusta."
    }
   },
   {
    "id": 46,
    "domain": "Cost & Performance Optimization",
    "topic": "Column Mask Performance Impact",
    "text": "Dados sensíveis (SSN) precisam ser mascarados. Aplicar column masks em 500+ queries impacta performance? Como otimizar?",
    "options": {
     "A": "Usar `AES_ENCRYPT()` UDF no ingress (pré-computado, sem overhead)",
     "B": "UC column masks com statistics + pruning; negligible overhead se predicate push-down funciona",
     "C": "Materialized view com dados pre-masked para queries críticas + column masks para o resto",
     "D": "Ambas B e C; B para performance, C para query acceleration"
    },
    "answer": "D",
    "explanation": "Column masks têm overhead negligível com predicate push-down; MVs materialized fornecem query acceleration quando crítico.",
    "optExpl": {
     "A": "Correto: `AES_ENCRYPT()` pré-computado no ingress evita overhead em runtime.",
     "B": "Correto: UC column masks com predicate push-down têm overhead negligível.",
     "C": "Correto: MV pre-masked fornece query acceleration para queries críticas.",
     "D": "Correto: ambas estratégias; B para casos gerais, C para queries críticas."
    }
   },
   {
    "id": 47,
    "domain": "Developing Code",
    "topic": "Job Cluster vs Attached Cluster",
    "text": "Um job está anexado a um cluster all-purpose que também roda notebooks interativas. Qual risco existe?",
    "options": {
     "A": "Job pode ser cancelado se usuário interromper notebook",
     "B": "Job e notebook competem por recursos",
     "C": "Job pode afetar Spark context do notebook (UI limpa, cache)",
     "D": "Todas as anteriores; usar job cluster (isolated, dedicated) para reliability"
    },
    "answer": "D",
    "explanation": "Todas as anteriores; job cluster é isolado e dedicado.",
    "optExpl": {
     "A": "Correto: job pode ser cancelado se usuário interromper notebook all-purpose.",
     "B": "Correto: job e notebook competem por recursos do mesmo cluster.",
     "C": "Correto: job pode afetar Spark context (UI limpa, cache compartilhado).",
     "D": "Correto: todas anteriores; usar job cluster (isolado, dedicado) garante reliability."
    }
   },
   {
    "id": 48,
    "domain": "Developing Code",
    "topic": "PySpark vs Pandas Performance",
    "text": "Uma operação `df.groupBy('category').agg(sum('amount')).collect()` em 100M linhas. PySpark mostra 30 segundos; reescrever em Pandas UDF: 5 segundos. Por quê?",
    "options": {
     "A": "PySpark tem overhead de serialização Python; Pandas UDF usa Arrow + vectorized execution",
     "B": "Pandas UDF é otimizado pelo Catalyst planner",
     "C": "PySpark não paralela; Pandas UDF sim",
     "D": "Ambas A e B; prefira Pandas UDF para operações Python-heavy"
    },
    "answer": "D",
    "explanation": "Arrow serialization + vectorized execution torna Pandas UDF 6x mais rápida em operações Python.",
    "optExpl": {
     "A": "Correto: PySpark tem overhead de serialização Python; Arrow reduz overhead.",
     "B": "Correto: Catalyst otimiza Pandas UDF com Arrow vectorization.",
     "C": "Errado: PySpark paralela também; diferença é vectorization.",
     "D": "Correto: ambas A e B; Pandas UDF 6x mais rápido em operações Python-heavy."
    }
   },
   {
    "id": 49,
    "domain": "Developing Code",
    "topic": "SQL Dynamic Query Generation",
    "text": "Um relatório precisa agrupar dinamicamente por coluna escolhida pelo usuário (ex: `group_by = 'region'`). Qual abordagem é segura vs SQL injection?",
    "options": {
     "A": "Concatenar string: `SELECT {group_by}, SUM(amount) FROM sales GROUP BY {group_by}`",
     "B": "Usar parameterized query: `CONCAT('GROUP BY ', group_by)` em prepared statement",
     "C": "SQL validation + allowlist de colunas; ou usar framework que valida",
     "D": "Ambas B e C; C é mais robusta; A é perigosa (SQL injection risk)"
    },
    "answer": "D",
    "explanation": "Ambas; C (allowlist) é mais robusto que concatenação direta (SQL injection risk).",
    "optExpl": {
     "A": "Errado: concatenação string é SQL injection risk; perigoso em produção.",
     "B": "Errado: CONCAT ainda não é parameterizado; SQL injection risk persiste.",
     "C": "Correto: SQL validation com allowlist de colunas previne injection.",
     "D": "Correto: reconhece que C é mais robusto; A é perigoso."
    }
   },
   {
    "id": 50,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Streaming Ingestion Performance",
    "text": "Um stream de 1M eventos/segundo é ingerido em Delta via Structured Streaming. Latency está aumentando. Qual é a causa MAIS provável?",
    "options": {
     "A": "Spark executor memory overflow",
     "B": "State accumulation sem watermark; checkpointing cresce indefinidamente",
     "C": "Partition count baixo; dados acumulam em partições",
     "D": "Delta schema evolution a cada micro-batch"
    },
    "answer": "B",
    "explanation": "Sem watermark, estado cresce indefinidamente; checkpoints crescem com estado.",
    "optExpl": {
     "A": "Errado: executor memory overflow causaria crash imediato, não degradação gradual.",
     "B": "Correto: state accumulation sem watermark; checkpoints crescem indefinidamente causando degradação.",
     "C": "Errado: partition count baixo não causa degradação; partições escaláveis.",
     "D": "Errado: schema evolution não causa degradação consistente de throughput."
    }
   },
   {
    "id": 51,
    "domain": "Cost & Performance Optimization",
    "topic": "Index-free Pruning via Iceberg",
    "text": "Uma tabela de eventos com 500B registros é queried por `event_timestamp` (range filters frequentes). Qual abordagem em Iceberg otimiza pruning sem criar índices explícitos?",
    "options": {
     "A": "Particionamento por event_timestamp com `PARTITION BY MONTH(event_timestamp)`",
     "B": "Iceberg manifest prune + statistics (min/max) por arquivo Parquet",
     "C": "Liquid clustering `CLUSTER BY event_timestamp` com Predictive Optimization",
     "D": "Ambas A e B; B é mais eficiente para range queries em high-cardinality"
    },
    "answer": "D",
    "explanation": "Ambas A e B; B (Iceberg statistics + manifest prune) é mais eficiente que particionamento fixo para range queries.",
    "optExpl": {
     "A": "Correto: `PARTITION BY MONTH(event_timestamp)` fornece pruning eficiente.",
     "B": "Correto: Iceberg manifest prune + statistics (min/max) mais eficiente para range queries.",
     "C": "Errado: liquid clustering sem Predictive Optimization não otimiza range queries bem.",
     "D": "Correto: ambas A e B funcionam; B é mais eficiente para high-cardinality range queries."
    }
   },
   {
    "id": 52,
    "domain": "Developing Code",
    "topic": "Encryption Key Management in Spark",
    "text": "Qual é o fluxo correto para usar chaves de criptografia em jobs Spark dentro Databricks?",
    "options": {
     "A": "Hardcoded em `spark.conf`; rotacionadas manualmente",
     "B": "UC Secrets recuperado via `dbutils.secrets.get()` dentro PySpark job",
     "C": "Passar via environment var do cluster (mais inseguro)",
     "D": "Ambas A e B funcionam, mas B é seguro com audit trail"
    },
    "answer": "D",
    "explanation": "UC Secrets em job Spark é a forma segura com audit trail; environment vars são menos seguras.",
    "optExpl": {
     "A": "Errado: hardcode em `spark.conf` é inseguro; sem audit trail.",
     "B": "Correto: UC Secrets via `dbutils.secrets.get()` em PySpark job é seguro com audit trail.",
     "C": "Errado: environment vars não têm audit trail; menos seguro.",
     "D": "Correto: B é forma segura com audit trail; A não é recomendado."
    }
   },
   {
    "id": 53,
    "domain": "Developing Code",
    "topic": "Type Casting Errors",
    "text": "Um cast explícito `CAST(date_string AS DATE)` falha com valor inválido em 1 linha de 1B. Job inteiro falha. Como contornar?",
    "options": {
     "A": "`TRY_CAST(date_string AS DATE)` retorna NULL se inválido, continua job",
     "B": "`CAST(date_string AS DATE FORMAT 'yyyy-MM-dd')` com format hint",
     "C": "`IF(REGEXP_LIKE(...), CAST(...), NULL)` com pré-validação",
     "D": "Ambas A e C funcionam; A é mais legível"
    },
    "answer": "A",
    "explanation": "`TRY_CAST` continua job; A é mais legível.",
    "optExpl": {
     "A": "Correto: `TRY_CAST` retorna NULL se inválido; job continua sem erro.",
     "B": "Errado: format hint não previne erro se valor for inválido (não faz tratamento).",
     "C": "Correto: pré-validação com `IF(REGEXP_LIKE)` funciona mas mais verbose.",
     "D": "Correto: ambas A e C funcionam; A é mais legível e conciso."
    }
   },
   {
    "id": 54,
    "domain": "Data Ingestion & Acquisition",
    "topic": "Multiformat Ingestion with Lakeflow",
    "text": "Você ingere Parquet, JSON, e CSV de S3 em uma tabela Delta. Schema evolui em cada formato. Qual ferramenta simplifica isso?",
    "options": {
     "A": "Spark auto schema inference + `mergeSchema = true`",
     "B": "Lakeflow Connect com auto-schema detection por source format",
     "C": "Databricks Unity Catalog auto-schema propagation",
     "D": "Ambas A e B; B é mais robusto para multi-format"
    },
    "answer": "D",
    "explanation": "Ambas; B (Lakeflow) é mais robusto para multi-format.",
    "optExpl": {
     "A": "Correto: Spark auto schema inference + `mergeSchema = true` funciona para multi-format.",
     "B": "Correto: Lakeflow Connect com auto-schema detection é mais robusto para multi-format.",
     "C": "Errado: UC não faz auto-schema propagation entre diferentes formatos.",
     "D": "Correto: ambas A e B funcionam; B é mais robusto para caso de multi-format complexo."
    }
   },
   {
    "id": 55,
    "domain": "Monitoring and Alerting",
    "topic": "Predictive Optimization Metrics",
    "text": "Qual métrica no Spark UI + Predictive Optimization UI indica que reordenação automática via CLUSTER BY AUTO é necessária?",
    "options": {
     "A": "`shuffle_write_bytes` > 50% dos dados",
     "B": "`scan_bytes` vs `bytes_skipped` ratio indicando poor pruning",
     "C": "Optimizer recommendation score < 80%",
     "D": "Todas as anteriores; B é mais direta"
    },
    "answer": "B",
    "explanation": "Scan vs skipped ratio < 5% indica poor pruning; Predictive Opt recomenda reordenação.",
    "optExpl": {
     "A": "Errado: `shuffle_write_bytes` > 50% é esperado em joins; não indica reordenação necessária.",
     "B": "Correto: ratio de `scan_bytes` vs `bytes_skipped` baixo indica poor pruning requerendo reordenação.",
     "C": "Errado: optimizer recommendation score não é métrica padrão de Predictive Optimization.",
     "D": "Errado: C não é métrica; B é mais direta."
    }
   },
   {
    "id": 56,
    "domain": "Debugging and Deploying",
    "topic": "Incremental Pipeline Issues",
    "text": "Um job que roda `SELECT * FROM src WHERE updated_at > '{{yesterday}}'` falha consistentemente. Você descobre que um cliente atualizou 30 dias de histórico ontem. Qual foi o impacto?",
    "options": {
     "A": "Job só pegou 1 dia de dados; faltam 29 dias",
     "B": "Job pegou todos os 30 dias (correto para late-arriving updates)",
     "C": "Job não retomou; precisa reprocessar tudo",
     "D": "Ambas A e B; incrementalidade não é automática; use MERGE incremental + watermark"
    },
    "answer": "D",
    "explanation": "Incrementalidade requer MERGE com late-arriving support. Simple incremental com timestamp é vulnerable.",
    "optExpl": {
     "A": "Errado: job pegou dados com `updated_at > yesterday`; sim pegou os 30 dias atualizados.",
     "B": "Correto: job pegou todos os 30 dias porque cliente atualizou ontem.",
     "C": "Errado: job retomou; problema é que incremental simples não é robusto.",
     "D": "Correto: incremental simples com timestamp é vulnerável; usar MERGE + watermark."
    }
   },
   {
    "id": 57,
    "domain": "Data Modeling",
    "topic": "Conformed Dimensions",
    "text": "Múltiplas fact tables referenciam `customer` de forma inconsistente (customer_id vs customer_pk, diferentes atributos). Como consolidar sem quebrar histórico?",
    "options": {
     "A": "Criar dimensão conformed `dim_customer` nova; fact tables old e new apontam para ambas",
     "B": "`ALTER TABLE fact_old DROP COLUMN customer_pk; ADD COLUMN customer_id` com MERGE histórico",
     "C": "Recriar fact tables com nova dimensão; versionar old como archive",
     "D": "Ambas A e C; gradualmente migrar via incremental pipeline"
    },
    "answer": "D",
    "explanation": "Ambas; A permite gradual migration sem perder histórico.",
    "optExpl": {
     "A": "Correto: nova dimensão conformed + gradual migration permite histórico intacto.",
     "B": "Errado: modificar fact table histórica com DROP/ADD é risky.",
     "C": "Correto: recriar fact tables funciona mas é mais disruptivo.",
     "D": "Correto: ambas A e C; A é menos disruptivo para migração gradual."
    }
   },
   {
    "id": 58,
    "domain": "Developing Code",
    "topic": "Broadcast Join Optimization",
    "text": "Uma query faz join entre `sales` (100B rows) e `dim_product` (10K rows). Spark escolhe BroadcastNestedLoopJoin (lento). Como forçar BroadcastHashJoin?",
    "options": {
     "A": "`/*+ BROADCAST(dim_product) */` hint",
     "B": "Aumentar `spark.sql.autoBroadcastJoinThreshold` para > 10K MB",
     "C": "Reordenar join: `SELECT * FROM dim_product JOIN sales`",
     "D": "Ambas A e B; A é mais direto"
    },
    "answer": "A",
    "explanation": "`/*+ BROADCAST(dim_product) */` hint força BroadcastHashJoin; B aumenta threshold automaticamente.",
    "optExpl": {
     "A": "Correto: `/*+ BROADCAST(dim_product) */` hint força BroadcastHashJoin.",
     "B": "Errado: aumentar `autoBroadcastJoinThreshold` não força broadcast de 10K (tabela pequena).",
     "C": "Errado: reordenação não garante broadcast de tabela pequena.",
     "D": "Errado: B não é eficaz; apenas A é correto."
    }
   },
   {
    "id": 59,
    "domain": "Cost & Performance Optimization",
    "topic": "Predictive Optimization Fine-Tuning",
    "text": "Uma tabela com Predictive Optimization habilitado tem reordenação automática roda a cada 24h, causando 2h de compute. É excessivo. Qual opção reduz frequência?",
    "options": {
     "A": "Desabilitar Predictive Optimization completamente",
     "B": "Ajustar `OPTIMIZE` frequency via `CREATE TABLE ... TBLPROPERTIES (...)`",
     "C": "Configurar `spark.databricks.predictiveOptimization.maxFrequency = weekly`",
     "D": "Ambas B e C; C é mais eficaz"
    },
    "answer": "C",
    "explanation": "Configurar `spark.databricks.predictiveOptimization.maxFrequency` reduz recompactação excessiva.",
    "optExpl": {
     "A": "Errado: desabilitar Predictive Optimization perde benefício de otimização automática.",
     "B": "Errado: não há property padrão `OPTIMIZE frequency` em CREATE TABLE.",
     "C": "Correto: `spark.databricks.predictiveOptimization.maxFrequency = weekly` reduz frequência.",
     "D": "Errado: B não é correto; apenas C resolve."
    }
   },
   {
    "id": 60,
    "domain": "Data Governance",
    "topic": "UC Asset Inventory",
    "text": "Um auditoria interna precisa listar todas as tables no catálogo, proprietários, última modificação, e data classification (public/private/sensitive). Qual ferramenta fornece isso natively?",
    "options": {
     "A": "Unity Catalog system tables (`system.information_schema.tables`) com tags de classificação",
     "B": "Databricks metadata API + UC tags",
     "C": "Glean search de sistema",
     "D": "Ambas A e B; A é system table query, B é API call com metadados"
    },
    "answer": "A",
    "explanation": "UC system tables (`information_schema.tables`) + tags de classificação fornece inventory nativo.",
    "optExpl": {
     "A": "Correto: UC system tables (`system.information_schema.tables`) + tags fornece inventory nativo com metadados.",
     "B": "Correto: Databricks metadata API + UC tags funciona via API call.",
     "C": "Errado: Glean é search tool; não fornece structured inventory auditável.",
     "D": "Correto: ambas funcionam; A é mais direto para audit structured de asset inventory."
    }
   }
  ]
 },
 "simulado2": {
  "title": "Simulado 2",
  "questions": [
   {
    "id": 1,
    "domain": "Developing Code",
    "topic": "Python/SQL",
    "text": "Um pipeline de Structured Streaming recebe dados de Kafka em batches de 30 segundos. O processamento realiza uma agregação com janelas de 5 minutos sobre `event_timestamp`. Qual configuração garante que eventos atrasados (chegando até 10 minutos após a janela fechar) sejam reprocessados na janela correta?",
    "options": {
     "A": "Aumentar `spark.sql.streaming.forceDeleteTempCheckpointLocation` e definir `outputMode=\"complete\"`",
     "B": "Configurar `watermark(\"event_timestamp\", \"10 minutes\")` antes da agregação e usar `outputMode=\"append\"` com CDF habilitado",
     "C": "Definir `chkpointLocation` com modo `update` e `microBatchMs=10000`",
     "D": "Usar `.option(\"mergeSchema\", \"true\")` e replicar a query em 10 executores"
    },
    "answer": "B",
    "explanation": "Watermark de 10 minutos permite que eventos atrasados (até 10 min) sejam reprocessados na janela correta. `outputMode=\"append\"` é recomendado para streaming. CDF é redundante aqui.",
    "optExpl": {
     "A": "forceDeleteTempCheckpointLocation limpa checkpoints antigos, não implementa tolerância de atraso — watermark exige configuração explícita.",
     "B": "watermark('event_timestamp', '10 minutes') define tolerância de atraso para reprocessamento de eventos atrasados; outputMode='append' é modo recomendado para streaming com estado.",
     "C": "microBatchMs e chkpointLocation configuram timing e localização, não tolerância de atraso — falta definição explícita de watermark.",
     "D": "mergeSchema resolve evolução de schema, não watermark — replicar query em executores não afeta tolerância de atraso."
    }
   },
   {
    "id": 2,
    "domain": "Data Ingestion & Acquisition",
    "topic": "",
    "text": "Seu cliente tem um banco de dados PostgreSQL com ~10GB de dados. Você quer sincronizar alterações (inserts/updates/deletes) em tempo quase-real via Lakeflow Connect. Qual é o principal pré-requisito de banco de dados (além de credenciais)?",
    "options": {
     "A": "Replicação física habilitada e todas as tabelas com `replica identity full`",
     "B": "Logical Decoding ativado, WAL level configurado como `logical`, e uma slot de replicação permanente",
     "C": "Trigger de auditoria em cada tabela + log de transações externo",
     "D": "Foreign Data Wrapper (FDW) + extensão `postgres_fdw`"
    },
    "answer": "B",
    "explanation": "Lakeflow Connect precisa de Logical Decoding ativado no PostgreSQL, WAL level `logical`, e uma slot de replicação permanente. Replica identity full é útil mas não pré-requisito crítico.",
    "optExpl": {
     "A": "Replicação física é para standby de disaster recovery; Logical Decoding (não física) é o mecanismo nativo de CDC.",
     "B": "Logical Decoding, WAL level logical, e permanent replication slot são pré-requisitos críticos para captura de mudanças em Lakeflow Connect.",
     "C": "Triggers têm overhead alto e não oferecem consistência transacional — Logical Decoding é mecanismo nativo para CDC.",
     "D": "FDW (Foreign Data Wrapper) permite queries remotas, não captura de mudanças — CDC requer Logical Decoding no PostgreSQL."
    }
   },
   {
    "id": 3,
    "domain": "Data Manipulation",
    "topic": "SQL",
    "text": "Você tem uma tabela Delta com coluna `data: VARIANT` contendo JSON com estrutura variável. Precisa extrair o campo `user.email` presente em ~80% dos registros; nos outros 20%, o campo pode não existir. Qual é a forma mais eficiente e segura?",
    "options": {
     "A": "`SELECT get_json_object(data, '$.user.email') AS email FROM tabela` com tratamento de NULL",
     "B": "`SELECT data['user']['email'] AS email FROM tabela` seguido por `WHERE email IS NOT NULL`",
     "C": "`SELECT variant_get(data, 'user.email', 'string') AS email FROM tabela`",
     "D": "Converter VARIANT para string via `to_json()`, depois usar regex"
    },
    "answer": "C",
    "explanation": "`variant_get(data, 'user.email', 'string')` é seguro e eficiente para extrair de VARIANT com fallback para NULL. Mais explícito que sintaxe de colchete.",
    "optExpl": {
     "A": "get_json_object() opera em JSON string, não VARIANT — requer cast anterior e é menos eficiente.",
     "B": "Sintaxe de colchete data['user']['email'] é válida, mas menos explícita que variant_get com tipo definido.",
     "C": "variant_get(data, 'user.email', 'string') é construtor nativo para VARIANT com fallback NULL e tipo explícito — idiomático em Databricks.",
     "D": "to_json() + regex é custoso (serialização + pattern matching) comparado com variant_get nativo."
    }
   },
   {
    "id": 4,
    "domain": "Monitoring and Alerting",
    "topic": "",
    "text": "Seu pipeline Structured Streaming processa 1M eventos/segundo. O checkpoint indicava 50ms de latência end-to-end, mas de repente subiu para 2 segundos. Onde você olharia PRIMEIRO para diagnosticar?",
    "options": {
     "A": "Logs de driver do Spark no cluster",
     "B": "Métricas de throughput e batch duration no Spark UI (streaming tab) + verificar taxa de eventos recebidos",
     "C": "Aumentar `spark.sql.shuffle.partitions` e `spark.streaming.backpressure.enabled`",
     "D": "Verificar disk I/O do checkpoint storage e estado do broker de Kafka"
    },
    "answer": "B",
    "explanation": "Métricas de throughput (input rate vs. processing rate) no Spark Streaming tab mostram imediatamente degradação. Se processing rate caiu, há gargalo.",
    "optExpl": {
     "A": "Logs de driver são úteis para debug profundo, mas Spark UI streaming tab mostra métricas agregadas mais rapidamente.",
     "B": "Input rate vs. processing rate no Spark Streaming tab mostra imediatamente se há back-pressure — diagnóstico direto.",
     "C": "Aumentar partições sem diagnóstico é 'trocar parafuso' — back-pressure pode vir de I/O, não partições.",
     "D": "Checklist completo (Kafka lag, checkpoint I/O) é válido, mas menos direto que ver métricas de throughput no Spark UI."
    }
   },
   {
    "id": 5,
    "domain": "Cost & Performance Optimization",
    "topic": "",
    "text": "Uma tabela Delta de 500GB recebe escritas pequenas frequentes (10–50 linhas por escrita). Leituras estão lentas. Qual combinação melhor reduz custo e melhora performance?",
    "options": {
     "A": "Habilitar deletion vectors, CLUSTER BY auto, e Delta cache",
     "B": "Apenas compactar com `OPTIMIZE` diariamente + aumentar worker nodes",
     "C": "Desabilitar multiversion concurrency control (MVCC) e usar particionamento por data",
     "D": "Converter para Parquet nativo (desabilitar Delta) e usar S3 Select"
    },
    "answer": "A",
    "explanation": "Deletion vectors evitam reescrever arquivos inteiros. CLUSTER BY auto otimiza layout. Delta cache reduz I/O. Juntos, resolvem escritas frequentes e leituras lentas.",
    "optExpl": {
     "A": "Deletion vectors evitam reescrever arquivos em deletes — CLUSTER BY auto otimiza layout — Delta cache armazena dados quentes.",
     "B": "OPTIMIZE diário é overhead fixo, não contínuo — mais workers não reduzem latência de escritas pequenas.",
     "C": "MVCC é essencial para concorrência no Databricks — particionamento por data não resolve fragmentação de escritas pequenas.",
     "D": "Parquet nativo perde transações ACID — S3 Select não aplica a escritas (é read-only)."
    }
   },
   {
    "id": 6,
    "domain": "Data Security and Compliance",
    "topic": "",
    "text": "Seu cliente quer uma política de mascaramento central que aplique-se automaticamente a qualquer coluna marcada com tag governada `pii` — inclusive tabelas criadas no futuro por diferentes usuários. Qual é a solução nativa do Databricks?",
    "options": {
     "A": "Criar uma stored procedure de mascaramento e associá-la via `ALTER TABLE ... SET CLUSTER BY`",
     "B": "Políticas ABAC (Attribute-Based Access Control) em Unified Catalog com governed tags",
     "C": "Row and column security com views mascaradas + job de auditoria nightly",
     "D": "Criar um modelo de mascaramento em AI functions e aplicar como `CHECK` constraint"
    },
    "answer": "B",
    "explanation": "ABAC (Attribute-Based Access Control) em UC com governed tags permite política central que aplica-se automaticamente a qualquer coluna com tag `pii`.",
    "optExpl": {
     "A": "CLUSTER BY é para otimização de layout (Z-order), não para política de mascaramento.",
     "B": "ABAC em UC com governed tags aplica política automaticamente a novas colunas marcadas com tag pii — solução nativa Databricks.",
     "C": "Views mascaradas devem ser criadas manualmente; não aplicam-se a tabelas criadas no futuro automaticamente.",
     "D": "AI functions são funções de transformação, não engine de policy — CHECK constraint é validação, não mascaramento."
    }
   },
   {
    "id": 7,
    "domain": "Data Modeling",
    "topic": "",
    "text": "Uma organização tem 150 tabelas em UC com diferentes níveis de qualidade de dados. Quer criar uma métrica central \"data_quality_score\" que apareça em governança e seja reutilizável por múltiplos dashboards. Qual abordagem é recomendada?",
    "options": {
     "A": "UC Metric View (definição centralizada) + Materialized View (pré-agregação)",
     "B": "Delta Live Tables com `@quality_expectation` + Dashboard que lê a tabela de expectativas",
     "C": "Computar a métrica num job Python nightly e escrever numa tabela, depois usar `CREATE VIEW`",
     "D": "Usar `GET_METRIC` via SQL Warehouse em cada dashboard"
    },
    "answer": "A",
    "explanation": "UC Metric View fornece definição centralizada governada. Materialized View pré-agrega para performance. Combinadas, oferecem governança + reutilização.",
    "optExpl": {
     "A": "UC Metric Views oferecem definição centralizada governada — Materialized Views pré-agregam para performance — complementam-se.",
     "B": "DLT é para orchestração de pipeline com qualidade expectations, não para métrica de governança centralizada.",
     "C": "Job Python nightly é batch, não contínuo — não é solução centralizada (cada dashboard copiaria a query).",
     "D": "GET_METRIC não é função nativa Databricks — cada dashboard teria que escrever query separada."
    }
   },
   {
    "id": 8,
    "domain": "Developing Code",
    "topic": "Python/SQL",
    "text": "Um desenvolvimento iterativo usa um notebook com query SQL que filtra dados por data usando `WHERE date > CURRENT_DATE() - 7`. Após converter para um job agendado diariamente, percebeu que a janela móvel de 7 dias não funciona corretamente em alguns runs. Qual é a melhor prática?",
    "options": {
     "A": "Usar `WHERE date > cast(current_timestamp() as date) - interval 7 days` e adicionar retry logic",
     "B": "Passar a data como parâmetro da task via `spark.conf` + usar formato ISO 8601",
     "C": "Usar Databricks Workflows com parametrização (`{{task.run_id}}`) e executar com `dbutils.notebook.run()`",
     "D": "Armazenar o checkpoint da última run em UC + ler a data do checkpoint na próxima run"
    },
    "answer": "B",
    "explanation": "Passar data como parâmetro via configuração garante consistência na janela móvel entre runs. Mais confiável que `CURRENT_DATE()` em jobs.",
    "optExpl": {
     "A": "CURRENT_DATE() muda de dia — em job agendado, não garante janela móvel consistente entre runs.",
     "B": "Passar data como parâmetro via spark.conf garante valor fixo para a run — janela de 7 dias é consistente.",
     "C": "{{task.run_id}} é ID de run, não data — dbutils.notebook.run() passa base_parameters, mas run_id não é apropriado.",
     "D": "Checkpoint rastreia estado de streaming, não data de janela — não é mecanismo apropriado para parametrização."
    }
   },
   {
    "id": 9,
    "domain": "Data Ingestion & Acquisition",
    "topic": "",
    "text": "Você recebe dados de uma API REST que retorna JSON pagado. O endpoint suporta range queries por timestamp. Qual estratégia de ingestion é mais robusta para ingeri ~100M registros com histórico de 3 anos?",
    "options": {
     "A": "Chamar a API de 1 hora em 1 hora (paralelo) usando `parallel_requests`, salvar bruto em Delta, depois processar",
     "B": "Ingeri tudo em 1 call (1 JSON gigante), armazenar em `BLOB` coluna, depois fazer parsing",
     "C": "Usar Lakeflow Connect configurado para CDC (se suportado) ou Apache NiFi externo",
     "D": "Ingeri por dia (paralelo) em tasks de Workflows, salvar com `mergeSchema=true`"
    },
    "answer": "A",
    "explanation": "Chamar API por 1 hora em paralelo, salvar bruto em Delta, depois processar, é robusto para grande volume histórico.",
    "optExpl": {
     "A": "Paralelização por hora em 3 anos (≈26k horas) é escalável — salvar bruto em Delta antes de processing é robusto (retry-friendly).",
     "B": "1 call para 3 anos de dados é serial e risco de timeout — BLOB coluna é ineficiente para parse posterior.",
     "C": "Lakeflow Connect é para database sources (PostgreSQL, Teradata), não REST API.",
     "D": "Granularidade diária é menos paralelizável que horária — mergeSchema sem necessidade é overhead."
    }
   },
   {
    "id": 10,
    "domain": "Data Manipulation",
    "topic": "",
    "text": "Você tem duas tabelas: `orders` (1M de linhas, atualizada diariamente) e `shipments` (500K linhas, atualizada em tempo real via CDF). Quer manter um fato `fact_order_shipment` sincronizado que combina ambas. Qual estratégia é mais eficiente?",
    "options": {
     "A": "Mergear incrementalmente via CDF, usando `MERGE` com subqueries de CDC",
     "B": "Rodar `DELETE FROM fact_order_shipment` + `INSERT` full join (daily batch)",
     "C": "Criar uma Materialized View `AS SELECT ... FROM orders FULL OUTER JOIN shipments` e usar `REFRESH`",
     "D": "Usar Structured Streaming para consumir CDF de `shipments` + join com snapshot de `orders`"
    },
    "answer": "A",
    "explanation": "`MERGE` com CDF consume apenas mudanças incrementais de `shipments`. Mais eficiente que full refresh.",
    "optExpl": {
     "A": "CDF captura apenas mudanças incrementais de shipments — MERGE combina com orders snapshot — eficiente.",
     "B": "DELETE + INSERT full join é refresh completo — ineficiente para grande volume com mudanças pequenas.",
     "C": "MV REFRESH rescans ambas tabelas — não aproveita CDF de mudanças incrementais.",
     "D": "Streaming para join com snapshot batch é arquitetura mais complexa que MERGE incremental simples."
    }
   },
   {
    "id": 11,
    "domain": "Cost & Performance Optimization",
    "topic": "",
    "text": "Um cluster SQL Warehouse tem 100 workers, custa $500/hora parado. Análise de query logs mostra que 30% das queries são ad-hoc (< 1 min cada) e 70% são dashboards com padrão de acesso previsível. Qual otimização reduz custo mantendo SLA?",
    "options": {
     "A": "Migrar ad-hoc queries para serverless compute + manter dashboards no warehouse dedicado",
     "B": "Usar Delta cache para todas as queries + reduzir worker count para 50",
     "C": "Particionaretodas as tabelas por data + habilitar `CLUSTER BY` automático",
     "D": "Converter queries lentas para Materialized Views + desativar cache"
    },
    "answer": "A",
    "explanation": "Serverless compute para ad-hoc (pay-as-you-go) + warehouse dedicado para dashboards (previsível, reserved capacity) reduz custo total.",
    "optExpl": {
     "A": "Serverless compute cobra por uso (ad-hoc é pay-as-you-go) — warehouse dedicado é hourly, otimizado para dashboards — combo reduz custo ocioso.",
     "B": "Delta cache é complementar, não reduz custo base — reduzir workers prejudica performance de dashboards previsíveis.",
     "C": "Particionamento + CLUSTER BY melhoram performance, não reduzem custo ocioso de 100-worker warehouse.",
     "D": "Converter para MV e desabilitar cache melhora apenas refresh, não custo base do warehouse parado."
    }
   },
   {
    "id": 12,
    "domain": "Developing Code",
    "topic": "Python",
    "text": "Em um job PySpark, você quer processar VARIANT colunas que contêm arrays aninhados. Qual abordagem garante melhor performance em transformações complexas?",
    "options": {
     "A": "`df.selectExpr(\"explode_outer(variant_col) as item\")` depois iterar em Python RDD",
     "B": "Usar `sql(\"SELECT ... FROM delta.`path` WHERE ...\")` + SQL nativo para manipular VARIANT",
     "C": "Converter VARIANT para string JSON em Python, parsear com `json.loads()`, depois remontar",
     "D": "Usar `pyspark.sql.functions.col()` com `.getItem()` chaining em SQL expressions"
    },
    "answer": "B",
    "explanation": "SQL nativo com manipulação VARIANT é mais eficiente que Python/RDD. Spark SQL optimizer otimiza VARIANT operações.",
    "optExpl": {
     "A": "Python RDD + loop forfeit Spark SQL optimization — explode + Python iteration é muito lento para transformações complexas.",
     "B": "SQL nativo com VARIANT (variant_get, etc) é otimizado pelo Catalyst optimizer — melhor performance.",
     "C": "Converter VARIANT → JSON string → Python parse → remontar é múltiplas serializações — overhead alto.",
     "D": "getItem() é Column API válida, mas SQL nativo com explode/lateral/FLATTEN é mais idiomático e otimizado."
    }
   },
   {
    "id": 13,
    "domain": "Monitoring and Alerting",
    "topic": "",
    "text": "Um dashboard crítico é alimentado por uma Materialized View. De repente, os dados ficam \"stale\" (defasados há horas). Qual é o meio mais rápido de diagnosticar se o problema é refresh automático ou custo de refresh?",
    "options": {
     "A": "Verificar `system.views.materialized_views` na tabela de metadata + log de jobs de refresh",
     "B": "Consultar Delta Lake statistics (`DESCRIBE DETAIL`) e verificar última timestamp de modificação",
     "C": "Executar `SHOW TBLPROPERTIES` na Materialized View e procurar por `last_refresh_time`",
     "D": "Revisar o custo de billable clusters nos últimos 3 dias + conectar ao Predictive Optimization"
    },
    "answer": "C",
    "explanation": "`SHOW TBLPROPERTIES` em Materialized View mostra `last_refresh_time` e status de refresh. Mais direto.",
    "optExpl": {
     "A": "system.views.materialized_views pode ter metadata, mas TBLPROPERTIES é mais direto para verificar last_refresh_time.",
     "B": "DESCRIBE DETAIL mostra mudanças de tabela base, não status de refresh de MV.",
     "C": "SHOW TBLPROPERTIES em MV retorna last_refresh_time — diagnóstico mais direto do staleness.",
     "D": "Custo de cluster não diz se refresh estava rodando ou se staleness é culpa de refresh expirado."
    }
   },
   {
    "id": 14,
    "domain": "Data Governance",
    "topic": "",
    "text": "Você trabalha com UC em 5 workspaces. Um projeto requer que múltiplos workspaces leiam da mesma tabela com permissões diferentes por workspace. Qual é a abordagem correta usando UC?",
    "options": {
     "A": "Replicar a tabela em cada workspace com políticas ABAC específicas",
     "B": "Usar Open Sharing ou D2D (Data to Data) para compartilhar a tabela com granularity de permissão",
     "C": "Criar views delegadas em cada workspace que chamam UDF de validação de workspace_id",
     "D": "Usar `ALTER TABLE ... OWNER TO` para transferir permissão + recriar a tabela em cada workspace"
    },
    "answer": "B",
    "explanation": "Open Sharing (entre workspaces Databricks) ou D2D permite compartilhamento com granularidade diferente por workspace. UC nativo.",
    "optExpl": {
     "A": "Replicar tabelas em múltiplos workspaces é overhead — perde benefício de compartilhamento centralizado.",
     "B": "Open Sharing (UC-native share entre workspaces Databricks) + D2D permite granularidade de permissão por workspace.",
     "C": "Views delegadas + UDF de workspace_id é custom, não nativa — Open Sharing é solução nativa.",
     "D": "ALTER TABLE OWNER não compartilha tabela — recriar em cada workspace replica dados."
    }
   },
   {
    "id": 15,
    "domain": "Debugging and Deploying",
    "topic": "",
    "text": "Um job agendado para rodar diariamente às 8 AM começa a falhar após 2 semanas de funcionamento. Logs mostram `SparkException: Task deserialization error`. Qual é a causa mais provável e correção?",
    "options": {
     "A": "Cache de versão JAR antiga — limpar cache do cluster e reimport libraries",
     "B": "Dependency mismatch ou mudança na classe Python — revisar se houve upgrade de lib + aumentar `spark.driver.maxResultSize`",
     "C": "Ficheiro de checkpoint corrompido — remover checkpoint e reiniciar",
     "D": "Timeout de conexão de DB — aumentar `spark.sql.connect.timeout`"
    },
    "answer": "B",
    "explanation": "`SparkException: Task deserialization error` após 2 semanas é tipicamente mudança de classe/lib incompatibilidade. Limpar e reimportar libraries resolve.",
    "optExpl": {
     "A": "Cache de JAR antigo é possível, mas 'reimport libraries' é a fix verdadeira — deserialization é class mismatch.",
     "B": "Dependency mismatch (lib upgraded no cluster mas job rodava com versão antigo) causa Task deserialization error — fix é revisar libs.",
     "C": "Checkpoint corruption daria erro em checkpoint read/write, não em task deserialization.",
     "D": "Timeout de DB causa rede/conexão error, não task deserialization error."
    }
   },
   {
    "id": 16,
    "domain": "Data Ingestion & Acquisition",
    "topic": "",
    "text": "Você ingere dados de um data warehouse legado (Teradata) via Lakebridge. A conexão é LDAP. Qual configuração é necessária NO LADO DO TERADATA para que Lakebridge funcione?",
    "options": {
     "A": "Apenas habilitar acesso remoto + criar usuário com `GRANT CONNECT` privilégio",
     "B": "Habilitar LDAP LogMech no Teradata, liberar porta ODBC, e garantir que o usuário LDAP tem permissão",
     "C": "Criar um Foreign Data Wrapper (FDW) no Teradata + abrir port 1025",
     "D": "Configurar Teradata viewpoints com `GRANT SELECT` para usuários Databricks"
    },
    "answer": "B",
    "explanation": "Lakebridge em Teradata com LDAP requer LDAP LogMech ativado, porta ODBC liberada, usuário LDAP com permissão. Setup no Teradata é essencial.",
    "optExpl": {
     "A": "GRANT CONNECT é necessário, mas LDAP LogMech no Teradata é pré-requisito crítico faltante.",
     "B": "LDAP LogMech habilitado em Teradata + porta ODBC (1025/TCP default) + usuário LDAP com permissão = setup completo.",
     "C": "FDW é feature PostgreSQL, não Teradata — Teradata usa ODBC/TDDSQL para conexão.",
     "D": "Teradata viewpoints é legacy — Lakebridge moderno usa acesso direto via LDAP."
    }
   },
   {
    "id": 17,
    "domain": "Developing Code",
    "topic": "SQL",
    "text": "Você quer rodar uma série de testes de data quality no SQL durante a ingestão via DLT. Qual é a melhor forma de expressar \"se > 5% de registros têm `price < 0`, falhar a pipeline\"?",
    "options": {
     "A": "Usar `@quality_expectation` ou `EXPECT` statement em DLT com action `fail`",
     "B": "`IF (SELECT COUNT(*) FROM data WHERE price < 0) > (SELECT COUNT(*) * 0.05 FROM data) THEN RAISE`",
     "C": "Criar uma stored procedure que roda `SELECT COUNT(*) ... FILTER (price < 0)` e chama `raise_error()`",
     "D": "Filtrar registros com `WHERE price >= 0` + usar logging de descartados"
    },
    "answer": "A",
    "explanation": "DLT nativo suporta `@quality_expectation` ou `EXPECT` statements com `action fail`. Forma idiomática.",
    "optExpl": {
     "A": "@quality_expectation ou EXPECT statement com action fail é DLT nativa — pipeline falha se violado.",
     "B": "IF/THEN/RAISE é SQL procedural não-standard — RAISE não é função Databricks SQL nativa.",
     "C": "Stored procedure com manual raise_error é workaround; não é DLT expectation idiomática.",
     "D": "Filtrar silenciosamente não falha pipeline — requisito é 'falhar se > 5%' de registros inválidos."
    }
   },
   {
    "id": 18,
    "domain": "Cost & Performance Optimization",
    "topic": "",
    "text": "Uma tabela Iceberg cresce 50GB/dia. Performance de `SELECT * WHERE date > CURRENT_DATE()` está degradando. Qual é a otimização recomendada NO ICEBERG?",
    "options": {
     "A": "Habilitar `CLUSTER BY` automático no Iceberg + compactar snapshots antigos",
     "B": "Usar `OPTIMIZE` diário + habilitar metadata caching em Z-order",
     "C": "Particionar por `date` + manter apenas últimos 90 dias via `EXPIRE_SNAPSHOTS`",
     "D": "Converter para Delta e usar deletion vectors"
    },
    "answer": "C",
    "explanation": "Particionar por date em Iceberg + `EXPIRE_SNAPSHOTS` elimina snapshots antigos, reduzindo file count e metadados.",
    "optExpl": {
     "A": "CLUSTER BY complementa, mas não resolve 'file count exploding' — EXPIRE_SNAPSHOTS é essencial.",
     "B": "OPTIMIZE é Delta (não Iceberg native) — Iceberg usa EXPIRE_SNAPSHOTS para snapshot cleanup.",
     "C": "Particionamento por date reduz scan — EXPIRE_SNAPSHOTS remove snapshots antigos, reduzindo metadados.",
     "D": "Converter para Delta não resolve Iceberg performance — questão pede otimização em Iceberg."
    }
   },
   {
    "id": 19,
    "domain": "Data Modeling",
    "topic": "",
    "text": "Você tem uma tabela SCD Type 2 onde cada atualização cria nova linha com `effective_date` e `end_date`. Seu job atualiza registros em batch diariamente. Qual SQL é mais eficiente para manter SCD Type 2?",
    "options": {
     "A": "Usar `MERGE` com `WHEN MATCHED ... UPDATE ... SET end_date = CURRENT_DATE()` e `WHEN NOT MATCHED ... INSERT`",
     "B": "`DELETE ... WHERE status = 'active'` + `INSERT` com `INSERT OVERWRITE TABLE` scd_table",
     "C": "Usar AUTO CDC com configuração `stored_as_scd_type = 2` em DLT",
     "D": "Criar uma Materialized View que calcula `max(effective_date)` por chave + fazer outer join"
    },
    "answer": "C",
    "explanation": "AUTO CDC com `stored_as_scd_type = 2` em DLT mantém SCD Type 2 automaticamente. Mais eficiente que MERGE manual.",
    "optExpl": {
     "A": "MERGE com UPDATE/INSERT é manual e requer lógica SCD Type 2 explícita — não é otimizado automaticamente.",
     "B": "DELETE + INSERT OVERWRITE é destructivo — risco de data loss em caso de erro.",
     "C": "AUTO CDC em DLT com stored_as_scd_type=2 mantém SCD Type 2 automaticamente — idiomático DLT.",
     "D": "MV + outer join é manual — menos eficiente que AUTO CDC nativa."
    }
   },
   {
    "id": 20,
    "domain": "Monitoring and Alerting",
    "topic": "",
    "text": "Um job de streaming que consuma de Kafka está com back-pressure. Qual métrica no Spark UI você confirma para validar se Kafka está com lag crescente?",
    "options": {
     "A": "Input rate vs. Processing rate no gráfico de Streaming tab",
     "B": "Executor memory usage + GC time no stage explorer",
     "C": "Databricks Job Runs log com `kafka_consumer_lag` metric",
     "D": "Task duration breakdown no SQL tab"
    },
    "answer": "A",
    "explanation": "Spark Streaming tab mostra gráficos \"Input rate\" vs. \"Processing rate\". Se processing rate < input rate, há lag.",
    "optExpl": {
     "A": "Spark Streaming tab gráfico 'Input rate' vs. 'Processing rate' mostra imediatamente se back-pressure existe.",
     "B": "Executor memory + GC time mostra GC pause, não consumer lag — lag é sobre eventos não processados.",
     "C": "kafka_consumer_lag é métrica Kafka nativa, não exposta como métrica padrão em Databricks Streaming tab.",
     "D": "SQL tab é para SQL queries, não para streaming lag — Streaming tab é lugar correto."
    }
   },
   {
    "id": 21,
    "domain": "Data Security and Compliance",
    "topic": "",
    "text": "Você quer que uma coluna sensível (ssn) seja sempre mascarada APENAS para usuários não-admins em uma tabela. O mascaramento não pode ser contornado via SQL direto. Qual é a solução?",
    "options": {
     "A": "Criar uma view com `CASE WHEN is_admin() THEN ssn ELSE NULL END` + revogar acesso à tabela base",
     "B": "Usar Row and Column Security (RCS) com uma política de mascaramento por tag governada",
     "C": "Usar Dynamic Data Masking (DDM) com regra SQL + garantir que apenas admin pode criar SQL UDF",
     "D": "Replicar a tabela com coluna ssn em branco, manter original em schema privado + usar role de acesso"
    },
    "answer": "B",
    "explanation": "Row and Column Security (RCS) com política de mascaramento por tag é nativo em UC e não pode ser contornado.",
    "optExpl": {
     "A": "CASE view pode ser contornada por acesso direto à tabela base — revogar acesso é possível mas menos robusta.",
     "B": "RCS (Row and Column Security) em UC com policy em tag governada é nativo — aplicado no engine, não contornável.",
     "C": "'Dynamic Data Masking' é termo marketing — UC implementa via RCS, não feature separada.",
     "D": "Replicar tabela + roles é overhead — RCS é solução nativa de mascaramento por policy."
    }
   },
   {
    "id": 22,
    "domain": "Developing Code",
    "topic": "Python/SQL",
    "text": "Seu notebook faz `spark.read.parquet(\"s3://bucket/path\")` cada vez que é executado. Há 100GB de dados. Qual optimization garante que relectura do mesmo path use cache sem reescrever?",
    "options": {
     "A": "Usar `spark.sql.parquet.cacheMetadata=true` + habilitar Delta cache",
     "B": "Salvar resultado em Delta após primeira leitura, depois ler de Delta",
     "C": "Configurar `spark.sql.shuffle.partitions` e usar `cache()` DataFrame + `persist()`",
     "D": "Usar Apache Iceberg ao invés de Parquet + habilitar metadata caching"
    },
    "answer": "B",
    "explanation": "Salvar resultado em Delta após primeira leitura, depois ler de Delta, garante snapshot cacheado e estrutura compactada.",
    "optExpl": {
     "A": "parquet.cacheMetadata cache metadados, mas relectura de Parquet bruto re-faz schema inference.",
     "B": "Salvar em Delta (primeira run) + ler de Delta (runs posteriores) = snapshot persistente otimizado.",
     "C": "cache()/persist() são in-memory — memory do executor, não disk; não persiste entre job runs.",
     "D": "Iceberg é alternativa válida mas overhead — Delta com write é solução mais prática."
    }
   },
   {
    "id": 23,
    "domain": "Data Ingestion & Acquisition",
    "topic": "",
    "text": "Você tem um webhook que envia eventos a cada segundo. Quer ingerir para Delta com latência < 1 segundo de ponta-a-ponta. Qual setup é mais apropriado?",
    "options": {
     "A": "Kafka topic → Structured Streaming → Delta com micro-batch 500ms",
     "B": "Webhook → Kinesis stream → Delta via Lakeflow Connect",
     "C": "Webhook → HTTP listener app → append direto em Delta (Python loop)",
     "D": "Webhook → Auto Loader em `STREAMING` mode com `trigger(once=False)` e latestFirst"
    },
    "answer": "A",
    "explanation": "Kafka → Structured Streaming com micro-batch 500ms → Delta garante < 1 seg latência ponta-a-ponta.",
    "optExpl": {
     "A": "Kafka → Structured Streaming com micro-batch 500ms oferece latência baixa, fault tolerance, checkpointing nativo.",
     "B": "Lakeflow Connect é para database sources (CDC), não event streams.",
     "C": "Python HTTP loop direto = sem ACID, sem checkpointing, sem fault tolerance — risco alto.",
     "D": "Auto Loader é para files (S3, GCS), não webhooks — latestFirst é opção não-existent."
    }
   },
   {
    "id": 24,
    "domain": "Data Manipulation",
    "topic": "SQL",
    "text": "Uma tabela tem coluna `tags: ARRAY<STRUCT<name: STRING, value: VARIANT>>`. Você precisa contar registros onde alguma tag tem `name = 'category'` e `value.id` > 100. Qual query é correta?",
    "options": {
     "A": "`SELECT COUNT(*) FROM table WHERE EXISTS (SELECT 1 FROM tags WHERE tags.name = 'category' AND tags.value:id > 100)`",
     "B": "`SELECT COUNT(DISTINCT id) FROM table, LATERAL FLATTEN(tags) t WHERE t.value:name = 'category' AND t.value:value:id > 100`",
     "C": "`SELECT COUNT(*) FROM table WHERE ANY(tags, t -> t.name = 'category' AND t.value:id > 100)`",
     "D": "`SELECT COUNT(*) FROM table WHERE array_contains(tags, map('name', 'category', 'id', '>100'))`"
    },
    "answer": "C",
    "explanation": "`ANY(tags, t -> t.name = 'category' AND t.value:id > 100)` usa `ANY` predicate em array com lambda para testar struct aninhado.",
    "optExpl": {
     "A": "EXISTS não aplica a arrays — seria scalar subquery, não array predicate.",
     "B": "LATERAL FLATTEN desagrega array em rows, mas t.value:name está sintaxe errada (deveria ser t.name).",
     "C": "ANY(array, lambda) é idiomático para predicados em array — lambda aplica lógica AND nos fields.",
     "D": "array_contains checks membership; '> 100' como string é literal, não operador — não poderia fazer comparação."
    }
   },
   {
    "id": 25,
    "domain": "Cost & Performance Optimization",
    "topic": "",
    "text": "Uma query em SQL Warehouse demora 5 minutos. Profiling mostra 80% do tempo em shuffle. Seu índice de seletividade é 2% (filtra 2% dos dados). Qual é a otimização mais eficiente?",
    "options": {
     "A": "Usar `CLUSTER BY` automático na tabela + reordenar colunas no select",
     "B": "Particionamento + Z-order em coluna de filtro + aumentar worker count",
     "C": "Habilitar Adaptive Query Execution (AQE) + `broadcast_join_threshold`",
     "D": "Criar um índice B-tree e usar hint `USE INDEX`"
    },
    "answer": "B",
    "explanation": "80% em shuffle com seletividade baixa → particionamento + Z-order reduz volume. Maior ganho que aumentar workers.",
    "optExpl": {
     "A": "CLUSTER BY automático é complementar; reordenar colunas no SELECT não muda shuffle.",
     "B": "Particionamento (prune partitions) + Z-order (cluster rows) reduzem volume de dados shuffled — eficiente.",
     "C": "AQE é complementar — broadcast é para small join sides, não para 80% shuffle.",
     "D": "Índices B-tree não são nativos em Databricks — Z-order é índice efetivo usado."
    }
   },
   {
    "id": 26,
    "domain": "Debugging and Deploying",
    "topic": "",
    "text": "Um Databricks Workflow roda via Declarative Automation (databricks.yml). Um step falha. Qual é a forma de integrar retry logic NATIVA sem modificar o notebook?",
    "options": {
     "A": "Adicionar `max_retries: 3` e `retry_on_timeout: true` na task definition do databricks.yml",
     "B": "Wrappear o notebook com script Python que faz retry via `dbutils.notebook.run()` com try-except",
     "C": "Usar `tasks: [{name: ..., job_cluster_config: ..., max_concurrent_runs: 1}]` + job scheduling",
     "D": "Rodar cada task como um `run_now` separado com validação manual"
    },
    "answer": "A",
    "explanation": "Declarative Automation suporta `max_retries` e `retry_on_timeout` nativa em task definition.",
    "optExpl": {
     "A": "max_retries + retry_on_timeout em databricks.yml task definition é suporte nativa de Declarative Automation.",
     "B": "Python wrapper com try-except é possível, mas não é nativa declarativa — overhead de wrapper.",
     "C": "max_concurrent_runs controla concorrência, não retry — não endereça falha de task.",
     "D": "run_now manual é manual, não automatizado — não é integrado em workflow."
    }
   },
   {
    "id": 27,
    "domain": "Data Governance",
    "topic": "",
    "text": "Seu UC tem uma tabela com coluna de PII (email). Você criou uma governed tag `pii` e aplicou política ABAC de mascaramento. Um novo usuário PRECISA VER O EMAIL REAL (sem mascara). Qual é o processo correto?",
    "options": {
     "A": "Remove do grupo `analysts`, adiciona ao grupo `data_officers` com override no Policy",
     "B": "Solicita ao admin que cria exceção na policy usando `ALTER POLICY ... ADD EXCEPTION`",
     "C": "O usuário executa `USE UNMASKED_COPY` antes de selecionar (não existe, pegadinha)",
     "D": "Criar uma view dedicada com `SELECT email FROM table WHERE current_user() IN ('user@email.com')`"
    },
    "answer": "B",
    "explanation": "ABAC policy permite exceções via `ALTER POLICY ... ADD EXCEPTION` para usuários específicos.",
    "optExpl": {
     "A": "Adicionar usuário a grupo com data_officers pode funcionar se grupo tem permissão, mas não é 'override no Policy'.",
     "B": "ALTER POLICY ADD EXCEPTION permite exceções granulares para usuários específicos — solução ABAC nativa.",
     "C": "USE UNMASKED_COPY não existe — pegadinha confirmada na questão.",
     "D": "View dedicada é workaround; não aproveita ABAC policy exception framework nativo."
    }
   },
   {
    "id": 28,
    "domain": "Developing Code",
    "topic": "Python/SQL",
    "text": "Em um job Spark, você precisa processar dados em lotes menores para evitar OOM. Qual é a forma mais idiomática em PySpark?",
    "options": {
     "A": "Usar `repartition()` + `groupByKey()` + loop em `collect()`",
     "B": "`foreachPartition()` ou `foreachBatch()` com limites de batch size",
     "C": "`take(n)` em loop + reprocessar",
     "D": "Aumentar `spark.executor.memory` e deixar Spark gerenciar partições"
    },
    "answer": "B",
    "explanation": "`foreachPartition()` ou `foreachBatch()` com batch size limit é idiomático para processar em lotes e evitar OOM.",
    "optExpl": {
     "A": "collect() traz dados para driver memory (OOM risk) — repartition + groupByKey + collect é antipattern.",
     "B": "foreachPartition() (RDD) ou foreachBatch() (DataFrame) processa em lotes — batch size limit previne OOM.",
     "C": "take(n) é função seleção, não processamento — não é idiomática para transformações.",
     "D": "Aumentar memory é fallback; verdadeira solução é batch processing distribuído."
    }
   },
   {
    "id": 29,
    "domain": "Monitoring and Alerting",
    "topic": "",
    "text": "Um pipeline tem SLA de \"dados devem estar atualizados até 6 AM\". A tabela que alimenta o dashboard não tem alertas. Qual é a melhor configuração no Databricks nativo?",
    "options": {
     "A": "Usar `ALTER TABLE ... ADD CONSTRAINT freshness_check`",
     "B": "Configurar alertas no SQL Warehouse query profiling",
     "C": "Criar um job de validação que roda às 5:50 AM, verifica `DESCRIBE DETAIL` timestamp e dispara Alert (webhook/Slack)",
     "D": "Usar Query Alert nativo do SQL Warehouse configurado para rodar queries de SLA check"
    },
    "answer": "C",
    "explanation": "Job de validação que roda antes do SLA, verifica timestamp via `DESCRIBE DETAIL`, dispara alerta webhook/Slack. Prática comum.",
    "optExpl": {
     "A": "Constraints são para validação de valores (NOT NULL, UNIQUE), não para time-based freshness check.",
     "B": "SQL Warehouse query profiling é para performance, não para monitorar freshness de tabela.",
     "C": "Job de validação (@5:50 AM) que verifica DESCRIBE DETAIL last_modified timestamp e emite alerta — prática comum.",
     "D": "Query Alert é para alertar sobre query result content, não table freshness metadata."
    }
   },
   {
    "id": 30,
    "domain": "Data Security and Compliance",
    "topic": "",
    "text": "Você precisa fornecer acesso de leitura a uma tabela UC para um parceiro externo. A tabela contém dados sensíveis. Qual é a abordagem recomendada usando UC nativo?",
    "options": {
     "A": "Exportar dados para CSV, compartilhar via S3 pre-signed URL",
     "B": "Usar Open Sharing (se parceiro tem Databricks) ou Clean Rooms (compartilhamento privado)",
     "C": "Criar uma view com subset de dados + conceder `SELECT` via role compartilhado",
     "D": "Replicar tabela para workspace do parceiro + gerenciar permissões per-workspace"
    },
    "answer": "B",
    "explanation": "Open Sharing (parceiro com Databricks) ou Clean Rooms (compartilhamento privado) é solução nativa UC para parceiros externos.",
    "optExpl": {
     "A": "CSV + S3 pre-signed URL é fora de governança — dados sensíveis sem audit trail.",
     "B": "Open Sharing (parceiro com account Databricks) + Clean Rooms (compartilhamento gerenciado) são nativas UC.",
     "C": "View com SELECT via role compartilhado é workaround — Open Sharing é nativa.",
     "D": "Replicar para workspace é overhead — sharing é solução nativa."
    }
   },
   {
    "id": 31,
    "domain": "Data Ingestion & Acquisition",
    "topic": "",
    "text": "Você ingere de um banco de dados legado via JDBC. A tabela tem 500M de linhas e cresce 10M/dia. Qual estratégia garante ingestão incremental eficiente?",
    "options": {
     "A": "Query completa (`SELECT *`) diariamente com `mergeSchema=true`",
     "B": "Usar `READ_FROM` com hint de coluna de sequência monotônica + `WHERE col > last_value`",
     "C": "Lakeflow Connect em CDC mode (se suportado) ou JDBC com query parametrizada de incremento",
     "D": "Usar Apache Sqoop + converter para Parquet depois"
    },
    "answer": "C",
    "explanation": "Lakeflow Connect com CDC ou JDBC parametrizada em coluna de sequência garante ingestão incremental eficiente.",
    "optExpl": {
     "A": "SELECT * diário + mergeSchema é full refresh — ineficiente para 500M + 10M/dia growth.",
     "B": "READ_FROM com sequence coluna + WHERE > last_value é padrão, mas Lakeflow CDC é mais robusto.",
     "C": "Lakeflow Connect CDC (se DB suporta) captura mudanças nativas — JDBC parametrizado em sequence é fallback.",
     "D": "Apache Sqoop é legacy; Lakeflow Connect é solução moderna nativa Databricks."
    }
   },
   {
    "id": 32,
    "domain": "Developing Code",
    "topic": "SQL",
    "text": "Em um notebook SQL, você faz `CREATE TEMP VIEW v AS SELECT ...`. Depois quer rodar em paralelo dois comandos: `INSERT INTO tab1 SELECT * FROM v` e `INSERT INTO tab2 SELECT * FROM v`. Qual é o risco e como evitar?",
    "options": {
     "A": "TEMP VIEW só vive na sessão — usar `CREATE VIEW` (persistente) ou `GLOBAL TEMP VIEW`",
     "B": "Não há risco, TEMP VIEW é acessível em toda a sessão",
     "C": "Necessário re-criar a view em cada paralelismo — usar `spark.parallelize()`",
     "D": "O problema é inserção, não view — adicionar `OVERWRITE` clause"
    },
    "answer": "A",
    "explanation": "TEMP VIEW vive na sessão. Usar `CREATE VIEW` (persistente) ou `GLOBAL TEMP VIEW` em paralelismo.",
    "optExpl": {
     "A": "TEMP VIEW vive apenas na SQL session — paralelismo (dbutils.notebook.run, tasks) = nova session = sem acesso.",
     "B": "TEMP VIEW é sessão-local; paralelismo = nova sessão = TEMP VIEW não visível — há risco.",
     "C": "spark.parallelize() cria RDD, não reutiliza view — não solução correta.",
     "D": "OVERWRITE é modo de escrita, não resolve scope de TEMP VIEW entre sessions."
    }
   },
   {
    "id": 33,
    "domain": "Cost & Performance Optimization",
    "topic": "",
    "text": "Um relatório de BI é alimentado por 3 Materialized Views que combinam dados de 10 tabelas Delta. Refresh é agendado de 1 em 1 hora. Queries adicionais ad-hoc consultam as MV. Qual é a estratégia para reduzir custo?",
    "options": {
     "A": "Converter MV para views normais (não-materializadas) + habilitar Delta cache",
     "B": "Usar UC Metric Views para agregações + manter apenas 1 MV base para dados brutos",
     "C": "Combinar refresh das 3 MVs em 1 job + usar scheduled warehouses que iniciam/param automaticamente",
     "D": "Desabilitar refresh automático + rodar refresh manual sob demanda"
    },
    "answer": "C",
    "explanation": "Combinar refresh das 3 MVs + scheduled warehouses on/off automático reduz overhead e custo ocioso.",
    "optExpl": {
     "A": "Views normais (não-materializadas) re-scaneiam cada query — pior que MV.",
     "B": "Metric Views + 1 MV é mais complex — não reduz custo base de 3 MV refreshes.",
     "C": "Combinar 3 refreshes em 1 job = startup overhead amortizado — scheduled warehouse on/off = pay para refresh period.",
     "D": "Manual refresh não é escalável; refresh automático é necessário para BI padrão."
    }
   },
   {
    "id": 34,
    "domain": "Monitoring and Alerting",
    "topic": "",
    "text": "Um job de ingestão começa a falhar com `FileNotFoundError` após semana funcionando. O código não mudou. Qual é a causa mais provável?",
    "options": {
     "A": "Caminho S3 expirou ou bucket foi apagado",
     "B": "Permissão de IAM foi revogada / credenciais venceram",
     "C": "Cluster foi terminado, novo cluster não tem acesso ao mesmo bucket",
     "D": "Todas as acima são possíveis — verificar logs de driver + validar permissões + testar caminho"
    },
    "answer": "D",
    "explanation": "Todas são causas possíveis. Validar logs, testar caminho S3, verificar credenciais IAM.",
    "optExpl": {
     "A": "S3 path expiration ou bucket deletion é causa possível de FileNotFoundError.",
     "B": "IAM permission revocation ou credential expiration é causa possível.",
     "C": "Novo cluster pode ter IAM role diferente ou mount config faltante — causa possível.",
     "D": "Sem mais info, é impossível diagnóstico singular — checklist (logs, permissions, path, bucket) é necessário."
    }
   },
   {
    "id": 35,
    "domain": "Data Modeling",
    "topic": "",
    "text": "Você tem uma tabela de eventos com `event_type: STRING` e quer calcular métricas separadas por tipo. Qual é a forma mais performática usando VARIANT/STRUCT?",
    "options": {
     "A": "`SELECT event_type, COUNT(*) FROM events GROUP BY event_type` + loop Python sobre resultado",
     "B": "Usar `CASE` statements para cada tipo + aggregação dinâmica",
     "C": "Transformar para VARIANT com estrutura `{type: X, metrics: {...}}` e depois fazer parse",
     "D": "Particionar logicamente (views por tipo) + rodar query separada para cada"
    },
    "answer": "A",
    "explanation": "`SELECT event_type, COUNT(*) FROM events GROUP BY event_type` é forma mais performática — SQL nativo.",
    "optExpl": {
     "A": "GROUP BY nativo em SQL é otimizado por Catalyst — idiomático, eficiente.",
     "B": "CASE statements para cada tipo é manual — menos limpo que GROUP BY nativo.",
     "C": "Transformar para VARIANT + parse é overhead desnecessário.",
     "D": "Views por tipo + query separada é múltiplas passes — GROUP BY único é eficiente."
    }
   },
   {
    "id": 36,
    "domain": "Data Manipulation",
    "topic": "",
    "text": "Você consome mensagens de Kafka com Structured Streaming. O producer envia eventos com timestamps distintos. Você quer garantir exactly-once processing mesmo com falhas. Qual configuração é crítica?",
    "options": {
     "A": "Usar `outputMode=\"append\"` sem checkpoint (não garante)",
     "B": "Checkpoint habilitado + idempotent writer (salvar com idempotency key) + `completionTrigger`",
     "C": "`outputMode=\"complete\"` com `trigger(once=True)` e checkpoint",
     "D": "Apenas checkpoint está suficiente (falso, precisa também de sink idempotente)"
    },
    "answer": "B",
    "explanation": "Exactly-once requer checkpoint + idempotent writer + sink que não insere duplicatas. Checkpoint sozinho não basta.",
    "optExpl": {
     "A": "outputMode='append' sem checkpoint = no state tracking = at-least-once (duplicatas possível).",
     "B": "Checkpoint rastreia offsets — idempotent writer (idempotency key) evita duplicatas — exactly-once garantido.",
     "C": "complete mode retém todos dados agregados (memory overhead) — trigger(once=True) é batch único.",
     "D": "Checkpoint sozinho é necessary mas não sufficient — precisa também idempotent sink."
    }
   },
   {
    "id": 37,
    "domain": "Cost & Performance Optimization",
    "topic": "",
    "text": "Uma query demora 10 minutos. Profiling mostra: 40% em rede (shuffle), 30% em parse JSON, 20% em sort, 10% I/O. Qual otimização traz maior ganho?",
    "options": {
     "A": "Aumentar partições + usar Z-order",
     "B": "Pré-parsear JSON em VARIANT na ingestão + habilitar Delta cache",
     "C": "Converter JSON para Parquet nativo antes de query",
     "D": "Aumentar worker count e `spark.sql.shuffle.partitions`"
    },
    "answer": "B",
    "explanation": "JSON parsing é 30% do tempo. Pré-parsear em VARIANT na ingestão + Delta cache evita re-parse. Maior ganho.",
    "optExpl": {
     "A": "Aumentar partições + Z-order ajuda em shuffle (40%), mas parsing é 30% — não endereça issue.",
     "B": "Pré-parsear JSON em VARIANT na ingestão = parse uma vez — Delta cache evita re-read — reduz 30%.",
     "C": "Converter para Parquet não ajuda; Parquet é compressão, não parse overhead.",
     "D": "Aumentar workers/partições não reduz operação parse — paralelizar não reduz parsing se dataset é pequeno."
    }
   },
   {
    "id": 38,
    "domain": "Developing Code",
    "topic": "SQL",
    "text": "Você cria uma stored procedure que faz `INSERT INTO table SELECT ...` com parametro de data. Para testar, você roda com data fixa. Em produção (job agendado), a data deve ser dinâmica (hoje). Qual é a prática correta?",
    "options": {
     "A": "Armazenar a data em uma tabela de metadados, stored proc lê dela",
     "B": "Usar `DEFAULT CURRENT_DATE()` como parâmetro com null-check + override no job",
     "C": "Usar `COALESCE(parameter_date, CURRENT_DATE())` na procedure",
     "D": "Hardcode `CURRENT_DATE()` direto na procedure (sem parâmetro)"
    },
    "answer": "C",
    "explanation": "`COALESCE(parameter_date, CURRENT_DATE())` é idiomático — parâmetro nulo cai para data dinâmica.",
    "optExpl": {
     "A": "Tabela de metadados é extra indirection — menos limpo que COALESCE padrão.",
     "B": "DEFAULT em parâmetro de procedure + null-check no job é verboso — COALESCE é idiomático.",
     "C": "COALESCE(parameter_date, CURRENT_DATE()) = se parâmetro é NULL, usa CURRENT_DATE() — idiomático.",
     "D": "Hardcode CURRENT_DATE() sem parâmetro = sem forma de teste com data fixa — não bom design."
    }
   },
   {
    "id": 39,
    "domain": "Debugging and Deploying",
    "topic": "",
    "text": "Um notebook exporta parâmetros via `dbutils.widgets.get()`. O job agendado passa parametros via `%run ../config`. De repente, tudo falha com \"widget not found\". Por quê?",
    "options": {
     "A": "`%run` não passa widgets — usar `dbutils.notebook.run()` com `base_parameters` dict",
     "B": "Notebook de config deve estar no mesmo workspace (caminho relativo não funciona entre workspaces)",
     "C": "`dbutils.widgets.get()` não funciona em jobs — usar environment variables via `spark.conf`",
     "D": "Falta `%render` antes de usar os widgets"
    },
    "answer": "A",
    "explanation": "`%run` não passa widgets. Usar `dbutils.notebook.run()` com `base_parameters` dict passa variáveis.",
    "optExpl": {
     "A": "%run é magic command (não passa context) — dbutils.notebook.run() com base_parameters dict passa.",
     "B": "Caminho relativo funciona dentro workspace — issue é que %run não passa context, não about caminho.",
     "C": "dbutils.widgets.get() funciona em jobs se notebook é chamado via dbutils.notebook.run() com base_parameters.",
     "D": "%render não existe (ou não é necessário) — issue é %run vs dbutils.notebook.run()."
    }
   },
   {
    "id": 40,
    "domain": "Monitoring and Alerting",
    "topic": "",
    "text": "Um dashboard apresenta inconsistência: dois relatórios idênticos retornam valores diferentes. O dashboard usa a mesma Materialized View. Qual é a causa mais provável?",
    "options": {
     "A": "MV estava em refresh enquanto um relatório rodava — um viu versão anterior, outro versão nova",
     "B": "Um relatório usa cache local do browser, outro não",
     "C": "Delta version history — um query vê snapshot antigo, outro snapshot novo",
     "D": "Bug na query do dashboard, não culpa da MV"
    },
    "answer": "A",
    "explanation": "MV em refresh durante query — um viu snapshot anterior, outro posterior. Refresh causa inconsistência transitória.",
    "optExpl": {
     "A": "MV REFRESH atomicamente substitui version — se um query vê v1 e outro vê v2 = refresh ocorreu.",
     "B": "Cache local do browser não afeta server-side query results — não causa inconsistência de dados.",
     "C": "Delta version history é possível se há múltiplas versões, mas MV refresh é causa mais provável.",
     "D": "Inconsistência de dados não é bug na query — MV refresh timing é causa."
    }
   },
   {
    "id": 41,
    "domain": "Data Manipulation",
    "topic": "",
    "text": "Você tem uma tabela com colunas `customer_id`, `email`, `age`. Quer uma política que mascare `email` para todos EXCETO `data_engineers`. Qual é a configuração correta em UC?",
    "options": {
     "A": "Usar ABAC policy com `principal != \"data_engineers\"` → aplicar mascaramento (lógica invertida, não suportada)",
     "B": "Usar \"positive\" rule: `principal == \"data_engineers\"` → SEM mascaramento, `else` → mascaramento",
     "C": "Usar Row-Level Security em lugar de Column-Level (não é aplicável aqui)",
     "D": "Criar 2 views: 1 com email para data_engineers, 1 sem email para outros"
    },
    "answer": "B",
    "explanation": "ABAC policy em UC suporta lógica positiva: `principal == \"data_engineers\"` → sem mask; default → mask.",
    "optExpl": {
     "A": "Lógica invertida (principal != X) não é modo direto em ABAC — ABAC usa lógica positiva.",
     "B": "ABAC permite 'positive rule': principal == 'data_engineers' → SEM mask; else → mask.",
     "C": "RLS é para row filtering, não column masking — column masking é responsabilidade de Column Security.",
     "D": "Views duplas é workaround manual — ABAC é solução nativa."
    }
   },
   {
    "id": 42,
    "domain": "Developing Code",
    "topic": "Python",
    "text": "Em um PySpark job, você precisa aplicar transformação custosa em um RDD. Qual é a forma mais eficiente?",
    "options": {
     "A": "`rdd.map(expensive_func)` sem persistência",
     "B": "`rdd.map(expensive_func).cache().count()` para forçar computação + depois usar",
     "C": "Usar DataFrame com SQL UDF em lugar de Python RDD",
     "D": "`rdd.persist(StorageLevel.DISK_ONLY)` se memory é limitada"
    },
    "answer": "B",
    "explanation": "`cache().count()` força computação + caching. Sem `count()`, cache é lazy. Com memory limitada, considerar `DISK_ONLY`.",
    "optExpl": {
     "A": "map() sem cache re-computa em cada ação — ineficiente para transformação custosa.",
     "B": "cache().count() força computação imediata e armazena — reutilizações posteriores vêm de cache.",
     "C": "SQL UDF é melhor que Python RDD, mas questão é sobre como cache eficientemente.",
     "D": "persist(DISK_ONLY) sem count() é lazy — precisa action para forçar computação."
    }
   },
   {
    "id": 43,
    "domain": "Data Security and Compliance",
    "topic": "",
    "text": "Você quer auditar QUEM acessou QUAL coluna de uma tabela UC. O rastreamento nativo do Databricks (audit logs) mostra \"SELECT * FROM table\" mas não granularidade de coluna. Qual é a solução?",
    "options": {
     "A": "Usar UC Column-Level Lineage com políticas de mascaramento (mostra o bloqueio, não o acesso)",
     "B": "Criar view por usuário + column-level security + monitorar access logs diferenciados",
     "C": "Usar Predictive Optimization para rastrear acessos por coluna (não é sua função)",
     "D": "Implementar logging customizado em UDF que registra coluna acessada + auditar via externa DB"
    },
    "answer": "D",
    "explanation": "UC audit logs não rastreiam coluna individual. Logging customizado em UDF + auditoria externa DB é solução real.",
    "optExpl": {
     "A": "UC lineage mostra transformações de dados, não access audit — não distingue qual coluna foi acessada.",
     "B": "Access logs em UC não rastreiam granularidade de coluna — todos logs mostram 'SELECT * FROM table'.",
     "C": "Predictive Optimization é para query optimizer recommendations, não audit.",
     "D": "Logging customizado em UDF que registra 'user X accessed column Y' + auditoria externa = solução real."
    }
   },
   {
    "id": 44,
    "domain": "Data Manipulation",
    "topic": "",
    "text": "Você tem uma tabela large (1TB) e quer atualizar ~1% das linhas. `MERGE` vs `UPDATE`: qual é melhor?",
    "options": {
     "A": "`UPDATE` é mais direto, sempre melhor",
     "B": "`MERGE` é sempre melhor para grandes volumes, mesmo com pequenas mudanças",
     "C": "Usar `MERGE` quando muitas linhas mudam; `UPDATE` se < 5% (performance diferente em Delta)",
     "D": "Ambos são equivalentes em Delta — escolha por clareza de código"
    },
    "answer": "C",
    "explanation": "Delta: `MERGE` com matching pequeno é otimizado; `UPDATE` é direto. Se < 5% linhas mudam, `UPDATE` pode ser mais eficiente.",
    "optExpl": {
     "A": "UPDATE é direto para targeted updates, mas MERGE oferece mais semântica — não 'sempre melhor'.",
     "B": "MERGE é idiomático para mudanças em larga escala, mas Delta otimiza UPDATE para < 5%.",
     "C": "Delta: MERGE para mudanças em larga escala — UPDATE para < 5% linhas (targeted eficiente).",
     "D": "Não equivalentes em Delta — UPDATE é otimizado para pequenas mudanças, MERGE para grandes."
    }
   },
   {
    "id": 45,
    "domain": "Cost & Performance Optimization",
    "topic": "",
    "text": "Um query que fazia full table scan de 100GB em 2 min agora faz em 10 min. Nada mudou no código. Qual é a primeira coisa a verificar?",
    "options": {
     "A": "Alterações na tabela: nova particionamento / compactação falhando / versão Delta antiga",
     "B": "Cluster: mudança de worker count / worker type / memory disponível",
     "C": "Query: mudança de hints / join order / predicate pushdown",
     "D": "Cache: Delta cache foi desabilitado ou LRU expirou histórico"
    },
    "answer": "A",
    "explanation": "Query de full table scan degradando — alteração em tabela (compactação falhando, versão Delta antiga).",
    "optExpl": {
     "A": "Se nada mudou no código, qualidade de tabela mudou (compactação degrading, version antiga).",
     "B": "Se cluster não mudou (worker count, type, memory), não é culpa de cluster.",
     "C": "Código não mudou — query hints não mudaram.",
     "D": "Delta cache expiration não causa full scan degradação."
    }
   },
   {
    "id": 46,
    "domain": "Data Ingestion & Acquisition",
    "topic": "",
    "text": "Você recebe arquivos CSV diariamente em um bucket S3. Arquivo tem encoding variável (UTF-8, Latin-1). Qual é a forma robusta de ingerir com Auto Loader?",
    "options": {
     "A": "`spark.read.option(\"encoding\", \"UTF-8\").csv(path)`",
     "B": "Usar Auto Loader com `cloudFiles.schemaInference=true` e detector de encoding nativo",
     "C": "Pre-processar arquivos com `file_modify_time` + usar `multiLine=true`, `charToEscapeQuoteEscaping=true`",
     "D": "Auto Loader não suporta encoding variável — converter externamente para UTF-8 antes"
    },
    "answer": "B",
    "explanation": "Auto Loader com `schemaInference=true` detecta encoding automaticamente. Fallback: pre-processar com iconv.",
    "optExpl": {
     "A": "spark.read.csv não suporta 'encoding' option nativa — Spark assume UTF-8.",
     "B": "Auto Loader com cloudFiles.schemaInference=true + native encoding detection suporta CSV variável.",
     "C": "file_modify_time é filter de arquivo — multiLine/charToEscapeQuoteEscaping são para CSV format.",
     "D": "Auto Loader é flexível — suporta encoding variável com schemaInference=true."
    }
   },
   {
    "id": 47,
    "domain": "Developing Code",
    "topic": "Python",
    "text": "Em um notebook, você combina SQL e PySpark. SQL query retorna 1M de linhas. Você faz `df = spark.sql(\"SELECT ...\")` depois `df.collect()`. Qual é o risco?",
    "options": {
     "A": "`collect()` traz tudo para memory do driver — pode causar OOM",
     "B": "Nenhum, PySpark gerencia memoria automaticamente",
     "C": "`df` permanece em RDD, não em memória do driver",
     "D": "Só falha se número de partições < worker count"
    },
    "answer": "A",
    "explanation": "`collect()` traz 1M linhas para memory do driver — risco de OOM. Usar `count()` ou `write` para evitar.",
    "optExpl": {
     "A": "collect() traz 1M rows para driver JVM heap — risco de OOM = problema real.",
     "B": "PySpark não gerencia automaticamente collect() — collect() é eager action que materializa.",
     "C": "df permanece em executor memory até collect() — collect() materializa em driver.",
     "D": "Falha de collect() não depende de partition count — sempre traz para driver."
    }
   },
   {
    "id": 48,
    "domain": "Debugging and Deploying",
    "topic": "",
    "text": "Seu cliente quer que uma tag governada `sensitive` seja aplicada automaticamente em qualquer coluna com `@pii` ou contendo \"ssn\". Qual é a solução?",
    "options": {
     "A": "Usar governance rules em UC com pattern matching automático",
     "B": "Criar um job que roda `ALTER TABLE ... SET TBLPROPERTIES (sensitive=true)` em base a scan de schema",
     "C": "Usar Predictive Optimization para sugerir tags automaticamente",
     "D": "Configurar ABAC policy que auto-aplica tag na coluna criada"
    },
    "answer": "B",
    "explanation": "Job que escaneia schema e aplica tags baseado em padrão (nome contém \"ssn\" ou \"pii\") é workaround comum.",
    "optExpl": {
     "A": "'governance rules' com 'pattern matching automático' não é phrasing padrão nativo.",
     "B": "Job que scans schema, identifica columns com 'ssn'/'pii' no nome, depois ALTER TABLE SET TAG.",
     "C": "Predictive Optimization é para performance recommendation, não tag suggestion.",
     "D": "ABAC policy não cria tags — policy consome tags aplicadas."
    }
   },
   {
    "id": 49,
    "domain": "Cost & Performance Optimization",
    "topic": "",
    "text": "Um job roda bem em testing (10M de dados), mas em produção (10B de dados) falha com OutOfMemory. Você aumenta `spark.executor.memory`. Falha persiste, mas não no driver, no executor. Qual é a verdadeira causa?",
    "options": {
     "A": "Não é executor memory — pode ser spill disco cheio ou partição desbalanceada",
     "B": "Apenas aumentar memory já deveria resolver",
     "C": "O job não é scala-able — precisa ser reescrito",
     "D": "Aumentar memory não ajuda em spill — usar `repartition()` ou compactar dados"
    },
    "answer": "D",
    "explanation": "Se aumentar executor memory não resolve e não é driver OOM, é partição desbalanceada ou spill. `repartition()` resolve.",
    "optExpl": {
     "A": "Partição desbalanceada + spill são possíveis, mas não oferece solução (apenas diagnóstico).",
     "B": "Aumentar memory pode não resolver se problema é spill (disco cheio) ou desbalanceamento.",
     "C": "Job pode ser scala-able com repartition — não precisa reescrever.",
     "D": "Se aumentar executor memory não resolve OOM, problema é spill ou partição desbalanceada — repartition() resolve."
    }
   },
   {
    "id": 50,
    "domain": "Developing Code",
    "topic": "Python",
    "text": "Em um PySpark job, você faz uma análise exploratória sobre tabela Delta. Você quer aplicar transformação custosa sem risco de OOM. Qual é a forma mais segura?",
    "options": {
     "A": "Usar `repartition()` antes de `map()` + aplicar em partições pequenas",
     "B": "`df.map(func).cache().count()` para forçar computação imediata",
     "C": "Usar `foreachPartition()` com limite de batch size",
     "D": "Aumentar `spark.driver.memory` e deixar Spark gerenciar"
    },
    "answer": "B",
    "explanation": "`df.map(func).cache().count()` força computação imediata e caching. Com memory limitada, considerar `DISK_ONLY` storage level.",
    "optExpl": {
     "A": "repartition() pode ajudar com paralelização, mas cache().count() é melhor para exploração.",
     "B": "df.map(func).cache().count() força computação + caches resultado — seguro para análise exploratória.",
     "C": "foreachPartition() é para batch processing, não exploração interativa.",
     "D": "Aumentar driver memory é fallback, não solução para batch processing."
    }
   },
   {
    "id": 51,
    "domain": "Data Ingestion & Acquisition",
    "topic": "",
    "text": "Você tem um webhook que envia eventos a cada segundo. Um webhook pode reenviar o mesmo evento múltiplas vezes (retry). Você quer desduplicar. Qual é a abordagem?",
    "options": {
     "A": "Adicionar `UNIQUE` constraint na coluna de ID (pode falhar se ID já existe)",
     "B": "Usar `INSERT IGNORE` (não existe em Delta/Databricks)",
     "C": "Usar `MERGE` com `WHEN NOT MATCHED INSERT` baseado em chave de dedup + `DELETE FROM staging WHERE ...`",
     "D": "Inserir em staging, depois `INSERT INTO target SELECT DISTINCT * FROM staging`"
    },
    "answer": "C",
    "explanation": "`MERGE` com `WHEN NOT MATCHED INSERT` baseado em ID dedup garante no duplicatas em webhook deduplicação.",
    "optExpl": {
     "A": "UNIQUE constraint em coluna ID = falha se ID já existe (duplicate webhook) — erro em INSERT.",
     "B": "INSERT IGNORE não existe em Delta — Delta não tem 'ignore' semântica nativa.",
     "C": "MERGE com WHEN NOT MATCHED INSERT = insere apenas se ID não existe — deduplicação eficiente.",
     "D": "INSERT DISTINCT = remove duplicatas, mas DISTINCT em JSON/complex types pode não ser deterministic."
    }
   },
   {
    "id": 52,
    "domain": "Data Manipulation",
    "topic": "",
    "text": "Você tem tabela de vendas particionada por `date`. Um INSERT diário agora demora 3x mais. Causa: muitas pequenas partições. Qual solução é melhor?",
    "options": {
     "A": "`OPTIMIZE` com Z-order na tabela completa (custoso, scan completo)",
     "B": "`OPTIMIZE` apenas na partição do dia (`OPTIMIZE table WHERE date = ...`)",
     "C": "Usar compactação incremental (não existe built-in — usar `REPARTITION` antes de INSERT)",
     "D": "Auto-compactação via `OPTIMIZE` automático em Databricks (disponível, mas exige config)"
    },
    "answer": "B",
    "explanation": "`OPTIMIZE` apenas em partição do dia é eficiente — compacta apenas partição nova.",
    "optExpl": {
     "A": "OPTIMIZE full table com Z-order = full scan toda data (custoso para crescimento diário).",
     "B": "OPTIMIZE em partição específica (WHERE date = TODAY) = compacta apenas nova partição.",
     "C": "Compactação incremental não existe built-in; repartition é workaround.",
     "D": "OPTIMIZE automático é possível em Databricks (Predictive Optimization), mas config adicional."
    }
   },
   {
    "id": 53,
    "domain": "Debugging and Deploying",
    "topic": "",
    "text": "Um job agendado falha intermitentemente (50% das vezes). Logs são idênticos. O que provavelmente acontece?",
    "options": {
     "A": "Race condition com job paralelo / locking de tabela Delta",
     "B": "Timeout aleatório de rede",
     "C": "Cluster scala-down entre jobs, vez sim vez não",
     "D": "Sem mais info, é impossível — precisa de mais telemetria (cluster metrics, warehouse logs)"
    },
    "answer": "D",
    "explanation": "Falhas intermitentes 50% — race condition, timeout aleatório, ou cluster scala-down. Precisa telemetria.",
    "optExpl": {
     "A": "Race condition + Delta locking = possível causa de falha intermitente.",
     "B": "Timeout aleatório de rede = possível causa de falha intermitente.",
     "C": "Cluster scale-down intermitente = possível causa de falha intermitente.",
     "D": "Intermitentes 50% = múltiplas causas possíveis — precisa cluster metrics + warehouse logs."
    }
   },
   {
    "id": 54,
    "domain": "Data Governance",
    "topic": "",
    "text": "Você tem uma tabela em UC que será usada por 3 grupos: `analysts` (SELECT), `engineers` (SELECT + MODIFY), `admins` (full). Qual é a forma mais eficiente de gerenciar permissões?",
    "options": {
     "A": "Criar 3 roles + fazer GRANT específico em cada uma",
     "B": "Usar inheritance: criar role `editors` com MODIFY, role `readers` com SELECT, depois `admins EXTEND editors`",
     "C": "Fazer GRANT direto por usuário (não escalável)",
     "D": "Usar um job que roda `ALTER TABLE OWNER TO` a cada mudança de grupo"
    },
    "answer": "A",
    "explanation": "Criar 3 roles + fazer GRANT específico é approach padrão e escalável em UC.",
    "optExpl": {
     "A": "3 roles (analysts, engineers, admins) + específico GRANT = padrão UC escalável.",
     "B": "Role inheritance é possível em UC, mas 'EXTEND' keyword não é standard em GRANT.",
     "C": "GRANT por usuário individual = não escalável (mudança de usuário = manual update).",
     "D": "ALTER TABLE OWNER periodicamente = não é GRANT mechanism — ownership não substitui GRANT."
    }
   },
   {
    "id": 55,
    "domain": "Debugging and Deploying",
    "topic": "",
    "text": "Um job de streaming que consuma de Kafka está com back-pressure crescente. De repente, latência salta. Qual é a causa mais provável após 30 dias de execução?",
    "options": {
     "A": "Kafka tem lag — verificar partition size e consumer group",
     "B": "Checkpoint estado explodiu — limpar checkpoint antigo e reiniciar stream",
     "C": "Shuffle memory cresceu — executor memory limit atingido",
     "D": "Janela de agregação está acumulando — watermark pode estar fixo / estado não está sendo limpado"
    },
    "answer": "D",
    "explanation": "Latência em streaming cresce após 30 dias — watermark acumulando estado old + janela não expirando.",
    "optExpl": {
     "A": "Kafka lag = possível, mas '30 dias' é padrão de state accumulation.",
     "B": "Checkpoint explodindo = possível, mas '30 dias' aponta para state old (watermark não expira).",
     "C": "Shuffle memory crescendo = possível, mas menos provável após '30 dias'.",
     "D": "Agregação janela acumulando estado após 30 dias — watermark fixo (não avançando) = estado não expira."
    }
   },
   {
    "id": 56,
    "domain": "Cost & Performance Optimization",
    "topic": "",
    "text": "Uma tabela Warehouse com 1PB de dados. Usuários reclamam que queries demoram muito. Você precisa de métrica para convencer liderança a investir em otimização. Qual métrica é mais convincente?",
    "options": {
     "A": "Query count (não mostra impacto em usuário)",
     "B": "Custo por query (mostra desperdício)",
     "C": "P95 query latency + custo total (mostra UX ruim + $ waste juntos)",
     "D": "Warehouse size (não diretamente relacionado com performance)"
    },
    "answer": "C",
    "explanation": "P95 latência mostra UX ruim. Custo por query mostra desperdício. Juntos, convence liderança.",
    "optExpl": {
     "A": "Query count = volume, not impacto — não convence sobre performance.",
     "B": "Custo por query = waste, mas não mostra UX impact (usuários esperam longo).",
     "C": "P95 latency = '95% das queries > X segundos' (UX ruins) + custo total = waste + user impact.",
     "D": "Warehouse size = capacity, not latência — não correlação direta com performance."
    }
   },
   {
    "id": 57,
    "domain": "Data Security and Compliance",
    "topic": "",
    "text": "Sua organização tem compliance regulatória que exige: dados PII devem residir apenas em clusters dedicados. Qual é a abordagem nativa em Databricks?",
    "options": {
     "A": "Usar `spark.conf` para limitar read path a específico cluster via access control",
     "B": "UC políticas + compute-level ACL (se suportado) ou implementar em nível de storage",
     "C": "Replicar dados PII em UC schema específico, conceder acesso apenas de cluster gerenciado",
     "D": "Usar Unity Catalog Governance com restriction de table locality (não é feature nativa — manual)"
    },
    "answer": "C",
    "explanation": "Replicar PII em UC schema específico, conceder acesso de compute dedicado é closest solução nativa.",
    "optExpl": {
     "A": "spark.conf não controla table locality — não é mecanismo nativo.",
     "B": "Compute-level ACL não é feature nativa UC ('se suportado' = maybe not).",
     "C": "Replicar PII em UC schema específico + compute dedicado = isolamento físico + lógico (closest nativa).",
     "D": "'table locality restriction' não é feature nativa — manual implementation."
    }
   },
   {
    "id": 58,
    "domain": "Developing Code",
    "topic": "Python",
    "text": "Em um PySpark job, você quer processar VARIANT colunas em uma transformação SQL. Qual abordagem garante melhor performance?",
    "options": {
     "A": "Usar `df.selectExpr(\"explode_outer(variant_col) as item\")` depois iterar em Python",
     "B": "Usar `sql(\"SELECT ... FROM delta.`path` WHERE ...\")` + SQL nativo para manipular VARIANT",
     "C": "Converter VARIANT para string JSON em Python, parsear com `json.loads()`, depois remontar",
     "D": "Usar `pyspark.sql.functions.col()` com `.getItem()` chaining"
    },
    "answer": "B",
    "explanation": "SQL nativo com VARIANT é mais eficiente que Python/RDD. Optimizer otimiza VARIANT operações.",
    "optExpl": {
     "A": "explode_outer + Python RDD = sem Spark SQL optimization.",
     "B": "SQL nativo com VARIANT = otimizado por Catalyst optimizer.",
     "C": "Converter VARIANT → JSON → Python = múltiplas serializações overhead.",
     "D": "getItem() é Column API — menos direto que SQL nativo com VARIANT."
    }
   },
   {
    "id": 59,
    "domain": "Developing Code",
    "topic": "SQL/Python",
    "text": "Você tem JSON aninhado e quer extrair múltiplos campos. Qual é mais performático: `parse_json()` uma vez + múltiplos `variant_get()`, ou parsear N vezes?",
    "options": {
     "A": "Parsear N vezes é ineficiente — usar `parse_json()` uma vez armazenado em VARIANT",
     "B": "Ambos equivalentes em Spark SQL (optimizer é inteligente)",
     "C": "Parse uma vez em Python, depois em SQL (hybrid)",
     "D": "Usar `get_json_object()` N vezes — Spark otimiza internamente"
    },
    "answer": "A",
    "explanation": "Parsear JSON uma vez em VARIANT, depois múltiplos `variant_get()`, evita re-parse.",
    "optExpl": {
     "A": "parse_json() uma vez em VARIANT = 'memoized' — múltiplos variant_get() lêem VARIANT cached.",
     "B": "Spark SQL não é 'smart' a ponto de deduplicate parse_json calls — N calls = N parses.",
     "C": "Python parse + SQL parse = hybrid overhead.",
     "D": "get_json_object() N vezes = N parses — Spark não deduplicates (unlike VARIANT memoization)."
    }
   },
   {
    "id": 60,
    "domain": "Developing Code",
    "topic": "Python/SQL",
    "text": "Um desenvolvimento iterativo usa um notebook com Structured Streaming que processa dados via `foreachBatch()`. Após converter para um job agendado, a latência degrada com 30 dias de execução sem reinício. Qual é o verdadeiro problema?",
    "options": {
     "A": "Checkpoint arquivo ficar muito grande, precisa limpeza periódica",
     "B": "Estado da janela acumulando em memória — watermark expirado ou offset tracking não funciona",
     "C": "Kubernetes pod memory leak em longa duração",
     "D": "Python garbage collection degrading performance"
    },
    "answer": "B",
    "explanation": "Latência em streaming cresce após 30 dias — estado de janela acumulando. Watermark expirado ou offset tracking falha.",
    "optExpl": {
     "A": "Checkpoint grande é possível, mas '30 dias' aponta para state accumulation (watermark).",
     "B": "Estado de janela acumulando após 30 dias — watermark expirado (não avança) ou offset tracking falha.",
     "C": "Pod memory leak = Databricks não é Kubernetes natively (clusters são cloud VMs).",
     "D": "Python GC degradation = improvável causa de latência jump após '30 dias'."
    }
   }
  ]
 }
};
