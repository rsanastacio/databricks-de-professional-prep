# Plano de 14 dias — Data Engineer Professional · EXAME NOVO (a partir de 9-out-2026)

> Guia vigente: **out/2026, coluna New Exam** · **60 questões** · 120 min · **só inglês** · aprovação ~70%.
> Filosofia: você já tem a base. Isto é **recap dirigido por peso + fechar os tópicos net-new + muitos simulados**.
> ⚠️ Exame é **em inglês**: treine a terminologia em inglês (os termos abaixo estão no idioma da prova).
> Regra de ouro: **todo erro em simulado vira nota no tracker** (§ Tracker) + revisão da doc do objetivo.

---

## Prioridade por peso (exame novo — 9 seções)

| Bloco | Seções | Peso somado | Prioridade |
|------|--------|-------------|-----------|
| **A — Código/pipelines** | S1 (23%) | 23% | 🔴 Máxima |
| **B — Custo/Performance** | S5 (15%) | 15% | 🔴 Máxima |
| **C — Ingestão + Manipulação** | S2 (12%) + S3 (12%) | 24% | 🟠 Alta |
| **D — Monitor + Debug/Deploy** | S4 (10%) + S8 (10%) | 20% | 🟠 Alta |
| **E — Segurança + Governança + Modelagem** | S6 (8%) + S7 (5%) + S9 (5%) | 18% | 🟡 Média |

Regra prática: se o tempo apertar, **nunca corte A e B** (38% do exame).

---

## Cronograma (2 semanas)

### Semana 1 — Diagnóstico + recap por domínio

**Dia 1 — Diagnóstico + realinhar vocabulário (2–3h)**
- Ler o **guia out/2026 (coluna New Exam)** inteiro + fazer as **10 sample questions oficiais novas** (no § Cheat sheet, com gabarito). São o material mais fiel que existe.
- Simulado a frio na Udemy (⚠️ é do exame antigo — serve p/ medir base, mas ignore os tópicos net-new que ele não cobre). Anotar % por seção no tracker.
- Montar 2 glossários: (a) **renomeações** (DLT→Lakeflow Declarative Pipelines, DABs→Declarative Automation Bundles, APPLY CHANGES→AUTO CDC, Repos→Git folders); (b) **termos em inglês** dos net-new (watermark, checkpoint, deletion vectors, governed tag, etc.).
- **Agendar a prova** (Webassessor) para data ≥ 9-out; exame em inglês.

**Dia 2 — S1 Código, parte 1 (Bloco A, 23%) (2–3h)**
- Estrutura de projeto Python p/ Declarative Automation Bundles (modular + CI/CD).
- Troubleshooting de dependências (PyPI/wheels/source archives) **em serverless, pipeline e bundle-deployed**.
- UDFs: **Pandas, Python E SQL, incluindo Unity Catalog functions**.
- Lakeflow Declarative Pipelines + Auto Loader p/ streaming; **streaming table vs materialized view** (decidir por latência/custo/refresh).
- Drill: 15–20 questões de S1.

**Dia 3 — S1 Código, parte 2 (2–3h) — NET-NEW pesado**
- **Structured Streaming stateful**: watermarks (limitar crescimento de estado), output modes, `foreachBatch`, checkpoints → recuperação **exactly-once** após falha do driver.
- AUTO CDC APIs + **SCD Type 1 e Type 2 via `stored_as_scd_type`**.
- Structured Streaming **vs** Lakeflow Declarative Pipelines (quando cada um).
- Lakeflow Jobs com control flow (**If/Else, For Each**).
- Compute/config: **serverless compute** (environments, dependency mgmt, **performance mode**), high-memory notebook tasks, auto-optimization (disallow retries).
- Testes: `assertDataFrameEqual`, `assertSchemaEqual`, `DataFrame.transform`.
- Drill: 15–20 questões; revisar erros do Dia 1 em S1.

**Dia 4 — S5 Custo & Performance (Bloco B, 15%) (2–3h)**
- Managed tables + **Predictive Optimization** + Liquid Clustering → reduzir overhead operacional.
- **Escolher a técnica certa**: deletion vectors vs Liquid Clustering vs **CLUSTER BY AUTO** por padrão de acesso.
- **Delta cache** p/ leituras repetidas.
- **CDF** p/ expor mudanças row-level (updates/deletes) e processamento incremental downstream.
- Query profile: gargalos (data skipping ruim, estratégia de join, shuffle).
- **Liquid Clustering vs partitioning/ZORDER** por tamanho de tabela e padrão de query.
- Drill: 15–20 questões + Query Profiler na prática (Free Edition).

**Dia 5 — S2 Ingestão & Aquisição (Bloco C, 12%) (2–3h) — NET-NEW pesado**
- Formatos: Delta, Parquet, JSON, **Iceberg**, CSV, Binary; fontes: message buses (**Kafka, Kinesis, Pub/Sub**) + cloud storage.
- CDC incremental com Lakeflow Pipelines tendo **Delta OU Iceberg** como formato alvo.
- **Lakeflow Connect**: conectores gerenciados de CDC de **SQL Server, MySQL, PostgreSQL** (inclui deletes, mínimo código).
- **OpenSharing** (D2D e Databricks-to-Open) + **Clean Rooms** (colaboração preservando privacidade).
- Lakehouse Federation com governança (UC permissions + credenciais de conexão).
- Drill: 15–20 questões de S2.

**Dia 6 — S3 Data Manipulation (Bloco C, 12%) (2–3h) — NET-NEW pesado**
- Transformações avançadas: window functions, joins, aggregations (Spark SQL + PySpark).
- **VARIANT**: modelar/consultar semi-estruturado com `parse_json`, `variant_get`, acesso por colon-path.
- **AI functions**: `ai_query` p/ inferência de modelo no pipeline (enriquecimento/classificação).
- **Data quality expectations** em Lakeflow Declarative Pipelines: quarantine / drop / fail em registros ruins.
- Drill: 15–20 questões de S3.

**Dia 7 — S4 Monitoramento (10%) + S8 Debug/Deploy (10%) + Simulado #2 (3h)**
- S4: **system tables** (billing, compute, access, lakeflow) p/ custo/auditoria/workload; REST API/CLI/**SDK**; event logs de pipelines; **Databricks Lakehouse alerts** (métricas governadas, qualidade, custo, SQL warehouse/query health, audit/segurança, qualidade de AI agent, branching de Lakeflow Job); Jobs UI/API.
- S8: diagnóstico via Spark UI/cluster logs/system tables/query profiles; **job repairs + parameter overrides**; event logs; deploy com **Declarative Automation Bundles**; **Git folders** p/ CI/CD.
- **Simulado completo #2** (cronometrado). Atualizar tracker.

### Semana 2 — Fechar lacunas + simulados em série

**Dia 8 — S6 Segurança (8%) + S7 Governança (5%) + S9 Modelagem (5%) (2–3h)**
- S6: ACLs least-privilege em securables do UC; **ABAC com governed tags** → row filters/column masks em escala (mascarar tudo que tem tag 'pii', inclusive tabelas futuras); anonimização/pseudonimização (hashing, tokenization, suppression, generalization); pipeline compliant batch+streaming com detecção/masking de PII; **data purging** (GDPR right-to-erasure / right-to-be-forgotten) com Delta + UC.
- S7: **UC tags e comments** p/ discoverability; **modelo de herança de permissões do UC** (grant no catálogo herda p/ schemas/objetos, inclusive criados depois).
- S9: layout de tabelas Delta/Iceberg (partition-to-grain, clustering por padrão de acesso, compaction/tamanho de arquivo); modelos dimensionais com **Materialized Views** (agregação pré-computada) + **UC Metric Views** (definição de métrica governada e reutilizável).
- Drill: 20 questões cobrindo S6+S7+S9.

**Dia 9 — Simulado #3 + revisão profunda de erros (3h)**
- 60 q cronometradas (sample oficiais + Udemy, filtrando tópicos antigos). Para **cada erro**: reler objetivo no guia + doc oficial. Registrar padrão no tracker.

**Dia 10 — Dia das lacunas (2–3h)**
- Atacar SÓ as 2–3 seções com pior % no tracker. Drills focados + doc. Priorizar net-new se estiverem fracos.

**Dia 11 — Simulado #4 + revisão (3h)** — meta ≥80% consistente.

**Dia 12 — Dia das armadilhas (2h)**
- Revisar § Cheat sheet + seu log de erros recorrentes. Redrill só do que errou em ≥2 simulados.

**Dia 13 — Simulado #5 em condição de prova (2.5h)**
- 120 min sem pausa/consulta. Meta ≥85%.
- **Reconfirmar o guia oficial** (o guia pede checar 2 semanas antes).
- Logística: Webassessor; **exame em inglês**; testar proctoring online.

**Dia 14 — Véspera leve (1–1.5h)**
- Só releitura do cheat sheet + glossários. Sem simulado novo. Dormir.

---

## Cheat sheet — 10 sample questions OFICIAIS do exame novo (com gabarito + porquê)
Padrão exato de como a prova nova formula. Memorize o raciocínio, não a resposta.

1. **Streaming table vs Materialized View** — MV quando o agregado precisa refletir **histórico completo + updates tardios** e refresh agendado; streaming table é append/incremental exactly-once e **não** recomputa agregado sobre histórico que muda. *(resp. C)*
2. **Streaming stateful** — **watermark** (limita estado descartando eventos muito atrasados) + **checkpoint** (restaura offsets/estado → exactly-once no restart). *(resp. A)*
3. **CDC de RDBMS** — **Lakeflow Connect** managed connector (Postgres/MySQL/SQL Server) ingere CDC incl. deletes com mínimo código; full reload/CSV/OpenSharing não servem. *(resp. B)*
4. **VARIANT** — `parse_json` p/ coluna VARIANT + `variant_get`/colon-path: storage eficiente e query flexível sem schema fixo. *(resp. A)*
5. **Atribuição de custo** — `system.billing.usage` join com system tables de compute/pricing no UC (dados governados e consultáveis). *(resp. C)*
6. **Write amplification em MERGE pequenos** — **deletion vectors** marcam updates/deletes row-level sem reescrever arquivos inteiros; compaction reconcilia depois. *(resp. B)*
7. **Mascarar PII em escala** — **política ABAC** sobre a governed tag 'pii' aplica column mask onde a tag existir, inclusive tabelas futuras (não mask por tabela nem views duplicadas). *(resp. D)*
8. **Herança de permissão UC** — grant de SELECT no **catálogo** é herdado por schemas/tabelas, **inclusive as criadas depois**. *(resp. A)*
9. **Deploy multi-ambiente** — bundle `databricks.yml` com per-target overrides + `databricks bundle deploy -t <target>` (reprodutível, CI/CD). *(resp. B)*
10. **Métrica consistente + dashboards lentos** — **UC Metric View** (definição governada única) + **Materialized Views** (agregados pré-computados que os dashboards leem). *(resp. C)*

### Outros itens de alta densidade por objetivo
- **SCD Type 1 vs 2** via `stored_as_scd_type` no AUTO CDC.
- **CLUSTER BY AUTO / Predictive Optimization**: manutenção automática de layout/clustering.
- **Delta cache** ≠ resultado de query; acelera leituras repetidas dos mesmos dados.
- **Clean Rooms**: colaboração entre parceiros sem expor dados brutos.
- **ai_query**: inferência/classificação dentro do pipeline SQL/DataFrame.
- **Iceberg como alvo**: pipelines CDC podem gravar em Delta *ou* Iceberg.
- **Lakehouse Federation**: query cross-source com governança UC (sem mover dados).

---

## Recursos
**Treinos oficiais (Databricks Academy) — alinhados ao guia:**
- Advanced Data Engineering with Databricks (ILT) — curso central.
- Advanced Techniques with Apache Spark™ Declarative Pipeline.
- Databricks Data Privacy (→ S6).
- Databricks Performance Optimization (→ S5).
- Automated Deployment with Declarative Automation Bundles (→ S8).

**Prática oficial:**
- **AI Prep Guide** (oficial Databricks): transforma um chatbot em tutor "primado" com o guia — gera checklist de 6–10 tarefas hands-on mapeadas aos objetivos.
- **Databricks Free Edition**: fazer os hands-on (VARIANT, ai_query, deletion vectors, DABs, Metric View, streaming stateful).
- **Docs oficiais**: fonte de verdade p/ os net-new (Iceberg, Lakeflow Connect, ABAC, Metric Views) — material de terceiros ainda não cobre.

**Simulados de terceiros (⚠️ exame ANTIGO — use com filtro):**
- Udemy "Practice Exams: Databricks Data Engineer Professional".
- GitHub Amrit-Hub — question hints. Vlad Siv — prep notes.
- Não cobrem net-new (streaming stateful, Iceberg, Lakeflow Connect, VARIANT, AI functions, ABAC, Metric Views). Complemente com Academy + docs.

**Logística:** Webassessor (webassessor.com/databricks). **Exame novo é só em inglês; marcar data ≥ 9-out.**

---

## Tracker de simulados (exame novo — 9 seções)
Meta: subir e estabilizar ≥85% antes do Dia 13.

| Simulado | Dia | Geral | S1 Code | S2 Ingest | S3 Manip | S4 Monit | S5 Cost/Perf | S6 Sec | S7 Gov | S8 Debug | S9 Model | Piores 3 |
|----------|-----|-------|---------|-----------|----------|----------|--------------|--------|--------|----------|----------|----------|
| #1 (frio) | 1  |       |         |           |          |          |              |        |        |          |          |          |
| #2        | 7  |       |         |           |          |          |              |        |        |          |          |          |
| #3        | 9  |       |         |           |          |          |              |        |        |          |          |          |
| #4        | 11 |       |         |           |          |          |              |        |        |          |          |          |
| #5        | 13 |       |         |           |          |          |              |        |        |          |          |          |

### Log de erros recorrentes (errou em ≥2 simulados)
| Tópico/objetivo | Seção | Por que errei | Doc/nota de correção |
|-----------------|-------|---------------|----------------------|
|                 |       |               |                      |
