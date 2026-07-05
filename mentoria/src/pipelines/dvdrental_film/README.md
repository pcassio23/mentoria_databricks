# Pipeline DVD Rental Film

Pipeline de dados para ingestão e transformação do catálogo de filmes da base PostgreSQL DVD Rental, seguindo arquitetura Medallion (Bronze → Silver → Gold).

---

## 📋 Visão Geral

Este pipeline implementa **carga incremental** de dados do PostgreSQL para o Databricks usando Lakeflow Spark Declarative Pipelines (SDP), com gerenciamento de estado via tabela de checkpoint.

### Características Principais

* **Arquitetura Medallion**: Bronze (raw) → Silver (clean) → Gold (aggregated)
* **Carga Incremental**: Processa apenas registros novos/modificados desde a última execução
* **Gerenciamento de Estado**: Usa tabela de checkpoint em Unity Catalog para rastreamento
* **Data Quality**: Validações automáticas com DLT expectations
* **Tradução PT-BR**: Colunas traduzidas na camada Silver para facilitar uso por analistas
* **CDC Automático**: Deduplicação e merge gerenciados pelo DLT

---

## 🏗️ Arquitetura do Pipeline

```
PostgreSQL (dvdrental.film)
    ↓
┌─────────────────────────────────────────────────────────┐
│ BRONZE LAYER                                            │
│ - Leitura incremental via JDBC                         │
│ - Filtro: WHERE last_update > checkpoint               │
│ - Preserva schema original                             │
│ - Adiciona metadata de ingestão                        │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│ SILVER LAYER                                            │
│ - Limpeza e validação de dados                         │
│ - Tradução de colunas (EN → PT-BR)                     │
│ - Padronização de tipos                                │
│ - Data quality expectations                            │
│ - CDC automático (SCD Type 1)                          │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│ GOLD LAYER                                              │
│ - Agregações analíticas                                │
│ - Views otimizadas para BI                             │
│ - Denormalização para performance                      │
└─────────────────────────────────────────────────────────┘
```

---

## 📁 Estrutura de Arquivos

```
dvdrental_film/
│
├── README.md                           # 📖 Este arquivo
│
├── transformations/                    # Código do pipeline (processado pelo DLT)
│   ├── bronze/
│   │   └── dvdrental_film.py          # Ingestão incremental do PostgreSQL
│   ├── silver/
│   │   └── dvdrental_filmes.py        # Limpeza, validação e tradução
│   └── gold/
│       └── filmes_analytics.py        # Agregações analíticas
│
└── scripts/                            # Scripts auxiliares (NÃO processados pelo DLT)
    ├── README.md                       # Instruções detalhadas dos scripts
    ├── setup_checkpoint_table.sql      # Setup inicial da tabela de checkpoint
    └── update_checkpoint.py            # Notebook para atualizar checkpoint (Task 2)
```

---

## 🎯 Camadas do Pipeline

### **Bronze Layer** (`transformations/bronze/dvdrental_film.py`)

**Responsabilidade**: Ingestão incremental da fonte PostgreSQL

**Tabela Destino**: `dev_catalog.bronze.dvdrental_film`

**Processo**:
1. Lê último checkpoint da tabela de controle
2. Query JDBC com filtro `WHERE last_update > checkpoint`
3. Carrega apenas registros novos/modificados
4. Adiciona `ingestion_timestamp` para auditoria

**Schema** (mantém nomes originais do PostgreSQL):
* `film_id`, `title`, `description`, `release_year`
* `language_id`, `rental_duration`, `rental_rate`
* `length`, `replacement_cost`, `rating`
* `last_update`, `special_features`, `fulltext`
* `ingestion_timestamp` (adicionado)

---

### **Silver Layer** (`transformations/silver/dvdrental_filmes.py`)

**Responsabilidade**: Limpeza, validação e tradução para PT-BR

**Tabela Destino**: `dev_catalog.silver.dvdrental_filmes`

**Processo**:
1. Lê incrementalmente da Bronze via streaming
2. Aplica transformações (trim, conversão de tipos, tradução)
3. Valida dados com DLT expectations
4. CDC automático com `dlt.apply_changes()` (SCD Type 1)

**Transformações Aplicadas**:
* ✅ Tradução de colunas (EN → PT-BR)
* ✅ Trim em campos de texto
* ✅ Upper case em `classificacao`
* ✅ Cast para `decimal(4,2)` e `decimal(5,2)`
* ✅ Adição de `data_processamento`

**Mapeamento de Colunas** (EN → PT-BR):

| Original (EN)      | Traduzido (PT-BR)       | Tipo            |
|--------------------|-------------------------|-----------------|
| film_id            | id_filme                | INT             |
| title              | titulo                  | STRING          |
| description        | descricao               | STRING          |
| release_year       | ano_lancamento          | INT             |
| language_id        | id_idioma               | INT             |
| rental_duration    | duracao_aluguel_dias    | INT             |
| rental_rate        | valor_aluguel           | DECIMAL(4,2)    |
| length             | duracao_minutos         | INT             |
| replacement_cost   | custo_reposicao         | DECIMAL(5,2)    |
| rating             | classificacao           | STRING          |
| last_update        | ultima_atualizacao      | TIMESTAMP       |
| special_features   | recursos_especiais      | STRING          |
| fulltext           | texto_completo          | STRING          |

**Data Quality Expectations**:

| Regra                    | Tipo         | Ação        |
|--------------------------|--------------|-------------|
| titulo não vazio         | expect_or_drop | Drop linha  |
| valor_aluguel > 0        | expect_or_drop | Drop linha  |
| duracao_aluguel_dias > 0 | expect_or_drop | Drop linha  |
| duracao_minutos > 0      | expect_or_drop | Drop linha  |
| custo_reposicao >= 0     | expect_or_drop | Drop linha  |
| classificacao válida     | expect       | Warning     |
| ano_lancamento válido    | expect       | Warning     |

---

### **Gold Layer** (`transformations/gold/filmes_analytics.py`)

**Responsabilidade**: Agregações e views analíticas

**Tabelas/Views**:
* Estatísticas por classificação etária
* Análise de rentabilidade (valor vs custo)
* Métricas de duração de filmes
* Outros agregados conforme necessidade do negócio

---

## 🔄 Gerenciamento de Checkpoint

### **Como Funciona**

O pipeline usa uma **tabela de controle** no Unity Catalog para rastrear o último timestamp processado:

```
dev_catalog.bronze.dlt_checkpoint_dvdrental_film
├─ table_name: "dvdrental_film"
├─ last_processed_timestamp: TIMESTAMP '2025-01-08 15:30:00'
└─ last_update_time: TIMESTAMP '2025-01-08 15:35:00'
```

**A cada execução**:
1. Bronze lê `MAX(last_processed_timestamp)` da tabela de checkpoint
2. Query JDBC filtra: `WHERE last_update > checkpoint`
3. Após sucesso, notebook externo atualiza checkpoint com novo `MAX(last_update)`

**Vantagens**:
* ✅ Lê apenas dados novos (eficiente)
* ✅ Estado persistido entre execuções
* ✅ Idempotente (re-execução segura)
* ✅ Suporta late-arriving data
* ✅ Tabela mantém apenas 1 registro (usa MERGE, não INSERT)

---

## 🚀 Setup e Execução

### **1. Setup Inicial** (executar 1x apenas)

Execute o script SQL para criar a tabela de checkpoint:

```sql
-- Ver: scripts/setup_checkpoint_table.sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.dlt_checkpoint_dvdrental_film (
    table_name STRING,
    last_processed_timestamp TIMESTAMP,
    last_update_time TIMESTAMP
);
```

### **2. Configurar Secrets do PostgreSQL**

```python
# Criar secret scope (se não existir)
dbutils.secrets.put(scope="postgres-secrets", key="postgres-host", value="<host>")
dbutils.secrets.put(scope="postgres-secrets", key="postgres-port", value="5432")
dbutils.secrets.put(scope="postgres-secrets", key="postgres-database", value="dvdrental")
dbutils.secrets.put(scope="postgres-secrets", key="postgres-user", value="<user>")
dbutils.secrets.put(scope="postgres-secrets", key="postgres-password", value="<password>")
```

### **3. Criar Pipeline DLT**

Via Databricks UI ou DABs (arquivo `resources/pipelines/dvdrental_film_pipeline.yml`):

```yaml
resources:
  pipelines:
    dvdrental_film:
      name: "[${var.env} ${workspace.current_user.userName}] pip_dvdrental_film"
      catalog: ${var.catalog}
      schema: ${var.bronze_schema}
      libraries:
        - glob:
            include: "/Workspace/Repos/pcassio23@gmail.com/mentoria_databricks/mentoria/src/pipelines/dvdrental_film/transformations/**"
      development: true
      serverless: true
      edition: ADVANCED
```

### **4. Criar Job com 2 Tasks**

**Task 1: Run Pipeline DLT**
* Type: `Pipeline`
* Pipeline: `[dev pcassio23] pip_dvdrental_film`
* Full Refresh: `false`

**Task 2: Update Checkpoint**
* Type: `Notebook`
* Path: `/Repos/pcassio23@gmail.com/mentoria_databricks/mentoria/src/pipelines/dvdrental_film/scripts/update_checkpoint.py`
* **Depends On**: Task 1 (executa apenas se Task 1 OK)
* Cluster: Shared or Serverless

### **5. Executar**

**Primeira Execução** (carga completa):
1. Executar pipeline manualmente
2. Pipeline lê todos os registros históricos (checkpoint = '1900-01-01')
3. Executar `update_checkpoint.py` manualmente para setar checkpoint inicial

**Execuções Subsequentes** (via Job):
1. Job dispara automaticamente (schedule ou manual)
2. Task 1 executa pipeline (lê apenas incremento)
3. Task 2 atualiza checkpoint (apenas se Task 1 OK)

---

## 🔍 Verificação e Monitoramento

### **Ver Checkpoint Atual**

```sql
SELECT * 
FROM dev_catalog.bronze.dlt_checkpoint_dvdrental_film;
```

### **Ver Registros Pendentes (próxima execução)**

```sql
SELECT COUNT(*) as pending_records
FROM dev_catalog.bronze.dvdrental_film
WHERE last_update > (
    SELECT COALESCE(MAX(last_processed_timestamp), TIMESTAMP '1900-01-01 00:00:00')
    FROM dev_catalog.bronze.dlt_checkpoint_dvdrental_film
);
```

### **Queries de Data Quality**

```sql
-- Ver expectativas violadas (registros dropados)
SELECT * FROM event_log('<pipeline_id>')
WHERE details:flow_progress.metrics IS NOT NULL;

-- Estatísticas da Silver
SELECT 
    COUNT(*) as total_filmes,
    COUNT(DISTINCT classificacao) as classificacoes_unicas,
    AVG(valor_aluguel) as valor_medio,
    AVG(duracao_minutos) as duracao_media
FROM dev_catalog.silver.dvdrental_filmes;
```

---

## 🛠️ Troubleshooting

### **Erro: DELTA_SOURCE_TABLE_IGNORE_CHANGES**

**Causa**: Tabela bronze tem commits de UPDATE/DELETE que streaming não suporta por padrão.

**Solução**: Já aplicada na linha 98 de `dvdrental_filmes.py`:
```python
df_bronze = spark.readStream.option("skipChangeCommits", "true").table("dvdrental_film")
```

### **Checkpoint Não Está Sendo Atualizado**

**Verificar**:
1. Task 2 do Job está configurada corretamente?
2. Task 2 tem dependência explícita de Task 1?
3. Logs do notebook `update_checkpoint.py` mostram sucesso?

### **Pipeline Lê Todos os Registros em Vez de Incremental**

**Verificar**:
1. Tabela de checkpoint existe e tem dados?
2. Query de verificação (acima) retorna checkpoint válido?
3. Secrets do PostgreSQL estão configurados corretamente?

### **Tabela de Checkpoint Crescendo Muito**

**Causa**: Versão antiga do código usava INSERT em vez de MERGE.

**Solução**: Já corrigida - código atual usa MERGE (mantém apenas 1 registro).

Se ainda estiver crescendo, executar manualmente:
```sql
TRUNCATE TABLE dev_catalog.bronze.dlt_checkpoint_dvdrental_film;
-- Depois executar update_checkpoint.py
```

---

## 📊 Arquitetura Técnica Detalhada

### **Pattern: Incremental JDBC sem CDC Nativo**

**Quando Usar**:
* ✅ Fonte JDBC sem CDC nativo (PostgreSQL, MySQL, SQL Server)
* ✅ Tabela tem coluna timestamp de modificação (`last_update`)
* ✅ Volume incremental é gerenciável
* ✅ Não precisa rastrear deletes (apenas inserts/updates)

**Quando NÃO Usar**:
* ❌ Precisa rastrear deletes → Use `apply_changes` com CDC real
* ❌ Volume muito grande → Use export para files + Auto Loader
* ❌ Fonte tem CDC nativo (Debezium, etc) → Use `apply_changes` com Kafka

### **View Intermediária na Silver (Linha 72)**

A temporary view `dvdrental_filmes_source` é **essencial** porque:

1. **Separação de Responsabilidades**: Transformações na view, CDC no `apply_changes()`
2. **Data Quality ANTES do CDC**: Expectations filtram dados ruins antes do target
3. **Schema Preparation**: Tradução de colunas aplicada uma única vez
4. **Pattern Recomendado**: Source (view) → `apply_changes()` → Target (streaming table)
5. **Performance**: Transformações em único select, sem múltiplas passagens

### **SCD Type 1 vs Type 2**

**Configuração Atual**: `stored_as_scd_type=1`

* **SCD Type 1**: Sobrescreve registro existente (mantém apenas versão atual)
* **SCD Type 2**: Mantém histórico completo (versioning)

Para habilitar histórico completo, mudar para:
```python
stored_as_scd_type=2
```

---

## 📚 Referências

* [Lakeflow Spark Declarative Pipelines (SDP)](https://docs.databricks.com/en/dlt/)
* [DLT apply_changes()](https://docs.databricks.com/en/delta-live-tables/cdc.html)
* [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
* [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/)

---

## 👤 Autor

**Paulo Costa** (pcassio23@gmail.com)  
Criado em: 2025-01-08  
Última atualização: 2025-01-08

---

## 📝 Histórico de Alterações

| Data       | Autor    | Alteração                                           |
|------------|----------|-----------------------------------------------------|
| 2025-01-08 | pcassio23 | Versão inicial do pipeline                         |
| 2025-01-08 | pcassio23 | Migração para apply_changes incremental            |
| 2025-01-08 | pcassio23 | Correção skipChangeCommits + checkpoint MERGE      |
| 2025-01-08 | pcassio23 | Adição de scripts auxiliares (pasta scripts/)      |
