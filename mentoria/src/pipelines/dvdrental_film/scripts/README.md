# Scripts de Pipeline - dvdrental_film

Scripts auxiliares para suporte à carga incremental do pipeline.

## 📁 Conteúdo

### 1. `setup_checkpoint_table.sql`
Script de setup inicial - cria tabela de controle para checkpoint.

**Quando executar:** UMA ÚNICA VEZ, antes da primeira execução do pipeline.

**Como executar:**
```sql
-- Executar no SQL Editor ou Databricks notebook
%sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.dlt_checkpoint_dvdrental_film (
    table_name STRING,
    last_processed_timestamp TIMESTAMP,
    last_update_time TIMESTAMP
);
```

---

### 2. `update_checkpoint.py`
Notebook Python que atualiza o checkpoint após execução do pipeline.

**Quando executar:** Após cada execução do pipeline DLT (via Job Task 2).

**Como funciona:**
* Lê `MAX(last_update)` da tabela bronze
* Faz `MERGE` na tabela de checkpoint (mantém apenas 1 registro)
* Evita crescimento desnecessário da tabela de controle

---

## 🔄 Fluxo de Execução Completo

### Setup Inicial (executar 1x)
```
1. Executar setup_checkpoint_table.sql
   ↓
2. Primeira execução do pipeline DLT
   ↓
3. Executar update_checkpoint.py manualmente
```

### Execução Recorrente (via Job)
```
Job: "dvdrental_film_incremental"
├── Task 1: Run Pipeline DLT
│   └── Pipeline: [dev pcassio23] pip_dvdrental_film
│
└── Task 2: Update Checkpoint
    └── Notebook: scripts/update_checkpoint.py
    └── Depends on: Task 1 (executa apenas se Task 1 OK)
```

---

## 📊 Verificação do Checkpoint

### Ver checkpoint atual:
```sql
SELECT * 
FROM dev_catalog.bronze.dlt_checkpoint_dvdrental_film;
```

### Ver quantos registros serão processados na próxima execução:
```sql
SELECT COUNT(*) as pending_records
FROM dev_catalog.bronze.dvdrental_film
WHERE last_update > (
    SELECT COALESCE(MAX(last_processed_timestamp), TIMESTAMP '1900-01-01 00:00:00')
    FROM dev_catalog.bronze.dlt_checkpoint_dvdrental_film
);
```

---

## ⚠️ Importante

* **NUNCA** inclua estes scripts na pasta `transformations/` - eles não são código do pipeline
* A tabela de checkpoint usa **MERGE** (não INSERT) para manter apenas 1 registro por tabela fonte
* O script `update_checkpoint.py` deve ser executado **APÓS** o pipeline, nunca durante
* Se a tabela de checkpoint não existir, a primeira execução do pipeline lerá todos os registros históricos

---

## 🔧 Troubleshooting

### Checkpoint não está sendo atualizado
* Verificar se Task 2 do Job está configurada corretamente
* Verificar logs do notebook `update_checkpoint.py`
* Confirmar que Task 2 tem dependência de Task 1

### Pipeline lê todos os registros em vez de incremental
* Verificar se tabela de checkpoint existe e tem dados
* Executar query de verificação acima
* Checar se MAX timestamp no checkpoint é recente

### Tabela de checkpoint crescendo muito
* Confirmar que está usando `update_checkpoint.py` (usa MERGE)
* Se ainda estiver usando INSERT, migrar para MERGE
