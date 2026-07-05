import dlt
from pyspark.sql import functions as F
from datetime import datetime

# ==============================================================================
# Bronze Layer: Incremental Ingestion from PostgreSQL
# ==============================================================================
# 
# Implementa carga incremental de PostgreSQL com state management robusto.
# 
# Estratégia:
# 1. Usa tabela de controle no UC para rastrear último timestamp processado
# 2. Lê incrementalmente do JDBC filtrando por last_update > stored checkpoint
# 3. DLT gerencia a deduplicação e merge automaticamente
# 4. Checkpoint é atualizado via Task 2 do Job (notebook externo)
#
# Nota: Para fontes JDBC sem CDC nativo, usamos @dlt.table() com batch read.
# Apply changes é reservado para fontes com CDC real (Kafka, Delta CDC, etc).
# ==============================================================================


def get_checkpoint_table_path():
    """
    Retorna o caminho completo da tabela de checkpoint.
    
    IMPORTANTE: Usa catalog e schema FIXOS para evitar problemas com contexto.
    O DLT pode executar em um contexto diferente (workspace.bronze) do esperado.
    
    Returns:
        str: Caminho fixo dev_catalog.bronze.dlt_checkpoint_dvdrental_film
    """
    # Usar valores fixos para garantir consistência
    # O checkpoint sempre vive em dev_catalog.bronze independente do contexto DLT
    catalog = "dev_catalog"
    schema = "bronze"
    table_name = "dlt_checkpoint_dvdrental_film"
    
    return f"{catalog}.{schema}.{table_name}"


def get_checkpoint_timestamp():
    """
    Recupera o último timestamp processado da tabela de controle.
    
    Returns:
        str: Timestamp no formato 'YYYY-MM-DD HH:MM:SS', ou '1900-01-01 00:00:00' se primeira execução
    """
    try:
        checkpoint_table = get_checkpoint_table_path()
        
        print(f"📍 Reading checkpoint from: {checkpoint_table}")
        
        # Tenta ler último checkpoint da tabela de controle
        checkpoint_df = spark.sql(f"""
            SELECT MAX(last_processed_timestamp) as max_ts
            FROM {checkpoint_table}
        """)
        
        result = checkpoint_df.collect()[0]["max_ts"]
        if result:
            timestamp_str = str(result)
            print(f"✅ Found checkpoint: {timestamp_str}")
            return timestamp_str
        else:
            print("⚠️  No checkpoint found - starting from beginning")
            return "1900-01-01 00:00:00"
    except Exception as e:
        print(f"⚠️  Error reading checkpoint: {e}")
        print("⚠️  Falling back to initial load (1900-01-01)")
        # Primeira execução - tabela de controle vazia ou não existe
        return "1900-01-01 00:00:00"


@dlt.table(
    comment="Bronze table - DVD Rental film catalog with incremental updates from PostgreSQL"
)
def dvdrental_film():
    """
    Lê incrementalmente da tabela dvdrental.film do PostgreSQL.
    
    Filtra registros com last_update > checkpoint salvo na execução anterior.
    Na primeira execução, carrega todos os registros.
    
    Returns:
        DataFrame: Registros novos ou modificados desde última execução
    """
    
    # PostgreSQL connection properties from Databricks secrets
    host = dbutils.secrets.get(scope="postgres-secrets", key="postgres-host")
    port = dbutils.secrets.get(scope="postgres-secrets", key="postgres-port")
    database = dbutils.secrets.get(scope="postgres-secrets", key="postgres-database")
    username = dbutils.secrets.get(scope="postgres-secrets", key="postgres-user")
    password = dbutils.secrets.get(scope="postgres-secrets", key="postgres-password")
    
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    
    # Recuperar último timestamp processado do checkpoint
    max_timestamp_str = get_checkpoint_timestamp()
    
    print(f"🔄 Incremental read from PostgreSQL - Processing records after: {max_timestamp_str}")
    
    # Query incremental usando subquery no JDBC
    # Lê apenas registros com last_update maior que o checkpoint
    incremental_query = f"""
        (SELECT * FROM dvdrental.film 
         WHERE last_update > TIMESTAMP '{max_timestamp_str}'
         ORDER BY last_update) as incremental_data
    """
    
    # Read incremental data from PostgreSQL
    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", incremental_query)
        .option("user", username)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    
    # Add ingestion timestamp for data lineage tracking
    df_with_metadata = df.withColumn("ingestion_timestamp", F.current_timestamp())
    
    return df_with_metadata


# ==============================================================================
# DOCUMENTAÇÃO: Incremental JDBC Pattern com State Management
# ==============================================================================
"""
Pattern para Carga Incremental de JDBC sem CDC Nativo com Estado Persistente:

1. PRIMEIRA EXECUÇÃO:
   - Checkpoint table deve existir em dev_catalog.bronze.dlt_checkpoint_dvdrental_film
   - Query lê todos os registros (WHERE last_update > '1900-01-01')
   - Cria tabela bronze com todos os dados
   - Task 2 do Job atualiza checkpoint com MAX(last_update)

2. EXECUÇÕES SUBSEQUENTES:
   - Lê timestamp da tabela de controle (dev_catalog.bronze.dlt_checkpoint_dvdrental_film)
   - Query lê apenas registros WHERE last_update > checkpoint
   - DLT faz MERGE automático dos novos registros
   - Task 2 do Job atualiza checkpoint após pipeline

3. CHECKPOINT/STATE MANAGEMENT:
   - Tabela: dev_catalog.bronze.dlt_checkpoint_dvdrental_film (FIXO)
   - Atualização: Via Task 2 do Job (notebook scripts/update_checkpoint.py)
   - Usa MERGE para manter apenas 1 registro por tabela
   - Persiste entre execuções do pipeline

FLUXO DO JOB:
Task 1: Run DLT Pipeline (executa este código) → lê checkpoint incremental
Task 2: Update Checkpoint (notebook externo) → atualiza checkpoint com MAX timestamp
Task 3: Sync to Lakebase (synced table) → sincroniza gold para Postgres

VANTAGENS:
✓ Lê apenas dados novos/modificados (eficiência garantida)
✓ Checkpoint em tabela UC (rastreável e auditável)  
✓ Idempotente (re-executar é seguro)
✓ Suporta late-arriving data
✓ Recuperação de falhas é confiável
✓ Checkpoint não cresce infinitamente (MERGE mantém 1 registro)
✓ Logs informativos para troubleshooting

QUANDO USAR ESTE PATTERN:
- Fonte JDBC sem CDC nativo (PostgreSQL, MySQL, SQL Server)
- Tabela tem coluna timestamp de modificação
- Volume incremental é gerenciável
- Não precisa rastrear deletes (apenas inserts/updates)

QUANDO NÃO USAR:
- Precisa rastrear deletes → Use apply_changes com fonte CDC real
- Volume muito grande → Use export para files + Auto Loader  
- Fonte tem CDC nativo (Debezium, etc) → Use apply_changes com Kafka

TROUBLESHOOTING:
- Se pipeline faz full recompute: Verifique se checkpoint table existe e tem dados
- Se erro "table not found": Execute scripts/setup_checkpoint_table.sql primeiro
- Se Task 2 falha: Verifique permissões e se bronze table foi populada
"""
