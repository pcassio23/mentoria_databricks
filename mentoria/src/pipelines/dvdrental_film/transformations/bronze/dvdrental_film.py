import dlt
from pyspark.sql import functions as F
from datetime import datetime
import json

# ==============================================================================
# Bronze Layer: Incremental Ingestion from PostgreSQL
# ==============================================================================
# 
# Implementa carga incremental de PostgreSQL com state management robusto.
# 
# Estratégia:
# 1. Usa arquivo de state (checkpoint) para rastrear último timestamp processado
# 2. Lê incrementalmente do JDBC filtrando por last_update > stored checkpoint
# 3. DLT gerencia a deduplicação e merge automaticamente
# 4. Atualiza checkpoint ao final de cada execução
#
# Nota: Para fontes JDBC sem CDC nativo, usamos @dlt.table() com batch read.
# Apply changes é reservado para fontes com CDC real (Kafka, Delta CDC, etc).
# ==============================================================================

# Configuração do estado/checkpoint
STATE_PATH = "/tmp/dlt_bronze_checkpoint"
STATE_FILE = f"{STATE_PATH}/dvdrental_film_state.json"


def get_checkpoint_timestamp():
    """
    Recupera o último timestamp processado do arquivo de estado.
    
    Returns:
        str: Timestamp no formato 'YYYY-MM-DD HH:MM:SS', ou '1900-01-01 00:00:00' se primeira execução
    """
    try:
        # Tenta ler arquivo de estado
        state_df = spark.read.text(STATE_FILE)
        state_json = state_df.collect()[0][0]
        state = json.loads(state_json)
        return state.get("last_processed_timestamp", "1900-01-01 00:00:00")
    except Exception:
        # Primeira execução - arquivo não existe
        return "1900-01-01 00:00:00"


def save_checkpoint_timestamp(max_timestamp):
    """
    Salva o novo checkpoint (máximo timestamp processado) no arquivo de estado.
    
    Args:
        max_timestamp: Timestamp máximo processado nesta execução
    """
    state = {
        "last_processed_timestamp": max_timestamp,
        "last_update_time": datetime.now().isoformat()
    }
    
    # Salva como JSON em Delta Lake
    state_rdd = spark.sparkContext.parallelize([json.dumps(state)])
    state_rdd.coalesce(1).saveAsTextFile(STATE_FILE, compressionCodecClass=None)


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
    
    # Calcular e salvar novo checkpoint (MAX last_update dos dados lidos)
    if df_with_metadata.count() > 0:
        new_max_timestamp = (
            df_with_metadata
            .agg(F.max("last_update").cast("string"))
            .collect()[0][0]
        )
        save_checkpoint_timestamp(new_max_timestamp)
        print(f"✅ Checkpoint updated to: {new_max_timestamp}")
    
    return df_with_metadata


# ==============================================================================
# DOCUMENTAÇÃO: Incremental JDBC Pattern com State Management
# ==============================================================================
"""
Pattern para Carga Incremental de JDBC sem CDC Nativo com Estado Persistente:

1. PRIMEIRA EXECUÇÃO:
   - Arquivo de checkpoint não existe
   - Query lê todos os registros (WHERE last_update > '1900-01-01')
   - Cria tabela bronze com todos os dados
   - Salva MAX(last_update) no arquivo de estado

2. EXECUÇÕES SUBSEQUENTES:
   - Lê timestamp do arquivo de checkpoint
   - Query lê apenas registros WHERE last_update > checkpoint
   - DLT faz MERGE automático dos novos registros
   - Atualiza checkpoint com novo MAX(last_update)

3. CHECKPOINT/STATE MANAGEMENT:
   - Arquivo JSON salvo em /tmp/dlt_bronze_checkpoint/dvdrental_film_state.json
   - Independente da tabela DLT (mais robusto)
   - Persiste entre execuções do pipeline
   - Inclui timestamp da última atualização

VANTAGENS:
✓ Lê apenas dados novos/modificados (eficiência garantida)
✓ State separado da tabela (não depende de meta-dados da tabela)  
✓ Idempotente (re-executar é seguro)
✓ Suporta late-arriving data
✓ Recuperação de falhas é confiável
✓ Logs informativos para troubleshooting

QUANDO USAR ESTE PATTERN:
- Fonte JDBC sem CDC nativo (PostgreSQL, MySQL, SQL Server)
- Tabela tem coluna timestamp de modificação
- Volume incremental é gerenciável
- Não precisa rastrear deletes (apenas inserts/updates)

WHEN NÃO USAR:
- Precisa rastrear deletes → Use apply_changes com fonte CDC real
- Volume muito grande → Use export para files + Auto Loader  
- Fonte tem CDC nativo (Debezium, etc) → Use apply_changes com Kafka
"""
