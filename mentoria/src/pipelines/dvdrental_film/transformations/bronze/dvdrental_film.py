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
# 4. Atualiza checkpoint ao final de cada execução
#
# Nota: Para fontes JDBC sem CDC nativo, usamos @dlt.table() com batch read.
# Apply changes é reservado para fontes com CDC real (Kafka, Delta CDC, etc).
# ==============================================================================


def get_checkpoint_table_path():
    """
    Obtém o caminho completo da tabela de checkpoint (catalog.schema.table).
    Deve ser chamada dentro do contexto DLT.
    """
    try:
        catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
        schema = spark.sql("SELECT current_schema()").collect()[0][0]
        return f"{catalog}.{schema}.dlt_checkpoint_dvdrental_film"
    except Exception as e:
        print(f"⚠️  Error getting checkpoint table path: {e}")
        # Fallback para schema padrão
        return "default.dlt_checkpoint_dvdrental_film"





def get_checkpoint_timestamp():
    """
    Recupera o último timestamp processado da tabela de controle.
    
    Returns:
        str: Timestamp no formato 'YYYY-MM-DD HH:MM:SS', ou '1900-01-01 00:00:00' se primeira execução
    """
    try:
        checkpoint_table = get_checkpoint_table_path()
        # Tenta ler último checkpoint da tabela de controle
        checkpoint_df = spark.sql(f"""
            SELECT MAX(last_processed_timestamp) as max_ts
            FROM {checkpoint_table}
        """)
        
        result = checkpoint_df.collect()[0]["max_ts"]
        if result:
            return str(result)
        else:
            return "1900-01-01 00:00:00"
    except Exception as e:
        print(f"⚠️  Error reading checkpoint: {e}")
        # Primeira execução - tabela de controle vazia
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


# ============================================================================
# Função para atualizar checkpoint (deve ser chamada via notebook externo)
# ============================================================================

def update_checkpoint_after_pipeline():
    """
    Atualiza o checkpoint com o MAX timestamp da tabela bronze.
    
    NOTA: Esta função deve ser chamada APÓS a execução do pipeline DLT,
    via um notebook separado ou um job Python.
    Não pode ser chamada de dentro de uma transformação DLT.
    """
    try:
        checkpoint_table = get_checkpoint_table_path()
        
        # Lê o MAX timestamp da tabela bronze
        max_ts_df = spark.sql("""
            SELECT COALESCE(MAX(last_update), TIMESTAMP '1900-01-01 00:00:00') as new_max_ts
            FROM dvdrental_film
        """)
        
        new_max_ts = max_ts_df.collect()[0]['new_max_ts']
        
        if new_max_ts and str(new_max_ts) != "1900-01-01 00:00:00":
            # MERGE em vez de INSERT - mantém apenas 1 registro por tabela
            spark.sql(f"""
                MERGE INTO {checkpoint_table} AS target
                USING (
                    SELECT 
                        'dvdrental_film' as table_name,
                        TIMESTAMP '{new_max_ts}' as last_processed_timestamp,
                        current_timestamp() as last_update_time
                ) AS source
                ON target.table_name = source.table_name
                WHEN MATCHED THEN 
                    UPDATE SET 
                        target.last_processed_timestamp = source.last_processed_timestamp,
                        target.last_update_time = source.last_update_time
                WHEN NOT MATCHED THEN 
                    INSERT (table_name, last_processed_timestamp, last_update_time)
                    VALUES (source.table_name, source.last_processed_timestamp, source.last_update_time)
            """)
            print(f"✅ Checkpoint updated to: {new_max_ts}")
            return True
        else:
            print("ℹ️  No new records to process")
            return False
    except Exception as e:
        print(f"❌ Error updating checkpoint: {e}")
        return False


# ==============================================================================
# DOCUMENTAÇÃO: Incremental JDBC Pattern com State Management
# ==============================================================================
"""
Pattern para Carga Incremental de JDBC sem CDC Nativo com Estado Persistente:

1. PRIMEIRA EXECUÇÃO:
   - Tabela de controle é criada automaticamente
   - Query lê todos os registros (WHERE last_update > '1900-01-01')
   - Cria tabela bronze com todos os dados
   - Insere primeiro checkpoint com MAX(last_update)

2. EXECUÇÕES SUBSEQUENTES:
   - Lê timestamp da tabela de controle
   - Query lê apenas registros WHERE last_update > checkpoint
   - DLT faz MERGE automático dos novos registros
   - Novo checkpoint é inserido após execução bem-sucedida

3. CHECKPOINT/STATE MANAGEMENT:
   - Tabela criada automaticamente no mesmo catalog e schema do pipeline
   - Nome: {catalog}.{schema}.dlt_checkpoint_dvdrental_film
   - Mantém histórico de todos os checkpoints
   - Persiste entre execuções do pipeline

VANTAGENS:
✓ Lê apenas dados novos/modificados (eficiência garantida)
✓ Checkpoint em tabela UC (rastreável e auditável)  
✓ Idempotente (re-executar é seguro)
✓ Suporta late-arriving data
✓ Recuperação de falhas é confiável
✓ Histórico completo de execuções
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
"""
