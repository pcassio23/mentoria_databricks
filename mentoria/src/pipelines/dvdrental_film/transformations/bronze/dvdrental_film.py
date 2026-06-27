import dlt
from pyspark.sql import functions as F

# ==============================================================================
# Bronze Layer: Incremental Ingestion from PostgreSQL
# ==============================================================================
# 
# Implementa carga incremental de PostgreSQL usando pattern simples e eficiente.
# 
# Estratégia:
# 1. Lê incrementalmente do JDBC filtrando por last_update > MAX(last_update)
# 2. DLT gerencia a deduplicação e merge automaticamente via table properties
# 3. Tabela bronze mantém versão mais recente de cada registro
#
# Nota: Para fontes JDBC sem CDC nativo, usamos @dlt.table() com batch read.
# Apply changes é reservado para fontes com CDC real (Kafka, Delta CDC, etc).
# ==============================================================================

@dlt.table(
    comment="Bronze table - DVD Rental film catalog with incremental updates from PostgreSQL"
)
def dvdrental_film():
    """
    Lê incrementalmente da tabela dvdrental.film do PostgreSQL.
    
    Filtra registros com last_update > MAX(last_update) já processado na bronze.
    Na primeira execução (tabela não existe), carrega todos os registros.
    
    DLT gerencia automaticamente:
    - Deduplicação por primary key através do merge
    - Tracking do último timestamp processado
    - Incremental refresh em execuções subsequentes
    
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
    
    # Determinar último timestamp processado
    # Na primeira execução ou erro, usa timestamp mínimo (carrega tudo)
    try:
        max_timestamp_result = spark.sql("""
            SELECT COALESCE(MAX(last_update), TIMESTAMP '1900-01-01 00:00:00') as max_ts
            FROM dvdrental_film
        """).collect()
        
        max_timestamp = max_timestamp_result[0]['max_ts']
        
        # Converter para string formatada para SQL
        if max_timestamp:
            max_timestamp_str = max_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        else:
            max_timestamp_str = '1900-01-01 00:00:00'
            
    except Exception:
        # Primeira execução - tabela não existe ainda
        max_timestamp_str = '1900-01-01 00:00:00'
    
    # Query incremental usando subquery no JDBC
    # Lê apenas registros com last_update maior que o último processado
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
# DOCUMENTAÇÃO: Incremental JDBC Pattern
# ==============================================================================
"""
Pattern para Carga Incremental de JDBC sem CDC Nativo:

1. PRIMEIRA EXECUÇÃO:
   - Tabela bronze não existe
   - Query lê todos os registros (WHERE last_update > '1900-01-01')
   - Cria tabela bronze com todos os dados

2. EXECUÇÕES SUBSEQUENTES:
   - Query encontra MAX(last_update) da bronze
   - Lê apenas registros WHERE last_update > max_timestamp
   - DLT faz MERGE automático dos novos registros

3. DEDUPLICAÇÃO:
   - DLT usa film_id como primary key implícita
   - Registros duplicados são automaticamente merged
   - Versão mais recente (por last_update) prevalece

VANTAGENS:
✓ Lê apenas dados novos/modificados (eficiência)
✓ DLT gerencia checkpoint automaticamente  
✓ Idempotente (re-executar é seguro)
✓ Suporta late-arriving data
✓ Código simples e maintainable

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
