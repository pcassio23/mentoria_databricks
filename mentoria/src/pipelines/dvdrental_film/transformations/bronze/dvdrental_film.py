import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==============================================================================
# Bronze Layer: Incremental Ingestion from PostgreSQL
# ==============================================================================
# 
# Implementa carga incremental usando dlt.apply_changes() mesmo sem CDC no 
# banco de origem. A estratégia é:
#
# 1. dvdrental_film_source (view): Lê incrementalmente do JDBC filtrando por
#    last_update > MAX(last_update) da tabela bronze
# 2. dvdrental_film (table): Target materializada via apply_changes que 
#    gerencia automaticamente upserts/deletes baseado em last_update
#
# Benefícios:
# - Lê apenas dados novos/modificados (eficiência)
# - DLT gerencia checkpoint automaticamente
# - Suporte a SCD Type 1 (sobrescreve) ou Type 2 (histórico)
# - Deduplicação automática por film_id
# ==============================================================================

@dlt.view(
    comment="Incremental source view - reads only new/updated records from PostgreSQL"
)
def dvdrental_film_source():
    """
    Lê incrementalmente da tabela dvdrental.film do PostgreSQL.
    
    Filtra registros com last_update > MAX(last_update) já processado na bronze.
    Na primeira execução (tabela não existe), carrega todos os registros.
    
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
            FROM LIVE.dvdrental_film
        """).collect()
        
        max_timestamp = max_timestamp_result[0]['max_ts']
        
        # Converter para string formatada para SQL
        if max_timestamp:
            max_timestamp_str = max_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        else:
            max_timestamp_str = '1900-01-01 00:00:00'
            
    except Exception as e:
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


# Apply changes to bronze target table
# DLT gerencia automaticamente:
# - Deduplicação por film_id (keys)
# - Ordenação por last_update (sequence_by) 
# - Merge incremental (SCD Type 1 - sobrescreve registros existentes)
dlt.apply_changes(
    target="dvdrental_film",
    source="dvdrental_film_source",
    keys=["film_id"],                    # Chave primária para deduplicação
    sequence_by="last_update",           # Coluna de ordenação temporal
    stored_as_scd_type=1,                # SCD Type 1: mantém apenas versão atual
    # except_column_list=None,           # Processar todas as colunas
    # ignore_null_updates=False,         # Processar updates mesmo com NULL
)


# ==============================================================================
# DOCUMENTAÇÃO: dlt.apply_changes()
# ==============================================================================
"""
Parâmetros do dlt.apply_changes():

target (str):
    Nome da tabela target que será criada/atualizada
    
source (str):
    Nome da view/table source com dados incrementais
    
keys (list):
    Lista de colunas que formam a chave primária
    Usado para matching durante merge (upsert)
    
sequence_by (str):
    Coluna timestamp/version para ordenar mudanças
    DLT usa isso para determinar qual registro é mais recente
    
stored_as_scd_type (int):
    1 = SCD Type 1: Sobrescreve registros existentes (mantém apenas atual)
    2 = SCD Type 2: Mantém histórico completo com colunas __start_at, __end_at
    
except_column_list (list, opcional):
    Colunas a EXCLUIR do merge (ex: colunas calculadas, metadata)
    
ignore_null_updates (bool, opcional):
    Se True, ignora updates onde todas as colunas são NULL
    Default: False

apply_as_deletes (str, opcional):
    Expressão SQL para identificar registros a deletar
    Ex: "operation = 'DELETE'" para processar soft deletes
    
apply_as_truncates (str, opcional):
    Expressão SQL para identificar quando truncar a tabela target
    
track_history_column_list (list, opcional):
    Para SCD Type 2: lista de colunas a rastrear mudanças
    Se não especificado, rastreia todas as colunas
    
track_history_except_column_list (list, opcional):
    Para SCD Type 2: colunas a NÃO rastrear mudanças no histórico

Fluxo de Execução:
1. dvdrental_film_source lê dados incrementais do PostgreSQL
2. apply_changes compara source com target usando 'film_id' (keys)
3. Para cada film_id:
   - Se não existe no target: INSERT
   - Se existe e last_update do source > target: UPDATE
   - Se existe e last_update do source <= target: IGNORA
4. Result: target sempre contém versão mais recente de cada filme

Vantagens desta abordagem:
✓ Lê apenas dados novos (eficiência de I/O)
✓ DLT gerencia checkpoint automaticamente
✓ Merge idempotente (re-executar é seguro)
✓ Suporta late-arriving data (registros atrasados)
✓ Code é mais simples que gerenciar merge manual
"""
