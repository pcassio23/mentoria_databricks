# Databricks notebook source
# ==============================================================================
# Update Checkpoint - Atualiza tabela de controle após execução do pipeline
# ==============================================================================
#
# Este notebook deve ser executado como Task 2 de um Job, APÓS a execução
# do pipeline DLT (Task 1).
#
# Usa MERGE para manter apenas 1 registro por tabela fonte na tabela de 
# checkpoint, evitando crescimento desnecessário.
# ==============================================================================

from datetime import datetime

def update_checkpoint():
    """
    Atualiza checkpoint usando MERGE - mantém apenas último timestamp por tabela.
    
    Returns:
        bool: True se checkpoint foi atualizado, False caso contrário
    """
    
    # Configurações (ajuste conforme seu ambiente)
    checkpoint_table = "dev_catalog.bronze.dlt_checkpoint_dvdrental_film"
    bronze_table = "dev_catalog.bronze.dvdrental_film"
    table_name = "dvdrental_film"
    
    try:
        print(f"🔄 Starting checkpoint update for {table_name}...")
        
        # Lê o MAX timestamp da tabela bronze
        max_ts_result = spark.sql(f"""
            SELECT COALESCE(MAX(last_update), TIMESTAMP '1900-01-01 00:00:00') as new_max_ts
            FROM {bronze_table}
        """).collect()
        
        if not max_ts_result:
            print("⚠️  No data found in bronze table")
            return False
        
        new_max_ts = max_ts_result[0]['new_max_ts']
        
        # Verifica se há dados novos para processar
        if new_max_ts and str(new_max_ts) != "1900-01-01 00:00:00":
            
            # MERGE: Atualiza se existe, insere se não existe
            # Mantém apenas 1 registro por table_name
            spark.sql(f"""
                MERGE INTO {checkpoint_table} AS target
                USING (
                    SELECT 
                        '{table_name}' as table_name,
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
            
            print(f"✅ Checkpoint updated successfully!")
            print(f"   Table: {table_name}")
            print(f"   New checkpoint: {new_max_ts}")
            print(f"   Update time: {datetime.now()}")
            
            return True
            
        else:
            print("ℹ️  No new records to process (table is empty or no updates)")
            return False
            
    except Exception as e:
        print(f"❌ Error updating checkpoint: {e}")
        import traceback
        traceback.print_exc()
        raise  # Re-raise para falhar a task do Job


# COMMAND ----------

# Executar atualização do checkpoint
result = update_checkpoint()

if result:
    print("\n" + "="*60)
    print("✅ CHECKPOINT UPDATE COMPLETED")
    print("="*60)
else:
    print("\n" + "="*60)
    print("⚠️  CHECKPOINT UPDATE SKIPPED (no new data)")
    print("="*60)

# COMMAND ----------

# ==============================================================================
# VERIFICAÇÃO: Query para consultar histórico de checkpoints
# ==============================================================================
# 
# Execute esta célula para verificar o checkpoint atual:

print("\n📊 Current Checkpoint Status:")
print("-" * 60)

display(spark.sql("""
    SELECT 
        table_name,
        last_processed_timestamp,
        last_update_time,
        DATEDIFF(CURRENT_TIMESTAMP(), last_update_time) as days_since_update
    FROM dev_catalog.bronze.dlt_checkpoint_dvdrental_film
    ORDER BY last_update_time DESC
"""))

