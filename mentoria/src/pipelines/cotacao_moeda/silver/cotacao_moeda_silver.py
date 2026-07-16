# Silver Layer - Transformação de Cotações BCB

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType

@dlt.table(
    name="silver.cotacao_moeda_bcb",  # Schema explícito: silver.nome_tabela
    comment="Cotações de moedas do Banco Central do Brasil - Camada Silver (cleaned & validated)",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_data_cotacao", "data_cotacao IS NOT NULL")
@dlt.expect_or_drop("valid_valor", "valor_compra > 0 OR valor_venda > 0")
def cotacao_moeda_bcb_silver():
    """
    Transforma dados brutos de cotações em dados limpos e validados.
    
    Transformações aplicadas:
    - Conversão de tipos de dados (strings para decimais e datas)
    - Remoção de registros duplicados
    - Validação de valores (não nulos, positivos)
    - Padronização de nomes de colunas
    - Adição de metadados de processamento silver
    
    Data Quality:
    - Registros com data_cotacao nula são removidos
    - Registros com valores negativos são removidos
    """
    
    return (
        dlt.read("cotacao_moeda_bcb")  # Lê da bronze (bronze.cotacao_moeda_bcb)
        .select(
            # Conversões de tipo e limpeza
            F.to_date(F.col("data_cotacao"), "yyyy-MM-dd").alias("data_cotacao"),
            F.col("moeda").alias("codigo_moeda"),
            F.col("nome_moeda"),
            F.col("tipo_cotacao"),
            
            # Conversão de valores para decimal
            F.col("valor_compra").cast(DecimalType(18, 4)).alias("valor_compra"),
            F.col("valor_venda").cast(DecimalType(18, 4)).alias("valor_venda"),
            F.col("paridade_compra").cast(DecimalType(18, 6)).alias("paridade_compra"),
            F.col("paridade_venda").cast(DecimalType(18, 6)).alias("paridade_venda"),
            
            # Metadados da bronze
            F.col("_bronze_ingest_timestamp"),
            F.col("_bronze_source_file"),
            
            # Metadados da silver
            F.current_timestamp().alias("_silver_processed_timestamp")
        )
        .dropDuplicates(["data_cotacao", "codigo_moeda", "tipo_cotacao"])
    )
