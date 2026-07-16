# Silver Layer - Transformação de Cotações BCB

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType

@dlt.table(
    name="silver.cotacao_moeda_bcb",
    comment="Cotações de moedas do Banco Central do Brasil - Camada Silver",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_cotacao", "cotacaoCompra > 0 OR cotacaoVenda > 0")
def cotacao_moeda_bcb_silver():
    """
    Transforma dados brutos de cotações BCB em formato analítico simplificado.
    
    Colunas finais:
    - moeda: Nome da moeda
    - data_referencia: Data de referência dos dados
    - tipoBoletim: Tipo do boletim BCB
    - cotacaoCompra: Valor de compra
    - cotacaoVenda: Valor de venda
    """
    
    return (
        dlt.read("cotacao_moeda_bcb")
        # Explode o array 'cotacoes' para criar uma linha por cotação
        .select(
            F.explode("cotacoes").alias("cotacao"),
            F.col("metadata")
        )
        # Seleciona apenas as colunas necessárias
        .select(
            # Metadados da moeda
            F.col("metadata.moeda"),
            F.to_date(F.col("metadata.data_referencia"), "yyyy-MM-dd").alias("data_referencia"),
            
            # Dados da cotação
            F.col("cotacao.tipoBoletim"),
            F.col("cotacao.cotacaoCompra").cast(DecimalType(18, 4)).alias("cotacaoCompra"),
            F.col("cotacao.cotacaoVenda").cast(DecimalType(18, 4)).alias("cotacaoVenda")
        )
        .dropDuplicates(["data_referencia", "moeda", "tipoBoletim"])
    )
