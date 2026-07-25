# ============================================================================
# Gold Layer - Variação Cambial Venda
# ============================================================================
# Criado em: 2026-07-16
# Criado por: pcassio23@gmail.com
# Descrição: Calcula a variação cambial diária entre abertura e fechamento
#            das moedas, utilizando dados da camada silver. Mantém histórico
#            completo com schema validado e timestamp de processamento.
#
# Histórico de Alterações:
# ----------------------------------------------------------------------------
# Data       | Autor                 | Descrição
# ----------------------------------------------------------------------------
# 2026-07-16 | pcassio23@gmail.com   | Criação inicial da camada gold
#            |                       | - Schema explícito com comentários UC
#            |                       | - Conversão para streaming table
#            |                       | - Cálculo de variação percentual
#            |                       | - Coluna de auditoria (processed_at)
# ============================================================================

import dlt
from pyspark.sql import functions as F

@dlt.table(
  name="gold.variacao_cambial_venda",
  comment="Variação cambial diária entre abertura e fechamento",
  table_properties={
    "quality": "gold",
    "pipelines.autoOptimize.managed": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "changedatafeed.enabled": "true"
    },
    schema="""
        moeda STRING COMMENT 'Nome da moeda estrangeira (ex: Dólar Americano, Euro)',
        data_cotacao DATE COMMENT 'Data da cotação (apenas data, sem hora)',
        cotacao_abertura DECIMAL(18,4) COMMENT 'Valor de abertura da moeda em reais (BRL)',
        cotacao_fechamento DECIMAL(18,4) COMMENT 'Valor de fechamento da moeda em reais (BRL)',
        variacao_percentual DECIMAL(18,2) COMMENT 'Variação percentual entre abertura e fechamento',
        processed_at TIMESTAMP COMMENT 'Timestamp de quando o registro foi processado na camada gold'
    """
)
@dlt.expect_or_drop("valid_cotacao", "cotacaoVenda > 0")

def variacao_cambial_venda():
    """
    Transforma os dados de cotacao de venda no modelo analitico de variacao cambial.
    
    Colunas finais: 
     - moeda: Nome da moeda estrangeira
     - data_cotacao: Data da cotação (apenas data)
     - cotacao_abertura: Valor de abertura em reais (BRL)
     - cotacao_fechamento: Valor de fechamento em reais (BRL)
     - variacao_percentual: Variação percentual entre abertura e fechamento
     - processed_at: Timestamp de processamento na camada gold
    """

    return (
        dlt.read_stream("silver.cotacao_moeda_bcb")
            .filter(F.col("tipoBoletim").isin("Abertura", "Fechamento"))
            .groupBy(
                F.to_date("dataHoraCotacao").alias("data_cotacao"),
                "moeda"
            )
            .agg(
                F.max(F.when(F.col("tipoBoletim") == "Abertura", F.col("cotacaoVenda"))).alias("cotacao_abertura"),
                F.max(F.when(F.col("tipoBoletim") == "Fechamento", F.col("cotacaoVenda"))).alias("cotacao_fechamento")
            )
            .withColumn(
                "variacao_percentual",
                F.round(
                    ((F.col("cotacao_fechamento") - F.col("cotacao_abertura")) / F.col("cotacao_abertura")) * 100, 2
                )
            )
            .withColumn("processed_at", F.current_timestamp())
    )