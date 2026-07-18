# ============================================================================
# Silver Layer - Transformação de Cotações BCB
# ============================================================================
# Criado em: 2026-07-16
# Criado por: pcassio23@gmail.com
# Descrição: Transforma dados brutos de cotações do Banco Central (bronze)
#            em formato analítico com schema validado. Mantém histórico
#            completo de cotações por moeda com timestamp preciso.
#
# Histórico de Alterações:
# ----------------------------------------------------------------------------
# Data       | Autor                 | Descrição
# ----------------------------------------------------------------------------
# 2026-07-16 | pcassio23@gmail.com   | Criação inicial da camada silver
#            |                       | - Schema explícito com comentários UC
#            |                       | - Conversão para streaming table
#            |                       | - Timestamp completo (dataHoraCotacao)
# ============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, TimestampType

@dlt.table(
    name="silver.cotacao_moeda_bcb",
    comment="Cotações de moedas do Banco Central do Brasil - Camada Silver",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    },
    schema="""
        moeda STRING COMMENT 'Nome da moeda estrangeira (ex: Dólar Americano, Euro)',
        dataHoraCotacao TIMESTAMP COMMENT 'Data e hora exata da cotação no formato timestamp',
        tipoBoletim STRING COMMENT 'Tipo do boletim do Banco Central (ex: Abertura, Intermediário, Fechamento)',
        cotacaoCompra DECIMAL(18,4) COMMENT 'Valor de compra da moeda em reais (BRL)',
        cotacaoVenda DECIMAL(18,4) COMMENT 'Valor de venda da moeda em reais (BRL)'
    """
)
@dlt.expect_or_drop("valid_cotacao", "cotacaoCompra > 0 OR cotacaoVenda > 0")
def cotacao_moeda_bcb_silver():
    """
    Transforma dados brutos de cotações BCB em formato analítico simplificado.
    
    Colunas finais:
    - moeda: Nome da moeda
    - dataHoraCotacao: Data e hora da cotação (timestamp completo)
    - tipoBoletim: Tipo do boletim BCB
    - cotacaoCompra: Valor de compra
    - cotacaoVenda: Valor de venda
    """
    
    return (
        dlt.read_stream("cotacao_moeda_bcb")
        # Explode o array 'cotacoes' para criar uma linha por cotação
        .select(
            F.explode("cotacoes").alias("cotacao"),
            F.col("metadata")
        )
        # Seleciona apenas as colunas necessárias
        .select(
            # Metadados da moeda
            F.col("metadata.moeda"),
            F.col("cotacao.dataHoraCotacao").cast(TimestampType()).alias("dataHoraCotacao"),
            
            # Dados da cotação
            F.col("cotacao.tipoBoletim"),
            F.col("cotacao.cotacaoCompra").cast(DecimalType(18, 4)).alias("cotacaoCompra"),
            F.col("cotacao.cotacaoVenda").cast(DecimalType(18, 4)).alias("cotacaoVenda")
        )
    )
