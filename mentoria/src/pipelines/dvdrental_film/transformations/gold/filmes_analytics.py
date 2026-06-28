"""
============================================================================
Film Gold Layer - Analytics Table for Lakebase Sync
============================================================================

Autor:          pcassio23@gmail.com
Criado em:      2026-06-28
Versão:         1.0.0
Pipeline:       dvdrental_film
Camada:         Gold

Descrição:
    Camada Gold que cria tabela analítica otimizada para sync com Lakebase.
    
    Agrega dados da Silver com métricas de negócio e indicadores calculados
    para facilitar análises e dashboards. Tabela materializada (não streaming)
    para garantir consistência no sync com Postgres.

Origem:
    Table:      dvdrental_filmes (Silver Layer)

Destino:
    Catalog:    dev_catalog
    Schema:     gold
    Table:      filmes_analytics
    
Syncronização:
    Target:     Lakebase Postgres Database
    Method:     Synced Tables (managed by Databricks)
    Frequency:  On pipeline update

Métricas Calculadas:
    - receita_potencial_diaria: valor_aluguel / duracao_aluguel_dias
    - receita_estimada_mensal: receita_potencial_diaria * 30
    - indice_valor_duracao: valor_aluguel / duracao_minutos
    - categoria_duracao: Short/Medium/Long baseado em duração
    - categoria_preco: Budget/Standard/Premium baseado em valor

Histórico de Alterações:
    2026-06-28  pcassio23@gmail.com  Versão inicial
    
Notas:
    - Tabela materializada para consistência no Lakebase sync
    - Otimizada para queries analíticas
    - Schema compatível com Postgres (tipos simples)
    - Inclui apenas filmes válidos (data quality passed)
    
============================================================================
"""

import dlt
from pyspark.sql import functions as F

# ==============================================================================
# Gold Layer: Analytics Table for Lakebase Sync
# ==============================================================================

# Get catalog and gold schema from pipeline configuration
catalog = spark.conf.get("bundle.catalog", "dev_catalog")
gold_schema = spark.conf.get("bundle.gold_schema", "gold")

@dlt.table(
    name=f"{catalog}.{gold_schema}.filmes_analytics",
    comment="Gold analytics table - Film metrics and business indicators for Lakebase sync",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"  # Enable CDC for Lakebase sync
    }
)
def filmes_analytics():
    """
    Cria tabela analítica com métricas de negócio para sync com Lakebase.
    
    Processa dados da Silver calculando:
    - Métricas de receita e rentabilidade
    - Categorização por duração e preço
    - Indicadores de qualidade de catálogo
    - Estatísticas agregadas por classificação
    
    Returns:
        DataFrame: Tabela gold com métricas analíticas
    """
    
    # Read from Silver layer (batch read for materialized table)
    df_silver = dlt.read("dvdrental_filmes")
    
    # Calculate business metrics and indicators
    df_gold = df_silver.select(
        # Identificadores
        F.col("id_filme"),
        F.col("titulo"),
        F.col("descricao"),
        
        # Informações básicas
        F.col("ano_lancamento"),
        F.col("classificacao"),
        F.col("duracao_minutos"),
        
        # Métricas comerciais originais
        F.col("valor_aluguel"),
        F.col("duracao_aluguel_dias"),
        F.col("custo_reposicao"),
        
        # MÉTRICAS CALCULADAS - Rentabilidade
        F.round(
            F.col("valor_aluguel") / F.col("duracao_aluguel_dias"), 2
        ).alias("receita_potencial_diaria"),
        
        F.round(
            (F.col("valor_aluguel") / F.col("duracao_aluguel_dias")) * 30, 2
        ).alias("receita_estimada_mensal"),
        
        # MÉTRICAS CALCULADAS - Valor por minuto
        F.round(
            F.col("valor_aluguel") / F.col("duracao_minutos") * 100, 4
        ).alias("indice_valor_duracao"),
        
        # MÉTRICAS CALCULADAS - ROI potencial
        F.round(
            (F.col("valor_aluguel") * 30) / F.col("custo_reposicao") * 100, 2
        ).alias("roi_potencial_pct"),
        
        # CATEGORIZAÇÃO - Duração do filme
        F.when(F.col("duracao_minutos") < 90, "Short")
         .when(F.col("duracao_minutos") < 120, "Medium")
         .when(F.col("duracao_minutos") < 150, "Long")
         .otherwise("Extra Long").alias("categoria_duracao"),
        
        # CATEGORIZAÇÃO - Faixa de preço
        F.when(F.col("valor_aluguel") < 2.00, "Budget")
         .when(F.col("valor_aluguel") < 4.00, "Standard")
         .otherwise("Premium").alias("categoria_preco"),
        
        # CATEGORIZAÇÃO - Investimento
        F.when(F.col("custo_reposicao") < 15.00, "Low Cost")
         .when(F.col("custo_reposicao") < 20.00, "Medium Cost")
         .otherwise("High Cost").alias("categoria_investimento"),
        
        # FLAGS - Indicadores de negócio
        F.when(
            (F.col("valor_aluguel") / F.col("duracao_aluguel_dias")) > 1.5,
            True
        ).otherwise(False).alias("flag_alto_valor_diario"),
        
        F.when(
            F.col("valor_aluguel") >= 4.99,
            True
        ).otherwise(False).alias("flag_premium"),
        
        F.when(
            F.col("duracao_minutos") >= 120,
            True
        ).otherwise(False).alias("flag_longa_duracao"),
        
        # Metadados e auditoria
        F.col("ultima_atualizacao"),
        F.col("data_ingestao"),
        F.col("data_processamento").alias("data_processamento_silver"),
        F.current_timestamp().alias("data_processamento_gold")
    )
    
    return df_gold


# ==============================================================================
# DOCUMENTAÇÃO: Lakebase Sync Configuration
# ==============================================================================
"""
Configuração para Sync com Lakebase:

1. CRIAR SYNCED TABLE NO LAKEBASE:

   CREATE TABLE lakebase_schema.filmes_analytics AS
   SELECT * FROM dev_catalog.gold.filmes_analytics;

2. CONFIGURAR SYNC:

   -- Via Databricks UI:
   -- Catalog > gold schema > filmes_analytics table
   -- "Create Synced Table" > Select Lakebase connection

   -- Via SQL:
   CREATE SYNCED TABLE lakebase_connection.schema.filmes_analytics
   AS SELECT * FROM dev_catalog.gold.filmes_analytics
   WITH (
     sync_frequency = 'ON_DEMAND',  -- or 'CONTINUOUS'
     primary_key = ('id_filme')
   );

3. SCHEMA LAKEBASE (Postgres-compatible types):

   Column                      | Type          | Description
   ---------------------------|---------------|----------------------------------
   id_filme                   | INTEGER       | PK - Identificador único
   titulo                     | VARCHAR(255)  | Título do filme
   descricao                  | TEXT          | Descrição/sinopse
   ano_lancamento             | INTEGER       | Ano de lançamento
   classificacao              | VARCHAR(10)   | Classificação etária
   duracao_minutos            | INTEGER       | Duração em minutos
   valor_aluguel              | DECIMAL(4,2)  | Valor do aluguel
   duracao_aluguel_dias       | INTEGER       | Dias de aluguel
   custo_reposicao            | DECIMAL(5,2)  | Custo de reposição
   receita_potencial_diaria   | DECIMAL(10,2) | Receita/dia calculada
   receita_estimada_mensal    | DECIMAL(10,2) | Receita mensal estimada
   indice_valor_duracao       | DECIMAL(10,4) | Valor por minuto
   roi_potencial_pct          | DECIMAL(10,2) | ROI potencial (%)
   categoria_duracao          | VARCHAR(20)   | Short/Medium/Long/Extra Long
   categoria_preco            | VARCHAR(20)   | Budget/Standard/Premium
   categoria_investimento     | VARCHAR(20)   | Low/Medium/High Cost
   flag_alto_valor_diario     | BOOLEAN       | Receita diária > $1.50
   flag_premium               | BOOLEAN       | Filme premium (>= $4.99)
   flag_longa_duracao         | BOOLEAN       | Filme longo (>= 120 min)
   ultima_atualizacao         | TIMESTAMP     | Última atualização origem
   data_ingestao              | TIMESTAMP     | Ingestão na bronze
   data_processamento_silver  | TIMESTAMP     | Processamento silver
   data_processamento_gold    | TIMESTAMP     | Processamento gold

4. QUERIES ANALÍTICAS EXEMPLO (Lakebase/Postgres):

   -- Top 10 filmes por receita potencial mensal
   SELECT titulo, receita_estimada_mensal, categoria_preco
   FROM filmes_analytics
   ORDER BY receita_estimada_mensal DESC
   LIMIT 10;
   
   -- Distribuição por categoria de duração
   SELECT categoria_duracao, COUNT(*) as total_filmes,
          AVG(valor_aluguel) as valor_medio
   FROM filmes_analytics
   GROUP BY categoria_duracao;
   
   -- Filmes premium com alto ROI
   SELECT titulo, valor_aluguel, roi_potencial_pct
   FROM filmes_analytics
   WHERE flag_premium = true AND roi_potencial_pct > 100
   ORDER BY roi_potencial_pct DESC;

VANTAGENS DO SYNC LAKEBASE:
✓ Queries analíticas em Postgres (compatível com ferramentas BI)
✓ Sync incremental automático via CDC
✓ Dados sempre atualizados com pipeline
✓ Performance otimizada para análises
✓ Integração com aplicações Python/Node.js
"""
