"""
============================================================================
Film Silver Layer - Limpeza e Validação de Dados
============================================================================

Autor:          pcassio23@gmail.com
Criado em:      2025-01-08
Versão:         1.0.0
Pipeline:       dvdrental_film
Camada:         Silver

Descrição:
    Camada Silver que aplica transformações de limpeza, deduplicação e
    validação de qualidade nos dados brutos da camada Bronze.
    
    Este módulo lê a tabela dvdrental_film, remove duplicatas, padroniza
    valores, traduz nomes de colunas para PT-BR e aplica regras de negócio.

Origem:
    Table:      dvdrental_film (Bronze Layer)

Destino:
    Catalog:    dev_catalog
    Schema:     silver
    Table:      dvdrental_filmes

Transformações Aplicadas:
    - Deduplicação baseada em film_id (mantém registro mais recente)
    - Validação de valores obrigatórios (título, taxa de aluguel, duração)
    - Padronização de classificação (apenas valores válidos)
    - Remoção de espaços extras em strings
    - Conversão de tipos para garantir consistência
    - Tradução de nomes de colunas para PT-BR

Mapeamento de Colunas (EN → PT-BR):
    film_id           → id_filme              (Identificador único do filme)
    title             → titulo                (Título do filme)
    description       → descricao             (Sinopse/descrição do filme)
    release_year      → ano_lancamento        (Ano de lançamento)
    language_id       → id_idioma             (Identificador do idioma)
    rental_duration   → duracao_aluguel_dias  (Período de aluguel em dias)
    rental_rate       → valor_aluguel         (Valor cobrado por aluguel)
    length            → duracao_minutos       (Duração do filme em minutos)
    replacement_cost  → custo_reposicao       (Custo para repor o filme)
    rating            → classificacao         (Classificação etária)
    last_update       → ultima_atualizacao    (Data/hora da última atualização)
    special_features  → recursos_especiais    (Features especiais do DVD)
    fulltext          → texto_completo        (Índice de busca full-text)

Histórico de Alterações:
    2025-01-08  pcassio23@gmail.com  Versão inicial
    
Notas:
    - Utiliza Window Functions para deduplicação eficiente
    - Data Quality Expectations garantem qualidade mínima dos dados
    - Mantém colunas de metadados (ingestion_timestamp, processing_timestamp)
    - Nomes em português facilitam uso por analistas de negócio
    
============================================================================
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Silver layer: Cleaned and validated film data with PT-BR column names


@dlt.table(
    comment="Silver layer - Dados limpos de filmes com colunas em PT-BR e validações de qualidade",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "id_filme"
    }
)
@dlt.expect_or_drop("valida_titulo", "titulo IS NOT NULL AND length(trim(titulo)) > 0")
@dlt.expect_or_drop("valida_valor_aluguel", "valor_aluguel > 0")
@dlt.expect_or_drop("valida_duracao_aluguel", "duracao_aluguel_dias > 0")
@dlt.expect_or_drop("valida_duracao_minutos", "duracao_minutos > 0")
@dlt.expect_or_drop("valida_custo_reposicao", "custo_reposicao >= 0")
@dlt.expect("valida_classificacao", "classificacao IN ('G', 'PG', 'PG-13', 'R', 'NC-17') OR classificacao IS NULL")
@dlt.expect("valida_ano_lancamento", "ano_lancamento >= 1900 AND ano_lancamento <= year(current_date())")
def dvdrental_filmes():
    """
    Processa dados da camada Bronze aplicando limpeza, validações e tradução de colunas.
    
    Transformações:
    - Remove duplicatas mantendo o registro mais recente por film_id
    - Aplica trim em campos de texto
    - Valida e padroniza valores
    - Traduz nomes de colunas para português brasileiro
    - Adiciona timestamp de processamento
    
    Returns:
        DataFrame: Dados limpos e validados com colunas em PT-BR
    """
    
    # Read from Bronze layer
    df_bronze = dlt.read("dvdrental_film")
    
    # Apply data cleaning transformations
    df_clean = (
        df_bronze
        # Trim string columns
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("description", F.trim(F.col("description")))
        .withColumn("rating", F.trim(F.col("rating")))
        
        # Standardize rating to uppercase (if not null)
        .withColumn("rating", 
                    F.when(F.col("rating").isNotNull(), 
                           F.upper(F.col("rating")))
                    .otherwise(None))
        
        # Ensure numeric fields are properly typed
        .withColumn("rental_rate", F.col("rental_rate").cast("decimal(4,2)"))
        .withColumn("replacement_cost", F.col("replacement_cost").cast("decimal(5,2)"))
        
        # Add processing timestamp
        .withColumn("processing_timestamp", F.current_timestamp())
    )
    
    # Deduplication: Keep most recent record per film_id based on last_update
    window_spec = Window.partitionBy("film_id").orderBy(F.col("last_update").desc())
    
    df_deduplicated = (
        df_clean
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    
    # Translate column names to PT-BR with descriptions
    df_translated = (
        df_deduplicated
        # Identificadores
        .withColumnRenamed("film_id", "id_filme")                           # Identificador único do filme (PK)
        .withColumnRenamed("language_id", "id_idioma")                      # Referência ao idioma do filme
        
        # Informações descritivas
        .withColumnRenamed("title", "titulo")                               # Nome/título do filme
        .withColumnRenamed("description", "descricao")                      # Sinopse ou descrição do enredo
        .withColumnRenamed("release_year", "ano_lancamento")                # Ano em que o filme foi lançado
        .withColumnRenamed("rating", "classificacao")                       # Classificação etária (G, PG, PG-13, R, NC-17)
        
        # Características técnicas
        .withColumnRenamed("length", "duracao_minutos")                     # Duração total do filme em minutos
        .withColumnRenamed("special_features", "recursos_especiais")        # Features do DVD (legendas, extras, etc)
        .withColumnRenamed("fulltext", "texto_completo")                    # Índice full-text para busca
        
        # Informações comerciais
        .withColumnRenamed("rental_duration", "duracao_aluguel_dias")      # Período padrão de aluguel em dias
        .withColumnRenamed("rental_rate", "valor_aluguel")                 # Preço cobrado por aluguel
        .withColumnRenamed("replacement_cost", "custo_reposicao")          # Custo para substituir o filme em estoque
        
        # Metadados e auditoria
        .withColumnRenamed("last_update", "ultima_atualizacao")            # Timestamp da última modificação no registro
        .withColumnRenamed("ingestion_timestamp", "data_ingestao")         # Timestamp de quando foi carregado na Bronze
        .withColumnRenamed("processing_timestamp", "data_processamento")    # Timestamp do processamento na Silver
    )
    
    return df_translated


# ============================================================================
# DOCUMENTAÇÃO DAS COLUNAS FINAIS
# ============================================================================
"""
Esquema Final da Tabela dvdrental_filmes (Silver):

IDENTIFICADORES:
- id_filme (INT)                : Chave primária, identificador único do filme
- id_idioma (INT)               : Chave estrangeira para tabela de idiomas

INFORMAÇÕES DESCRITIVAS:
- titulo (STRING)               : Nome do filme (obrigatório, não vazio)
- descricao (STRING)            : Sinopse do filme, descrição do enredo
- ano_lancamento (INT)          : Ano de lançamento (validado: 1900 até ano atual)
- classificacao (STRING)        : Classificação etária (G, PG, PG-13, R, NC-17 ou NULL)

CARACTERÍSTICAS TÉCNICAS:
- duracao_minutos (INT)         : Duração do filme em minutos (obrigatório, > 0)
- recursos_especiais (ARRAY)    : Features especiais do DVD (ex: legendas, trailers)
- texto_completo (TSVECTOR)     : Índice de busca textual full-text

INFORMAÇÕES COMERCIAIS:
- duracao_aluguel_dias (INT)    : Período padrão de locação em dias (obrigatório, > 0)
- valor_aluguel (DECIMAL(4,2))  : Preço por aluguel (obrigatório, > 0)
- custo_reposicao (DECIMAL(5,2)): Custo de reposição em estoque (obrigatório, >= 0)

METADADOS E AUDITORIA:
- ultima_atualizacao (TIMESTAMP): Data/hora da última modificação no sistema origem
- data_ingestao (TIMESTAMP)     : Data/hora de carga da camada Bronze
- data_processamento (TIMESTAMP): Data/hora de processamento da camada Silver

VALIDAÇÕES APLICADAS:
✓ titulo não pode ser nulo ou vazio
✓ valor_aluguel deve ser maior que zero
✓ duracao_aluguel_dias deve ser maior que zero
✓ duracao_minutos deve ser maior que zero
✓ custo_reposicao deve ser maior ou igual a zero
⚠ classificacao deve estar na lista válida ou ser NULL (warning, não descarta)
⚠ ano_lancamento deve estar entre 1900 e ano atual (warning, não descarta)
"""


# ============================================================================
# Alternative approach using dlt.apply_changes() for SCD Type 1
# ============================================================================
# Uncomment this section if you prefer to use apply_changes instead of manual deduplication

"""
dlt.create_streaming_table("dvdrental_filmes_scd")

dlt.apply_changes(
    target="dvdrental_filmes_scd",
    source="dvdrental_film",
    keys=["film_id"],
    sequence_by="last_update",
    stored_as_scd_type=1,  # SCD Type 1: Keep only latest version
    except_column_list=["ingestion_timestamp"],  # Exclude metadata columns from merge
    # Apply column renaming in the target table
    stored_as_scd_type=1
)
"""
