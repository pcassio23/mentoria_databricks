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
    - Schema inference automático para melhor manutenibilidade
    
============================================================================
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Silver layer: Cleaned and validated film data with PT-BR column names


@dlt.table(
    name="silver.dvdrental_filmes",
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
    - Converte tipos PostgreSQL para tipos Spark apropriados
    - Adiciona timestamp de processamento
    
    Returns:
        DataFrame: Dados limpos e validados com colunas em PT-BR
    """
    
    # Read from Bronze layer (uses default schema from pipeline config)
    df_bronze = dlt.read("dvdrental_film")
    
    # Apply data cleaning transformations using withColumns for better performance
    df_clean = df_bronze.withColumns({
        # Trim string columns
        "title": F.trim(F.col("title")),
        "description": F.trim(F.col("description")),
        "rating": F.when(
            F.trim(F.col("rating")).isNotNull(), 
            F.upper(F.trim(F.col("rating")))
        ).otherwise(None),
        
        # Convert PostgreSQL smallint (short) to integer
        "language_id": F.col("language_id").cast("integer"),
        "length": F.col("length").cast("integer"),
        "rental_duration": F.col("rental_duration").cast("integer"),
        
        # Ensure numeric fields are properly typed
        "rental_rate": F.col("rental_rate").cast("decimal(4,2)"),
        "replacement_cost": F.col("replacement_cost").cast("decimal(5,2)"),
        
        # Convert PostgreSQL array to string (comma-separated)
        "special_features": F.when(
            F.col("special_features").isNotNull(), 
            F.array_join(F.col("special_features"), ", ")
        ).otherwise(None),
        
        # Add processing timestamp
        "processing_timestamp": F.current_timestamp()
    })
    
    # Deduplication: Keep most recent record per film_id based on last_update
    window_spec = Window.partitionBy("film_id").orderBy(F.col("last_update").desc())
    
    df_deduplicated = (
        df_clean
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    
    # Translate column names to PT-BR and select in the order defined in schema
    df_translated = df_deduplicated.select(
        # Identificadores
        F.col("film_id").cast("integer").alias("id_filme"),
        F.col("language_id").alias("id_idioma"),
        
        # Informações descritivas
        F.col("title").alias("titulo"),
        F.col("description").alias("descricao"),
        F.col("release_year").alias("ano_lancamento"),
        F.col("rating").alias("classificacao"),
        
        # Características técnicas
        F.col("length").alias("duracao_minutos"),
        F.col("special_features").alias("recursos_especiais"),
        F.col("fulltext").alias("texto_completo"),
        
        # Informações comerciais
        F.col("rental_duration").alias("duracao_aluguel_dias"),
        F.col("rental_rate").alias("valor_aluguel"),
        F.col("replacement_cost").alias("custo_reposicao"),
        
        # Metadados e auditoria
        F.col("last_update").alias("ultima_atualizacao"),
        F.col("ingestion_timestamp").alias("data_ingestao"),
        F.col("processing_timestamp").alias("data_processamento")
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
- recursos_especiais (STRING)   : Features especiais do DVD (ex: legendas, trailers)
- texto_completo (STRING)       : Índice de busca textual full-text

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
