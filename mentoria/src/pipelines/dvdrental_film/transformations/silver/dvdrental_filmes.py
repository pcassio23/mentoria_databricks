"""
============================================================================
Film Silver Layer - Limpeza e Validação de Dados com Carga Incremental
============================================================================

Autor:          pcassio23@gmail.com
Criado em:      2025-01-08
Versão:         2.0.0
Pipeline:       dvdrental_film
Camada:         Silver

Descrição:
    Camada Silver que processa incrementalmente dados da Bronze aplicando
    transformações de limpeza, deduplicação e validação de qualidade.
    
    Usa dlt.apply_changes() para processar apenas registros novos/modificados
    da camada Bronze, aplicando tradução de colunas para PT-BR e validações.

Origem:
    Table:      dvdrental_film (Bronze Layer)

Destino:
    Catalog:    dev_catalog
    Schema:     silver
    Table:      dvdrental_filmes

Transformações Aplicadas:
    - Processamento incremental baseado em last_update
    - Deduplicação por film_id (SCD Type 1)
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
    2025-01-08  pcassio23@gmail.com  v2.0 - Migrado para apply_changes incremental
    
Notas:
    - dlt.apply_changes() gerencia automaticamente CDC e deduplicação
    - Data Quality Expectations aplicadas via expectations (não constraints)
    - Nomes em português facilitam uso por analistas de negócio
    - Comentários aplicados no Unity Catalog para governança de dados
    
============================================================================
"""

import dlt
from pyspark.sql import functions as F

# ==============================================================================
# Silver Layer: Incremental Processing with Data Quality
# ==============================================================================


@dlt.view(
    comment="Silver source view - applies cleaning and transformations to bronze data"
)
@dlt.expect_or_drop("valida_titulo", "titulo IS NOT NULL AND length(trim(titulo)) > 0")
@dlt.expect_or_drop("valida_valor_aluguel", "valor_aluguel > 0")
@dlt.expect_or_drop("valida_duracao_aluguel", "duracao_aluguel_dias > 0")
@dlt.expect_or_drop("valida_duracao_minutos", "duracao_minutos > 0")
@dlt.expect_or_drop("valida_custo_reposicao", "custo_reposicao >= 0")
@dlt.expect("valida_classificacao", "classificacao IN ('G', 'PG', 'PG-13', 'R', 'NC-17') OR classificacao IS NULL")
@dlt.expect("valida_ano_lancamento", "ano_lancamento >= 1900 AND ano_lancamento <= year(current_date())")
def dvdrental_filmes_source():
    """
    Processa dados da Bronze aplicando limpeza, validações e tradução de colunas.
    
    Esta view serve como source para apply_changes, aplicando:
    - Trim em campos de texto
    - Conversão de tipos (decimal, timestamp)
    - Tradução de colunas para PT-BR
    - Adição de timestamp de processamento
    - Data quality expectations (validações)
    
    Returns:
        DataFrame: Dados limpos e validados com colunas em PT-BR
    """
    
    # Read from Bronze layer (lê incrementalmente via streaming)
    df_bronze = spark.readStream.table("dvdrental_film")
    
    # Apply transformations and translate columns in a single select
    # Performance otimizado: todas transformações em uma única operação
    df_transformed = df_bronze.select(
        # Identificadores
        F.col("film_id").alias("id_filme"),
        F.col("language_id").alias("id_idioma"),
        
        # Informações descritivas (com trim)
        F.trim(F.col("title")).alias("titulo"),
        F.trim(F.col("description")).alias("descricao"),
        F.col("release_year").alias("ano_lancamento"),
        F.when(
            F.trim(F.col("rating")).isNotNull(), 
            F.upper(F.trim(F.col("rating")))
        ).otherwise(None).alias("classificacao"),
        
        # Características técnicas
        F.col("length").alias("duracao_minutos"),
        F.col("special_features").alias("recursos_especiais"),
        F.col("fulltext").alias("texto_completo"),
        
        # Informações comerciais (com cast para decimal)
        F.col("rental_duration").alias("duracao_aluguel_dias"),
        F.col("rental_rate").cast("decimal(4,2)").alias("valor_aluguel"),
        F.col("replacement_cost").cast("decimal(5,2)").alias("custo_reposicao"),
        
        # Metadados e auditoria
        F.col("last_update").alias("ultima_atualizacao"),
        F.col("ingestion_timestamp").alias("data_ingestao"),
        F.current_timestamp().alias("data_processamento")
    )
    
    return df_transformed


# Step 1: Create target streaming table
dlt.create_streaming_table(
    name="dvdrental_filmes",
    comment="Silver table - Cleaned and validated film catalog with PT-BR column names"
)

# Step 2: Apply changes to silver target table
# DLT gerencia automaticamente:
# - Processamento incremental da bronze
# - Deduplicação por id_filme
# - Ordenação por ultima_atualizacao
# - Merge idempotente (SCD Type 1)
dlt.apply_changes(
    target="dvdrental_filmes",
    source="dvdrental_filmes_source",
    keys=["id_filme"],                    # Chave primária (film_id traduzido)
    sequence_by="ultima_atualizacao",     # Ordenação temporal (last_update traduzido)
    stored_as_scd_type=1,                 # SCD Type 1: mantém apenas versão atual
    # except_column_list=None,            # Processar todas as colunas
)


# ==============================================================================
# COMENTÁRIOS DAS COLUNAS - Unity Catalog
# ==============================================================================
# 
# Nota: Quando usando dlt.apply_changes(), os comentários das colunas devem
# ser aplicados APÓS a criação da tabela via ALTER TABLE ou ao definir
# o schema explicitamente.
# 
# Para adicionar comentários após deploy:
# 
# COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.id_filme IS 
#   'Chave primária - Identificador único do filme';
# 
# Ou usar SQL para aplicar todos os comentários:

COMMENTS_SQL = """
-- Identificadores
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.id_filme IS 
    'Chave primária - Identificador único do filme';
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.id_idioma IS 
    'Chave estrangeira para tabela de idiomas';

-- Informações descritivas
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.titulo IS 
    'Nome/título do filme';
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.descricao IS 
    'Sinopse ou descrição do enredo';
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.ano_lancamento IS 
    'Ano em que o filme foi lançado';
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.classificacao IS 
    'Classificação etária (G, PG, PG-13, R, NC-17)';

-- Características técnicas
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.duracao_minutos IS 
    'Duração total do filme em minutos';
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.recursos_especiais IS 
    'Features especiais do DVD (legendas, making of, cenas deletadas, etc)';
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.texto_completo IS 
    'Índice de busca textual full-text para pesquisa de conteúdo';

-- Informações comerciais
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.duracao_aluguel_dias IS 
    'Período padrão de locação em dias';
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.valor_aluguel IS 
    'Preço cobrado por aluguel';
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.custo_reposicao IS 
    'Custo para substituir o filme em estoque';

-- Metadados e auditoria
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.ultima_atualizacao IS 
    'Timestamp da última modificação no sistema origem';
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.data_ingestao IS 
    'Timestamp de quando foi carregado na camada Bronze';
COMMENT ON COLUMN dev_catalog.silver.dvdrental_filmes.data_processamento IS 
    'Timestamp do processamento na camada Silver';
"""


# ==============================================================================
# DOCUMENTAÇÃO FINAL
# ==============================================================================
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
- data_processamento (TIMESTAMP): Data/hora de processamento na camada Silver

VALIDAÇÕES APLICADAS:
✓ titulo não pode ser nulo ou vazio (expect_or_drop)
✓ valor_aluguel deve ser maior que zero (expect_or_drop)
✓ duracao_aluguel_dias deve ser maior que zero (expect_or_drop)
✓ duracao_minutos deve ser maior que zero (expect_or_drop)
✓ custo_reposicao deve ser maior ou igual a zero (expect_or_drop)
⚠ classificacao deve estar na lista válida ou ser NULL (expect - warning apenas)
⚠ ano_lancamento deve estar entre 1900 e ano atual (expect - warning apenas)

PROCESSAMENTO INCREMENTAL:
- Lê apenas registros novos/modificados da bronze (via streaming read)
- DLT usa ultima_atualizacao (sequence_by) para ordenar mudanças
- Deduplicação automática por id_filme (keys)
- SCD Type 1: sobrescreve registros existentes com versão mais recente
- Re-execução é idempotente e segura

FLUXO DE DADOS:
PostgreSQL → Bronze (incremental via apply_changes) → Silver (incremental via apply_changes)

Cada camada processa apenas o delta de dados, garantindo eficiência.
"""
