-- ==============================================================================
-- Setup Checkpoint Table - Executar UMA ÚNICA VEZ antes da primeira execução
-- ==============================================================================
--
-- Este script cria a tabela de controle que armazena o checkpoint (último
-- timestamp processado) para carga incremental do PostgreSQL.
--
-- INSTRUÇÕES:
-- 1. Execute este SQL ANTES da primeira execução do pipeline
-- 2. Ajuste catalog/schema conforme necessário
-- 3. Só precisa executar UMA vez (IF NOT EXISTS protege contra re-execução)
-- ==============================================================================

-- Criar tabela de checkpoint
CREATE TABLE IF NOT EXISTS prod_catalog.bronze.dlt_checkpoint_dvdrental_film (
    table_name STRING COMMENT 'Nome da tabela fonte (ex: dvdrental_film)',
    last_processed_timestamp TIMESTAMP COMMENT 'Último timestamp processado com sucesso',
    last_update_time TIMESTAMP COMMENT 'Timestamp de quando este checkpoint foi atualizado'
)
COMMENT 'Tabela de controle para carga incremental - mantém checkpoint do último timestamp processado';

-- Verificar se a tabela foi criada
SELECT 
    'Checkpoint table created successfully!' as status,
    COUNT(*) as current_records
FROM prod_catalog.bronze.dlt_checkpoint_dvdrental_film;

-- ==============================================================================
-- QUERIES DE VERIFICAÇÃO (úteis para troubleshooting)
-- ==============================================================================

-- Ver checkpoint atual
-- SELECT * FROM prod_catalog.bronze.dlt_checkpoint_dvdrental_film;

-- Ver quantos registros serão processados na próxima execução (exemplo)
-- SELECT COUNT(*) as pending_records
-- FROM dvdrental.film  -- Ajustar para sua fonte
-- WHERE last_update > (
--     SELECT COALESCE(MAX(last_processed_timestamp), TIMESTAMP '1900-01-01 00:00:00')
--     FROM prod_catalog.bronze.dlt_checkpoint_dvdrental_film
-- );
