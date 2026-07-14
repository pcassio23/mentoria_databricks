# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingestão Cotação BCB para Volume - Carga Diária
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC  Detalhe | Informação |
# MAGIC ---------|------------|
# MAGIC  Criado Originalmente Por | Paulo Costa |
# MAGIC  Tabela de Dados de Saída | `{catalog}.default.landing_zone/cotacoes` (Volume UC) |
# MAGIC  Origem Fonte de Dados de Entrada | API Banco Central (BCB) + BrasilAPI |
# MAGIC  Destino Fonte de Dados de Saída | Arquivos JSON por moeda |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros
# MAGIC
# MAGIC  Parâmetro | Tipo | Descrição | Valor Padrão |
# MAGIC -----------|------|------------|---------------|
# MAGIC  `catalog` | Variável | Catalog UC (passado via esteira) | `dev_catalog` |
# MAGIC  `schema` | Fixo | Schema UC | `default` |
# MAGIC  `volume_name` | Fixo | Nome do volume UC | `landing_zone` |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC  Data       | Desenvolvido Por         | Motivo                                         |
# MAGIC :----------:|--------------------------|-----------------------------------------------|
# MAGIC  14/07/2026 | Paulo Costa              | Criação do notebook de ingestão de cotações USD, EUR, GBP do BCB com lógica de dia útil/feriados. Arquivos JSON separados por moeda. |

# COMMAND ----------

# DBTITLE 1,Ingestão - Arquivos separados por moeda
import requests
import json
import logging
from datetime import datetime, timedelta

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ========================================
# CONFIGURAÇÕES
# ========================================
# Variável de ambiente (passada via esteira)
catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "dev_catalog"

# Valores fixos
schema = "default"
volume_name = "landing_zone"

logging.info(f"Configuração: {catalog}.{schema}.{volume_name}")

# Moedas a serem buscadas
moedas = ['USD', 'EUR', 'GBP']

# Função para obter feriados do ano
def obter_feriados(ano):
    try:
        url_feriados = f"https://brasilapi.com.br/api/feriados/v1/{ano}"
        response = requests.get(url_feriados, timeout=10)
        if response.ok:
            feriados = response.json()
            # Extrai apenas as datas (formato YYYY-MM-DD)
            return [f['date'] for f in feriados]
        return []
    except:
        return []

# Função para encontrar o último dia útil
def obter_dia_util():
    data_ref = datetime.now() - timedelta(days=1)  # d-1
    ano = data_ref.year
    feriados = obter_feriados(ano)
    
    logging.info(f"Verificando feriados de {ano}...")
    logging.info(f"Total de feriados encontrados: {len(feriados)}")
    
    # Tenta até 10 dias atrás para encontrar um dia útil
    for i in range(10):
        data_teste = data_ref - timedelta(days=i)
        data_str = data_teste.strftime('%Y-%m-%d')
        dia_semana = data_teste.weekday()  # 0=Segunda, 5=Sábado, 6=Domingo
        
        # Verifica se é fim de semana
        if dia_semana == 5:  # Sábado
            logging.debug(f"{data_teste.strftime('%d/%m/%Y')} é sábado, pulando...")
            continue
        if dia_semana == 6:  # Domingo
            logging.debug(f"{data_teste.strftime('%d/%m/%Y')} é domingo, pulando...")
            continue
            
        # Verifica se é feriado
        if data_str in feriados:
            logging.debug(f"{data_teste.strftime('%d/%m/%Y')} é feriado, pulando...")
            continue
        
        # Encontrou um dia útil
        logging.info(f"Dia útil encontrado: {data_teste.strftime('%d/%m/%Y')}")
        return data_teste
    
    # Fallback: retorna d-1 se não encontrar nada
    return data_ref

# Obtém o dia útil
data_cotacao = obter_dia_util()

# Busca apenas o dia útil específico (data_inicial = data_final)
data_inicial = data_cotacao.strftime('%m-%d-%Y')
data_final = data_cotacao.strftime('%m-%d-%Y')

logging.info(f"Buscando cotações de {len(moedas)} moedas")
logging.info(f"Data: {data_cotacao.strftime('%d/%m/%Y')}")
logging.info(f"Moedas: {', '.join(moedas)}")

# Salva um arquivo JSON para cada moeda
moedas_sucesso = []
moedas_erro = []
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/cotacoes"

# Cria o diretório se não existir
import os
os.makedirs(volume_path, exist_ok=True)
arquivos_salvos = []

for moeda in moedas:
    try:
        url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='{moeda}'&@dataInicial='{data_inicial}'&@dataFinalCotacao='{data_final}'&$format=json"
        
        response = requests.get(url, timeout=15)
        
        if response.ok:
            data = response.json()
            if 'value' in data and len(data['value']) > 0:
                # Salva arquivo individual para esta moeda
                filename = f"cotacao_{moeda}_{timestamp}.json"
                filepath = f"{volume_path}/{filename}"
                
                resultado = {
                    "metadata": {
                        "data_coleta": timestamp,
                        "data_referencia": data_cotacao.strftime('%Y-%m-%d'),
                        "moeda": moeda,
                        "total_registros": len(data['value'])
                    },
                    "cotacoes": data['value']
                }
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(resultado, f, ensure_ascii=False, indent=2)
                
                moedas_sucesso.append(moeda)
                arquivos_salvos.append(filepath)
                logging.info(f"✓ {moeda}: {len(data['value'])} registros → {filename}")
            else:
                moedas_erro.append(moeda)
                logging.warning(f"{moeda}: Sem dados")
        else:
            moedas_erro.append(moeda)
            logging.error(f"{moeda}: Erro {response.status_code}")
            
    except Exception as e:
        moedas_erro.append(moeda)
        logging.error(f"{moeda}: {str(e)[:100]}")

logging.info("="*60)

if arquivos_salvos:
    logging.info(f"✓ {len(arquivos_salvos)} arquivos salvos com sucesso!")
    for arquivo in arquivos_salvos:
        logging.info(f"📁 {arquivo}")
    logging.info(f"💱 Moedas: {', '.join(moedas_sucesso)}")
else:
    logging.warning("Nenhuma cotação encontrada.")

