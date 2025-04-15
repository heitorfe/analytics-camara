import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import pandas as pd
import requests
from prefect import task
from tqdm import tqdm

from app.ingestion.utils import (
    save_dataframe, 
    load_dataframe, 
    get_last_update_date, 
    update_last_update_date
)
from app.config import YESTERDAY, TODAY

logger = logging.getLogger(__name__)

# -- Funções de API incorporadas --

def fazer_requisicao(url, parametros=None, returnar_df=True):
    """
    Realiza uma requisição HTTP para a API da Câmara dos Deputados.
    
    Args:
        url: URL da API
        parametros: Parâmetros da requisição
        returnar_df: Se True, retorna um DataFrame, senão retorna o JSON original
        
    Returns:
        DataFrame ou dict com os dados da API, ou None se a requisição falhar
    """
    resposta = requests.get(url, params=parametros)
    if resposta.status_code == 200:
        dados = resposta.json()
        if returnar_df and 'dados' in dados and type(dados['dados']) == list: 
            return pd.DataFrame(dados['dados'])
        else:
           return dados
        
    return None

# -- Funções de extração --

@task(name="Extract Deputados")
def extract_deputados(mode: str = "full") -> pd.DataFrame:
    """
    Extrai dados de deputados diretamente da API de detalhes da Câmara.
    Esta função unifica a extração de dados básicos e detalhados dos deputados.
    
    Args:
        mode: 'full' para extração completa, 'incremental' para incremental
        
    Returns:
        DataFrame com dados detalhados dos deputados
    """
    logger.info(f"Extracting deputados with unified approach in {mode} mode")
    
    # Primeiro obtém a lista de IDs de deputados ativos
    url = 'https://dadosabertos.camara.leg.br/api/v2/deputados'
    df_deputies_list = fazer_requisicao(url)
    
    if df_deputies_list is None or df_deputies_list.empty:
        logger.warning("No deputados list found")
        return None
    
    # Agora obtém os detalhes completos para cada deputado
    deputados_data = []
    ids = df_deputies_list['id'].tolist()
    
    logger.info(f"Extracting details for {len(ids)} deputados")
    
    for deputy_id in tqdm(ids, desc="Extracting deputados details"):
        url = f'https://dadosabertos.camara.leg.br/api/v2/deputados/{deputy_id}'
        response = fazer_requisicao(url, returnar_df=False)
        
        if response and 'dados' in response:
            # Process the detailed data into a flattened structure
            deputy_data = response['dados']
            
            # Extract ultimo_status fields
            ultimo_status = deputy_data.get('ultimoStatus', {})
            
            # Create a flattened record
            flat_data = {
                # Basic info
                'id': deputy_data.get('id'),
                'uri': deputy_data.get('uri'),
                'nomeCivil': deputy_data.get('nomeCivil'),
                'cpf': deputy_data.get('cpf'),
                'sexo': deputy_data.get('sexo'),
                'urlWebsite': deputy_data.get('urlWebsite'),
                'dataNascimento': deputy_data.get('dataNascimento'),
                'dataFalecimento': deputy_data.get('dataFalecimento'),
                'ufNascimento': deputy_data.get('ufNascimento'),
                'municipioNascimento': deputy_data.get('municipioNascimento'),
                'escolaridade': deputy_data.get('escolaridade'),
                
                # Include social media if available
                'redeSocial': deputy_data.get('redeSocial', []),
                
                # Último status fields - mantém nomes originais da API para compatibilidade
                'ultimo_status': ultimo_status,
                'nome': ultimo_status.get('nome'),
                'siglaPartido': ultimo_status.get('siglaPartido'),
                'uriPartido': ultimo_status.get('uriPartido'),
                'siglaUf': ultimo_status.get('siglaUf'),
                'idLegislatura': ultimo_status.get('idLegislatura'),
                'urlFoto': ultimo_status.get('urlFoto'),
                'email': ultimo_status.get('email'),
                'data': ultimo_status.get('data'),
                'nomeEleitoral': ultimo_status.get('nomeEleitoral'),
                'situacao': ultimo_status.get('situacao'),
                'condicaoEleitoral': ultimo_status.get('condicaoEleitoral'),
                'descricaoStatus': ultimo_status.get('descricaoStatus')
            }
            
            # Add gabinete data if present
            gabinete = ultimo_status.get('gabinete', {})
            if gabinete:
                flat_data['gabinete'] = gabinete
                flat_data['gabinete_nome'] = gabinete.get('nome')
                flat_data['gabinete_predio'] = gabinete.get('predio')
                flat_data['gabinete_sala'] = gabinete.get('sala')
                flat_data['gabinete_andar'] = gabinete.get('andar')
                flat_data['gabinete_telefone'] = gabinete.get('telefone')
                flat_data['gabinete_email'] = gabinete.get('email')
            
            deputados_data.append(flat_data)
        else:
            logger.warning(f"No details found for deputado {deputy_id}")
    
    if not deputados_data:
        logger.warning("No deputados details found")
        return None
    
    # Create DataFrame with all data
    df_deputados = pd.DataFrame(deputados_data)
    
    # Save raw data
    save_dataframe(df_deputados, "deputados")
    update_last_update_date("deputados")
    
    return df_deputados

@task(name="Extract Votacoes")
def extract_votacoes(mode: str = "full", data_inicio: Optional[str] = None, data_fim: Optional[str] = None) -> pd.DataFrame:
    """
    Extrai dados de votações da API da Câmara.
    
    Args:
        mode: 'full' para extração completa, 'incremental' para incremental
        data_inicio: Data de início para extração (formato: YYYY-MM-DD)
        data_fim: Data de fim para extração (formato: YYYY-MM-DD)
        
    Returns:
        DataFrame com dados das votações
    """
    if mode == "incremental":
        # Para incremental, usa a última data de atualização ou ontem como data_inicio
        if data_inicio is None:
            data_inicio = get_last_update_date("votacoes") or YESTERDAY
        
        # Para incremental, usa hoje como data_fim se não for informado
        if data_fim is None:
            data_fim = TODAY
    
    logger.info(f"Extracting votacoes in {mode} mode from {data_inicio} to {data_fim}")
    
    # Gera intervalos mensais para evitar limitações da API
    data_inicio_dt = datetime.strptime(data_inicio, '%Y-%m-%d') if data_inicio else datetime(2023, 1, 1)
    data_fim_dt = datetime.strptime(data_fim, '%Y-%m-%d') if data_fim else datetime.now()
    
    # Gera intervalos mensais
    intervalos = []
    current_date = data_inicio_dt
    while current_date < data_fim_dt:
        next_month = current_date.replace(day=28) + timedelta(days=4)  # Move para o próximo mês
        next_month = next_month.replace(day=1)  # Primeiro dia do próximo mês
        end_date = min(next_month - timedelta(days=1), data_fim_dt)  # Último dia do mês corrente ou data_fim
        
        intervalos.append((
            current_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ))
        
        current_date = next_month
    
    # Busca dados para cada intervalo
    all_votacoes = []
    
    for inicio, fim in tqdm(intervalos, desc="Extracting votacoes by interval"):
        logger.info(f"Fetching votacoes from {inicio} to {fim}")
        
        url = 'https://dadosabertos.camara.leg.br/api/v2/votacoes'
        parametros = {'dataInicio': inicio, 'dataFim': fim}
        votacoes_interval = fazer_requisicao(url, parametros)
        
        if votacoes_interval is not None and not votacoes_interval.empty:
            all_votacoes.append(votacoes_interval)
        else:
            logger.warning(f"No votacoes found for interval {inicio} to {fim}")
    
    if not all_votacoes:
        logger.warning("No votacoes data found")
        return None
    
    # Concatena todos os intervalos
    df_votacoes = pd.concat(all_votacoes, ignore_index=True)
    
    # Salva dados brutos em disco
    save_dataframe(df_votacoes, "votacoes")
    
    # Atualiza data da última atualização
    update_last_update_date("votacoes", data_fim)
    
    return df_votacoes

@task(name="Extract Votos")
def extract_votos(df_votacoes: Optional[pd.DataFrame] = None, mode: str = "full") -> pd.DataFrame:
    """
    Extrai votos para cada sessão de votação.
    
    Args:
        df_votacoes: DataFrame com sessões de votação
        mode: 'full' para todas as votações, 'incremental' para votações recentes
        
    Returns:
        DataFrame com dados dos votos
    """
    if df_votacoes is None:
        # Tenta carregar do disco se não for fornecido
        df_votacoes = load_dataframe("votacoes")
    
    if df_votacoes is None or df_votacoes.empty:
        logger.warning("No votacoes data available for extracting votos")
        return None
    
    # Para modo incremental, filtra para votações recentes
    if mode == "incremental":
        last_update = get_last_update_date("votos")
        if last_update:
            last_update_dt = datetime.strptime(last_update, '%Y-%m-%d')
            df_votacoes = df_votacoes[pd.to_datetime(df_votacoes['data']).dt.date >= last_update_dt.date()]
    
    votacao_ids = df_votacoes['id'].unique().tolist()
    logger.info(f"Extracting votos for {len(votacao_ids)} votacoes")
    
    all_votos = []
    
    for votacao_id in tqdm(votacao_ids, desc="Extracting votos"):
        url = f'https://dadosabertos.camara.leg.br/api/v2/votacoes/{votacao_id}/votos'
        votos = fazer_requisicao(url)
        
        if votos is not None and not votos.empty:
            votos['idVotacao'] = votacao_id
            all_votos.append(votos)
        else:
            logger.warning(f"No votos found for votacao {votacao_id}")
    
    if not all_votos:
        logger.warning("No votos data found")
        return None
    
    # Concatena todos os votos
    df_votos = pd.concat(all_votos, ignore_index=True)
    
    # Salva dados brutos em disco
    save_dataframe(df_votos, "votos")
    
    # Atualiza data da última atualização
    update_last_update_date("votos")
    
    return df_votos

@task(name="Extract Discursos")
def extract_discursos(df_deputados: Optional[pd.DataFrame] = None, mode: str = "full") -> pd.DataFrame:
    """
    Extrai discursos para cada deputado.

    Args:
        df_deputados: DataFrame com deputados
        mode: 'full' para todos os deputados, 'incremental' para deputados recentes
    Returns:
        DataFrame com dados dos discursos
    """
    if df_deputados is None:
        # Tenta carregar do disco se não for fornecido
        df_deputados = load_dataframe("deputados")

    if df_deputados is None or df_deputados.empty:
        logger.warning("No deputados data available for extracting speeches")
        return None
    
    # Para modo incremental, filtra para deputados recentes
    if mode == "incremental":
        last_update = get_last_update_date("discursos")
        if last_update:
            last_update_dt = datetime.strptime(last_update, '%Y-%m-%d')
            # Tenta filtrar com base na data, se estiver disponível
            if 'data' in df_deputados.columns:
                df_deputados = df_deputados[pd.to_datetime(df_deputados['data']).dt.date >= last_update_dt.date()]

    deputados_ids = df_deputados['id'].unique().tolist()
    logger.info(f"Extracting discursos for {len(deputados_ids)} deputados")

    all_discursos = []

    for deputado_id in tqdm(deputados_ids, desc="Extracting discursos"):
        url = f'https://dadosabertos.camara.leg.br/api/v2/deputados/{deputado_id}/discursos'
        discursos = fazer_requisicao(url)

        if discursos is not None and not discursos.empty:
            discursos['idDeputado'] = deputado_id
            all_discursos.append(discursos)
        else:
            logger.warning(f"No discursos found for deputado {deputado_id}")

    if not all_discursos:
        logger.warning("No discursos data found")
        return None
    
    # Concatena todos os discursos
    df_discursos = pd.concat(all_discursos, ignore_index=True)

    # Salva dados brutos em disco
    save_dataframe(df_discursos, "discursos")

    # Atualiza data da última atualização
    update_last_update_date("discursos")

    return df_discursos


