import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import pandas as pd
from prefect import task
from tqdm import tqdm

from app.ingestion.api import (
    obter_deputados, 
    obter_detalhe_deputado, 
    obter_votacoes, 
    obter_votos_votacao,
    obter_discursos_deputado
)
from app.ingestion.utils import (
    save_dataframe, 
    load_dataframe, 
    get_last_update_date, 
    update_last_update_date
)
from app.config import YESTERDAY, TODAY

logger = logging.getLogger(__name__)

@task(name="Extract Deputados")
def extract_deputados(mode: str = "full") -> pd.DataFrame:
    """
    Extract deputies data from the API.
    
    Args:
        mode: 'full' for full extraction, 'incremental' for incremental
        
    Returns:
        DataFrame with deputies data
    """
    logger.info(f"Extracting deputados in {mode} mode")
    
    # For deputies, we always do a full extraction as there's no date filter in the API
    df_deputados = obter_deputados()
    
    if df_deputados is None or df_deputados.empty:
        logger.warning("No deputados data found")
        return None
    
    # Save raw data to disk
    save_dataframe(df_deputados, "deputados")
    
    # Update last update date
    update_last_update_date("deputados")
    
    return df_deputados

@task(name="Extract Deputados Details")
def extract_deputados_details(df_deputados: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Extract detailed information for each deputy.
    
    Args:
        df_deputados: DataFrame with basic deputy information
        
    Returns:
        DataFrame with detailed deputy information
    """
    if df_deputados is None:
        # Try to load from disk if not provided
        df_deputados = load_dataframe("deputados")
    
    if df_deputados is None or df_deputados.empty:
        logger.warning("No deputados data available for extracting details")
        return None
    
    deputados_details = []
    ids = df_deputados['id'].tolist()
    
    logger.info(f"Extracting details for {len(ids)} deputados")
    
    for id in tqdm(ids, desc="Extracting deputados details"):
        detalhe = obter_detalhe_deputado(id)
        if detalhe and 'dados' in detalhe:
            deputados_details.append(detalhe['dados'])
        else:
            logger.warning(f"No details found for deputado {id}")
    
    if not deputados_details:
        logger.warning("No deputado details found")
        return None
    
    df_details = pd.DataFrame(deputados_details)
    
    # Save raw data to disk
    save_dataframe(df_details, "detalhes_deputados")
    
    # Update last update date
    update_last_update_date("detalhes_deputados")
    
    return df_details

@task(name="Extract Votacoes")
def extract_votacoes(mode: str = "full", data_inicio: Optional[str] = None, data_fim: Optional[str] = None) -> pd.DataFrame:
    """
    Extract voting sessions data from the API.
    
    Args:
        mode: 'full' for full extraction, 'incremental' for incremental
        data_inicio: Start date for extraction (format: YYYY-MM-DD)
        data_fim: End date for extraction (format: YYYY-MM-DD)
        
    Returns:
        DataFrame with voting sessions data
    """
    if mode == "incremental":
        # For incremental, use last update date or yesterday as data_inicio
        if data_inicio is None:
            data_inicio = get_last_update_date("votacoes") or YESTERDAY
        
        # For incremental, use today as data_fim if not provided
        if data_fim is None:
            data_fim = TODAY
    
    logger.info(f"Extracting votacoes in {mode} mode from {data_inicio} to {data_fim}")
    
    # Generate monthly intervals to avoid API limitations
    data_inicio_dt = datetime.strptime(data_inicio, '%Y-%m-%d') if data_inicio else datetime(2023, 1, 1)
    data_fim_dt = datetime.strptime(data_fim, '%Y-%m-%d') if data_fim else datetime.now()
    
    # Generate monthly intervals
    intervalos = []
    current_date = data_inicio_dt
    while current_date < data_fim_dt:
        next_month = current_date.replace(day=28) + timedelta(days=4)  # Move to next month
        next_month = next_month.replace(day=1)  # First day of next month
        end_date = min(next_month - timedelta(days=1), data_fim_dt)  # Last day of current month or data_fim
        
        intervalos.append((
            current_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ))
        
        current_date = next_month
    
    # Fetch data for each interval
    all_votacoes = []
    
    for inicio, fim in tqdm(intervalos, desc="Extracting votacoes by interval"):
        logger.info(f"Fetching votacoes from {inicio} to {fim}")
        votacoes_interval = obter_votacoes(dataInicio=inicio, dataFim=fim)
        
        if votacoes_interval is not None and not votacoes_interval.empty:
            all_votacoes.append(votacoes_interval)
        else:
            logger.warning(f"No votacoes found for interval {inicio} to {fim}")
    
    if not all_votacoes:
        logger.warning("No votacoes data found")
        return None
    
    # Concatenate all intervals
    df_votacoes = pd.concat(all_votacoes, ignore_index=True)
    
    # Save raw data to disk
    save_dataframe(df_votacoes, "votacoes")
    
    # Update last update date
    update_last_update_date("votacoes", data_fim)
    
    return df_votacoes

@task(name="Extract Votos")
def extract_votos(df_votacoes: Optional[pd.DataFrame] = None, mode: str = "full") -> pd.DataFrame:
    """
    Extract votes for each voting session.
    
    Args:
        df_votacoes: DataFrame with voting sessions
        mode: 'full' for all votacoes, 'incremental' for recent votacoes
        
    Returns:
        DataFrame with votes data
    """
    if df_votacoes is None:
        # Try to load from disk if not provided
        df_votacoes = load_dataframe("votacoes")
    
    if df_votacoes is None or df_votacoes.empty:
        logger.warning("No votacoes data available for extracting votos")
        return None
    
    # For incremental mode, filter to recent votacoes
    if mode == "incremental":
        last_update = get_last_update_date("votos")
        if last_update:
            last_update_dt = datetime.strptime(last_update, '%Y-%m-%d')
            df_votacoes = df_votacoes[pd.to_datetime(df_votacoes['data']).dt.date >= last_update_dt.date()]
    
    votacao_ids = df_votacoes['id'].unique().tolist()
    logger.info(f"Extracting votos for {len(votacao_ids)} votacoes")
    
    all_votos = []
    
    for votacao_id in tqdm(votacao_ids, desc="Extracting votos"):
        votos = obter_votos_votacao(votacao_id)
        
        if votos is not None and not votos.empty:
            votos['idVotacao'] = votacao_id
            all_votos.append(votos)
        else:
            logger.warning(f"No votos found for votacao {votacao_id}")
    
    if not all_votos:
        logger.warning("No votos data found")
        return None
    
    # Concatenate all votes
    df_votos = pd.concat(all_votos, ignore_index=True)
    
    # Save raw data to disk
    save_dataframe(df_votos, "votos")
    
    # Update last update date
    update_last_update_date("votos")
    
    return df_votos


@task(name="Extract Discursos")
def extract_discursos(df_deputados : Optional[pd.DataFrame] = None, mode: str = "full" ) -> pd.DataFrame:
    """
    Extract speeches for each deputy.

    Args:
        df_deputados: DataFrame with deputies
        mode: 'full' for all deputados, 'incremental' for recent deputados
    Returns:
        DataFrame with speeches data
    """
    if df_deputados is None:
        # Try to load from disk if not provided
        df_deputados = load_dataframe("deputados")

    if df_deputados is None or df_deputados.empty:
        logger.warning("No deputados data available for extracting speeches")
        return None
    
    # For incremental mode, filter to recent deputados
    if mode == "incremental":
        last_update = get_last_update_date("discursos")
        if last_update:
            last_update_dt = datetime.strptime(last_update, '%Y-%m-%d')
            df_deputados = df_deputados[pd.to_datetime(df_deputados['data']).dt.date >= last_update_dt.date()]

    deputados_ids = df_deputados['id'].unique().tolist()
    logger.info(f"Extracting discursos for {len(deputados_ids)} deputados")

    all_discursos = []

    for discurso_id in tqdm(deputados_ids, desc="Extracting discursos"):
        discursos = obter_discursos_deputado(id=discurso_id)

        if discursos is not None and not discursos.empty:
            discursos['idDeputado'] = discurso_id
            all_discursos.append(discursos)
        else:
            logger.warning(f"No discursos found for deputado {discurso_id}")


    if not all_discursos:
        logger.warning("No discursos data found")
        return None
    
    # Concatenate all speeches
    df_discursos = pd.concat(all_discursos, ignore_index=True)

    # Save raw data to disk
    save_dataframe(df_discursos, "discursos")

    # Update last update date
    update_last_update_date("discursos")

    return df_discursos


