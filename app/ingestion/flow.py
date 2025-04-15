import logging
from typing import Dict, List, Optional, Any
import argparse
from datetime import datetime, timedelta
from prefect import flow, get_run_logger

from app.ingestion.extract import (
    extract_deputados,
    extract_votacoes,
    extract_votos,
    extract_discursos
)
from app.ingestion.transform import (
    transform_deputados,
    transform_votacoes,
    transform_votos,
    transform_discursos
)
from app.ingestion.load import (
    load_deputados,
    load_votacoes,
    load_votos,
    load_discursos
)
from app.config import YESTERDAY, TODAY

@flow(name="Deputados ETL Flow")
def deputados_etl_flow(mode: str = "full") -> Dict[str, int]:
    """
    Flow for extracting, transforming, and loading deputies data.
    Uses a unified approach that treats basic and detailed data as a single entity.
    
    Args:
        mode: 'full' for full extraction, 'incremental' for incremental
        
    Returns:
        Statistics about the ETL process
    """
    logger = get_run_logger()
    logger.info(f"Starting deputados ETL flow in {mode} mode")
    
    # Extract - unified extraction directly from detailed endpoint
    logger.info("Starting extraction phase for deputados")
    df_deputados = extract_deputados(mode=mode)
    
    if df_deputados is None or df_deputados.empty:
        logger.warning("No deputados data extracted")
        return {"deputados_extracted": 0, "deputados_loaded": 0}
    
    # Transform
    logger.info("Starting transformation phase for deputados")
    # Transform deputies data using the unified format
    df_deputados_clean = transform_deputados(df_deputados=df_deputados)
    
    # Load
    logger.info("Starting loading phase for deputados")
    num_loaded = load_deputados(
        df_deputados_clean=df_deputados_clean    )
    
    stats = {
        "deputados_extracted": len(df_deputados) if df_deputados is not None else 0,
        "deputados_loaded": num_loaded
    }
    
    logger.info(f"Deputados ETL flow completed with stats: {stats}")
    return stats

@flow(name="Votacoes ETL Flow")
def votacoes_etl_flow(mode: str = "full", 
                     data_inicio: Optional[str] = None, 
                     data_fim: Optional[str] = None) -> Dict[str, int]:
    """
    Flow for extracting, transforming, and loading voting sessions data.
    
    Args:
        mode: 'full' for full extraction, 'incremental' for incremental
        data_inicio: Start date for extraction (format: YYYY-MM-DD)
        data_fim: End date for extraction (format: YYYY-MM-DD)
        
    Returns:
        Statistics about the ETL process
    """
    logger = get_run_logger()
    if mode == "incremental" and data_inicio is None:
        data_inicio = YESTERDAY
        data_fim = TODAY
        
    logger.info(f"Starting votacoes ETL flow in {mode} mode from {data_inicio} to {data_fim}")
    
    # Extract
    logger.info("Starting extraction phase for votacoes")
    df_votacoes = extract_votacoes(mode=mode, data_inicio=data_inicio, data_fim=data_fim)
    
    # Transform
    logger.info("Starting transformation phase for votacoes")
    df_votacoes_clean = transform_votacoes(df_votacoes=df_votacoes)
    
    # Load
    logger.info("Starting loading phase for votacoes")
    num_loaded = load_votacoes(df_votacoes_clean=df_votacoes_clean)
    
    stats = {
        "votacoes_extracted": len(df_votacoes) if df_votacoes is not None else 0,
        "votacoes_loaded": num_loaded
    }
    
    logger.info(f"Votacoes ETL flow completed with stats: {stats}")
    return stats

@flow(name="Votos ETL Flow")
def votos_etl_flow(mode: str = "full", df_votacoes: Optional[Any] = None) -> Dict[str, int]:
    """
    Flow for extracting, transforming, and loading votes data.
    
    Args:
        mode: 'full' for full extraction, 'incremental' for incremental
        df_votacoes: DataFrame with voting sessions from previous task
        
    Returns:
        Statistics about the ETL process
    """
    logger = get_run_logger()
    logger.info(f"Starting votos ETL flow in {mode} mode")
    
    # Extract
    logger.info("Starting extraction phase for votos")
    df_votos = extract_votos(df_votacoes=df_votacoes, mode=mode)
    
    # Transform
    logger.info("Starting transformation phase for votos")
    df_votos_clean = transform_votos(df_votos=df_votos)
    
    # Load
    logger.info("Starting loading phase for votos")
    num_loaded = load_votos(df_votos_clean=df_votos_clean)
    
    stats = {
        "votos_extracted": len(df_votos) if df_votos is not None else 0,
        "votos_loaded": num_loaded
    }
    
    logger.info(f"Votos ETL flow completed with stats: {stats}")
    return stats

@flow(name="Discursos ETL Flow")
def discursos_etl_flow(mode: str = "full", df_deputados: Optional[Any] = None) -> Dict[str, int]:
    """
    Flow for extracting, transforming, and loading speeches data.
    
    Args:
        mode: 'full' for full extraction, 'incremental' for incremental
        df_deputados: DataFrame with deputies from previous task
        
    Returns:
        Statistics about the ETL process
    """
    logger = get_run_logger()
    logger.info(f"Starting discursos ETL flow in {mode} mode")
    
    # Extract
    logger.info("Starting extraction phase for discursos")
    df_discursos = extract_discursos(df_deputados=df_deputados, mode=mode)
    
    # Transform
    logger.info("Starting transformation phase for discursos")
    df_discursos_clean = transform_discursos(df_discursos=df_discursos)
    
    # Load
    logger.info("Starting loading phase for discursos")
    num_loaded = load_discursos(df_discursos_clean=df_discursos_clean)
    
    stats = {
        "discursos_extracted": len(df_discursos) if df_discursos is not None else 0,
        "discursos_loaded": num_loaded
    }
    
    logger.info(f"Discursos ETL flow completed with stats: {stats}")
    return stats

@flow(name="Camara Analytics ETL Flow")
def camara_analytics_etl_flow(
    mode: str = "full",
    entities: List[str] = None,
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None
) -> Dict[str, Dict[str, int]]:
    """
    Main flow for the Camara Analytics ETL process.
    
    Args:
        mode: 'full' for full extraction, 'incremental' for incremental
        entities: List of entities to process ('deputados', 'votacoes', 'votos'), None for all
        data_inicio: Start date for extraction (format: YYYY-MM-DD)
        data_fim: End date for extraction (format: YYYY-MM-DD)
        
    Returns:
        Combined statistics from all flows
    """
    if entities is None:
        entities = ["deputados", "votacoes", "votos", "discursos"]
    elif isinstance(entities, str):
        if entities.lower() == "all":
            entities = ["deputados", "votacoes", "votos", "discursos"] 
        else:
            entities = [entities]
    
    logger = get_run_logger()
    logger.info(f"Starting Camara Analytics ETL flow in {mode} mode for entities: {entities}")
    
    stats = {}
    
    # Process deputies
    if "deputados" in entities:
        stats["deputados"] = deputados_etl_flow(mode=mode)
    
    # Process voting sessions
    if "votacoes" in entities:
        stats["votacoes"] = votacoes_etl_flow(mode=mode, data_inicio=data_inicio, data_fim=data_fim)
    
    # Process votes (depends on voting sessions)
    if "votos" in entities:
        # If we processed votacoes, we can pass the DataFrame directly
        df_votacoes = None if "votacoes" not in entities else extract_votacoes.fn(mode=mode, data_inicio=data_inicio, data_fim=data_fim)
        stats["votos"] = votos_etl_flow(mode=mode, df_votacoes=df_votacoes)
    
    if "discursos" in entities:
        # If we processed deputados, we can pass the DataFrame directly
        # Carrega do disco em vez de reutilizar o DataFrame anterior, pois extract_deputados agora retorna detalhes
        df_deputados = None
        if "deputados" in entities:
            # Carrega os dados básicos de deputados do disco
            from app.ingestion.utils import load_dataframe
            df_deputados = load_dataframe("deputados")
        
        stats["discursos"] = discursos_etl_flow(mode=mode, df_deputados=df_deputados)

    logger.info(f"Camara Analytics ETL flow completed with stats: {stats}")
    return stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Camara Analytics ETL process")
    parser.add_argument("--mode", type=str, choices=["full", "incremental"], default="incremental",
                        help="ETL mode (full or incremental)")
    parser.add_argument("--entity", type=str, default="all",
                        help="Entity to process (deputados, votacoes, votos, or all)")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Start date for extraction (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None,
                        help="End date for extraction (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Run the flow
    results = camara_analytics_etl_flow(
        mode=args.mode,
        entities=[args.entity] if args.entity != "all" else None,
        data_inicio=args.start_date,
        data_fim=args.end_date
    )
    
    # Print summary
    print("\n--- ETL Process Summary ---")
    for entity, stats in results.items():
        print(f"\n{entity.upper()}:")
        for key, value in stats.items():
            print(f"  {key}: {value}")