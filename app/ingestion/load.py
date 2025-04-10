from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict, Any, Optional, Union, Callable
import logging
from datetime import datetime, date
import pandas as pd

# Import database
from app.database.database import SessionLocal
from app.database.models import Deputado, Despesa, Discurso, Votacao, Voto, Base

# Import API functions
from .api import (
    obter_deputados, obter_detalhe_deputado, obter_despesas_deputado, 
    obter_discursos_deputado, obter_votacoes, obter_votos_votacao
)

# Import transform functions
from .transform import (
    transform_deputado, transform_despesa, transform_discurso,
    transform_votacao, transform_voto, transform_dataframe_to_models
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data(
    models: List[Base], 
    session: Session, 
    incremental: bool = True, 
    check_exists_func: Optional[Callable] = None
) -> int:
    """
    Load data into database.
    
    Args:
        models: List of SQLAlchemy model instances to insert/update
        session: SQLAlchemy session
        incremental: If True, only insert if not exists or update if changed
        check_exists_func: Function to check if record already exists (incremental mode)
        
    Returns:
        Number of records inserted/updated
    """
    if not models:
        return 0
    
    count = 0
    
    try:
        if incremental and check_exists_func:
            for model in models:
                exists = check_exists_func(session, model)
                if not exists:
                    session.add(model)
                    count += 1
        else:
            session.add_all(models)
            count = len(models)
        
        session.commit()
        return count
    
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error loading data: {str(e)}")
        raise
    
    except Exception as e:
        session.rollback()
        logger.error(f"Unexpected error: {str(e)}")
        raise

def check_deputado_exists(session: Session, deputado: Deputado) -> bool:
    """Check if deputy already exists in database"""
    return session.query(Deputado).filter(Deputado.id == deputado.id).first() is not None

def check_despesa_exists(session: Session, despesa: Despesa) -> bool:
    """Check if expense already exists in database"""
    return session.query(Despesa).filter(
        Despesa.deputado_id == despesa.deputado_id,
        Despesa.ano == despesa.ano,
        Despesa.mes == despesa.mes,
        Despesa.cod_documento == despesa.cod_documento
    ).first() is not None

def check_discurso_exists(session: Session, discurso: Discurso) -> bool:
    """Check if speech already exists in database"""
    return session.query(Discurso).filter(
        Discurso.deputado_id == discurso.deputado_id,
        Discurso.data_hora_inicio == discurso.data_hora_inicio
    ).first() is not None

def check_votacao_exists(session: Session, votacao: Votacao) -> bool:
    """Check if voting session already exists in database"""
    return session.query(Votacao).filter(Votacao.id == votacao.id).first() is not None

def check_voto_exists(session: Session, voto: Voto) -> bool:
    """Check if vote already exists in database"""
    return session.query(Voto).filter(
        Voto.votacao_id == voto.votacao_id,
        Voto.deputado_id == voto.deputado_id
    ).first() is not None

def clear_table(session: Session, model_class) -> None:
    """Clear all records from a table"""
    session.query(model_class).delete()
    session.commit()

def load_deputados(
    session: Optional[Session] = None, 
    incremental: bool = True,
    **api_params
) -> int:
    """
    Load deputies from API to database.
    
    Args:
        session: SQLAlchemy session (optional, will create one if not provided)
        incremental: If True, only insert new deputies or update existing ones
        **api_params: Parameters to pass to the API function
        
    Returns:
        Number of deputies loaded
    """
    logger.info("Loading deputies data...")
    
    # Create session if not provided
    session_provided = session is not None
    if not session_provided:
        session = SessionLocal()
    
    try:
        # If full load, clear the table first
        if not incremental:
            clear_table(session, Deputado)
        
        # Get deputies list from API
        deputados_df = obter_deputados(**api_params)
        if deputados_df is None or deputados_df.empty:
            logger.warning("No deputies data found.")
            return 0
        
        count = 0
        # Process each deputy to get detailed information
        for _, row in deputados_df.iterrows():
            deputado_id = row['id']
            detalhe = obter_detalhe_deputado(deputado_id)
            
            if detalhe and 'dados' in detalhe:
                deputado_data = detalhe['dados']
                deputado_model = transform_deputado(deputado_data)
                
                # Check if deputado already exists (for incremental load)
                if incremental:
                    existing = session.query(Deputado).filter(Deputado.id == deputado_id).first()
                    if existing:
                        # Update existing record with new data
                        for key, value in deputado_model.__dict__.items():
                            if key != '_sa_instance_state':
                                setattr(existing, key, value)
                    else:
                        session.add(deputado_model)
                else:
                    session.add(deputado_model)
                
                count += 1
                
                # Commit every 100 records to avoid large transactions
                if count % 100 == 0:
                    session.commit()
                    logger.info(f"Processed {count} deputies so far...")
        
        # Final commit
        session.commit()
        logger.info(f"Successfully loaded {count} deputies.")
        return count
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error loading deputies: {str(e)}")
        raise
    
    finally:
        # Only close if we created the session
        if not session_provided:
            session.close()

def load_despesas(
    deputado_id: int,
    session: Optional[Session] = None,
    incremental: bool = True,
    **api_params
) -> int:
    """
    Load expenses for a deputy from API to database.
    
    Args:
        deputado_id: ID of the deputy
        session: SQLAlchemy session (optional, will create one if not provided)
        incremental: If True, only insert new expenses
        **api_params: Parameters to pass to the API function
        
    Returns:
        Number of expenses loaded
    """
    logger.info(f"Loading expenses for deputy ID {deputado_id}...")
    
    # Create session if not provided
    session_provided = session is not None
    if not session_provided:
        session = SessionLocal()
    
    try:
        # If full load, clear existing expenses for this deputy
        if not incremental:
            session.query(Despesa).filter(Despesa.deputado_id == deputado_id).delete()
            session.commit()
        
        # Get expenses from API
        despesas_df = obter_despesas_deputado(deputado_id, **api_params)
        if despesas_df is None or despesas_df.empty:
            logger.warning(f"No expenses found for deputy ID {deputado_id}.")
            return 0
        
        # Transform to model instances
        despesas_models = transform_dataframe_to_models(
            despesas_df, 
            transform_despesa, 
            deputado_id=deputado_id
        )
        
        # Load to database
        count = load_data(
            despesas_models, 
            session, 
            incremental,
            check_despesa_exists if incremental else None
        )
        
        logger.info(f"Successfully loaded {count} expenses for deputy ID {deputado_id}.")
        return count
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error loading expenses for deputy ID {deputado_id}: {str(e)}")
        raise
    
    finally:
        # Only close if we created the session
        if not session_provided:
            session.close()

def load_discursos(
    deputado_id: int,
    session: Optional[Session] = None,
    incremental: bool = True,
    **api_params
) -> int:
    """
    Load speeches for a deputy from API to database.
    
    Args:
        deputado_id: ID of the deputy
        session: SQLAlchemy session (optional, will create one if not provided)
        incremental: If True, only insert new speeches
        **api_params: Parameters to pass to the API function
        
    Returns:
        Number of speeches loaded
    """
    logger.info(f"Loading speeches for deputy ID {deputado_id}...")
    
    # Create session if not provided
    session_provided = session is not None
    if not session_provided:
        session = SessionLocal()
    
    try:
        # If full load, clear existing speeches for this deputy
        if not incremental:
            session.query(Discurso).filter(Discurso.deputado_id == deputado_id).delete()
            session.commit()
        
        # Get speeches from API
        discursos_df = obter_discursos_deputado(deputado_id, **api_params)
        if discursos_df is None or discursos_df.empty:
            logger.warning(f"No speeches found for deputy ID {deputado_id}.")
            return 0
        
        # Transform to model instances
        discursos_models = transform_dataframe_to_models(
            discursos_df, 
            transform_discurso, 
            deputado_id=deputado_id
        )
        
        # Load to database
        count = load_data(
            discursos_models, 
            session, 
            incremental,
            check_discurso_exists if incremental else None
        )
        
        logger.info(f"Successfully loaded {count} speeches for deputy ID {deputado_id}.")
        return count
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error loading speeches for deputy ID {deputado_id}: {str(e)}")
        raise
    
    finally:
        # Only close if we created the session
        if not session_provided:
            session.close()

def load_votacoes(
    session: Optional[Session] = None,
    incremental: bool = True,
    **api_params
) -> int:
    """
    Load voting sessions from API to database.
    
    Args:
        session: SQLAlchemy session (optional, will create one if not provided)
        incremental: If True, only insert new voting sessions
        **api_params: Parameters to pass to the API function
        
    Returns:
        Number of voting sessions loaded
    """
    logger.info("Loading voting sessions data...")
    
    # Create session if not provided
    session_provided = session is not None
    if not session_provided:
        session = SessionLocal()
    
    try:
        # If full load, clear the table first
        if not incremental:
            clear_table(session, Votacao)
        
        # Get voting sessions from API
        votacoes_df = obter_votacoes(**api_params)
        if votacoes_df is None or votacoes_df.empty:
            logger.warning("No voting sessions found.")
            return 0
        
        # Transform to model instances
        votacoes_models = transform_dataframe_to_models(
            votacoes_df, 
            transform_votacao
        )
        
        # Load to database
        count = load_data(
            votacoes_models, 
            session, 
            incremental,
            check_votacao_exists if incremental else None
        )
        
        logger.info(f"Successfully loaded {count} voting sessions.")
        return count
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error loading voting sessions: {str(e)}")
        raise
    
    finally:
        # Only close if we created the session
        if not session_provided:
            session.close()

def load_votos(
    votacao_id: str,
    session: Optional[Session] = None,
    incremental: bool = True
) -> int:
    """
    Load votes for a voting session from API to database.
    
    Args:
        votacao_id: ID of the voting session
        session: SQLAlchemy session (optional, will create one if not provided)
        incremental: If True, only insert new votes
        
    Returns:
        Number of votes loaded
    """
    logger.info(f"Loading votes for voting session ID {votacao_id}...")
    
    # Create session if not provided
    session_provided = session is not None
    if not session_provided:
        session = SessionLocal()
    
    try:
        # If full load, clear existing votes for this voting session
        if not incremental:
            session.query(Voto).filter(Voto.votacao_id == votacao_id).delete()
            session.commit()
        
        # Get votes from API
        votos_df = obter_votos_votacao(votacao_id)
        if votos_df is None or votos_df.empty:
            logger.warning(f"No votes found for voting session ID {votacao_id}.")
            return 0
        
        # Transform to model instances
        votos_models = transform_dataframe_to_models(
            votos_df, 
            transform_voto, 
            votacao_id=votacao_id
        )
        
        # Load to database
        count = load_data(
            votos_models, 
            session, 
            incremental,
            check_voto_exists if incremental else None
        )
        
        logger.info(f"Successfully loaded {count} votes for voting session ID {votacao_id}.")
        return count
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error loading votes for voting session ID {votacao_id}: {str(e)}")
        raise
    
    finally:
        # Only close if we created the session
        if not session_provided:
            session.close()

def run_full_etl(incremental: bool = True, **api_params) -> Dict[str, int]:
    """
    Run full ETL process for all data types.
    
    Args:
        incremental: If True, use incremental loading, else full loading
        **api_params: Parameters to pass to the API functions
        
    Returns:
        Dictionary with counts of records loaded for each entity
    """
    logger.info(f"Starting {'incremental' if incremental else 'full'} ETL process...")
    
    session = SessionLocal()
    try:
        results = {}
        
        # Load deputies
        results['deputados'] = load_deputados(session, incremental, **api_params)
        
        # Get list of deputy IDs
        deputados_ids = [id for (id,) in session.query(Deputado.id).all()]
        
        # Load expenses for each deputy
        despesas_count = 0
        for deputado_id in deputados_ids:
            despesas_count += load_despesas(deputado_id, session, incremental, **api_params)
        results['despesas'] = despesas_count
        
        # Load speeches for each deputy
        discursos_count = 0
        for deputado_id in deputados_ids:
            discursos_count += load_discursos(deputado_id, session, incremental, **api_params)
        results['discursos'] = discursos_count
        
        # Load voting sessions
        results['votacoes'] = load_votacoes(session, incremental, **api_params)
        
        # Get list of voting session IDs
        votacoes_ids = [id for (id,) in session.query(Votacao.id).all()]
        
        # Load votes for each voting session
        votos_count = 0
        for votacao_id in votacoes_ids:
            votos_count += load_votos(votacao_id, session, incremental)
        results['votos'] = votos_count
        
        logger.info(f"ETL process completed successfully.")
        logger.info(f"Summary: {results}")
        return results
    
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        raise
    
    finally:
        session.close()
