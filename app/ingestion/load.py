import logging
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
from prefect import task
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from app.database.models import Deputado, Votacao, Voto, Discurso
from app.config import DATABASE_URL
from app.ingestion.utils import load_dataframe, commit_to_db
from app.ingestion.transform import (
    transform_deputado,
    transform_votacao,
    transform_voto,
    transform_discurso
)

logger = logging.getLogger(__name__)

def get_db_session() -> Tuple[Session, Any]:
    """
    Create a database session.
    
    Returns:
        Tuple with session and engine
    """
    try:
        engine = create_engine(DATABASE_URL)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = SessionLocal()
        return session, engine
    except Exception as e:
        logger.error(f"Error creating database session: {e}")
        raise

@task(name="Load Deputados", retries=3, retry_delay_seconds=30)
def load_deputados(df_deputados_clean: Optional[pd.DataFrame] = None) -> int:
    """
    Load deputies data into the database. Adds new records and updates existing ones.
    
    Args:
        df_deputados_clean: Processed DataFrame with deputies data
        
    Returns:
        Number of deputies loaded
    """
    if df_deputados_clean is None:
        df_deputados_clean = load_dataframe("deputados", processed=True)
    
    if df_deputados_clean is None or df_deputados_clean.empty:
        logger.warning("No processed deputados data available for loading")
        return 0
    
    logger.info(f"Loading {len(df_deputados_clean)} deputados into database")
    
    # Get database session
    try:
        db, engine = get_db_session()
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return 0
    
    try:
        # Transform DataFrame records to list of SQLAlchemy models
        deputados_to_load = []
        for _, row in df_deputados_clean.iterrows():
            try:
                row_dict = row.to_dict()
                deputado = transform_deputado(row_dict)
                deputados_to_load.append(deputado)
            except Exception as e:
                logger.error(f"Error transforming deputado record {row.get('id', 'unknown')}: {e}")
        
        # Use SQLAlchemy's merge operation for each record
        # This will update existing records and create new ones
        num_loaded = 0
        for deputado in deputados_to_load:
            try:
                db.merge(deputado)  # Updates if exists, inserts if new
                num_loaded += 1
            except Exception as e:
                logger.error(f"Error merging deputado {deputado.id}: {e}")
        
        # Commit all changes
        db.commit()
        
        logger.info(f"Successfully loaded {num_loaded} deputados")
        return num_loaded
    
    except Exception as e:
        db.rollback()
        logger.error(f"Error loading deputados: {e}")
        return 0
    
    finally:
        db.close()
        engine.dispose()

@task(name="Load Votacoes", retries=3, retry_delay_seconds=30)
def load_votacoes(df_votacoes_clean: Optional[pd.DataFrame] = None) -> int:
    """
    Load voting sessions data into the database.
    
    Args:
        df_votacoes_clean: Processed DataFrame with voting sessions data
        
    Returns:
        Number of voting sessions loaded
    """
    if df_votacoes_clean is None:
        df_votacoes_clean = load_dataframe("votacoes", processed=True)
    
    if df_votacoes_clean is None or df_votacoes_clean.empty:
        logger.warning("No processed votacoes data available for loading")
        return 0
    
    logger.info(f"Loading {len(df_votacoes_clean)} votacoes into database")
    
    # Get database session
    try:
        db, engine = get_db_session()
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return 0
    
    num_new = 0
    num_updated = 0
    
    try:
        # First, determine which voting sessions are new (not in the database)
        existing_ids_query = text("SELECT id FROM votacoes")
        try:
            existing_ids = {row[0] for row in db.execute(existing_ids_query)}
        except SQLAlchemyError as e:
            logger.error(f"Error querying existing votacoes: {e}")
            existing_ids = set()
        
        # Get the list of all IDs from the DataFrame - ensure they're strings
        all_ids = set(df_votacoes_clean['id'].astype(str).unique())
        
        # Determine new and existing voting sessions
        new_ids = all_ids - existing_ids
        existing_ids_to_update = all_ids & existing_ids
        
        logger.info(f"Found {len(new_ids)} new votacoes and {len(existing_ids_to_update)} existing votacoes to update")
        
        # Process for insertion/update
        votacoes_to_add = []
        
        # Add new voting sessions
        for _, row in df_votacoes_clean[df_votacoes_clean['id'].astype(str).isin(new_ids)].iterrows():
            try:
                row_dict = row.to_dict()
                # Ensure id is a string
                row_dict['id'] = str(row_dict['id'])
                votacao = transform_votacao(row_dict)
                votacoes_to_add.append(votacao)
            except Exception as e:
                logger.error(f"Error processing votacao {row.get('id', 'unknown')}: {e}")
        
        # Update existing voting sessions
        for _, row in df_votacoes_clean[df_votacoes_clean['id'].astype(str).isin(existing_ids_to_update)].iterrows():
            try:
                row_dict = row.to_dict()
                # Ensure id is a string
                row_id = str(row_dict['id'])
                
                # Get the existing voting session from the database
                existing_votacao = db.query(Votacao).filter(Votacao.id == row_id).first()
                
                if existing_votacao:
                    # Update fields
                    existing_votacao.uri = row_dict.get('uri', existing_votacao.uri)
                    existing_votacao.data = row_dict.get('data', existing_votacao.data)
                    existing_votacao.data_hora_registro = row_dict.get('dataHoraRegistro', existing_votacao.data_hora_registro)
                    existing_votacao.sigla_orgao = row_dict.get('siglaOrgao', existing_votacao.sigla_orgao)
                    existing_votacao.uri_orgao = row_dict.get('uriOrgao', existing_votacao.uri_orgao)
                    existing_votacao.proposicao_objeto = row_dict.get('proposicaoObjeto', existing_votacao.proposicao_objeto)
                    existing_votacao.tipo_votacao = row_dict.get('tipoVotacao', existing_votacao.tipo_votacao)
                    existing_votacao.ultima_apresentacao_proposicao = row_dict.get('ultimaApresentacaoProposicao', existing_votacao.ultima_apresentacao_proposicao)
                    existing_votacao.aprovacao = row_dict.get('aprovacao', existing_votacao.aprovacao)
                    num_updated += 1
            except Exception as e:
                logger.error(f"Error updating votacao {row.get('id', 'unknown')}: {e}")
        
        # Commit new voting sessions to database
        try:
            num_new = commit_to_db(db, votacoes_to_add, "votacoes (new)")
            db.commit()  # Commit updates
        except IntegrityError as e:
            db.rollback()
            logger.error(f"Integrity error loading votacoes: {e}")
            # Try inserting one by one to salvage what we can
            num_new = 0
            for votacao in votacoes_to_add:
                try:
                    db.add(votacao)
                    db.commit()
                    num_new += 1
                except IntegrityError:
                    db.rollback()
        except Exception as e:
            db.rollback()
            logger.error(f"Error committing votacoes: {e}")
        
        return num_new + num_updated
    
    except Exception as e:
        db.rollback()
        logger.error(f"Error loading votacoes: {e}")
        return 0
    finally:
        db.close()
        engine.dispose()

@task(name="Load Votos", retries=3, retry_delay_seconds=30)
def load_votos(df_votos_clean: Optional[pd.DataFrame] = None) -> int:
    """
    Load votes data into the database.
    
    Args:
        df_votos_clean: Processed DataFrame with votes data
        
    Returns:
        Number of votes loaded
    """
    if df_votos_clean is None:
        df_votos_clean = load_dataframe("votos", processed=True)
    
    if df_votos_clean is None or df_votos_clean.empty:
        logger.warning("No processed votos data available for loading")
        return 0
    
    logger.info(f"Loading {len(df_votos_clean)} votos into database")
    
    # Get database session
    try:
        db, engine = get_db_session()
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return 0
    
    try:
        # Check if we have the necessary foreign keys in the database
        votacao_ids = set(df_votos_clean['idVotacao'].astype(str).unique())
        
        # Filter out rows with missing or invalid deputado IDs
        df_votos_clean = df_votos_clean.dropna(subset=['deputadoId'])
        try:
            df_votos_clean['deputadoId'] = df_votos_clean['deputadoId'].astype(int)
        except (ValueError, TypeError) as e:
            logger.error(f"Error converting deputadoId to int: {e}")
            # Try to salvage valid IDs
            valid_ids = []
            for idx, row in df_votos_clean.iterrows():
                try:
                    row['deputadoId'] = int(row['deputadoId'])
                    valid_ids.append(idx)
                except (ValueError, TypeError):
                    pass
            df_votos_clean = df_votos_clean.loc[valid_ids]
        
        if df_votos_clean.empty:
            logger.warning("No valid votos data after filtering for valid deputado IDs")
            return 0
        
        deputado_ids = set(df_votos_clean['deputadoId'].astype(int).unique())
        
        # Verify votacao_ids exist in database
        db_votacao_ids_query = text("SELECT id FROM votacoes")
        try:
            db_votacao_ids = {str(row[0]) for row in db.execute(db_votacao_ids_query)}
        except SQLAlchemyError as e:
            logger.error(f"Error querying votacao IDs: {e}")
            db_votacao_ids = set()
        
        missing_votacao_ids = votacao_ids - db_votacao_ids
        
        # Verify deputado_ids exist in database
        db_deputado_ids_query = text("SELECT id FROM deputados")
        try:
            db_deputado_ids = {row[0] for row in db.execute(db_deputado_ids_query)}
        except SQLAlchemyError as e:
            logger.error(f"Error querying deputado IDs: {e}")
            db_deputado_ids = set()
        
        missing_deputado_ids = deputado_ids - db_deputado_ids
        
        if missing_votacao_ids:
            logger.warning(f"Found {len(missing_votacao_ids)} votacao IDs not in the database")
        
        if missing_deputado_ids:
            logger.warning(f"Found {len(missing_deputado_ids)} deputado IDs not in the database")
        
        # Filter out votes with missing foreign keys
        valid_df = df_votos_clean[
            (df_votos_clean['idVotacao'].astype(str).isin(db_votacao_ids)) & 
            (df_votos_clean['deputadoId'].astype(int).isin(db_deputado_ids))
        ]
        
        if valid_df.empty:
            logger.warning("No valid votos to load after filtering for existing foreign keys")
            return 0
        
        logger.info(f"Loading {len(valid_df)} valid votos after filtering")
        
        # Find existing votes to avoid duplicates
        # Create a composite key of votacao_id and deputado_id for comparison
        valid_df['composite_key'] = valid_df.apply(
            lambda row: f"{row['idVotacao']}_{row['deputadoId']}", 
            axis=1
        )
        
        # Query for existing composite keys
        try:
            existing_votos_query = text("""
                SELECT CONCAT(votacao_id, '_', deputado_id) as composite_key
                FROM votos
            """)
            existing_keys = {row[0] for row in db.execute(existing_votos_query)}
        except SQLAlchemyError as e:
            logger.error(f"Error querying existing votos: {e}")
            existing_keys = set()
        
        # Filter for new votes only
        new_df = valid_df[~valid_df['composite_key'].isin(existing_keys)]
        
        logger.info(f"Found {len(new_df)} new votos to add")
        
        # Transform and add new votes
        votos_to_add = []
        for _, row in new_df.iterrows():
            try:
                row_dict = {
                    'votacao_id': row['idVotacao'],
                    'deputado_id': int(row['deputadoId']),
                    'data_registro_voto': row['dataRegistroVoto'],
                    'tipo_voto': row['tipoVoto']
                }
                voto = transform_voto(row_dict, str(row['idVotacao']))
                votos_to_add.append(voto)
            except Exception as e:
                logger.error(f"Error processing voto for votacao {row.get('idVotacao', 'unknown')}, deputado {row.get('deputadoId', 'unknown')}: {e}")
        
        # Commit new votes to database
        try:
            num_added = commit_to_db(db, votos_to_add, "votos")
            return num_added
        except IntegrityError as e:
            db.rollback()
            logger.error(f"Integrity error loading votos: {e}")
            # Try inserting one by one to salvage what we can
            num_added = 0
            for voto in votos_to_add:
                try:
                    db.add(voto)
                    db.commit()
                    num_added += 1
                except IntegrityError:
                    db.rollback()
            return num_added
        except Exception as e:
            db.rollback()
            logger.error(f"Error committing votos: {e}")
            return 0
    
    except Exception as e:
        db.rollback()
        logger.error(f"Error loading votos: {e}")
        return 0
    finally:
        db.close()
        engine.dispose()

@task(name="Load Discursos", retries=3, retry_delay_seconds=30)
def load_discursos(df_discursos_clean: Optional[pd.DataFrame] = None) -> int:
    """
    Load speeches data into the database.

    Args:
        df_discursos_clean: Processed DataFrame with speeches data
    Returns:
        Number of speeches loaded
    """

    if df_discursos_clean is None:
        df_discursos_clean = load_dataframe("discursos", processed=True)

    if df_discursos_clean is None or df_discursos_clean.empty:
        logger.warning("No processed discursos data available for loading")
        return 0
    
    logger.info(f"Loading {len(df_discursos_clean)} discursos into database")

    # Get database session
    try:
        db, engine = get_db_session()
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return 0
    
    num_new = 0
    num_updated = 0

    try:
        # First, determine which speeches are new (not in the database)
        existing_ids_query = text("SELECT id FROM discursos")
        try:
            existing_ids = {row[0] for row in db.execute(existing_ids_query)}
        except SQLAlchemyError as e:
            logger.error(f"Error querying existing discursos: {e}")
            existing_ids = set()

        # Get the list of all IDs from the DataFrame
        all_ids = set(df_discursos_clean['id'].unique())

        # Determine new and existing speeches
        new_ids = all_ids - existing_ids
        existing_ids_to_update = all_ids & existing_ids

        logger.info(f"Found {len(new_ids)} new discursos and {len(existing_ids_to_update)} existing discursos to update")

        # Process for insertion/update
        discursos_to_add = []
        # Add new speeches
        for _, row in df_discursos_clean[df_discursos_clean['id'].isin(new_ids)].iterrows():
            try:
                row_dict = row.to_dict()
                # Transform to SQLAlchemy model
                discurso = transform_discurso(row_dict)
                discursos_to_add.append(discurso)
            except Exception as e:
                logger.error(f"Error processing discurso {row.get('id', 'unknown')}: {e}")

        # Update existing speeches
        for _, row in df_discursos_clean[df_discursos_clean['id'].isin(existing_ids_to_update)].iterrows():
            try:
                row_dict = row.to_dict()
                # Get the existing speech from the database
                existing_discurso = db.query(Discurso).filter(Discurso.id == row_dict['id']).first()

                if existing_discurso:
                    # Update fields
                    existing_discurso.uri = row_dict.get('uri', existing_discurso.uri)
                    existing_discurso.data = row_dict.get('data', existing_discurso.data)
                    existing_discurso.id_deputado = row_dict.get('idDeputado', existing_discurso.id_deputado)
                    existing_discurso.id_votacao = row_dict.get('idVotacao', existing_discurso.id_votacao)
                    num_updated += 1
            except Exception as e:
                logger.error(f"Error updating discurso {row.get('id', 'unknown')}: {e}")
        # Commit new speeches to database
        try:
            num_new = commit_to_db(db, discursos_to_add, "discursos (new)")
            db.commit()  # Commit updates

        except IntegrityError as e:
            db.rollback()
            logger.error(f"Integrity error loading discursos: {e}")
            # Try inserting one by one to salvage what we can
            num_new = 0
            for discurso in discursos_to_add:
                try:
                    db.add(discurso)
                    db.commit()
                    num_new += 1
                except IntegrityError:
                    db.rollback()
        except Exception as e:
            db.rollback()
            logger.error(f"Error committing discursos: {e}")

        return num_new + num_updated

    except Exception as e:
        db.rollback()
        logger.error(f"Error loading discursos: {e}")
        return 0
    finally:
        db.close()
        engine.dispose()




