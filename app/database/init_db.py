import os
import logging
from sqlalchemy.orm import Session
from .database import engine, SessionLocal
from .models import Base
from app.ingestion.extract import extract_deputados, extract_deputados_details, extract_votacoes, extract_votos
from app.ingestion.transform import transform_deputados, transform_deputados_details, transform_votacoes, transform_votos
from app.ingestion.load import load_deputados, load_votacoes, load_votos

logger = logging.getLogger(__name__)

def init_db():
    """
    Initialize the database by creating all tables and loading initial data.
    """
    # Create tables
    logger.info("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Tables created successfully.")
    
    # Ask user if they want to load initial data
    response = input("Do you want to load initial data from the API? (y/n): ")
    
    if response.lower() == 'y':
        logger.info("Loading initial data...")
        
        # Get a database session
        db = SessionLocal()
        
        try:
            # Extract data
            df_deputados = extract_deputados.fn(mode="full")
            df_details = extract_deputados_details.fn(df_deputados=df_deputados)
            df_votacoes = extract_votacoes.fn(mode="full", data_inicio="2023-01-01")
            df_votos = extract_votos.fn(df_votacoes=df_votacoes, mode="full")
            
            # Transform data
            df_deputados_clean = transform_deputados.fn(df_deputados=df_deputados)
            df_details_clean = transform_deputados_details.fn(df_detalhes=df_details)
            df_votacoes_clean = transform_votacoes.fn(df_votacoes=df_votacoes)
            df_votos_clean = transform_votos.fn(df_votos=df_votos)
            
            # Load data
            deputados_loaded = load_deputados.fn(df_deputados_clean=df_deputados_clean, df_details_clean=df_details_clean)
            votacoes_loaded = load_votacoes.fn(df_votacoes_clean=df_votacoes_clean)
            votos_loaded = load_votos.fn(df_votos_clean=df_votos_clean)
            
            logger.info(f"Loaded {deputados_loaded} deputados, {votacoes_loaded} votacoes, and {votos_loaded} votos.")
            
            print(f"\nInitial data loaded successfully:")
            print(f"- {deputados_loaded} deputados")
            print(f"- {votacoes_loaded} votacoes")
            print(f"- {votos_loaded} votos")
            
        except Exception as e:
            logger.error(f"Error loading initial data: {e}")
            db.rollback()
            raise
        finally:
            db.close()
    else:
        logger.info("Skipping initial data loading.")
        
    return True

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()