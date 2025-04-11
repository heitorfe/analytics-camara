from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração da URL do banco de dados
SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost/camara_analytics")

# Criar engine do SQLAlchemy
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# Factory de sessão
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Classe base para modelos ORM
Base = declarative_base()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
