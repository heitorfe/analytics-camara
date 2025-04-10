from .database import engine, Base
from .models import Deputado, Despesa, Discurso, Votacao, Voto

def create_tables():
    """Create all database tables if they don't exist."""
    Base.metadata.create_all(bind=engine)
    print("Database tables created successfully.")

if __name__ == "__main__":
    create_tables()
