import argparse
import logging
from app.database.init_db import init_db

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Initialize database and run application')
    parser.add_argument('--init-db', action='store_true', help='Initialize and populate the database')
    
    args = parser.parse_args()
    
    if args.init_db:
        logger.info("Initializing database...")
        init_db()
        logger.info("Database initialization complete")
    else:
        logger.info("Skipping database initialization")
        logger.info("To run the Streamlit app, use: 'streamlit run app/dashboard/app.py'")

if __name__ == "__main__":
    main()
