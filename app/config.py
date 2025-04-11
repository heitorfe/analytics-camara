import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/camara_analytics")

# API configuration
API_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

# Data storage paths
RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "processed")

# Create directories if they don't exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# Default date range for incremental loads (yesterday)
DEFAULT_INCREMENTAL_DAYS = 1
YESTERDAY = (datetime.now() - timedelta(days=DEFAULT_INCREMENTAL_DAYS)).strftime("%Y-%m-%d")
TODAY = datetime.now().strftime("%Y-%m-%d")

# Logging configuration
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "etl.log")