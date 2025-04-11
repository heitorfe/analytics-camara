import logging
import os
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session
from pathlib import Path

from app.config import LOG_FILE, RAW_DATA_DIR, PROCESSED_DATA_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def save_dataframe(df: pd.DataFrame, name: str, processed: bool = False) -> str:
    """
    Save a DataFrame to parquet file.
    
    Args:
        df: DataFrame to save
        name: Name of the file (without extension)
        processed: Whether to save to processed directory
        
    Returns:
        Path to the saved file
    """
    if df is None or df.empty:
        logger.warning(f"DataFrame {name} is empty, not saving.")
        return None
    
    directory = PROCESSED_DATA_DIR if processed else RAW_DATA_DIR
    os.makedirs(directory, exist_ok=True)
    
    file_path = os.path.join(directory, f"{name}.parquet")
    df.to_parquet(file_path, index=False)
    logger.info(f"Saved {name} to {file_path}")
    return file_path

def load_dataframe(name: str, processed: bool = False) -> pd.DataFrame:
    """
    Load a DataFrame from parquet file.
    
    Args:
        name: Name of the file (without extension)
        processed: Whether to load from processed directory
        
    Returns:
        Loaded DataFrame or None if file doesn't exist
    """
    directory = PROCESSED_DATA_DIR if processed else RAW_DATA_DIR
    file_path = os.path.join(directory, f"{name}.parquet")
    
    if not os.path.exists(file_path):
        logger.warning(f"File {file_path} does not exist.")
        return None
    
    df = pd.read_parquet(file_path)
    logger.info(f"Loaded {name} from {file_path}")
    return df

def get_last_update_date(entity_name: str) -> str:
    """
    Get the last update date for an entity.
    
    Args:
        entity_name: Name of the entity
        
    Returns:
        Date string in format YYYY-MM-DD or None if no previous data
    """
    log_file = os.path.join(RAW_DATA_DIR, f"{entity_name}_last_update.txt")
    
    if not os.path.exists(log_file):
        return None
    
    with open(log_file, "r") as f:
        date_str = f.read().strip()
    
    logger.info(f"Last update date for {entity_name}: {date_str}")
    return date_str

def update_last_update_date(entity_name: str, date_str: str = None) -> None:
    """
    Update the last update date for an entity.
    
    Args:
        entity_name: Name of the entity
        date_str: Date string in format YYYY-MM-DD, defaults to today
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    log_file = os.path.join(RAW_DATA_DIR, f"{entity_name}_last_update.txt")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    with open(log_file, "w") as f:
        f.write(date_str)
    
    logger.info(f"Updated last update date for {entity_name}: {date_str}")

def create_directory_if_not_exists(dir_path: str) -> None:
    """
    Create a directory if it doesn't exist.
    
    Args:
        dir_path: Path to the directory
    """
    Path(dir_path).mkdir(parents=True, exist_ok=True)
    logger.debug(f"Ensured directory exists: {dir_path}")

def commit_to_db(db: Session, objects, entity_name: str) -> int:
    """
    Commit objects to database.
    
    Args:
        db: Database session
        objects: List of objects to commit
        entity_name: Name of the entity for logging
    
    Returns:
        Number of objects committed
    """
    if not objects:
        logger.warning(f"No {entity_name} to commit.")
        return 0
    
    try:
        db.add_all(objects)
        db.commit()
        logger.info(f"Committed {len(objects)} {entity_name} to database.")
        return len(objects)
    except Exception as e:
        db.rollback()
        logger.error(f"Error committing {entity_name} to database: {e}")
        raise