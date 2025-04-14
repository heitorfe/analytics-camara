#!/usr/bin/env python
"""
Script to run ETL processes via command line or cron jobs.
"""
import argparse
import logging
from datetime import datetime, timedelta

from app.config import LOG_FILE
from app.ingestion.flow import camara_analytics_etl_flow

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Camara Analytics ETL process")
    parser.add_argument("--mode", type=str, choices=["full", "incremental"], default="incremental",
                        help="ETL mode (full or incremental)")
    parser.add_argument("--entity", type=str, default="all", choices= ["deputados", "votacoes", "votos", "discursos", "all"],
                        help="Entity to process (deputados, votacoes, votos, or all)")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Start date for extraction (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None,
                        help="End date for extraction (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=None,
                        help="Number of days back from today to start extraction")
    
    args = parser.parse_args()
    
    # Handle date calculation if days parameter is provided
    if args.days and not args.start_date:
        start_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
        args.start_date = start_date
        logger.info(f"Calculated start date from --days: {start_date}")
    
    try:
        logger.info(f"Starting ETL process in {args.mode} mode for entity: {args.entity}")
        
        # Run the flow
        results = camara_analytics_etl_flow(
            mode=args.mode,
            entities=[args.entity] if args.entity != "all" else None,
            data_inicio=args.start_date,
            data_fim=args.end_date
        )
        
        # Print summary
        logger.info("\n--- ETL Process Summary ---")
        for entity, stats in results.items():
            logger.info(f"\n{entity.upper()}:")
            for key, value in stats.items():
                logger.info(f"  {key}: {value}")
        
        logger.info("ETL process completed successfully")
    except Exception as e:
        logger.error(f"ETL process failed: {e}", exc_info=True)
        raise