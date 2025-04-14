"""
Scheduler for ETL tasks using APScheduler instead of cron.
"""
import logging
import os
import subprocess
import sys
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/app/logs/scheduler.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("scheduler")

def run_etl_task(mode, entity, log_file):
    """Run the ETL task with the specified parameters."""
    cmd = f"cd /app && python -m app.ingestion.cron_etl --mode {mode} --entity {entity}"
    
    logger.info(f"Running ETL task: {cmd}")
    
    # Ensure the log directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    with open(log_file, 'a') as log:
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=log,
            stderr=subprocess.STDOUT
        )
    
    logger.info(f"ETL task started with PID {process.pid}")
    return process

def configure_scheduler():
    """Configure and start the scheduler with the required jobs."""
    scheduler = BackgroundScheduler()
    
    # Incremental ETL for all entities every hour
    scheduler.add_job(
        run_etl_task,
        CronTrigger(minute=0),  # Every hour at minute 0
        args=["incremental", "all", "/app/etl_cron_hourly.log"],
        id="hourly_incremental",
        name="Hourly Incremental ETL"
    )
    
    # Full ETL for deputados at 2:00 AM daily
    scheduler.add_job(
        run_etl_task,
        CronTrigger(hour=2, minute=0),  # Every day at 2:00 AM
        args=["full", "deputados", "/app/etl_cron_deputados_full.log"],
        id="daily_deputados",
        name="Daily Full Deputados ETL"
    )
    
    # Full ETL for all entities at 3:00 AM on Sundays
    scheduler.add_job(
        run_etl_task,
        CronTrigger(day_of_week=0, hour=3, minute=0),  # Every Sunday at 3:00 AM
        args=["full", "all", "/app/etl_cron_full.log"],
        id="weekly_full",
        name="Weekly Full ETL"
    )
    
    logger.info("Starting scheduler...")
    scheduler.start()
    logger.info("Scheduler started.")
    
    return scheduler

if __name__ == "__main__":
    logger.info("Setting up scheduler...")
    scheduler = configure_scheduler()
    
    try:
        # Keep the script running
        while True:
            # Log status every 12 hours for monitoring
            next_run_time = min([job.next_run_time for job in scheduler.get_jobs()])
            logger.info(f"Scheduler running. Next job at: {next_run_time}")
            
            # Sleep for 12 hours
            import time
            time.sleep(43200)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()
        logger.info("Scheduler shut down successfully.")
