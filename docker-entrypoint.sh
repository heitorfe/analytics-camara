#!/bin/bash
set -e

# Create data directories if they don't exist
mkdir -p /app/data/raw
mkdir -p /app/data/processed
mkdir -p /app/logs

# If DATABASE_URL is not set, use default
if [ -z "$DATABASE_URL" ]; then
  export DATABASE_URL="postgresql://postgres:postgres@db:5432/camara_analytics"
fi

# Set up cron job
echo "Setting up cron jobs..."
crontab /app/crontab
service cron start

# Initialize the database if requested
if [ "$1" = "init-db" ]; then
  echo "Initializing the database..."
  python -m app.database.create_tables
  
  # Run initial ETL in full mode
  echo "Running initial ETL process..."
  python -m app.ingestion.cron_etl --mode full --entity all
elif [ "$1" = "etl" ]; then
  # Run ETL with specified parameters
  ARGS="${@:2}"
  echo "Running ETL process with arguments: $ARGS"
  python -m app.ingestion.cron_etl $ARGS
fi

# Keep container running
echo "Startup completed, keeping container running..."
tail -f /app/logs/etl.log