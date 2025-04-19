#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define constants
AIRFLOW_HOME="./run/code/airflow"
DAGS_DIR="./run/code/airflow/dags"

# Step 1: Create necessary directories
if [ ! -d "$AIRFLOW_HOME" ]; then
  echo "Creating Airflow home directory..."
  mkdir -p "$AIRFLOW_HOME"
fi

if [ ! -d "$DAGS_DIR" ]; then
  echo "Creating DAGs directory..."
  mkdir -p "$DAGS_DIR"
fi

# Step 2: Initialize Airflow database
echo "Initializing Airflow database..."
docker-compose run --rm airflow-webserver airflow db init

# Step 3: Create an Airflow user
echo "Creating an Airflow admin user..."
docker-compose run --rm airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# Step 4: Start all containers
echo "Starting all containers..."
docker-compose up -d

# Step 5: Verify Airflow is running
echo "Waiting for Airflow webserver to start..."
sleep 10
curl -f http://localhost:8080 || { echo "Airflow webserver failed to start."; exit 1; }

# Step 7: Restart Airflow to load new DAGs
echo "Restarting Airflow to load new DAGs..."
docker-compose restart airflow-webserver airflow-scheduler

# Final message
echo "Setup complete. Airflow is now scheduling scrape_sentiments."