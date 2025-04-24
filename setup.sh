#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define constants
AIRFLOW_HOME="./run/airflow"
DAGS_DIR="./code/airflow/dags"
SSH_DIR="./airflow/.ssh"
SPARK_SSH_DIR="./spark/.ssh"

# Step 1: Create necessary directories
mkdir -p "$AIRFLOW_HOME" "$DAGS_DIR" "$SSH_DIR" "$SPARK_SSH_DIR"

# Step 2: Generate SSH key pair without passphrase
ssh-keygen -t rsa -b 4096 -f "$SSH_DIR/id_rsa" -N ""

# Step 3: Set appropriate permissions
chmod 700 "$SSH_DIR"
chmod 600 "$SSH_DIR/id_rsa"

# Step 4: Copy the public key to Spark's authorized_keys
cp "$SSH_DIR/id_rsa.pub" "$SPARK_SSH_DIR/authorized_keys"
mkdir -p ./deployment/python/spark/.ssh/
cp "$SPARK_SSH_DIR/authorized_keys" ./deployment/python/spark/.ssh/

# Step 5: Initialize Airflow database
echo "Initializing Airflow database..."
docker-compose run --rm airflow-webserver airflow db init

# Step 6: Create an Airflow admin user
echo "Creating an Airflow admin user..."
docker-compose run --rm airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# Step 7: Start all containers
echo "Starting all containers..."
docker-compose up -d --build

# Step 8: Wait for Airflow webserver to start
echo "Waiting for Airflow webserver to start..."
sleep 30
curl -f http://localhost:8080 || { echo "Airflow webserver failed to start."; exit 1; }

# Step 9: Add SSH connection to Airflow
echo "Adding SSH connection to Airflow..."
docker-compose run --rm airflow-webserver airflow connections add 'ssh_scrape_sentiment' \
  --conn-type ssh \
  --conn-host spark_container \
  --conn-login spark \
  --conn-port 22 \
  --conn-extra '{"key_file": "/home/airflow/.ssh/id_rsa", "no_host_key_check": true}'

# Step 10: Restart Airflow to load new configurations
echo "Restarting Airflow to load new configurations..."
docker-compose restart airflow-webserver airflow-scheduler

# Final message
echo "Setup complete. Airflow is now scheduling scrape_sentiments."