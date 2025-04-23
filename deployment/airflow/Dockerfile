# Use the latest Apache Airflow image with Python 3.10
FROM apache/airflow:2.10.5

# Set the working directory
WORKDIR /opt/airflow

# Install additional dependencies if needed
RUN pip install --no-cache-dir apache-airflow-providers-postgres apache-airflow-providers-ssh
