from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from airflow.providers.ssh.operators.ssh import SSHOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Python function for SLA and failure alerts
def sla_alert(context):
    print("SLA has been missed!")
    requests.post("http://mock-api/notify", json={"message": "SLA missed for task: {}".format(context['task_instance_key_str'])})

def task_failure_alert(context):
    print("Task has failed!")
    requests.post("http://mock-api/notify", json={"message": "Task failed: {}".format(context['task_instance_key_str'])})

# Update default_args to include SLA and failure callbacks
default_args.update({
    'on_failure_callback': task_failure_alert,
})

# Define the DAG
dag = DAG(
    'scrape_info',
    default_args=default_args,
    description='A simple DAG to scrape information',
    schedule_interval='0 19 * * 1-5',  # 7 PM every weekday
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the Python function to be executed
def scrape_info():
    print("Scraping information...")

# Define the task with SLA and failure callback
scrape_task = PythonOperator(
    task_id='scrape_task',
    python_callable=scrape_info,
    dag=dag,
    sla=timedelta(minutes=10),
    on_failure_callback=task_failure_alert,
)

# Define the SSHOperator task to execute a command in the scrape_sentiment container
exec_command_task = SSHOperator(
    task_id='exec_command_in_scrape_sentiment',
    ssh_conn_id='docker_ssh',
    command='docker exec scrape_sentiment python main.py --date "{{ ds }}" --delta-table-path="/app/data/sentiment-info" --website-to-scrape="yourstory"',
    dag=dag,
)

# Set task dependencies (if any)
# In this case, there's only one task