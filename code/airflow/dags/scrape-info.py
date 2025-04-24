from datetime import datetime, timedelta
from airflow import DAG
import requests
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.empty import EmptyOperator

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


start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

# Define the SSHOperator task to execute a command in the scrape_sentiment container
scrap_finshots = SSHOperator(
    task_id='scrape_finshots',
    command="""bash -c 'python3 /app/scrape_sentiments/main.py --date "{{ macros.ds_add(ds, -3) }}" --delta-table-path="/app/data/sentiment-info" --website-to-scrape="finshots"' """,
    ssh_conn_id='ssh_scrape_sentiment',  # Set SSH connection timeout to 1 hour
    retries=5,
    retry_delay=timedelta(minutes=10),
    sla=timedelta(minutes=30),  # Example SLA of 30 minutes
    on_failure_callback=task_failure_alert,
    on_success_callback=None,
    cmd_timeout = 3600*60,
    dag=dag,
    get_pty=True,  # Use pseudo-terminal for SSH command execution
)


scrap_yourstory = SSHOperator(
    task_id='scrape_yourstory',
    command="""bash -c 'python3 /app/scrape_sentiments/main.py --date "{{ macros.ds_add(ds, -3) }}" --delta-table-path="/app/data/sentiment-info" --website-to-scrape="yourstory"' """,
    ssh_conn_id='ssh_scrape_sentiment',  # Set SSH connection timeout to 1 hour
    retries=5,
    retry_delay=timedelta(minutes=10),
    sla=timedelta(minutes=30),  # Example SLA of 30 minutes
    on_failure_callback=task_failure_alert,
    on_success_callback=None,
    cmd_timeout = 3600*60,
    dag=dag,
    get_pty=True,
)

# Set task dependencies (if any)
start >> scrap_finshots >> scrap_yourstory >> end  # Set the task to run