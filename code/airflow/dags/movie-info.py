from datetime import datetime, timedelta
from airflow import DAG
import requests
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
    'movie_info',
    default_args=default_args,
    description='A simple DAG to scrape information',
    schedule_interval='0 20 * * 1-5',  # 8 PM every weekday
    start_date=datetime(2023, 1, 1),
    catchup=False,
)


start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

wait_for_scrape_info = ExternalTaskSensor(
    task_id='wait_for_scrape_info',
    external_dag_id='scrape_info',
    external_task_id='end',
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    timeout=3600,  # Timeout after 1 hour
    mode='reschedule',
    execution_delta=timedelta(hours=1, minutes=0),
    soft_fail=True,
    dag=dag,
)



raw_data_pull = SSHOperator(
    task_id='raw_data_pull',
    command="""
    cd /app/deal_with_movies
    bash download.sh
""",
    ssh_conn_id='ssh_scrape_sentiment',  # Set SSH connection timeout to 1 hour
    retries=5,
    retry_delay=timedelta(minutes=10),
    sla=timedelta(minutes=30),  # Example SLA of 30 minutes
    on_failure_callback=task_failure_alert,
    on_success_callback=None,
    cmd_timeout=3600 * 60,
    dag=dag,
    get_pty=True,
)

mean_age = SSHOperator(
    task_id='mean_age',
    command="""
    cd /app/deal_with_movies
    bash -c 'python3 main.py  --task "mean_age" --delta-table-path "/app/data/movie-gold" --base-data-path="/app/data/movie-100k-u/ml-100k" '
""",
    ssh_conn_id='ssh_scrape_sentiment',  # Set SSH connection timeout to 1 hour
    retries=5,
    retry_delay=timedelta(minutes=10),
    sla=timedelta(minutes=30),  # Example SLA of 30 minutes
    on_failure_callback=task_failure_alert,
    on_success_callback=None,
    cmd_timeout=3600 * 60,
    dag=dag,
    get_pty=True,
)

top_20_highest_rated_movies = SSHOperator(
    task_id='top_20_highest_rated_movies',
    command="""
    cd /app/deal_with_movies
    bash -c 'python3 main.py  --task "top_20_highest_rated_movies" --delta-table-path "/app/data/movie-gold" --base-data-path="/app/data/movie-100k-u/ml-100k" '
""",
    ssh_conn_id='ssh_scrape_sentiment',  # Set SSH connection timeout to 1 hour
    retries=5,
    retry_delay=timedelta(minutes=10),
    sla=timedelta(minutes=30),  # Example SLA of 30 minutes
    on_failure_callback=task_failure_alert,
    on_success_callback=None,
    cmd_timeout=3600 * 60,
    dag=dag,
    get_pty=True,
)

top_genre_rated_by_user = SSHOperator(
    task_id='top_genre_rated_by_user',
    command="""
    cd /app/deal_with_movies
    bash -c 'python3 main.py  --task "top_genre_rated_by_user" --delta-table-path "/app/data/movie-gold" --base-data-path="/app/data/movie-100k-u/ml-100k" '
    """,
    ssh_conn_id='ssh_scrape_sentiment',  # Set SSH connection timeout to 1 hour
    retries=5,
    retry_delay=timedelta(minutes=10),
    sla=timedelta(minutes=30),  # Example SLA of 30 minutes
    on_failure_callback=task_failure_alert,
    on_success_callback=None,
    cmd_timeout=3600 * 60,
    dag=dag,
    get_pty=True,
)


similar_movies = SSHOperator(
    task_id='similar_movies',
    command="""
    cd /app/deal_with_movies
    bash -c 'python3 main.py  --task "similar_movies" --delta-table-path "/app/data/movie-gold" --base-data-path="/app/data/movie-100k-u/ml-100k" '
""",
    ssh_conn_id='ssh_scrape_sentiment',  # Set SSH connection timeout to 1 hour
    retries=5,
    retry_delay=timedelta(minutes=10),
    sla=timedelta(minutes=30),  # Example SLA of 30 minutes
    on_failure_callback=task_failure_alert,
    on_success_callback=None,
    cmd_timeout=3600 * 60,
    dag=dag,
    get_pty=True,
)



wait_for_scrape_info >> start >> raw_data_pull >> mean_age >> top_20_highest_rated_movies >> top_genre_rated_by_user >> similar_movies >> end