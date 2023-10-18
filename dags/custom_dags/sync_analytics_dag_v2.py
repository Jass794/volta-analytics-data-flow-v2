import os
import sys

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from dotenv import load_dotenv
from datetime import datetime

sys.path.append('/dags/sync_analytics/')

load_dotenv(f'{os.getcwd()}/.env')
load_dotenv(f"{os.getcwd()}/.{os.getenv('SERVER')}.env")

# Define default_args and DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sync_analytics',
    default_args=default_args,
    description='Execute sync analytics using bash',
    schedule_interval="*/10 * * * *",
    catchup=False,

)

# BashOperator to execute the Python script using the python command
execute_python_script_task = BashOperator(
    task_id='sync_analytics',
    bash_command=f"cd /opt/airflow/dags/sync_analytics && python sync_analytics_v2.py {os.getenv('SERVER')}",
    env={'PORTAL_ADMIN_API_TOKEN': os.getenv('PORTAL_ADMIN_API_TOKEN'),
         'ANALYTICS_SYNC_PORTAL_API_TOKEN': os.getenv('ANALYTICS_SYNC_PORTAL_API_TOKEN'),
         'GMAIL_APP_PASSWORD': os.getenv('GMAIL_APP_PASSWORD')},
    dag=dag,
)
