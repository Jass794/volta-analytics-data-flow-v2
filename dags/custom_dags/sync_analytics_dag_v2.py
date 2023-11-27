import os
import sys

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from dotenv import load_dotenv
from datetime import datetime
from config.configs import airflow_default_dag_args

sys.path.append('/dags/sync_analytics/')

load_dotenv(f'{os.getcwd()}/.env')
load_dotenv(f"{os.getcwd()}/.{os.getenv('SERVER')}.env")


dag = DAG(
    'sync_analytics',
    default_args=airflow_default_dag_args,
    description='Execute sync analytics using bash',
    schedule_interval="*/18 * * * *",
    max_active_runs=1,
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
