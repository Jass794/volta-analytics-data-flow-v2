import os
import sys

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from datetime import datetime

sys.path.append('/dags/sync_analytics/')

from sync_analytics.sync_analytics import sync_analytics_wrapper

load_dotenv(f'{os.getcwd()}/.env')

with DAG(
        dag_id="sync_analytics",
        schedule_interval="*/15 * * * *",  # Schedule every 15 minutes
        default_args={
            "owner": "airflow",
            "retries": 0,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2023, 9, 5)
        },
        catchup=False,
        max_active_runs=1
        ) as f:

    sync_analytics_execute = PythonOperator(
        task_id="sync_analytics",
        python_callable=sync_analytics_wrapper,
        op_args=[os.getenv('SERVER')],
        provide_context=True,
    )
