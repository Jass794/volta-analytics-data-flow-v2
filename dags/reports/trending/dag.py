import os
import sys
from pprint import pprint

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from datetime import datetime

sys.path.append('./dags/reports/')
sys.path.append('./dags/reports/trending/')

from daily_trending_report import run_trending_report

load_dotenv(f'{os.getcwd()}/.env')

with DAG(
        dag_id="trending_report",
        schedule_interval="25 1 * * *",
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
        task_id="trending_report",
        python_callable=run_trending_report,
        op_args=[os.getenv('SERVER')],
        provide_context=True,
    )
