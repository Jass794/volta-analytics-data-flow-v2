import sys
sys.path.append('/dags/reports/trending/')

import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from datetime import datetime
from config.configs import airflow_default_dag_args
from reports.trending.daily_trending_report import run_trending_report

load_dotenv(f'{os.getcwd()}/.env')

with DAG(
        dag_id="trending_report",
        schedule_interval="25 1 * * *",
        default_args=airflow_default_dag_args,
        catchup=False,
        max_active_runs=1
) as f:
    sync_analytics_execute = PythonOperator(
        task_id="trending_report",
        python_callable=run_trending_report,
        op_args=[os.getenv('SERVER')],
        provide_context=True,
    )
