import sys
sys.path.append('/dags/reports/')

import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from datetime import datetime

from reports.events.events_report import process_events_report

load_dotenv(f'{os.getcwd()}/.env')

with DAG(
        dag_id="events_report",
        schedule_interval="35 3 * * *",
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
        task_id="events_report",
        python_callable=process_events_report,
        op_args=[os.getenv('SERVER')],
        provide_context=True,
    )
