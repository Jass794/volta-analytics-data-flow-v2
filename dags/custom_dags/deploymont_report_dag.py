import sys
sys.path.append('/dags/reports/')

import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv


from reports.deployment.daily_deployment_report import run_deployment_report

load_dotenv(f'{os.getcwd()}/.env')

# Define the default_args dictionary
airflow_default_dag_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),  # Update with your desired start date
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': 'analytics-data-flow-errors@voltainsite.com'
}

with DAG(
        dag_id="deployment_report",
        schedule_interval="25 2 * * *",
        default_args=airflow_default_dag_args,
        catchup=False,
        max_active_runs=1
) as f:
    deployment_execute = PythonOperator(
        task_id="deployment_report",
        python_callable=run_deployment_report,
        op_args=[os.getenv('SERVER')],
        provide_context=True,
        execution_timeout=timedelta(hours=3),
    )
