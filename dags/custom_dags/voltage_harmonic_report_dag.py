import sys

sys.path.append('/dags/reports/')

import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from dotenv import load_dotenv
from reports.hat.daily_hat_report_v2 import generate_hat_report

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
        dag_id="voltage_hat_report",
        schedule_interval="35 4 * * *",
        default_args=airflow_default_dag_args,
        catchup=False,
        max_active_runs=1
) as f:
    voltage_hat_scan_execute = BashOperator(
        task_id='current_hat_scan',
        bash_command=f"cd /opt/airflow/dags && python -m reports.hat.hat_scan -t Voltage {os.getenv('SERVER')}",
        dag=f,
        execution_timeout=timedelta(hours=1),
    )

    voltage_hat_report_execute = PythonOperator(
        task_id="voltage_hat_report",
        python_callable=generate_hat_report,
        op_args=['Voltage', os.getenv('SERVER')],
        execution_timeout=timedelta(hours=1),
        provide_context=True,
    )

    voltage_hat_scan_execute >> voltage_hat_report_execute

