import sys

sys.path.append('/dags/reports/')

import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from datetime import datetime
from config.configs import airflow_default_dag_args


from reports.hat.daily_hat_scan_v2 import run_hat_scan
from reports.hat.daily_hat_report_v2 import generate_hat_report

load_dotenv(f'{os.getcwd()}/.env')

with DAG(
        dag_id="current_hat_report",
        schedule_interval="50 3 * * *",
        default_args=airflow_default_dag_args,
        catchup=False,
        max_active_runs=1
) as f:

    # Run the example script
    current_hat_scan_execute = BashOperator(
        task_id='current_hat_scan',
        bash_command=f"cd /opt/airflow/dags && python -m reports.hat.hat_scan -t Current {os.getenv('SERVER')}",
        dag=f,
        execution_timeout=timedelta(hours=1),
    )



    current_hat_report_execute = PythonOperator(
        task_id="current_hat_report",
        python_callable=generate_hat_report,
        op_args=['Current', os.getenv('SERVER')],
        execution_timeout=timedelta(hours=1),
        provide_context=True,
    )

    current_hat_scan_execute >> current_hat_report_execute

