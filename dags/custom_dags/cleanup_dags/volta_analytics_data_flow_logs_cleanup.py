import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Define the default_args dictionary
airflow_default_dag_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),  # Update with your desired start date
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['analytics-data-flow-errors@voltainsite.com']
}

# Instantiate a DAG
dag = DAG(
    'volta_analytics_dataflow_logs_cleanup',
    default_args=airflow_default_dag_args,
    description='Analytics Dataflow repo logs clean up',
    schedule_interval='@daily',  # Set the schedule interval as needed
    max_active_runs=1,
    catchup=False)


# Run the example script
logs_clean_up_task = BashOperator(
    task_id='process_standard_alerts',
    bash_command=f"cd /opt/airflow/dags/volta-analytics-data-flow && python main.py manage_logs {os.getenv('SERVER')}",
    dag=dag,
    execution_timeout=timedelta(hours=1),
)

# Set task dependencies
logs_clean_up_task
