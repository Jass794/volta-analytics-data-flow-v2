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
    'email': 'analytics-data-flow-errors@voltainsite.com'
}

# Instantiate a DAG
dag = DAG(
    'harmonic_alerts_scan',
    default_args=airflow_default_dag_args,
    description='Create Harmonic Alerts',
    schedule_interval='0 5 * * *',
    max_active_runs=1,
    catchup=False)

# Set the virtual environment path
venv_path = "/opt/airflow/virtual_env/volta-analytics-data-flow_venv"
requirements_path = "/opt/airflow/dags/volta-analytics-data-flow/requirements.txt"

# Define the BashOperators for each step

# Check if the virtual environment exists before creating
check_venv_task = BashOperator(
    task_id='check_venv',
    bash_command=f"if [ -d {venv_path} ]; then echo 'Virtual environment exists'; else echo 'Virtual environment does not exist'; fi",
    dag=dag,
)

# Create virtual environment only if it doesn't exist
create_venv_task = BashOperator(
    task_id='create_venv',
    bash_command=f"if [ ! -d {venv_path} ]; then python3 -m venv --copies {venv_path}; fi",
    dag=dag,
)

# Install dependencies only if requirements.txt is present and not already satisfied
install_deps_task = BashOperator(
    task_id='install_dependencies',
    bash_command=f"if [ -f {requirements_path} ] && ! (source {venv_path}/bin/activate && pip freeze | grep -q -F -x -f {requirements_path}); then source {venv_path}/bin/activate && pip install --upgrade -r {requirements_path}; fi",
    dag=dag,
)

# Run the example script
create_harmonic_alerts = BashOperator(
    task_id='create_harmonic_alerts',
    bash_command=f"source {venv_path}/bin/activate && cd /opt/airflow/dags/volta-analytics-data-flow &&  python main.py current_harmonic_freq_alert {os.getenv('SERVER')}",
    dag=dag,
    execution_timeout=timedelta(hours=1),
)

# Set task dependencies
check_venv_task >> create_venv_task >> install_deps_task >> create_harmonic_alerts
