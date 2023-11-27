from datetime import datetime, timedelta

class NotificationConfigs:
    email_on_failure = 'analytics-data-flow-errors@voltainsite.com'



# Define the default_args dictionary
airflow_default_dag_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),  # Update with your desired start date
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': NotificationConfigs.email_on_failure
}