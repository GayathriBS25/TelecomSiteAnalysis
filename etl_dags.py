from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'telecom_site_analysis',
    default_args=default_args,
    description='A DAG for telecom site analysis',
    schedule_interval='@daily',  # Runs daily
    catchup=False  # Avoid running past dates if the scheduler was off
)

# Delayed import inside the function to avoid long import times during parsing
def run_analysis():
    from telecom_site_analysis import process_and_store_data  
    process_and_store_data()

# Define the task
analysis_task = PythonOperator(
    task_id='run_analysis',
    python_callable=run_analysis,
    dag=dag,
)

# Set task dependencies
analysis_task
