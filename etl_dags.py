from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from telecom_site_analysis import process_and_store_data  

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
       description='',
       schedule_interval='@daily',  # Runs daily
   )
   # Define the task
analysis_task = PythonOperator(
       task_id='run_analysis',
       python_callable=process_and_store_data,
       dag=dag,
   )
   # Set task dependencies (in this case, only one task)
analysis_task