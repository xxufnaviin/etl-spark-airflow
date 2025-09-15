# Airflow DAG for weather ETL job
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(".")

from jobs.etl_job import extract, transform

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for weather data collection and processing',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['weather', 'etl', 'spark']
)

def extract_weather_data(**context):
    """Extract weather data for all regions"""
    weather_data = extract("ALL")
    return weather_data

def transform_weather_data(**context):
    """Transform the extracted weather data"""
    # Get data from previous task
    ti = context['ti']
    weather_data = ti.xcom_pull(task_ids='extract_task')
    
    if weather_data:
        transform(weather_data)
    else:
        raise ValueError("No weather data received from extract task")

# Define tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_weather_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_weather_data,
    dag=dag
)

# Set task dependencies
extract_task >> transform_task