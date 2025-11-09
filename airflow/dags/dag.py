# Airflow DAG for weather ETL job
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    schedule_interval='0 7 * * *',  # Run everyday at 7am
    catchup=False,
    tags=['weather', 'etl', 'spark']
)

def extract_weather_data(**context):
    weather_data = extract("ALL")
    return weather_data

def transform_weather_data(**context):
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