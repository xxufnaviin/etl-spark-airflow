# Airflow DAG for weather ETL job
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from jobs.etl_job import extract, transform, load


default_args = {
    'owner': 'naviin raj',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

REGION = "ALL"
BUCKET_NAME = "etl-spark-airflow" # replace with your own bucket name


dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for weather data collection and processing',
    schedule_interval='0 7 * * *',  # Run everyday at 7am
    catchup=False,
    tags=['weather', 'etl', 'spark']
)

def extract_weather_data(**context):
    weather_data = extract(REGION)
    return weather_data

def transform_weather_data(**context):
    ti = context['ti']
    weather_data = ti.xcom_pull(task_ids='extract_task')

    if weather_data:
        weather_data_csv = transform(weather_data)
    else:
        raise ValueError("No weather data received from extract task")
    return weather_data_csv 

def load_weather_data(**context):
    ti = context['ti']
    weather_data_csv = ti.xcom_pull(task_ids='transform_task')

    if weather_data_csv:
        load(BUCKET_NAME, weather_data_csv, REGION)
    else:
        raise ValueError("No dataframe received for loading")



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

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_weather_data,
    dag=dag
)

# Set task dependencies
extract_task >> transform_task >> load_task