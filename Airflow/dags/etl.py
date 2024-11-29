import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


# 1. ETL Pipeline with Airflow

def extract_data():
    # Simulating data extraction from a source
    data = pd.read_parquet('/workspaces/AwanaAcceleratorAI/Module-1/Project/data/raw/green_tripdata_2024-01.parquet')
    return data

def transform_data(data):
    # Perform some transformations
    #data['date'] = pd.to_datetime(data['date'])
    #data['amount'] = data['amount'].fillna(data['amount'].mean())
    return data

def load_data(data):
    # Load data into a target destination
    return data.to_csv('/workspaces/AwanaAcceleratorAI/Module-1/Lab Notebooks/labdata/yellow_tripdata_2024-01.parquet', index=False)

default_args = {
    'owner': 'Braulio Otavalo',
    'start_date': days_ago(0),
    'email': ['test@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
} 


dag = DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for data processing',
    schedule=timedelta(seconds=15),
)

t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

t1 >> t2 >> t3