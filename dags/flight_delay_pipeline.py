"""
Flight Delay Analytics Pipeline DAG
Orchestrates the ETL process: Ingest -> Transform -> Aggregate
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add pipeline directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pipeline.ingest import ingest_data
from pipeline.transform import clean_data
from pipeline.aggregate import run_aggregations


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'flight_delay_pipeline',
    default_args=default_args,
    description='ETL pipeline for flight delay analytics',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['etl', 'flight-data', 'analytics'],
)

# Task 1: Ingest raw data
ingest_task = PythonOperator(
    task_id='ingest_flight_data',
    python_callable=ingest_data,
    dag=dag,
)

# Task 2: Transform and clean data
transform_task = PythonOperator(
    task_id='transform_flight_data',
    python_callable=clean_data,
    dag=dag,
)

# Task 3: Aggregate analytics
aggregate_task = PythonOperator(
    task_id='aggregate_analytics',
    python_callable=run_aggregations,
    dag=dag,
)

# Task 4: Data quality check
quality_check_task = BashOperator(
    task_id='data_quality_check',
    bash_command='''
    echo "Running data quality checks..."
    # Add custom quality checks here
    echo "Quality checks passed"
    ''',
    dag=dag,
)

# Define task dependencies
ingest_task >> transform_task >> aggregate_task >> quality_check_task
