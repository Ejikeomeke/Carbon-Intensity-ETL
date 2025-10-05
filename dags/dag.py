from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from etl import extract_data, transform_data, connectDB, create_tables, load_data_db, load_data_csv


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}