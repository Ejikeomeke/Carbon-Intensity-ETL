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


# DAG defined
dag = DAG(
    'carbon_intensity_etl2',
    default_args=default_args,
    description='ETL Pipeline for Carbon Intensity Data',
    schedule ='@daily',  # Run daily
    catchup=False,  # Disable catchup to avoid backfilling
)


def extract(**kwargs):
    start = date(2024, 1, 1)
    BASE_URL = f"https://api.carbonintensity.org.uk/regional/intensity/{start}/pt24h"
    print("Commenced Data Extraction!")
    #data = extract_data(URL=BASE_URL)
    data = read_json()
    kwargs['ti'].xcom_push(key='extracted_data', value=data)  # Push data to XCom
    print("Ended Data Extraction!")
    