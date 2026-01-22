from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys

# Add src to path so we can import our modules
# In Airflow, plugins/ or simple path append works. 
sys.path.append(os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "src"))

from ingestion import ingest_to_bronze
from transformations import process_bronze_to_silver, process_silver_to_gold

# Global constants for paths
DATALAKE_ROOT = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "datalake")
BRONZE_PATH = os.path.join(DATALAKE_ROOT, "bronze", "breweries_raw.json")
SILVER_PATH = os.path.join(DATALAKE_ROOT, "silver", "breweries")
GOLD_PATH = os.path.join(DATALAKE_ROOT, "gold", "brewery_summary.parquet")

def run_ingestion(**kwargs):
    ingest_to_bronze(BRONZE_PATH)

def run_silver_transformation(**kwargs):
    process_bronze_to_silver(BRONZE_PATH, SILVER_PATH)

def run_gold_aggregation(**kwargs):
    process_silver_to_gold(SILVER_PATH, GOLD_PATH)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'brewery_pipeline',
    default_args=default_args,
    description='Pipeline to ingest and process brewery data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['brewery', 'medallion'],
) as dag:

    t1_ingest_bronze = PythonOperator(
        task_id='ingest_bronze',
        python_callable=run_ingestion,
    )

    t2_transform_silver = PythonOperator(
        task_id='transform_silver',
        python_callable=run_silver_transformation,
    )

    t3_aggregate_gold = PythonOperator(
        task_id='aggregate_gold',
        python_callable=run_gold_aggregation,
    )

    t1_ingest_bronze >> t2_transform_silver >> t3_aggregate_gold
