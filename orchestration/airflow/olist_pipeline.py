from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

# 👇 CHANGE THIS to your actual path
PROJECT_PATH = "/Users/carlosmacias/Desktop/olist data pipeline/olist-data-pipeline"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

logger = logging.getLogger(__name__)

with DAG(
    dag_id="olist_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["data_pipeline"],
    default_args=default_args,
) as dag:

    logger.info("Initializing olist_pipeline DAG")

    ingest_orders = BashOperator(
        task_id="ingest_orders",
        bash_command=f"""
        set -e
        echo "Starting orders ingestion..."
        cd '{PROJECT_PATH}' &&
        source venv/bin/activate &&
        python ingestion/ingest_data.py orders
        echo "Finished orders ingestion."
        """
    )

    ingest_customers = BashOperator(
        task_id="ingest_customers",
        bash_command=f"""
        set -e
        echo "Starting customers ingestion..."
        cd '{PROJECT_PATH}' &&
        source venv/bin/activate &&
        python ingestion/ingest_data.py customers
        echo "Finished customers ingestion."
        """
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        set -e
        echo "Starting dbt run..."
        cd '{PROJECT_PATH}/dbt' &&
        source ../venv/bin/activate &&
        dbt run
        echo "Finished dbt run."
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        set -e
        echo "Starting dbt test..."
        cd '{PROJECT_PATH}/dbt' &&
        source ../venv/bin/activate &&
        dbt test
        echo "Finished dbt test."
        """
    )

    # 👇 Task order
    ingest_orders >> ingest_customers >> dbt_run >> dbt_test

