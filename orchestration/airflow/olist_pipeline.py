from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

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
    tags=["data_pipeline", "aws", "snowflake"],
    default_args=default_args,
) as dag:

    logger.info("Initializing olist_pipeline DAG")

    upload_and_load_orders = BashOperator(
        task_id="upload_and_load_orders",
        bash_command=f"""
        set -e
        echo "Starting orders upload to S3 and Snowflake load..."
        cd '{PROJECT_PATH}' &&
        source venv/bin/activate &&
        python ingestion/ingest_data.py orders
        echo "Finished orders upload and load."
        """
    )

    upload_and_load_customers = BashOperator(
        task_id="upload_and_load_customers",
        bash_command=f"""
        set -e
        echo "Starting customers upload to S3 and Snowflake load..."
        cd '{PROJECT_PATH}' &&
        source venv/bin/activate &&
        python ingestion/ingest_data.py customers
        echo "Finished customers upload and load."
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

    [upload_and_load_orders, upload_and_load_customers] >> dbt_run >> dbt_test