from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.utils import (
    minio_config,
    get_logger,
)

from src.dataloader import (
    MinioClient
)
log = get_logger(__name__)

def check_infrastructure_integrity(**context):
    """
    Wrapper function for Airflow PythonOperator.
    """

    # 1. Create client
    storage = MinioClient()

    # 2. Setup initial bucket from config
    storage.ensure_bucket_exists()

    return "Infrastructure Ready"

# DAG Definition
with DAG(
    dag_id='infra_daily_integrity_check',
    # Set to a fixed past date to allow immediate scheduling
    start_date=datetime(2026, 3, 23),
    # Runs once every 24 hours at midnight (00:00)
    schedule_interval='@daily',
    # Skip missed runs between start_date and current time
    catchup=False,
    # Default settings applied to all tasks within this DAG
    default_args={
        'owner': 'data_eng',
        'retries': 3,
        'retry_delay': timedelta(minutes=10),
    },
    # Metadata for filtering in the Airflow UI
    tags=['infrastructure', 'maintenance', 'bronze_layer']
) as dag:

    # Task: Execute the integrity check via PythonOperator
    maintenance_task = PythonOperator(
        task_id='verify_minio_buckets',
        python_callable=check_infrastructure_integrity,
        # Provide the Airflow context (logical_date, etc.) to the function
        provide_context=True
    )