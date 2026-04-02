from airflow.sdk import dag, task
from datetime import datetime
from airflow.sensors.python import PythonSensor

from src.deltalake.csv_deltalake_loader import CSVDeltalakeLoader
from src.utils import get_logger
from src import minio_client, duckdb_client

logger = get_logger(__name__)


def poke_minio_for_real_data(**kwargs):


    if not minio_client.verify_empty_bucket("landing-zone","temporal-landing/"):
        logger.info("Empty bucket detected.")
        return False
    return True

@dag(
    dag_id='temporal_to_persistent',
    start_date=datetime(2026, 3, 30),
    schedule='@once',
    catchup=False,
    tags=['infrastructure', 'dataset','persistent landing','deltalake']
)
def temporal_to_persistent_dag():
    """
    Main DAG definition for the Temporal to Persistent workflow.
    """

    # Monitors the landing zone for ANY file.
    # Using wildcard '*' allows the pipeline to be triggered by any incoming data.
    wait_for_ingestion = PythonSensor(
        task_id='wait_for_temporal_data',
        python_callable=poke_minio_for_real_data,
        mode='reschedule',
        poke_interval=60,
        timeout=60 * 60,
        # No need to pass dag= here when using @dag decorator
    )

    # This task encapsulates the business logic for sorting files.
    @task
    def move_data_task():
        """
            Executes the move_bucket operation to sort files into
            structured/raw or unstructured/image directories.
        """

        logger.info("Executing bucket migration and classification logic.")

        minio_client.move_bucket(
            source_bucket='landing-zone',
            source_prefix='temporal-landing/',
            destination_bucket='landing-zone',
            destination_prefix='persistent-landing/'
        )

        return "move complete."


    wait_for_ingestion >> move_data_task()

# Instantiate the DAG
temporal_to_persistent_dag()