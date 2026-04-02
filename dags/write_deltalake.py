from airflow.sdk import dag, task
from datetime import datetime
from airflow.sensors.python import PythonSensor

from src.deltalake.catalog_builder import CatalogBuilder
from src.deltalake.csv_deltalake_loader import CSVDeltalakeLoader
from src.utils import get_logger
from src import minio_client,duckdb_client
logger = get_logger(__name__)

@dag(
    dag_id='write_deltalake',
    start_date=datetime(2026, 3, 30),
    schedule='@once',
    catchup=False,
    tags=['infrastructure', 'dataset','persistent landing','deltalake']
)
def write_deltalake_dag():
    @task
    def structured_to_deltalake_task():
        logger.info("Starting structured data migration task.")
        logger.info("Source: landing-zone | Destination Prefix: persistent-landing/structured/raw/")

        loader = CSVDeltalakeLoader(minio_client, duckdb_client)

        loader.load_and_transform("landing-zone", "persistent-landing/structured/raw/")
        logger.info("Structured data migration completed successfully.")
    structured_to_deltalake_task()

    @task
    def write_image_catalog_task(**kwargs):
        logger.info("Initializing CatalogBuilder for Image Sync.")

        try:
            builder = CatalogBuilder(minio_client, duckdb_client)

            # Log the specific parameters for traceability
            target = "landing-zone/persistent-landing/structured/file_catalog/"

            logger.info(f"Target Delta Table: {target}")

            # Execution
            builder.run_image_ingestion("landing-zone", "persistent-landing/unstructured/image/", target)

            logger.info("Image catalog sync completed successfully.")
        except Exception as e:
            logger.error(f"Catalog Task Failed: {str(e)}", exc_info=True)
            raise  # Ensure Airflow marks the task as failed
    write_image_catalog_task()
write_deltalake_dag()