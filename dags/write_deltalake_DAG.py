from airflow.sdk import dag, task
from datetime import datetime,timedelta
from airflow.sensors.python import PythonSensor
from airflow.sensors.time_delta import TimeDeltaSensor

@dag(
    dag_id='write_deltalake',
    start_date=datetime(2026, 3, 30),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=['infrastructure', 'dataset','persistent landing','deltalake']
)
def write_deltalake_dag():
    @task.branch
    def check_trigger_source(**context):
        """
        Check if the DAG was triggered by the Ingestion DAG or manually.
        If auto-triggered, go to the 3-minute buffer.
        If manually triggered, skip the buffer.
        """
        conf = context.get('dag_run').conf or {}

        # If the 'auto_ingest' flag is found, route to the wait sensor
        if conf and conf.get('trigger_source') == 'temporal_to_persistent_flow':
            return 'buffer_wait_3_mins'

        # Otherwise (Manual Trigger), go straight to the deltalake task
        return ['structured_to_deltalake_task', 'write_image_catalog_task']
    # 3-minute "Regret Window" for automated runs
    # mode='reschedule' ensures we don't waste worker slots while waiting
    buffer_wait = TimeDeltaSensor(
        task_id='buffer_wait_3_mins',
        delta=timedelta(minutes=3),
        mode='reschedule',
    )

    @task(trigger_rule='none_failed_min_one_success')
    def structured_to_deltalake_task():
        from src.deltalake.csv_deltalake_loader import CSVDeltalakeLoader
        from src.utils import get_logger
        from src import minio_client, duckdb_client
        logger = get_logger(__name__)

        logger.info("Starting structured data migration task.")
        logger.info("Source: landing-zone | Destination Prefix: persistent-landing/structured/raw/")

        loader = CSVDeltalakeLoader(minio_client, duckdb_client)

        loader.load_and_transform("landing-zone", "persistent-landing/structured/raw/")
        logger.info("Structured data migration completed successfully.")

    @task(trigger_rule='none_failed_min_one_success')
    def write_image_catalog_task(**kwargs):
        from src.deltalake.catalog_builder import CatalogBuilder
        from src.utils import get_logger
        from src import minio_client, duckdb_client
        logger = get_logger(__name__)
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

    branch_op = check_trigger_source()
    t1 = structured_to_deltalake_task()
    t2 = write_image_catalog_task()

    branch_op >> buffer_wait >> [t1, t2]
    branch_op >> [t1, t2]


write_deltalake_dag()