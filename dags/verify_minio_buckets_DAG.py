from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.dataloader import MinioClient
from src.utils import get_logger

# Initialize project-specific logger
logger = get_logger(__name__)


@dag(
    dag_id='infra_daily_integrity_check',
    start_date=datetime(2026, 3, 23),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'data_eng',
        'retries': 3,
        'retry_delay': timedelta(minutes=10),
    },
    tags=['infrastructure', 'maintenance', 'landing_zone']
)
def infrastructure_maintenance():
    """
    Infrastructure Integrity Pipeline.

    This DAG performs daily health checks on the storage layer (MinIO).
    It ensures that connectivity is active and all required buckets
    defined in the configuration are initialized before downstream
    ingestion DAGs begin execution.
    """

    @task(task_id='verify_minio_buckets')
    def check_infrastructure_integrity(**context):
        """
        Validates MinIO connectivity and initializes bucket structures.

        Returns:
            str: Confirmation message used for XCom signaling.
        """

        try:
            logger.info("Initializing MinIO client for integrity check.")

            # 1. Initialize the storage client
            storage = MinioClient()

            # 2. Ensure bucket hierarchy exists as per minio.yaml
            logger.info("Verifying bucket existence and permissions...")
            storage.ensure_bucket_exists()

            logger.info("Infrastructure check completed successfully.")
            return "Infrastructure Ready"
        except Exception as e:
            ti = context['ti']
            ti.xcom_push(key="return_value", value="Infrastructure Check Failed")
            logger.exception(e)
            raise
    # Execute the task
    check_infrastructure_integrity()


# Instantiate the DAG
infrastructure_maintenance()