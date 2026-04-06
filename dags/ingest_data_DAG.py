from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from src.utils import kaggle_config,huggingface_config
# Standard providers are still imported this way




@dag(
    dag_id='ingest_dataset',
    start_date=datetime(2026, 3, 30),
    schedule='@once',
    catchup=False,
    tags=['infrastructure', 'dataset'],
    is_paused_upon_creation=False
)
def ingest_dataset_dag():
    """
    Ingestion Pipeline following Airflow 3.0 Task SDK standards.
    """

    @task(retries=60, retry_delay=timedelta(seconds=60))
    def wait_for_infra_ready_task(**context):
        ti = context['ti']

        # Pull all prior XComs from this task
        xcom_values = ti.xcom_pull(
            dag_id='infra_daily_integrity_check',
            task_ids='verify_minio_buckets',
            key='return_value',
            include_prior_dates=True  # fetch all previous XComs
        )

        if not xcom_values:
            raise ValueError("No XComs found yet for infra DAG")

        # Get the last XCom (the latest successful run)
        last_value = xcom_values[-1]

        if last_value == "Infrastructure Ready":
            return True
        else:
            raise ValueError(f"Infrastructure DAG not ready yet, got {last_value}")
    wait_for_infra = wait_for_infra_ready_task()
    # 2. Define the task using the new SDK @task decorator
    @task(map_index_template="{{ 'ingest_' + source_config['name'].replace('-', '_') }}")
    def run_kaggle_ingest(source_config: dict):
        from src.utils import get_logger
        from src.dataloader.kaggle_loader import KaggleLoader
        from src import minio_client
        loader = KaggleLoader(minio_client)
        logger = get_logger(__name__)
        logger.info(f"Processing dataset: {source_config['name']}")

        if source_config.get('type') == "csv":
            loader.fetch_and_upload_csv(
                handle=source_config['handle'],
                file_name=source_config['file'],
                name=source_config['name'],
                **source_config.get('params', {})
            )
        elif source_config.get('type') == "image":
            loader.fetch_and_upload_image(
                handle=source_config['handle'],
                name=source_config['name'],
            )
        else:
            logger.info(f"currently not supported: {source_config['name']}")

    ingest_tasks = []
    # 3. Dynamic Task Generation
    # Ensure this block only runs if kaggle_config is valid
    if kaggle_config:
        # Using .override() is the SDK way to set dynamic task IDs
        ingest_instance_kaggle = run_kaggle_ingest.expand(source_config=kaggle_config)
        ingest_tasks.append(ingest_instance_kaggle)
        # Set explicit dependency
        wait_for_infra >> ingest_instance_kaggle


    @task(map_index_template="{{ 'ingest_' + source_config['name'].replace('-', '_') }}")
    def run_huggingface_ingest(source_config: dict):
        from src.utils import get_logger
        from src.dataloader.huggingface_loader import HuggingfaceDataLoader
        from src import minio_client
        loader = HuggingfaceDataLoader(minio_client)
        logger = get_logger(__name__)
        logger.info(f"Processing dataset: {source_config['name']}")

        if source_config.get('type') == "csv":
            loader.fetch_and_upload(
                path=source_config['path'],
                name=source_config['name'],
                split= source_config['split'],
                file_type=source_config['type'],
            )

        else:
            logger.info(f"currently not supported: {source_config['name']}")

    if huggingface_config:
        # Using .override() is the SDK way to set dynamic task IDs
        ingest_instance_hf = run_huggingface_ingest.expand(source_config=huggingface_config)
        ingest_tasks.append(ingest_instance_hf)
        # Set explicit dependency
        wait_for_infra >> ingest_instance_hf

    trigger_move = TriggerDagRunOperator(
        task_id='trigger_persistent_move',
        trigger_dag_id='temporal_to_persistent',
        conf={"trigger_source": "auto_ingest"},  # Signal for automated buffer
        wait_for_completion=False,
    )

    if ingest_tasks:
        ingest_tasks >> trigger_move

# Instantiate the DAG object
ingest_dataset_dag()