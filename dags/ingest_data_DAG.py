from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from src.utils import load_kaggle_config, load_huggingface_config
# Standard providers are still imported this way

kaggle_config = load_kaggle_config()
huggingface_config = load_huggingface_config()



@dag(
    dag_id='ingest_dataset',
    start_date=datetime(2026, 3, 30),
    schedule=None,
    catchup=False,
    tags=['infrastructure', 'dataset', 'manual'],
    is_paused_upon_creation=False
)
def ingest_dataset_dag():
    """
    Ingest configured Kaggle and Hugging Face datasets into temporal landing.

    The DAG can be triggered manually or can continue the demo pipeline by
    triggering temporal_to_persistent after all ingestion tasks succeed.
    """

    @task(retries=60, retry_delay=timedelta(seconds=60))
    def wait_for_infra_ready_task(**context):
        from src import get_minio_client
        from src.utils import get_logger

        minio_client = get_minio_client(role="writer")
        logger = get_logger(context.get("task_id"))

        try:
            logger.info(
                "Checking MinIO bucket existence dag_id=ingest_dataset task_id=%s run_id=%s logical_date=%s",
                context.get("task_id"),
                context.get("run_id"),
                context.get("logical_date"),
            )

            minio_client.client.head_bucket(Bucket="landing-zone")

            logger.info("Infra ready bucket=landing-zone dag_id=ingest_dataset task_id=%s", context.get("task_id"))

        except Exception as e:
            logger.exception("Infra not ready")
            raise
    wait_for_infra = wait_for_infra_ready_task()
    # 2. Define the task using the new SDK @task decorator
    @task(map_index_template="{{ 'ingest_' + source_config['name'].replace('-', '_') }}")
    def run_kaggle_ingest(source_config: dict, **context):
        from src.utils import get_logger
        from src.utils.time_anchor import logical_date_from_context
        from src.dataloader.kaggle_loader import KaggleLoader
        from src import get_minio_client
        minio_client = get_minio_client(role="writer")
        loader = KaggleLoader(minio_client)
        logger = get_logger(__name__)
        logical_date = logical_date_from_context(context)
        logger.info(
            "Processing Kaggle dataset dag_id=ingest_dataset task_id=%s run_id=%s logical_date=%s source_assets=%s output_assets=%s",
            context.get("task_id"),
            context.get("run_id"),
            logical_date,
            source_config.get("handle"),
            f"landing-zone/temporal-landing/{source_config.get('object_name') or source_config['name']}",
        )

        if source_config.get('type') == "csv":
            loader.fetch_and_upload_csv(
                handle=source_config['handle'],
                file_name=source_config['file'],
                name=source_config['name'],
                object_name=source_config.get('object_name'),
                table_name=source_config.get('table_name'),
                expected_rows=source_config.get('expected_rows'),
                expected_columns=source_config.get('expected_columns'),
                logical_date=logical_date,
                **source_config.get('params', {})
            )
        elif source_config.get('type') == "image":
            loader.fetch_and_upload_image(
                handle=source_config['handle'],
                name=source_config['name'],
                logical_date=logical_date,
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
    def run_huggingface_ingest(source_config: dict, **context):
        from src.utils import get_logger
        from src.utils.time_anchor import logical_date_from_context
        from src.dataloader.huggingface_loader import HuggingfaceDataLoader
        from src import get_minio_client
        minio_client = get_minio_client(role="writer")
        loader = HuggingfaceDataLoader(minio_client)
        logger = get_logger(__name__)
        logical_date = logical_date_from_context(context)
        logger.info(
            "Processing Hugging Face dataset dag_id=ingest_dataset task_id=%s run_id=%s logical_date=%s source_assets=%s output_assets=%s",
            context.get("task_id"),
            context.get("run_id"),
            logical_date,
            source_config.get("path"),
            f"landing-zone/temporal-landing/{source_config.get('object_name') or source_config['name']}",
        )

        if source_config.get('type') == "csv":
            loader.fetch_and_upload(
                path=source_config['path'],
                name=source_config['name'],
                split= source_config['split'],
                file_type=source_config['type'],
                object_name=source_config.get('object_name'),
                table_name=source_config.get('table_name'),
                expected_rows=source_config.get('expected_rows'),
                expected_columns=source_config.get('expected_columns'),
                logical_date=logical_date,
            )

        else:
            logger.info(f"currently not supported: {source_config['name']}")

    if huggingface_config:
        # Using .override() is the SDK way to set dynamic task IDs
        ingest_instance_hf = run_huggingface_ingest.expand(source_config=huggingface_config)
        ingest_tasks.append(ingest_instance_hf)
        # Set explicit dependency
        wait_for_infra >> ingest_instance_hf

    @task
    def log_trigger_persistent_move_task(**context):
        from src.utils import get_logger
        from src.utils.airflow_context import trigger_context

        logger = get_logger("dags.ingest_dataset")
        run_context = trigger_context(context, "ingest_dataset")
        logger.info(
            "Triggering downstream DAG dag_id=%s task_id=%s run_id=%s logical_date=%s trigger_source=%s source_dag_id=%s source_run_id=%s output_assets=%s triggered_downstream_dag_id=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            run_context["logical_date"],
            run_context["trigger_source"],
            run_context["source_dag_id"],
            run_context["source_run_id"],
            "landing-zone/temporal-landing/",
            "temporal_to_persistent",
        )

    trigger_move = TriggerDagRunOperator(
        task_id='trigger_persistent_move',
        trigger_dag_id='temporal_to_persistent',
        conf={
            "trigger_source": "auto_ingest",
            "source_dag_id": "ingest_dataset",
            "source_run_id": "{{ run_id }}",
            "source_logical_date": "{{ logical_date.isoformat() }}",
            "pipeline_mode": "auto_chained",
        },
        wait_for_completion=False,
    )

    if ingest_tasks:
        ingest_tasks >> log_trigger_persistent_move_task() >> trigger_move

# Instantiate the DAG object
ingest_dataset_dag()
