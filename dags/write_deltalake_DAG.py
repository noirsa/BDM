from airflow.sdk import dag, task
from datetime import datetime,timedelta
from airflow.sensors.python import PythonSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id='write_deltalake',
    start_date=datetime(2026, 3, 30),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=['infrastructure', 'dataset','persistent landing','deltalake']
)
def write_deltalake_dag():
    """Write persistent landing CSV/image metadata into Delta assets.

    This DAG remains manual-triggerable and can also trigger trusted_zone when
    the persistent landing Delta write completes successfully.
    """

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
    def structured_to_deltalake_task(**context):
        from src.deltalake.csv_deltalake_loader import CSVDeltalakeLoader
        from src.utils import get_logger
        from src.utils.airflow_context import trigger_context
        from src.utils.time_anchor import logical_date_from_context
        from src import get_minio_client, get_duckdb_client
        minio_client = get_minio_client(role="writer")
        duckdb_client = get_duckdb_client(role="writer")
        logical_date = logical_date_from_context(context)
        logger = get_logger(__name__)
        run_context = trigger_context(context, "write_deltalake")

        logger.info(
            "Starting structured Delta write dag_id=%s task_id=%s run_id=%s logical_date=%s trigger_source=%s source_dag_id=%s source_run_id=%s source_assets=%s output_assets=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            logical_date,
            run_context["trigger_source"],
            run_context["source_dag_id"],
            run_context["source_run_id"],
            "landing-zone/persistent-landing/structured/raw/",
            "landing-zone/persistent-landing/structured/raw/*.delta",
        )

        loader = CSVDeltalakeLoader(minio_client, duckdb_client)

        loader.load_and_transform("landing-zone", "persistent-landing/structured/raw/", logical_date=logical_date)
        logger.info("Structured Delta write completed dag_id=write_deltalake task_id=%s output_assets=%s", run_context["task_id"], "landing-zone/persistent-landing/structured/raw/")

    @task(trigger_rule='none_failed_min_one_success')
    def write_image_catalog_task(**context):
        from src.deltalake.catalog_builder import CatalogBuilder
        from src.utils import get_logger
        from src.utils.airflow_context import trigger_context
        from src.utils.time_anchor import logical_date_from_context
        from src import get_minio_client, get_duckdb_client
        minio_client = get_minio_client(role="writer")
        duckdb_client = get_duckdb_client(role="writer")
        logical_date = logical_date_from_context(context)
        logger = get_logger(__name__)
        run_context = trigger_context(context, "write_deltalake")
        logger.info(
            "Initializing image catalogue Delta write dag_id=%s task_id=%s run_id=%s logical_date=%s trigger_source=%s source_dag_id=%s source_run_id=%s source_assets=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            logical_date,
            run_context["trigger_source"],
            run_context["source_dag_id"],
            run_context["source_run_id"],
            "landing-zone/persistent-landing/unstructured/image/",
        )

        try:
            builder = CatalogBuilder(minio_client, duckdb_client)

            # Log the specific parameters for traceability
            target = "landing-zone/persistent-landing/structured/file_catalog/"

            logger.info("Target image catalogue output_assets=%s", target)

            # Execution
            builder.run_image_ingestion("landing-zone", "persistent-landing/unstructured/image/", target, logical_date=logical_date)

            logger.info("Image catalogue Delta write completed dag_id=write_deltalake task_id=%s output_assets=%s", run_context["task_id"], target)
        except Exception as e:
            logger.error(f"Catalog Task Failed: {str(e)}", exc_info=True)
            raise  # Ensure Airflow marks the task as failed

    @task(trigger_rule='none_failed_min_one_success')
    def log_trigger_trusted_zone_task(**context):
        from src.utils import get_logger
        from src.utils.airflow_context import trigger_context

        logger = get_logger("dags.write_deltalake")
        run_context = trigger_context(context, "write_deltalake")
        logger.info(
            "Triggering downstream DAG dag_id=%s task_id=%s run_id=%s logical_date=%s trigger_source=%s source_dag_id=%s source_run_id=%s output_assets=%s triggered_downstream_dag_id=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            run_context["logical_date"],
            run_context["trigger_source"],
            run_context["source_dag_id"],
            run_context["source_run_id"],
            "landing-zone/persistent-landing/structured/raw/,landing-zone/persistent-landing/structured/file_catalog/",
            "trusted_zone",
        )

    branch_op = check_trigger_source()
    t1 = structured_to_deltalake_task()
    t2 = write_image_catalog_task()
    log_trigger_trusted = log_trigger_trusted_zone_task()
    trigger_trusted_zone = TriggerDagRunOperator(
        task_id='trigger_trusted_zone',
        trigger_dag_id='trusted_zone',
        conf={
            "trigger_source": "write_deltalake_flow",
            "source_dag_id": "write_deltalake",
            "source_run_id": "{{ run_id }}",
            "source_logical_date": "{{ (dag_run.conf or {}).get('source_logical_date') or logical_date.isoformat() }}",
            "pipeline_mode": "auto_chained",
        },
        wait_for_completion=False,
    )

    branch_op >> buffer_wait >> [t1, t2]
    branch_op >> [t1, t2]
    [t1, t2] >> log_trigger_trusted >> trigger_trusted_zone


write_deltalake_dag()
