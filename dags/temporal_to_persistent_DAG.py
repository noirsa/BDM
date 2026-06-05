from airflow.sdk import dag, task
from datetime import datetime,timedelta
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from airflow.sensors.time_delta import TimeDeltaSensor


def poke_minio_for_real_data(**context):
    from src.utils import get_logger
    from src import get_minio_client

    minio_client = get_minio_client(role="writer")
    conf = context.get('dag_run').conf or {}
    source = "Automated" if conf.get('trigger_source') == 'auto_ingest' else "Manual"
    logger = get_logger(context.get('task_id'))

    if minio_client.verify_empty_bucket("landing-zone", "temporal-landing/"):
        if source == "Automated":
            raise Exception("Expected data but found none.")
        logger.info(f"[{source}] Checking temporal-landing... Bucket is still empty. skip...")
        raise AirflowSkipException("No files found in temporal-landing, skipping downstream tasks.")

    logger.info(f"[{source}] Data detected in temporal-landing! Triggering move task now.")
    return True

@dag(
    dag_id='temporal_to_persistent',
    start_date=datetime(2026, 3, 30),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=['infrastructure', 'dataset','persistent landing','deltalake']
)
def temporal_to_persistent_dag():
    """
    Main DAG definition for the Temporal to Persistent workflow.
    """

    @task.branch
    def check_trigger_source(**context):
        """
        Check if the DAG was triggered by the Ingestion DAG or manually.
        If auto-triggered, go to the 3-minute buffer.
        If manually triggered, skip the buffer.
        """
        conf = context.get('dag_run').conf

        # If the 'auto_ingest' flag is found, route to the wait sensor
        if conf and conf.get('trigger_source') == 'auto_ingest':
            return 'buffer_wait_3_mins'

        # Otherwise (Manual Trigger), go straight to the move task
        return 'wait_for_temporal_data'

    # 3-minute "Regret Window" for automated runs
    # mode='reschedule' ensures we don't waste worker slots while waiting
    buffer_wait = TimeDeltaSensor(
        task_id='buffer_wait_3_mins',
        delta=timedelta(minutes=3),
        mode='reschedule',
    )
    # Monitors the landing zone for ANY file.
    # Using wildcard '*' allows the pipeline to be triggered by any incoming data.
    wait_for_ingestion = PythonSensor(
        task_id='wait_for_temporal_data',
        python_callable=poke_minio_for_real_data,
        mode='reschedule',
        poke_interval=60,
        timeout=60 * 3,
        trigger_rule='none_failed_min_one_success'
    )

    # This task encapsulates the business logic for sorting files.
    @task(trigger_rule='none_failed_min_one_success')
    def move_data_task(**context):
        """
            Executes the move_bucket operation to sort files into
            structured/raw, unstructured/image, or semistructured directories.
        """
        from src.utils import get_logger
        from src.utils.time_anchor import logical_date_from_context
        from src import get_minio_client

        minio_client = get_minio_client(role="writer")
        logical_date = logical_date_from_context(context)
        logger = get_logger("move_data_task")

        logger.info("Executing bucket migration and classification logic.")

        minio_client.move_bucket(
            source_bucket='landing-zone',
            source_prefix='temporal-landing/',
            destination_bucket='landing-zone',
            destination_prefix='persistent-landing/',
            logical_date=logical_date,
        )

        return "move complete."

    trigger_delta_write = TriggerDagRunOperator(
        task_id='trigger_deltalake_write',
        trigger_dag_id='write_deltalake',
        conf={
            "trigger_source": "temporal_to_persistent_flow",
            "source_logical_date": "{{ (dag_run.conf or {}).get('source_logical_date') or logical_date.isoformat() }}",
        },
        wait_for_completion=False,
    )

    branch_op = check_trigger_source()

    branch_op >> buffer_wait >> wait_for_ingestion

    branch_op >> wait_for_ingestion

    move_task_instance = move_data_task()

    wait_for_ingestion >> move_task_instance >> trigger_delta_write


# Instantiate the DAG
temporal_to_persistent_dag()
