"""Trusted Zone DAGs for production cleaning and catalogue evidence."""

from __future__ import annotations

from typing import Any

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
import pendulum


def _build_pipeline() -> Any:
    """Create the TrustedZonePipeline with shared project clients."""
    from src import get_duckdb_client, get_minio_client
    from src.trusted_zone import TrustedZonePipeline

    return TrustedZonePipeline(
        minio_client=get_minio_client(role="writer"),
        duckdb_client=get_duckdb_client(role="writer"),
    )


@dag(
    dag_id="trusted_zone",
    start_date=pendulum.datetime(2026, 3, 30, tz="Europe/Madrid"),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["trusted-zone", "cleaning", "manual"],
)
def trusted_zone_dag():
    """Manual Trusted Zone production cleaning DAG.

    Operators manually trigger this DAG. Airflow then runs the downstream
    structured, unstructured, and catalogue tasks automatically according to
    the dependencies below. When the manual DAG succeeds, it can also trigger
    the exploitation DAGs sequentially without removing their manual mode.
    """

    @task
    def bootstrap_trusted_zone_task(**context):
        """Prepare target databases, prefixes, and source availability checks."""
        from src.utils import get_logger
        from src.utils.airflow_context import trigger_context
        from src.utils.time_anchor import logical_date_from_context

        logger = get_logger("dags.trusted_zone")
        run_context = trigger_context(context, "trusted_zone")
        logical_date = logical_date_from_context(context)
        logger.info(
            "Starting trusted bootstrap dag_id=%s task_id=%s run_id=%s logical_date=%s trigger_source=%s source_dag_id=%s source_run_id=%s source_assets=%s output_assets=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            logical_date,
            run_context["trigger_source"],
            run_context["source_dag_id"],
            run_context["source_run_id"],
            "landing-zone/persistent-landing/",
            "trusted-zone/,bi_analytics,trusted_zone_semi-structured",
        )
        pipeline = _build_pipeline()
        pipeline.bootstrap_trusted_zone(logical_date)

    @task
    def clean_structured_data_task(**context):
        """Convert persistent structured Delta tables into ClickHouse trusted tables."""
        from src.utils import get_logger
        from src.utils.airflow_context import trigger_context
        from src.utils.time_anchor import logical_date_from_context

        logger = get_logger("dags.trusted_zone")
        run_context = trigger_context(context, "trusted_zone")
        logical_date = logical_date_from_context(context)
        logger.info(
            "Starting trusted structured cleaning dag_id=%s task_id=%s run_id=%s logical_date=%s trigger_source=%s source_dag_id=%s source_run_id=%s source_assets=%s output_assets=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            logical_date,
            run_context["trigger_source"],
            run_context["source_dag_id"],
            run_context["source_run_id"],
            "landing-zone/persistent-landing/structured/raw/",
            "bi_analytics.{global_warming_dataset,temperature_change,co2_emission_by_vehicles,natural_disaster_tweets}",
        )
        pipeline = _build_pipeline()
        pipeline.clean_structured_data(logical_date)

    @task
    def clean_unstructured_data_task(**context):
        """Standardize image objects and write the trusted image asset catalogue."""
        from src.utils import get_logger
        from src.utils.airflow_context import trigger_context
        from src.utils.time_anchor import logical_date_from_context

        logger = get_logger("dags.trusted_zone")
        run_context = trigger_context(context, "trusted_zone")
        logical_date = logical_date_from_context(context)
        logger.info(
            "Starting trusted unstructured cleaning dag_id=%s task_id=%s run_id=%s logical_date=%s trigger_source=%s source_dag_id=%s source_run_id=%s source_assets=%s output_assets=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            logical_date,
            run_context["trigger_source"],
            run_context["source_dag_id"],
            run_context["source_run_id"],
            "landing-zone/persistent-landing/unstructured/image/,landing-zone/persistent-landing/structured/file_catalog/",
            "trusted-zone/unstructured/image/,trusted-zone/file_catalog/",
        )
        pipeline = _build_pipeline()
        pipeline.clean_unstructured_data(logical_date)

    @task
    def build_trusted_catalogue_task(**context):
        """Build catalogue metadata over Trusted Zone outputs."""
        from src.utils import get_logger
        from src.utils.airflow_context import trigger_context
        from src.utils.time_anchor import logical_date_from_context

        logger = get_logger("dags.trusted_zone")
        run_context = trigger_context(context, "trusted_zone")
        logical_date = logical_date_from_context(context)
        logger.info(
            "Starting trusted catalogue construction dag_id=%s task_id=%s run_id=%s logical_date=%s trigger_source=%s source_dag_id=%s source_run_id=%s source_assets=%s output_assets=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            logical_date,
            run_context["trigger_source"],
            run_context["source_dag_id"],
            run_context["source_run_id"],
            "bi_analytics,trusted_zone_semi-structured,trusted-zone/file_catalog/",
            "trusted-zone/catalogue/",
        )
        pipeline = _build_pipeline()
        pipeline.build_trusted_catalogue(logical_date)

    @task
    def log_exploitation_triggers_task(**context):
        """Log the sequential exploitation DAGs that will be triggered."""
        from src.utils import get_logger
        from src.utils.airflow_context import trigger_context

        downstream = [
            "exploitation_zone_structured",
            "exploitation_zone_semistructured",
            "exploitation_zone_image_vectorization",
        ]
        logger = get_logger("dags.trusted_zone")
        run_context = trigger_context(context, "trusted_zone")
        logger.info(
            "Triggering downstream DAGs sequentially dag_id=%s task_id=%s run_id=%s logical_date=%s trigger_source=%s source_dag_id=%s source_run_id=%s output_assets=%s triggered_downstream_dag_id=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            run_context["logical_date"],
            run_context["trigger_source"],
            run_context["source_dag_id"],
            run_context["source_run_id"],
            "trusted-zone/catalogue/,bi_analytics,trusted_zone_semi-structured,trusted-zone/file_catalog/",
            ",".join(downstream),
        )

    bootstrap = bootstrap_trusted_zone_task()
    structured = clean_structured_data_task()
    unstructured = clean_unstructured_data_task()
    catalogue = build_trusted_catalogue_task()
    log_exploitation_triggers = log_exploitation_triggers_task()
    trigger_structured_exploitation = TriggerDagRunOperator(
        task_id="trigger_exploitation_zone_structured",
        trigger_dag_id="exploitation_zone_structured",
        conf={
            "trigger_source": "trusted_zone_flow",
            "source_dag_id": "trusted_zone",
            "source_run_id": "{{ run_id }}",
            "source_logical_date": "{{ (dag_run.conf or {}).get('source_logical_date') or logical_date.isoformat() }}",
            "pipeline_mode": "auto_chained",
        },
        wait_for_completion=True,
        poke_interval=30,
    )
    trigger_semistructured_exploitation = TriggerDagRunOperator(
        task_id="trigger_exploitation_zone_semistructured",
        trigger_dag_id="exploitation_zone_semistructured",
        conf={
            "trigger_source": "trusted_zone_flow",
            "source_dag_id": "trusted_zone",
            "source_run_id": "{{ run_id }}",
            "source_logical_date": "{{ (dag_run.conf or {}).get('source_logical_date') or logical_date.isoformat() }}",
            "pipeline_mode": "auto_chained",
        },
        wait_for_completion=True,
        poke_interval=30,
    )
    trigger_image_exploitation = TriggerDagRunOperator(
        task_id="trigger_exploitation_zone_image_vectorization",
        trigger_dag_id="exploitation_zone_image_vectorization",
        conf={
            "trigger_source": "trusted_zone_flow",
            "source_dag_id": "trusted_zone",
            "source_run_id": "{{ run_id }}",
            "source_logical_date": "{{ (dag_run.conf or {}).get('source_logical_date') or logical_date.isoformat() }}",
            "pipeline_mode": "auto_chained",
        },
        wait_for_completion=True,
        poke_interval=30,
    )

    bootstrap >> structured >> unstructured >> catalogue >> log_exploitation_triggers
    (
        log_exploitation_triggers
        >> trigger_structured_exploitation
        >> trigger_semistructured_exploitation
        >> trigger_image_exploitation
    )


@dag(
    dag_id="trusted_zone_semistructured_daily",
    start_date=pendulum.datetime(2026, 3, 30, tz="Europe/Madrid"),
    schedule="0 0 * * *",
    catchup=False,
    is_paused_upon_creation=False,
    tags=["trusted-zone", "semi-structured", "daily"],
)
def trusted_zone_semistructured_daily_dag():
    """Daily semi-structured Trusted Zone cleaner.

    This is the only automatically scheduled Trusted Zone workflow. The 00:00
    run receives the previous data interval as Airflow logical_date, so it
    cleans yesterday's Kafka partition:
    persistent-landing/semistructured/<topic>/<yyyymmdd>/.
    """

    @task
    def clean_semistructured_data_task(**context):
        """Clean yesterday's Kafka JSON partitions into MongoDB."""
        from src.utils import get_logger
        from src.utils.airflow_context import trigger_context
        from src.utils.time_anchor import logical_date_from_context

        logger = get_logger("dags.trusted_zone_semistructured_daily")
        run_context = trigger_context(context, "trusted_zone_semistructured_daily")
        logical_date = logical_date_from_context(context)
        logger.info(
            "Starting trusted semi-structured cleaning dag_id=%s task_id=%s run_id=%s logical_date=%s trigger_source=%s source_dag_id=%s source_run_id=%s source_assets=%s output_assets=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            logical_date,
            run_context["trigger_source"],
            run_context["source_dag_id"],
            run_context["source_run_id"],
            "landing-zone/persistent-landing/semistructured/<topic>/<yyyymmdd>/",
            "trusted_zone_semi-structured.{airquality_barcelona,weather_barcelona}",
        )
        pipeline = _build_pipeline()
        pipeline.clean_semistructured_data(logical_date)

    clean_semistructured_data_task()


trusted_zone_dag()
trusted_zone_semistructured_daily_dag()
