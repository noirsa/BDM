from __future__ import annotations

from typing import Any

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
    the dependencies below.
    """

    @task
    def bootstrap_trusted_zone_task(**context):
        """Prepare target databases, prefixes, and source availability checks."""
        from src.utils.time_anchor import logical_date_from_context

        pipeline = _build_pipeline()
        pipeline.bootstrap_trusted_zone(logical_date_from_context(context))

    @task
    def clean_structured_data_task(**context):
        """Convert persistent structured Delta tables into ClickHouse trusted tables."""
        from src.utils.time_anchor import logical_date_from_context

        pipeline = _build_pipeline()
        pipeline.clean_structured_data(logical_date_from_context(context))

    @task
    def clean_unstructured_data_task(**context):
        """Standardize image objects and write the trusted image asset catalogue."""
        from src.utils.time_anchor import logical_date_from_context

        pipeline = _build_pipeline()
        pipeline.clean_unstructured_data(logical_date_from_context(context))

    @task
    def build_trusted_catalogue_task(**context):
        """Build catalogue metadata over Trusted Zone outputs."""
        from src.utils.time_anchor import logical_date_from_context

        pipeline = _build_pipeline()
        pipeline.build_trusted_catalogue(logical_date_from_context(context))

    bootstrap = bootstrap_trusted_zone_task()
    structured = clean_structured_data_task()
    unstructured = clean_unstructured_data_task()
    catalogue = build_trusted_catalogue_task()

    bootstrap >> structured >> unstructured >> catalogue


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
        from src.utils.time_anchor import logical_date_from_context

        pipeline = _build_pipeline()
        pipeline.clean_semistructured_data(logical_date_from_context(context))

    clean_semistructured_data_task()


trusted_zone_dag()
trusted_zone_semistructured_daily_dag()
