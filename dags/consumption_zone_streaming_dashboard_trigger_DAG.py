"""Consumption DAG for hourly streaming dashboard supervision."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="consumption_zone_streaming_dashboard_trigger",
    start_date=pendulum.datetime(2026, 3, 30, tz="Europe/Madrid"),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["consumption-zone", "streaming", "dashboard", "hourly"],
)
def consumption_zone_streaming_dashboard_trigger_dag():
    """Supervise the live dashboard stream and record a launch/health event.

    This DAG runs hourly, remains manual-triggerable, and may be auto-triggered
    by the semi-structured exploitation DAG as streaming consumption evidence.
    It restarts the dashboard streaming process when the Spark application is
    missing or unhealthy.
    """

    @task
    def supervise_streaming_dashboard_task(**context: Any) -> dict[str, Any]:
        from src.consumption_zone import StreamingDashboardTrigger
        from src.consumption_zone.airflow_time import consumption_airflow_time_from_context
        from src.utils import get_logger
        from src.utils.airflow_context import trigger_context

        logger = get_logger("dags.consumption_zone_streaming_dashboard_trigger")
        logical_date = consumption_airflow_time_from_context(context, logger)
        run_context = trigger_context(context, "consumption_zone_streaming_dashboard_trigger")
        logger.info(
            "Starting streaming dashboard supervision dag_id=%s task_id=%s run_id=%s logical_date=%s trigger_source=%s source_dag_id=%s source_run_id=%s source_assets=%s output_assets=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            logical_date,
            run_context["trigger_source"],
            run_context["source_dag_id"],
            run_context["source_run_id"],
            "kafka.weather-barcelona,kafka.airquality-barcelona",
            "exploitation_analytics.streaming_dashboard_launch_log",
        )
        result = StreamingDashboardTrigger().run(logical_date)
        logger.info(
            "Streaming dashboard supervision completed dag_id=%s task_id=%s run_id=%s status_counts=%s output_assets=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            result,
            "exploitation_analytics.streaming_dashboard_launch_log",
        )
        return result

    supervise_streaming_dashboard_task()


consumption_zone_streaming_dashboard_trigger_dag()
