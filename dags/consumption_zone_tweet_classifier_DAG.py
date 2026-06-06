"""Consumption DAG for natural-disaster tweet classifier metrics."""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="consumption_zone_tweet_classifier",
    start_date=pendulum.datetime(2026, 3, 30, tz="Europe/Madrid"),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["consumption-zone", "classifier", "clickhouse", "manual"],
)
def consumption_zone_tweet_classifier_dag():
    """Train/evaluate the natural disaster tweet classifier from exploitation outputs.

    This DAG remains manual-triggerable and may be auto-triggered by the
    structured exploitation DAG when fact_tweet_features is refreshed.
    """

    @task
    def train_tweet_classifier_task(**context: Any) -> dict[str, Any]:
        from src.consumption_zone import NaturalDisasterTweetClassifierPipeline
        from src.consumption_zone.airflow_time import consumption_airflow_time_from_context
        from src.utils import get_logger
        from src.utils.airflow_context import trigger_context

        logger = get_logger("dags.consumption_zone_tweet_classifier")
        logical_date = consumption_airflow_time_from_context(context, logger)
        run_context = trigger_context(context, "consumption_zone_tweet_classifier")
        logger.info(
            "Starting tweet classifier consumption dag_id=%s task_id=%s run_id=%s logical_date=%s trigger_source=%s source_dag_id=%s source_run_id=%s source_assets=%s output_assets=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            logical_date,
            run_context["trigger_source"],
            run_context["source_dag_id"],
            run_context["source_run_id"],
            "exploitation_analytics.fact_tweet_features",
            "exploitation_analytics.model_tweet_classifier_metrics",
        )
        result = NaturalDisasterTweetClassifierPipeline().run(logical_date)
        logger.info(
            "Tweet classifier consumption completed dag_id=%s task_id=%s run_id=%s row_count=%s output_assets=%s",
            run_context["dag_id"],
            run_context["task_id"],
            run_context["run_id"],
            result,
            "exploitation_analytics.model_tweet_classifier_metrics",
        )
        return result

    train_tweet_classifier_task()


consumption_zone_tweet_classifier_dag()
