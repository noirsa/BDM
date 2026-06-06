"""Small Airflow context helpers used by DAG logging and trigger evidence."""

from __future__ import annotations

from typing import Any


def dag_run_conf(context: dict[str, Any]) -> dict[str, Any]:
    """Return dag_run.conf without forcing DAG files to depend on Airflow models."""
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) if dag_run is not None else None
    return conf if isinstance(conf, dict) else {}


def task_id_from_context(context: dict[str, Any]) -> str | None:
    """Read the task id from the most common Airflow context locations."""
    task_instance = context.get("task_instance") or context.get("ti")
    return getattr(task_instance, "task_id", None) or context.get("task_id")


def trigger_context(context: dict[str, Any], dag_id: str) -> dict[str, Any]:
    """Build a compact, non-secret context dictionary for DAG evidence logs."""
    conf = dag_run_conf(context)
    return {
        "dag_id": dag_id,
        "task_id": task_id_from_context(context),
        "run_id": context.get("run_id"),
        "logical_date": context.get("logical_date"),
        "trigger_source": conf.get("trigger_source", "manual"),
        "source_dag_id": conf.get("source_dag_id"),
        "source_run_id": conf.get("source_run_id"),
        "source_logical_date": conf.get("source_logical_date"),
        "pipeline_mode": conf.get("pipeline_mode", "manual"),
    }
