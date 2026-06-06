from __future__ import annotations

from typing import Any

from src.utils.time_anchor import coerce_logical_date, logical_date_from_context


def consumption_airflow_time_from_context(context: dict[str, Any], logger: Any) -> Any:
    """Return a deterministic Airflow timestamp for manual consumption runs."""
    try:
        return logical_date_from_context(context)
    except ValueError:
        dag_run = context.get("dag_run")
        run_after = getattr(dag_run, "run_after", None) if dag_run is not None else None
        if run_after is None:
            raise
        logger.info("Airflow logical_date is empty for this manual run; using dag_run.run_after=%s", run_after)
        return coerce_logical_date(run_after)
