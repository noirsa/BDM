from datetime import datetime, timezone


def coerce_logical_date(value):
    """Normalize Airflow logical_date values into timezone-aware datetimes."""
    if value is None:
        raise ValueError("logical_date is required for deterministic landing-zone paths")

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError(f"Unsupported logical_date type: {type(value)!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt


def logical_date_from_context(context):
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    return coerce_logical_date(conf.get("source_logical_date") or context.get("logical_date"))


def logical_date_iso(logical_date):
    return coerce_logical_date(logical_date).isoformat()


def logical_date_suffix(logical_date):
    """Return a deterministic Unix timestamp suffix in milliseconds."""
    dt = coerce_logical_date(logical_date).astimezone(timezone.utc)
    return str(int(dt.timestamp() * 1000))


def date_partition(logical_date):
    dt = coerce_logical_date(logical_date)
    return f"year={dt:%Y}/month={dt:%m}/day={dt:%d}"


def compact_date_partition(logical_date):
    """Return a single directory-friendly logical date such as 20260524."""
    return coerce_logical_date(logical_date).strftime("%Y%m%d")
