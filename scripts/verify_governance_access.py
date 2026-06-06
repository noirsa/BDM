from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any


def record(checks: list[dict[str, Any]], name: str, status: str, detail: str) -> None:
    checks.append({"name": name, "status": status, "detail": detail})


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def check_clickhouse(checks: list[dict[str, Any]]) -> None:
    try:
        import clickhouse_connect
    except Exception as exc:
        record(checks, "clickhouse_import", "skipped", f"clickhouse-connect unavailable: {exc}")
        return

    host = env("CLICKHOUSE_HOST", "clickhouse")
    port = int(env("CLICKHOUSE_HTTP_PORT", "8123"))

    accounts = [
        ("analytics_admin", env("CLICKHOUSE_USER", "analytics"), env("CLICKHOUSE_PASSWORD", "analytics_secret"), "SELECT currentUser()"),
        (
            "trusted_structured_writer",
            env("CLICKHOUSE_TRUSTED_USER", "trusted_structured_writer"),
            env("CLICKHOUSE_TRUSTED_PASSWORD", "trusted_structured_writer_password"),
            "SELECT currentUser()",
        ),
        (
            "consumption_service",
            env("CLICKHOUSE_CONSUMPTION_USER", "consumption_service"),
            env("CLICKHOUSE_CONSUMPTION_PASSWORD", "consumption_service_password"),
            "SELECT currentUser()",
        ),
    ]

    for label, username, password, query in accounts:
        try:
            client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password)
            try:
                current_user = client.query(query).first_row[0]
                record(checks, f"clickhouse_{label}_login", "pass", f"logged in as {current_user}")
            finally:
                client.close()
        except Exception as exc:
            record(checks, f"clickhouse_{label}_login", "fail", str(exc))

    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=env("CLICKHOUSE_CONSUMPTION_USER", "consumption_service"),
            password=env("CLICKHOUSE_CONSUMPTION_PASSWORD", "consumption_service_password"),
        )
        try:
            table_exists = client.query(
                """
                SELECT count()
                FROM system.tables
                WHERE database = 'exploitation_analytics'
                  AND name = 'fact_tweet_features'
                """
            ).first_row[0]
            if table_exists:
                row_count = client.query("SELECT count() FROM exploitation_analytics.fact_tweet_features").first_row[0]
                record(checks, "clickhouse_consumption_source_select", "pass", f"fact_tweet_features rows={row_count}")
            else:
                record(checks, "clickhouse_consumption_source_select", "skipped", "fact_tweet_features not created yet")
        finally:
            client.close()
    except Exception as exc:
        record(checks, "clickhouse_consumption_source_select", "fail", str(exc))


def minio_client(access_key: str, secret_key: str) -> Any:
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=env("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def check_minio(checks: list[dict[str, Any]], write_checks: bool) -> None:
    try:
        import botocore.exceptions
    except Exception as exc:
        record(checks, "minio_import", "skipped", f"boto3/botocore unavailable: {exc}")
        return

    writer = minio_client(env("MINIO_WRITER_ACCESS_KEY", "bdm_writer"), env("MINIO_WRITER_SECRET_KEY", "bdm_writer_password"))
    reader = minio_client(env("MINIO_READER_ACCESS_KEY", "bdm_reader"), env("MINIO_READER_SECRET_KEY", "bdm_reader_password"))

    for label, client in (("writer", writer), ("reader", reader)):
        try:
            buckets = sorted(bucket["Name"] for bucket in client.list_buckets().get("Buckets", []))
            record(checks, f"minio_{label}_list_buckets", "pass", ",".join(buckets))
        except Exception as exc:
            record(checks, f"minio_{label}_list_buckets", "fail", str(exc))

    if not write_checks:
        record(checks, "minio_writer_reader_write_policy", "skipped", "run with --write-checks to verify temporary write/deny behavior")
        return

    key = f"governance_smoke_test/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.txt"
    try:
        writer.put_object(Bucket="landing-zone", Key=key, Body=b"governance smoke test")
        writer.delete_object(Bucket="landing-zone", Key=key)
        record(checks, "minio_writer_put_delete", "pass", "writer can create and clean a temporary object")
    except Exception as exc:
        record(checks, "minio_writer_put_delete", "fail", str(exc))

    try:
        reader.put_object(Bucket="landing-zone", Key=key, Body=b"reader should not write")
        record(checks, "minio_reader_put_denied", "fail", "reader unexpectedly wrote a temporary object")
    except botocore.exceptions.ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        record(checks, "minio_reader_put_denied", "pass", f"reader write denied with HTTP {status}")
    except Exception as exc:
        record(checks, "minio_reader_put_denied", "pass", f"reader write denied: {exc}")


def check_mongo(checks: list[dict[str, Any]]) -> None:
    try:
        from pymongo import MongoClient
    except Exception as exc:
        record(checks, "mongo_import", "skipped", f"pymongo unavailable: {exc}")
        return

    uri = env(
        "MONGODB_TRUSTED_URI",
        "mongodb://trusted_semistructured_writer:trusted_semistructured_writer_password@mongo:27017/trusted_zone_semi-structured",
    )
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        try:
            client.admin.command("ping")
            db_name = env("TRUSTED_SEMISTRUCTURED_MONGO_DB", "trusted_zone_semi-structured")
            collections = sorted(client[db_name].list_collection_names())
            record(checks, "mongo_trusted_writer_ping", "pass", f"{db_name} collections={len(collections)}")
        finally:
            client.close()
    except Exception as exc:
        record(checks, "mongo_trusted_writer_ping", "fail", str(exc))


def check_milvus(checks: list[dict[str, Any]]) -> None:
    try:
        from pymilvus import MilvusClient
    except Exception as exc:
        record(checks, "milvus_import", "skipped", f"pymilvus unavailable: {exc}")
        return

    uri = env("MILVUS_URI", "http://milvus:19530")
    accounts = [
        ("root", env("MILVUS_ROOT_USER", "root"), env("MILVUS_ROOT_PASSWORD", "Milvus")),
        ("writer", env("MILVUS_WRITER_USER", "bdm_vector_writer"), env("MILVUS_WRITER_PASSWORD", "bdm_vector_writer_password")),
        ("reader", env("MILVUS_READER_USER", "bdm_vector_reader"), env("MILVUS_READER_PASSWORD", "bdm_vector_reader_password")),
    ]
    for label, username, password in accounts:
        try:
            client = MilvusClient(uri=uri, user=username, password=password)
            try:
                collections = client.list_collections()
                record(checks, f"milvus_{label}_login", "pass", f"collections={len(collections)}")
            finally:
                client.close()
        except Exception as exc:
            record(checks, f"milvus_{label}_login", "fail", str(exc))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run non-secret governance/RBAC smoke checks.")
    parser.add_argument("--write-checks", action="store_true", help="run temporary MinIO write/deny checks")
    args = parser.parse_args()

    checks: list[dict[str, Any]] = []
    check_clickhouse(checks)
    check_minio(checks, write_checks=args.write_checks)
    check_mongo(checks)
    check_milvus(checks)

    print(json.dumps({"checks": checks}, indent=2, sort_keys=True))
    return 1 if any(check["status"] == "fail" for check in checks) else 0


if __name__ == "__main__":
    sys.exit(main())
