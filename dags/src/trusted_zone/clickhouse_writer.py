from __future__ import annotations

import os
from typing import Any

from .base import BaseTrustedZoneService


class ClickHouseTrustedWriter(BaseTrustedZoneService):
    """Writer for Trusted Zone structured outputs in ClickHouse."""

    def get_client(self, database_name: str = "bi_analytics") -> Any:
        import clickhouse_connect

        return clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
            # Use the least-privilege Trusted Zone writer by default. The
            # analytics account remains available as a maintenance fallback.
            username=os.getenv("CLICKHOUSE_TRUSTED_USER", os.getenv("CLICKHOUSE_USER", "analytics")),
            password=os.getenv("CLICKHOUSE_TRUSTED_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", "analytics_secret")),
            database=database_name,
        )

    def ensure_database(self, database_name: str) -> None:
        """Create or validate the target ClickHouse database."""
        import clickhouse_connect

        # Database bootstrap remains a maintenance operation. Existing
        # analytics/admin fallback is preserved; normal table writes use the
        # least-privilege trusted writer through get_client().
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "analytics"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "analytics_secret"),
            database="default",
        )
        try:
            client.command(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            version = client.query("SELECT version()").result_rows[0][0]
            self.logger.info("ClickHouse database %s is ready on server %s", database_name, version)
        finally:
            client.close()

    def create_table(self, ddl_sql: str) -> None:
        """Execute generated ClickHouse DDL."""
        client = self.get_client()
        try:
            client.command(ddl_sql)
        finally:
            client.close()

    def write_dataframe(self, dataframe: Any, table_name: str, database_name: str) -> None:
        """Write a cleaned dataframe into ClickHouse from the driver."""
        client = self.get_client(database_name=database_name)
        try:
            payload = [tuple(row) for row in dataframe.collect()]
            if payload:
                client.insert(table=table_name, data=payload, column_names=dataframe.columns)
            self.logger.info("Inserted %s rows into %s.%s", len(payload), database_name, table_name)
        finally:
            client.close()

    def sync_dataframe_parallel(
        self,
        dataframe: Any,
        ddl_sql: str,
        table_name: str,
        database_name: str,
    ) -> None:
        """Port notebook parallel sync logic into production code."""
        driver_client = self.get_client(database_name=database_name)
        try:
            driver_client.command(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
            driver_client.command(ddl_sql)
        finally:
            driver_client.close()

        column_names = dataframe.columns
        host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        port = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
        username = os.getenv("CLICKHOUSE_TRUSTED_USER", os.getenv("CLICKHOUSE_USER", "analytics"))
        password = os.getenv("CLICKHOUSE_TRUSTED_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", "analytics_secret"))

        partition_count = dataframe.rdd.getNumPartitions()
        self.logger.info(
            "Starting trusted structured ClickHouse batch sync database=%s table=%s batches=%s",
            database_name,
            table_name,
            partition_count,
        )

        def write_partition(partition_index, rows_iter):
            import clickhouse_connect

            worker_client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database_name,
            )
            try:
                payload = [tuple(row) for row in rows_iter]
                if payload:
                    worker_client.insert(table=table_name, data=payload, column_names=column_names)
                return [{"batch_id": partition_index, "rows": len(payload)}]
            finally:
                worker_client.close()

        batch_results = dataframe.rdd.mapPartitionsWithIndex(write_partition).collect()
        for batch_result in sorted(batch_results, key=lambda item: item["batch_id"]):
            self.logger.info(
                "Trusted structured ClickHouse batch database=%s table=%s batch=%s/%s rows=%s",
                database_name,
                table_name,
                int(batch_result["batch_id"]) + 1,
                partition_count,
                int(batch_result["rows"]),
            )
        total_inserted = sum(int(batch_result["rows"]) for batch_result in batch_results)
        self.logger.info("Parallel sync completed for %s.%s rows=%s", database_name, table_name, total_inserted)

    def preview_table(self, table_name: str, database_name: str, sample_size: int = 3) -> list[dict[str, Any]]:
        """Return a small sample for validation logs."""
        client = self.get_client(database_name=database_name)
        try:
            headers = [
                row[0]
                for row in client.query(
                    f"SELECT name FROM system.columns WHERE database = '{database_name}' AND table = '{table_name}' ORDER BY position"
                ).result_rows
            ]
            rows = client.query(f"SELECT * FROM {database_name}.{table_name} LIMIT {sample_size}").result_rows
            return [dict(zip(headers, row)) for row in rows]
        finally:
            client.close()
