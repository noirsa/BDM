from __future__ import annotations

from typing import Any

from .base import BaseTrustedZoneService
from .clickhouse_writer import ClickHouseTrustedWriter
from .quality_checks import TrustedQualityChecks


class TrustedZoneInfrastructure(BaseTrustedZoneService):
    """Infrastructure checks for Trusted Zone DAG startup."""

    def __init__(self, minio_client: Any | None = None, duckdb_client: Any | None = None):
        super().__init__(minio_client=minio_client, duckdb_client=duckdb_client)
        self.clickhouse_writer = ClickHouseTrustedWriter(minio_client=minio_client, duckdb_client=duckdb_client)
        self.quality_checks = TrustedQualityChecks(minio_client=minio_client, duckdb_client=duckdb_client)

    def ensure_clickhouse_targets(self) -> None:
        """Ensure Trusted Zone ClickHouse databases/tablespaces are available."""
        self.clickhouse_writer.ensure_database("bi_analytics")

    def ensure_mongo_targets(self) -> None:
        """Ensure Trusted Zone MongoDB databases/collections are reachable."""
        import os

        from pymongo import MongoClient

        # Use the Trusted Zone MongoDB writer by default. The root URI fallback
        # is preserved for maintenance.
        client = MongoClient(os.getenv("MONGODB_TRUSTED_URI", os.getenv("MONGODB_URI", "mongodb://mongo:mongo@mongo:27017")))
        try:
            client.admin.command("ping")
            database_name = os.getenv("TRUSTED_SEMISTRUCTURED_MONGO_DB", "trusted_zone_semi-structured")
            client[database_name].list_collection_names()
            self.logger.info("MongoDB trusted database %s is reachable", database_name)
        finally:
            client.close()

    def ensure_minio_prefixes(self) -> None:
        """Ensure expected MinIO prefixes and buckets are available."""
        self.ensure_bucket("landing-zone")
        self.ensure_bucket("trusted-zone")
        if self.minio_client is not None and hasattr(self.minio_client, "create_placeholder"):
            self.minio_client.create_placeholder("trusted-zone", "unstructured/image/")
            self.minio_client.create_placeholder("trusted-zone", "rejected/structured/")
            self.minio_client.create_placeholder("trusted-zone", "rejected/semi_structured/")
            self.minio_client.create_placeholder("trusted-zone", "rejected/unstructured/image/")
            self.minio_client.create_placeholder("trusted-zone", "file_catalog/")
            self.minio_client.create_placeholder("trusted-zone", "persistent-landing/structured/file_catalog/")
            self.minio_client.create_placeholder("trusted-zone", "catalogue/summary/")

    def validate_persistent_landing_inputs(self, logical_date: Any) -> None:
        """Validate persistent landing inputs before trusted cleaning starts."""
        from src.utils import compact_date_partition, load_kafka_config

        required_prefixes = [
            "persistent-landing/structured/",
            "persistent-landing/structured/file_catalog/",
        ]
        for prefix in required_prefixes:
            if not self.prefix_has_objects("landing-zone", prefix):
                raise FileNotFoundError(f"Required landing-zone prefix is empty or missing: {prefix}")

        date_folder = compact_date_partition(logical_date)
        for topic_config in load_kafka_config():
            prefix = f"persistent-landing/semistructured/{topic_config['name']}/{date_folder}/"
            if not self.prefix_has_objects("landing-zone", prefix):
                self.logger.info("Semi-structured daily prefix has no files yet: %s", prefix)

    def bootstrap(self, logical_date: Any) -> None:
        """Run all Trusted Zone infrastructure checks."""
        self.logger.info("Bootstrap Trusted Zone infrastructure logical_date=%s", logical_date)
        self.ensure_clickhouse_targets()
        self.ensure_mongo_targets()
        self.ensure_minio_prefixes()
        self.validate_persistent_landing_inputs(logical_date)
