from __future__ import annotations

from typing import Any

from .base import BaseTrustedZoneService
from .catalogue_builder import TrustedCatalogueBuilder
from .infrastructure import TrustedZoneInfrastructure
from .semi_structured_cleaner import SemiStructuredTrustedCleaner
from .structured_cleaner import StructuredTrustedCleaner
from .unstructured_cleaner import UnstructuredTrustedCleaner


class TrustedZonePipeline(BaseTrustedZoneService):
    """Facade used by trusted_zone_DAG.py.

    Keep Airflow DAG files focused on orchestration. Notebook logic lives in
    the specialized Trusted Zone classes referenced here.
    """

    def __init__(self, minio_client: Any, duckdb_client: Any):
        super().__init__(minio_client=minio_client, duckdb_client=duckdb_client)
        self.infrastructure = TrustedZoneInfrastructure(minio_client=minio_client, duckdb_client=duckdb_client)
        self.structured_cleaner = StructuredTrustedCleaner(minio_client=minio_client, duckdb_client=duckdb_client)
        self.semistructured_cleaner = SemiStructuredTrustedCleaner(minio_client=minio_client, duckdb_client=duckdb_client)
        self.unstructured_cleaner = UnstructuredTrustedCleaner(minio_client=minio_client, duckdb_client=duckdb_client)
        self.catalogue_builder = TrustedCatalogueBuilder(minio_client=minio_client, duckdb_client=duckdb_client)

    def bootstrap_trusted_zone(self, logical_date: Any) -> None:
        """Create/check trusted-zone databases, buckets, folders, and source roots."""
        self.logger.info("Trusted bootstrap for logical_date=%s", logical_date)
        self.infrastructure.bootstrap(logical_date)

    def clean_structured_data(self, logical_date: Any) -> None:
        """Run Trusted Zone structured notebook logic in production code."""
        self.logger.info("Trusted structured cleaning for logical_date=%s", logical_date)
        self.structured_cleaner.clean_all(logical_date)

    def clean_semistructured_data(self, logical_date: Any) -> None:
        """Run semi-structured JSON cleaning and flattening logic."""
        self.logger.info("Trusted semi-structured cleaning for logical_date=%s", logical_date)
        self.semistructured_cleaner.clean_all(logical_date)

    def clean_unstructured_data(self, logical_date: Any) -> None:
        """Run image/unstructured trusted cleaning logic."""
        self.logger.info("Trusted unstructured cleaning for logical_date=%s", logical_date)
        self.unstructured_cleaner.clean_all(logical_date)

    def build_trusted_catalogue(self, logical_date: Any) -> None:
        """Build catalogue tables over trusted-zone outputs."""
        self.logger.info("Trusted catalogue construction for logical_date=%s", logical_date)
        self.catalogue_builder.build_all(logical_date)
