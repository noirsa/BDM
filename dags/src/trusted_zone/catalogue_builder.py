from __future__ import annotations

from typing import Any

from src.catalog.json_metadata import get_deep_keys
from src.utils import get_storage_options

from .base import BaseTrustedZoneService
from .clickhouse_writer import ClickHouseTrustedWriter
from .governance import CATALOGUE_POLICY_FIELDS, GOVERNANCE_METADATA_FIELDS, catalogue_policy_metadata, governance_metadata, is_allowed_image_key
from .quality_checks import TrustedQualityChecks


class TrustedCatalogueBuilder(BaseTrustedZoneService):
    """Builder for catalogue records over Trusted Zone outputs."""

    def __init__(self, minio_client: Any | None = None, duckdb_client: Any | None = None):
        super().__init__(minio_client=minio_client, duckdb_client=duckdb_client)
        self.quality_checks = TrustedQualityChecks(minio_client=minio_client, duckdb_client=duckdb_client)
        self.clickhouse_writer = ClickHouseTrustedWriter(minio_client=minio_client, duckdb_client=duckdb_client)

    def collect_structured_metadata(self, database_name: str = "bi_analytics") -> list[dict[str, Any]]:
        """Collect schema and row counts for trusted structured ClickHouse tables."""
        client = self.clickhouse_writer.get_client(database_name=database_name)
        try:
            tables = client.query(
                """
                SELECT table, sum(rows)
                FROM system.parts
                WHERE database = {database:String} AND active = 1
                GROUP BY table
                ORDER BY table
                """,
                parameters={"database": database_name},
            ).result_rows
            records: list[dict[str, Any]] = []
            for table_name, row_count in tables:
                columns = client.query(
                    """
                    SELECT name, type
                    FROM system.columns
                    WHERE database = {database:String} AND table = {table:String}
                    ORDER BY position
                    """,
                    parameters={"database": database_name, "table": table_name},
                ).result_rows
                schema_column_names = {name for name, _ in columns}
                lineage = {}
                if {"source_file_path", "schema_version", "validation_status", "ingestion_time"}.issubset(schema_column_names):
                    lineage_rows = client.query(
                        f"""
                        SELECT any(source_file_path), any(schema_version), any(validation_status), max(ingestion_time)
                        FROM {database_name}.{table_name}
                        """
                    ).result_rows
                    if lineage_rows:
                        lineage = {
                            "source_location": lineage_rows[0][0],
                            "schema_version": lineage_rows[0][1],
                            "validation_status": lineage_rows[0][2],
                            "updated_at": lineage_rows[0][3],
                        }
                records.append(
                    {
                        "zone": "trusted",
                        "asset_type": "structured_table",
                        "storage_system": "clickhouse",
                        "source_type": "structured",
                        "dataset_name": table_name,
                        "target_name": f"{database_name}.{table_name}",
                        "trusted_location": f"clickhouse://{database_name}/{table_name}",
                        "record_count": int(row_count or 0),
                        "file_count": None,
                        "column_or_key_count": len(columns),
                        "schema_json": {name: ch_type for name, ch_type in columns},
                        "source_path": lineage.get("source_location", f"s3a://landing-zone/persistent-landing/structured/{table_name}/"),
                        "source_location": lineage.get("source_location", f"s3a://landing-zone/persistent-landing/structured/{table_name}/"),
                        "schema_version": lineage.get("schema_version", "trusted_v1"),
                        "validation_status": lineage.get("validation_status", "valid"),
                        "created_at": lineage.get("updated_at"),
                        "updated_at": lineage.get("updated_at"),
                        "upstream_zone": "persistent_landing",
                        "downstream_usage": "exploitation_zone_structured_marts",
                    }
                )
            return records
        finally:
            client.close()

    def collect_semistructured_metadata(
        self,
        mongo_database_name: str = "trusted_zone_semi-structured",
    ) -> list[dict[str, Any]]:
        """Collect schema and document counts for trusted MongoDB collections."""
        import os

        from pymongo import MongoClient

        # Use the Trusted Zone MongoDB writer by default. The root URI fallback
        # is preserved for maintenance.
        client = MongoClient(os.getenv("MONGODB_TRUSTED_URI", os.getenv("MONGODB_URI", "mongodb://mongo:mongo@mongo:27017")))
        try:
            database = client[mongo_database_name]
            records: list[dict[str, Any]] = []
            for collection_name in sorted(database.list_collection_names()):
                sample = database[collection_name].find_one({}, {"_id": 0}) or {}
                nested_keys, _ = get_deep_keys(sample) if sample else ([], 0)
                validation_status = "rejected" if collection_name.endswith("_rejected") else sample.get("validation_status", "valid")
                records.append(
                    {
                        "zone": "trusted",
                        "asset_type": "semi_structured_collection",
                        "storage_system": "mongodb",
                        "source_type": "semi_structured",
                        "dataset_name": collection_name,
                        "target_name": f"{mongo_database_name}.{collection_name}",
                        "trusted_location": f"mongodb://{mongo_database_name}/{collection_name}",
                        "record_count": database[collection_name].count_documents({}),
                        "file_count": None,
                        "column_or_key_count": len(nested_keys),
                        "schema_json": nested_keys,
                        "source_path": sample.get("source_file_path", "s3a://landing-zone/persistent-landing/semistructured/"),
                        "source_location": sample.get("source_file_path", "s3a://landing-zone/persistent-landing/semistructured/"),
                        "schema_version": sample.get("schema_version", f"{collection_name}_v1"),
                        "validation_status": validation_status,
                        "created_at": sample.get("ingestion_time") or sample.get("rejected_at"),
                        "updated_at": sample.get("ingestion_time") or sample.get("rejected_at"),
                        "upstream_zone": "persistent_landing",
                        "downstream_usage": "exploitation_zone_semistructured_marts" if validation_status == "valid" else "quality_review",
                    }
                )
            return records
        finally:
            client.close()

    def collect_unstructured_metadata(self, catalog_path: str) -> list[dict[str, Any]]:
        """Collect metadata from trusted image/catalog outputs."""
        spark_session = self.create_spark_session("trusted_zone_catalogue_read", include_delta=True)
        try:
            dataframe = spark_session.read.format("delta").load(catalog_path)
            return [
                {
                    "zone": "trusted",
                    "asset_type": "unstructured_catalogue",
                    "storage_system": "minio_delta",
                    "source_type": "unstructured",
                    "dataset_name": "image",
                    "target_name": catalog_path,
                    "trusted_location": catalog_path,
                    "record_count": dataframe.count(),
                    "file_count": dataframe.count(),
                    "column_or_key_count": len(dataframe.columns),
                    "schema_json": {field.name: field.dataType.simpleString() for field in dataframe.schema.fields},
                    "source_path": "s3a://landing-zone/persistent-landing/structured/file_catalog/",
                    "source_location": "s3a://landing-zone/persistent-landing/structured/file_catalog/",
                    "schema_version": "unstructured_image_v1",
                    "validation_status": "valid",
                    "created_at": None,
                    "updated_at": None,
                    "upstream_zone": "persistent_landing",
                    "downstream_usage": "exploitation_zone_image_vectorization",
                }
            ]
        finally:
            spark_session.stop()

    def build_catalogue_dataframe(self, logical_date: Any) -> Any:
        """Combine structured, semi-structured, and unstructured metadata."""
        records: list[dict[str, Any]] = []
        for collector in (
            self.collect_structured_metadata,
            self.collect_semistructured_metadata,
            lambda: self.collect_unstructured_metadata("s3a://trusted-zone/file_catalog/"),
        ):
            try:
                records.extend(collector())
            except Exception as exc:
                self.logger.warning("Trusted catalogue collector failed: %s", exc, exc_info=True)
        for record in records:
            record["logical_date"] = self.logical_date_string(logical_date)
            metadata = governance_metadata(
                source_system="trusted-zone",
                ingestion_time=self.logical_date_string(logical_date),
                source_file_path=str(record.get("source_path", "")),
                validation_status=str(record.get("validation_status", "valid")),
                schema_version=str(record.get("schema_version", "trusted_v1")),
            )
            for field_name, field_value in metadata.items():
                record.setdefault(field_name, field_value)
            for field_name, field_value in catalogue_policy_metadata(record).items():
                record.setdefault(field_name, field_value)
        return records

    def write_catalogue(self, catalogue_dataframe: Any, target_path: str) -> None:
        """Write/update the Trusted Zone catalogue idempotently."""
        import json

        import pandas as pd
        from deltalake import write_deltalake

        if not catalogue_dataframe:
            self.logger.info("No trusted catalogue summary records to write")
            return
        records = []
        for record in catalogue_dataframe:
            normalized = dict(record)
            normalized["schema_json"] = json.dumps(normalized.get("schema_json", {}), ensure_ascii=False, sort_keys=True)
            records.append(normalized)
        write_deltalake(
            target_path,
            pd.DataFrame(records),
            mode="overwrite",
            schema_mode="merge",
            storage_options=get_storage_options(),
        )
        self.quality_checks.validate_write_result(target_path, len(records))

    def build_all(self, logical_date: Any) -> None:
        """Run full Trusted Zone catalogue construction."""
        self.logger.info("Run trusted catalogue construction logical_date=%s", logical_date)
        self.build_trusted_image_catalogue(logical_date)
        records = self.build_catalogue_dataframe(logical_date)
        self.write_catalogue(records, "s3://trusted-zone/catalogue/summary/")

    def build_trusted_image_catalogue(self, logical_date: Any) -> None:
        """Mirror the trusted image catalogue construction notebook."""
        import hashlib
        import io
        import json
        import os

        import pandas as pd
        from deltalake import write_deltalake
        from PIL import Image

        expected_columns = [
            "file_id",
            "landing_file_id",
            "source_type",
            "file_type",
            "event_time",
            "record_count",
            "metadata_blob",
            "processed_at",
            *GOVERNANCE_METADATA_FIELDS,
            *CATALOGUE_POLICY_FIELDS,
        ]
        records: list[dict[str, Any]] = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket="trusted-zone", Prefix="unstructured/image/"):
            for obj in page.get("Contents", []):
                src_key = obj["Key"]
                if src_key.endswith("/") or obj["Size"] == 0:
                    self.logger.warning("Invalid trusted image skipped source=s3://trusted-zone/%s reason=empty_or_directory", src_key)
                    continue
                if not is_allowed_image_key(src_key):
                    self.logger.warning("Invalid trusted image skipped source=s3://trusted-zone/%s reason=unsupported_extension", src_key)
                    continue
                metadata: dict[str, Any] = {}
                try:
                    response = self.s3_client.get_object(Bucket="trusted-zone", Key=src_key)
                    try:
                        content = response["Body"].read()
                    finally:
                        response["Body"].close()
                    image = Image.open(io.BytesIO(content))
                    width, height = image.size
                    metadata = response.get("Metadata", {})
                    metadata_blob = {
                        "label": metadata.get("label"),
                        "url": metadata.get("url"),
                        "file_size_bytes": obj["Size"],
                        "content_type": response.get("ContentType"),
                        "width": width,
                        "height": height,
                        "aspect_ratio": round(width / height, 2) if height > 0 else 0,
                        "image_mode": image.mode,
                        "is_corrupted": False,
                        "md5": hashlib.md5(content).hexdigest(),
                    }
                except Exception as exc:
                    self.logger.warning("Error parsing trusted image %s: %s", src_key, exc)
                    continue
                filename = os.path.basename(src_key)
                name, _ = os.path.splitext(filename)
                source_path = f"s3://trusted-zone/{src_key}"
                policy_metadata = catalogue_policy_metadata(
                    {
                        "dataset_name": "image",
                        "source_type": metadata.get("source", "unstructured"),
                        "validation_status": "valid",
                    }
                )
                records.append(
                    {
                        "file_id": filename,
                        "landing_file_id": f"landing-zone/persistent-landing/unstructured/image/{name}.jpg",
                        "source_type": metadata.get("source", "unknown"),
                        "file_type": "Image",
                        "event_time": self.logical_date_string(logical_date),
                        "record_count": 1,
                        "metadata_blob": json.dumps(metadata_blob, ensure_ascii=False),
                        "processed_at": self.logical_date_string(logical_date),
                        **governance_metadata(
                            source_system="trusted-zone",
                            ingestion_time=self.logical_date_string(logical_date),
                            source_file_path=source_path,
                            validation_status="valid",
                        ),
                        **policy_metadata,
                    }
                )
        if not records:
            self.logger.info("No trusted images found for catalogue construction")
            return
        dataframe = pd.DataFrame(records)
        for column in expected_columns:
            if column not in dataframe.columns:
                dataframe[column] = None
        dataframe = dataframe[expected_columns].fillna(
            {
                "file_id": "",
                "landing_file_id": "",
                "source_type": "unknown",
                "file_type": "Image",
                "metadata_blob": "{}",
                "owner": "data_engineering_team",
                "data_steward": "bdm_project_team",
                "data_classification": "public_image_metadata",
                "pii_flag": "no_direct_pii",
                "retention_policy": "course_project_retained_until_assessment_archive",
            }
        )
        write_deltalake(
            "s3://trusted-zone/persistent-landing/structured/file_catalog/",
            dataframe,
            mode="overwrite",
            schema_mode="merge",
            storage_options=get_storage_options(),
        )
        self.logger.info("Trusted image catalogue written with %s records", len(dataframe))
