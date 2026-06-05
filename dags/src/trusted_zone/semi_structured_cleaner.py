from __future__ import annotations

import json
import os
import re
from typing import Any

from src.utils import compact_date_partition, load_kafka_config

from .base import BaseTrustedZoneService
from .governance import governance_metadata, has_required_keys
from .quality_checks import TrustedQualityChecks
from .rejected_writer import TrustedRejectedWriter


class SemiStructuredTrustedCleaner(BaseTrustedZoneService):
    """Cleaner for Kafka JSON semi-structured data."""

    REQUIRED_FIELDS: dict[str, list[str]] = {
        "weather_barcelona": ["time", "temperature"],
        "airquality_barcelona": ["id", "name", "locality", "coordinates"],
    }

    def __init__(self, minio_client: Any | None = None, duckdb_client: Any | None = None):
        super().__init__(minio_client=minio_client, duckdb_client=duckdb_client)
        self.quality_checks = TrustedQualityChecks(minio_client=minio_client, duckdb_client=duckdb_client)
        self.rejected_writer = TrustedRejectedWriter(minio_client=minio_client, duckdb_client=duckdb_client)

    def source_prefixes(self, logical_date: Any) -> list[str]:
        """Build source prefixes from kafka.yaml and the ingestion DAG layout."""
        date_folder = compact_date_partition(logical_date)
        topic_configs = load_kafka_config()
        prefixes = [
            f"persistent-landing/semistructured/{topic_config['name']}/{date_folder}/"
            for topic_config in topic_configs
        ]
        self.logger.info("Semi-structured source prefixes: %s", prefixes)
        return prefixes

    def discover_json_files(self, bucket_name: str, source_prefix: str) -> list[str]:
        """List JSON/NDJSON files below one semi-structured source prefix."""
        keys = [
            key
            for key in self.list_object_keys(bucket_name, source_prefix)
            if key.endswith((".json", ".ndjson")) and "/_tmp/" not in key and not key.endswith(".tmp")
        ]
        self.logger.info("Discovered %s JSON files under %s/%s", len(keys), bucket_name, source_prefix)
        return keys

    def read_json_records(self, bucket_name: str, object_key: str) -> list[dict[str, Any]]:
        """Read and parse one JSON object from MinIO."""
        response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        try:
            payload = response["Body"].read().decode("utf-8")
        finally:
            response["Body"].close()
        payload = payload.strip()
        if not payload:
            return []
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            parsed = [json.loads(line) for line in payload.splitlines() if line.strip()]
        if isinstance(parsed, dict):
            return [parsed]
        if isinstance(parsed, list):
            return parsed
        raise ValueError(f"Unsupported JSON payload in {bucket_name}/{object_key}: {type(parsed)!r}")

    def normalize_records(self, records: list[dict[str, Any]], topic_name: str) -> list[dict[str, Any]]:
        """Normalize nested records according to the trusted semi-structured notebook."""
        table_name = self.clean_table_name(topic_name)
        normalized_records: list[dict[str, Any]] = []
        flattened_records = self.flatten_airquality_snapshots(records) if table_name == "airquality_barcelona" else records
        for record in flattened_records:
            if not isinstance(record, dict):
                continue
            normalized: dict[str, Any] = {}
            for key, value in record.items():
                clean_key = self.clean_column_name(key)
                if isinstance(value, str):
                    value = value.strip().lower()
                elif table_name == "airquality_barcelona" and isinstance(value, (dict, list)):
                    value = json.dumps(value, ensure_ascii=False)
                normalized[clean_key] = value
            if table_name == "airquality_barcelona":
                self.add_airquality_flat_fields(record, normalized)
            normalized_records.append(normalized)
        return normalized_records

    def add_airquality_flat_fields(self, record: dict[str, Any], normalized: dict[str, Any]) -> None:
        """Add stable flattened fields while keeping original nested JSON values."""
        for source_key, target_prefix in (
            ("country", "country"),
            ("owner", "owner"),
            ("provider", "provider"),
        ):
            nested = record.get(source_key)
            if isinstance(nested, dict):
                for nested_key in ("id", "name", "code"):
                    if nested_key in nested:
                        value = nested[nested_key]
                        normalized[f"{target_prefix}_{nested_key}"] = value.strip().lower() if isinstance(value, str) else value
        coordinates = record.get("coordinates")
        if isinstance(coordinates, dict):
            normalized["latitude"] = coordinates.get("latitude")
            normalized["longitude"] = coordinates.get("longitude")

    def write_to_mongo(
        self,
        collection_name: str,
        records: list[dict[str, Any]],
        partition_date: str,
    ) -> None:
        """Write trusted semi-structured records to MongoDB."""
        if not records:
            self.logger.info("No records to write for MongoDB collection=%s partition=%s", collection_name, partition_date)
            return
        from pymongo import MongoClient

        client = MongoClient(os.getenv("MONGODB_TRUSTED_URI", os.getenv("MONGODB_URI", "mongodb://mongo:mongo@mongo:27017")))
        try:
            database = client[os.getenv("TRUSTED_SEMISTRUCTURED_MONGO_DB", "trusted_zone_semi-structured")]
            collection = database[collection_name]
            collection.delete_many({"trusted_partition_date": partition_date})
            batch_size = int(os.getenv("TRUSTED_MONGO_BATCH_SIZE", "5000"))
            for index in range(0, len(records), batch_size):
                collection.insert_many(records[index : index + batch_size], ordered=False)
            collection.create_index("trusted_partition_date")
            self.logger.info("Loaded %s records into MongoDB collection=%s partition=%s", len(records), collection_name, partition_date)
        finally:
            client.close()

    def validate_records(
        self,
        records: list[dict[str, Any]],
        table_name: str,
        source_path: str,
        logical_date: Any,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Keep only valid semi-structured records before writing trusted data."""
        required_fields = self.REQUIRED_FIELDS.get(table_name, [])
        valid_records: list[dict[str, Any]] = []
        rejected_records: list[dict[str, Any]] = []
        for record in records:
            if has_required_keys(record, required_fields):
                valid_records.append(record)
            else:
                rejected_records.append(
                    {
                        "zone": "trusted",
                        "asset_type": "semi_structured_document",
                        "dataset_name": table_name,
                        "reason": "missing_required_fields",
                        "required_fields": required_fields,
                        "record_keys": sorted(record),
                        "source_file_path": source_path,
                        "rejected_at": self.logical_date_string(logical_date),
                        "raw_record": record,
                        **governance_metadata(
                            source_system="kafka",
                            ingestion_time=self.logical_date_string(logical_date),
                            source_file_path=source_path,
                            validation_status="rejected",
                            schema_version=f"{table_name}_v1",
                        ),
                    }
                )
                self.logger.warning(
                    "Invalid semi-structured record skipped table=%s source=%s required_fields=%s record_keys=%s",
                    table_name,
                    source_path,
                    required_fields,
                    sorted(record),
                )
        return valid_records, rejected_records

    def write_rejected_records(self, table_name: str, rejected_records: list[dict[str, Any]]) -> None:
        """Write invalid semi-structured records into a dedicated rejected collection."""
        database_name = os.getenv("TRUSTED_SEMISTRUCTURED_MONGO_DB", "trusted_zone_semi-structured")
        self.rejected_writer.write_mongo_rejections(
            database_name=database_name,
            collection_name=f"{table_name}_rejected",
            records=rejected_records,
        )

    def clean_topic(self, logical_date: Any, topic_config: dict[str, Any]) -> None:
        """Clean one Kafka topic into the Trusted Zone."""
        topic_name = topic_config["name"]
        date_folder = compact_date_partition(logical_date)
        prefix = f"persistent-landing/semistructured/{topic_name}/{date_folder}/"
        object_keys = self.discover_json_files("landing-zone", prefix)
        if not object_keys:
            self.logger.info("No semi-structured files for topic=%s partition=%s; skipping", topic_name, date_folder)
            return

        normalized: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for object_key in object_keys:
            # Streaming DAG files are stored under topic/date partitions; this
            # intentionally differs from the exploratory notebook aggregate JSON.
            source_path = f"s3://landing-zone/{object_key}"
            try:
                source_records = self.read_json_records("landing-zone", object_key)
            except (json.JSONDecodeError, ValueError) as exc:
                self.logger.error("Invalid semi-structured file skipped source=%s error=%s", source_path, exc)
                self.rejected_writer.write_minio_event(
                    domain="semi_structured",
                    dataset_name=self.clean_table_name(topic_name),
                    logical_date=logical_date,
                    event={
                        "reason": "invalid_json_payload",
                        "source_file_path": source_path,
                        "error": str(exc),
                    },
                )
                continue

            normalized_records = self.normalize_records(source_records, topic_name)
            normalized_records, rejected_records = self.validate_records(
                normalized_records,
                self.clean_table_name(topic_name),
                source_path,
                logical_date,
            )
            rejected.extend(rejected_records)
            for record in normalized_records:
                record["trusted_partition_date"] = date_folder
                record["trusted_source_topic"] = topic_name
                record.update(
                    governance_metadata(
                        source_system="kafka",
                        ingestion_time=self.logical_date_string(logical_date),
                        source_file_path=source_path,
                        validation_status="valid",
                    )
                )
            normalized.extend(normalized_records)
        self.write_rejected_records(self.clean_table_name(topic_name), rejected)
        self.quality_checks.validate_non_empty_records(normalized, self.clean_table_name(topic_name))
        self.write_to_mongo(self.clean_table_name(topic_name), normalized, date_folder)

    def clean_all(self, logical_date: Any) -> None:
        """Clean all configured semi-structured topics."""
        self.logger.info("Run semi-structured trusted cleaning logical_date=%s", logical_date)
        for topic_config in load_kafka_config():
            self.clean_topic(logical_date, topic_config)

    @staticmethod
    def clean_column_name(name: str) -> str:
        cleaned = str(name).encode("ascii", "ignore").decode()
        cleaned = cleaned.lower().strip()
        cleaned = re.sub(r"[^\w]+", "_", cleaned)
        cleaned = re.sub(r"_+", "_", cleaned)
        return cleaned.strip("_")

    @staticmethod
    def clean_table_name(name: str) -> str:
        cleaned = name.replace("-", "_")
        cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", cleaned)
        return cleaned.lower().strip("_")

    @staticmethod
    def flatten_airquality_snapshots(records: list[Any]) -> list[dict[str, Any]]:
        stations: list[dict[str, Any]] = []
        for record in records:
            snapshots = record if isinstance(record, list) and record and isinstance(record[0], list) else [record]
            for snapshot in snapshots:
                if isinstance(snapshot, list):
                    stations.extend(item for item in snapshot if isinstance(item, dict))
                elif isinstance(snapshot, dict):
                    stations.append(snapshot)
        return stations
