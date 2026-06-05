from __future__ import annotations

import hashlib
import json
from typing import Any

from .base import BaseTrustedZoneService


class TrustedRejectedWriter(BaseTrustedZoneService):
    """Persist rejected Trusted Zone records/events for audit and replay."""

    def write_minio_event(
        self,
        *,
        domain: str,
        dataset_name: str,
        logical_date: Any,
        event: dict[str, Any],
    ) -> str:
        """Write a single rejected event as JSON under trusted-zone/rejected/."""
        logical_date_text = self.logical_date_string(logical_date)
        payload = {
            "zone": "trusted",
            "validation_status": "rejected",
            "domain": domain,
            "dataset_name": dataset_name,
            "logical_date": logical_date_text,
            **event,
        }
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()[:16]
        safe_date = logical_date_text.replace(":", "").replace("-", "").replace("+", "z")
        safe_dataset = "".join(char if char.isalnum() or char in {"_", "-"} else "_" for char in dataset_name)
        key = f"rejected/{domain}/{safe_dataset}/{safe_date}_{digest}.json"
        self.ensure_bucket("trusted-zone")
        self.s3_client.put_object(
            Bucket="trusted-zone",
            Key=key,
            Body=encoded,
            ContentType="application/json",
        )
        self.logger.warning("Rejected Trusted Zone event written to s3://trusted-zone/%s", key)
        return f"s3://trusted-zone/{key}"

    def write_mongo_rejections(
        self,
        *,
        database_name: str,
        collection_name: str,
        records: list[dict[str, Any]],
    ) -> None:
        """Write rejected semi-structured records into MongoDB."""
        if not records:
            return
        import os

        from pymongo import MongoClient

        client = MongoClient(os.getenv("MONGODB_TRUSTED_URI", os.getenv("MONGODB_URI", "mongodb://mongo:mongo@mongo:27017")))
        try:
            collection = client[database_name][collection_name]
            collection.insert_many(records, ordered=False)
            collection.create_index("rejected_at")
            collection.create_index("source_file_path")
            self.logger.warning("Wrote %s rejected documents into MongoDB %s.%s", len(records), database_name, collection_name)
        finally:
            client.close()
