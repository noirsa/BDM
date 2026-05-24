import os
from botocore.exceptions import ClientError

from src.utils import get_logger
from src.utils.time_anchor import compact_date_partition, logical_date_iso, logical_date_suffix


class LandingZoneRouter:
    """Routes temporal landing objects into the persistent landing layout."""

    def __init__(self, minio_client):
        self.minio_client = minio_client
        self.client = minio_client.client
        self.logger = get_logger(__name__)

    def _classify_object(self, bucket, key):
        head = self.client.head_object(Bucket=bucket, Key=key)
        content_type = head.get("ContentType", "")
        metadata = head.get("Metadata", {})

        if content_type.startswith("image/"):
            return "image", metadata
        if content_type in ("application/json", "application/x-ndjson"):
            return "semistructured", metadata
        return "structured", metadata

    def _build_destination_key(self, src_key, category, metadata, destination_prefix, logical_date):
        ext = src_key.split(".")[-1].split("?")[0]
        run_suffix = logical_date_suffix(logical_date)

        if category == "structured":
            landing_name = metadata.get("landing_name")
            base_name = os.path.splitext(os.path.basename(landing_name or src_key))[0]
            # Keep raw structured CSVs directly under raw/. The logical date remains
            # in the file name and metadata for deterministic retries.
            return f"{destination_prefix}structured/raw/{base_name}_{run_suffix}.{ext}"

        if category == "image":
            base_name = os.path.splitext(os.path.basename(src_key))[0]
            return f"{destination_prefix}unstructured/image/{base_name}_{run_suffix}.{ext}"

        base_name = os.path.splitext(os.path.basename(src_key))[0]
        date_folder = compact_date_partition(logical_date)
        return f"{destination_prefix}semistructured/{date_folder}/{base_name}_{run_suffix}.{ext}"

    def route(self, source_bucket, source_prefix, destination_bucket, destination_prefix, logical_date):
        paginator = self.client.get_paginator("list_objects_v2")
        count = 1

        for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
            for obj in page.get("Contents", []):
                src_key = obj["Key"]

                if obj["Size"] == 0 and src_key.endswith("/"):
                    continue

                try:
                    category, metadata = self._classify_object(source_bucket, src_key)
                    dest_key = self._build_destination_key(
                        src_key,
                        category,
                        metadata,
                        destination_prefix,
                        logical_date,
                    )
                    metadata["logical_date"] = metadata.get("logical_date") or logical_date_iso(logical_date)
                    self.logger.info("Routing %s object: %s -> %s", category, src_key, dest_key)
                    self.minio_client._move_file(source_bucket, src_key, destination_bucket, dest_key)
                except ClientError:
                    self.logger.exception("S3 error while routing object %s", src_key)
                    raise

            self.logger.info(
                "Successfully routed page %s of %s/%s to %s/%s",
                count,
                source_bucket,
                source_prefix,
                destination_bucket,
                destination_prefix,
            )
            count += 1
