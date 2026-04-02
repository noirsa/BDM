
import time
from datetime import datetime

from deltalake import DeltaTable, write_deltalake
import polars as pl
from .base_deltalakeloader import BaseDeltalakeLoader
from ..utils import ImageParser
import os
import json
import pandas as pd


def _extract_timestamp_from_filename(filename):
    # Strip extension and split by underscore
    name_part = os.path.splitext(filename)[0]
    raw_ts = name_part.split('_')[-1]

    try:
        # Convert string epoch to a readable datetime object
        dt_object = datetime.fromtimestamp(int(raw_ts))
        return dt_object
    except (ValueError, IndexError):
        # Fallback if the filename doesn't follow the pattern
        return datetime.now()


class CatalogBuilder(BaseDeltalakeLoader):

    def run_image_ingestion(self, bucket, prefix, target_path):
        """
        Executes a specialized ingestion task to catalog image files.
        It scans the S3 prefix, extracts technical metadata using an external parser,
        and appends the results to a Delta Lake table.

        param:
            bucket: The name of the S3/MinIO bucket to scan.
            prefix: The directory prefix to filter image objects.
            target_path: The destination Delta Table path (e.g., s3://catalog/images).
        """

        self.logger.info(f"Starting adding images in : s3://{bucket}/{prefix} to catalog : {target_path}")
        try:

            paginator = self.minio_client.client.get_paginator("list_objects_v2")
            processed_keys = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):

                page_records = []
                for obj in page.get("Contents", []):
                    key = obj["Key"]

                    # Skip directory placeholders and empty files
                    if key.endswith("/") or obj.get('Size', 0) == 0:
                        continue

                    try:
                        # Fetch object data using the underlying client
                        response = self.minio_client.client.get_object(Bucket=bucket, Key=key)
                        content = response['Body'].read()
                        s3_metadata = response.get('Metadata', {})
                        if s3_metadata.get("status") == "processed":
                            self.logger.debug(f"Skipping already processed file: {key}")
                            continue
                        # Extract technical metadata via external ImageParser
                        # Expected to return a dict with: width, height, md5, etc.
                        metadata_blob = ImageParser.to_metadata_blob(content, s3_metadata, "image")


                        # Construct the standardized Catalog Row
                        filename = os.path.basename(key)
                        record = {
                            "file_id": filename,
                            "source_type": s3_metadata.get('source'),
                            "file_type": "Image",
                            "event_time": _extract_timestamp_from_filename(filename),
                            "record_count": 1,
                            "metadata_blob": json.dumps(metadata_blob),
                            "processed_at": pd.Timestamp.now()
                        }
                        page_records.append(record)
                        self.logger.debug(f"Processed image: {filename}")
                        processed_keys.append(key)
                    except Exception as e:
                        self.logger.error(f"Failed to parse image {key}: {str(e)}")
                        continue

                    # 5. Batch commit to Delta Lake per page for memory efficiency
                if page_records:
                    metadata_df = pd.DataFrame(page_records)
                    write_deltalake(
                        "s3://landing-zone/persistent-landing/structured/file_catalog/",
                        metadata_df,
                        mode="append",
                        schema_mode="merge",
                        storage_options=self.storage_options
                    )
                    for key in processed_keys:
                        self.minio_client.mark_as_processed(bucket, key)
                    self.logger.info(f"Batch uploaded: {len(page_records)} images.")
            self.logger.info(f"Image ingestion completed for s3://{bucket}/{prefix}")

        except Exception as e:
            self.logger.exception(f"Catalog ingestion failed: {str(e)}")
        raise

