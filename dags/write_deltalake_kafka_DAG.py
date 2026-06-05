from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
import os









@dag(
    dag_id='daily_kafka_data_catalog_update',
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    is_paused_upon_creation=False,

    tags=["catalog", "metadata"]
)
def daily_catalog_update():
    @task(retries=5, retry_delay=timedelta(minutes=5))
    def check_catalog_integrity():
        """Ensure the Delta Lake catalog storage is accessible."""
        from src import get_minio_client
        minio_client = get_minio_client(role="writer")
        minio_client.client.head_bucket(Bucket="landing-zone")
        return True
    @task()
    def build_catalog_by_folder(**context):
        """
        Scan daily JSON files for a specific topic, extract metadata,
        and batch write to a Delta Lake catalog table.
        """
        from src.utils import load_kafka_config, get_storage_options
        from src import get_minio_client
        from src.utils import get_logger
        from src.utils.time_anchor import logical_date_from_context
        from src.catalog.json_metadata import get_deep_keys
        import json
        from deltalake import DeltaTable, write_deltalake

        import pandas as pd
        minio_client = get_minio_client(role="writer")
        kafka_config = load_kafka_config()
        storage_options = get_storage_options(role="writer")
        logical_date = logical_date_from_context(context)
        logger = get_logger(__name__)
        catalog_path = "s3://landing-zone/persistent-landing/structured/file_catalog/"
        processed_groups = set()
        try:
            dt = DeltaTable(catalog_path, storage_options=storage_options)
            existing_df = dt.to_pandas(
                columns=["file_path", "source_type"],
                filters=[("file_type", "==", "JSON")]
            )

            if not existing_df.empty:
                processed_groups = set(existing_df['file_path'])
                logger.info(f"Detected {len(processed_groups)} folders already in catalog.")
        except Exception as e:
            logger.info(f"Catalog table not found or empty, starting fresh. Error: {e}")
        for config in kafka_config:
            name = config["name"]
            prefix = f"persistent-landing/semistructured/{name}/"
            bucket = "landing-zone"
            paginator = minio_client.client.get_paginator("list_objects_v2")
            try:
                pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
                count = 1
                for page in pages:
                    all_metadata = []

                    if "Contents" not in page:
                        logger.warning(f"No files found for prefix: {prefix}")
                        continue

                    for obj in page.get("Contents", []):
                        file_key = obj["Key"]
                        if obj['Size'] == 0 or file_key in processed_groups:
                            continue

                        try:
                            # Read and Parse JSON content
                            response = minio_client.client.get_object(Bucket=bucket, Key=file_key)
                            content = json.loads(response['Body'].read().decode('utf-8'))

                            # Extract structural metadata (keys and nesting depth)
                            keys_list, level = get_deep_keys(content)

                            # Calculate record count by flattening nested lists
                            temp_data = content
                            for _ in range(level):
                                if isinstance(temp_data, list) and len(temp_data) > 0:
                                    temp_data = [item for sublist in temp_data for item in
                                                 (sublist if isinstance(sublist, list) else [sublist])]
                            record_count = len(temp_data)

                            # Use object storage time; Kafka filenames are offset-based for retry idempotency.
                            filename = os.path.basename(file_key)
                            event_time = obj.get("LastModified") or logical_date

                            # Construct the metadata blob (JSON string for flexible schema)
                            metadata_blob = {
                                "nesting_level": level,
                                "schema_keys": keys_list,
                                "file_size_bytes": obj['Size']
                            }

                            # Append to local list for batching
                            all_metadata.append({
                                "file_id": filename,
                                "file_path": file_key,
                                "source_type": name,
                                "file_type": "JSON",
                                "event_time": event_time,
                                "record_count": record_count,
                                "metadata_blob": json.dumps(metadata_blob),
                                "processed_at": pd.Timestamp(logical_date)
                            })

                        except Exception as file_err:
                            logger.exception(f"Error processing file {file_key}: {file_err}")
                    if all_metadata:
                        df = pd.DataFrame(all_metadata)
                        catalog_path = "s3://landing-zone/persistent-landing/structured/file_catalog/"

                        logger.info(f"Writing {len(all_metadata)} rows to Delta Table at {catalog_path}")

                        write_deltalake(
                            catalog_path,
                            df,
                            mode="append",
                            schema_mode="merge",
                            storage_options=storage_options
                        )
                        logger.info(f"Successfully updated catalog for {name} for page {count}")
                    else:
                        logger.info(f"No unprocessed records found to catalog for {name} for page {count}")
                    count += 1

            except Exception as e:
                logger.critical(f"Catalog task failed for topic {name}: {e}")
                raise  # Re-raise to trigger Airflow retry
    check_catalog_integrity() >> build_catalog_by_folder()

daily_catalog_update()
