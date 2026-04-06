from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
import os









@dag(
    dag_id='daily_catalog_update',
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    is_paused_upon_creation=False,

    tags=["catalog", "metadata"]
)
def daily_catalog_update():
    @task()
    def build_catalog_for_day():
        """
        Scan daily JSON files for a specific topic, extract metadata,
        and batch write to a Delta Lake catalog table.
        """
        from src.utils import kafka_config,storage_options
        from src import minio_client
        from src.utils import get_logger,get_deep_keys
        import json
        from deltalake import DeltaTable, write_deltalake

        import pandas as pd
        logger = get_logger(__name__)
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y_%m_%d")
        for config in kafka_config:
            name = config["name"]
            prefix = f"persistent-landing/semistructured/{name}/{yesterday}/"
            bucket = "landing-zone"
            paginator = minio_client.client.get_paginator("list_objects_v2")
            all_metadata = []
            try:
                pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
                for page in pages:
                    if "Contents" not in page:
                        logger.warning(f"No files found for prefix: {prefix}")
                        continue

                    for obj in page.get("Contents", []):
                        file_key = obj["Key"]
                        if obj['Size'] == 0:
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

                            # Parse Event Time from filename (expects timestamp at the end)
                            filename = os.path.basename(file_key)
                            try:
                                # Example filename: 2026_04_06_1712415600.json
                                raw_ts = os.path.splitext(filename)[0].split('_')[-1]
                                event_time = datetime.fromtimestamp(int(raw_ts))
                            except Exception as ts_err:
                                logger.error(f"Failed to parse timestamp from {filename}: {ts_err}")
                                event_time = datetime.now()

                            # Construct the metadata blob (JSON string for flexible schema)
                            metadata_blob = {
                                "nesting_level": level,
                                "schema_keys": keys_list,
                                "file_size_bytes": obj['Size']
                            }

                            # Append to local list for batching
                            all_metadata.append({
                                "file_id": filename,
                                "source_type": name,
                                "file_type": "JSON",
                                "event_time": event_time,
                                "record_count": record_count,
                                "metadata_blob": json.dumps(metadata_blob),
                                "processed_at": pd.Timestamp.now()
                            })

                        except Exception as file_err:
                            logger.Exception(f"Error processing file {file_key}: {file_err}")
                if all_metadata:
                    df = pd.DataFrame(all_metadata)
                    catalog_path = "s3://landing-zone/persistent-landing/structured/file_catalog/"

                    logger.info(f"Writing {len(all_metadata)} rows to Delta Table at {catalog_path}")

                    write_deltalake(
                        catalog_path,
                        df,
                        mode="append",
                        schema_mode="merge",
                        # Partitioning by source_type (topic) significantly improves query performance
                        partition_by=["source_type"],
                        storage_options=storage_options
                    )
                    logger.info(f"Successfully updated catalog for {name}")
                else:
                    logger.info(f"No records found to catalog for {name}")

            except Exception as e:
                logger.critical(f"Catalog task failed for topic {name}: {e}")
                raise  # Re-raise to trigger Airflow retry
    build_catalog_for_day()

daily_catalog_update()