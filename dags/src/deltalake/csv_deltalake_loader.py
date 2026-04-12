import time
from deltalake import DeltaTable, write_deltalake
import polars as pl
from .base_deltalakeloader import BaseDeltalakeLoader
import duckdb

class CSVDeltalakeLoader(BaseDeltalakeLoader):

    def load_and_transform(self, bucket, prefix):

        pending_keys = self.minio_client.get_pending_keys(bucket, prefix=prefix)
        for key in pending_keys:
            timestamp = int(time.time())
            folder = "/".join(key.split("/")[:-2])
            filename = key.split("/")[-1]


            dataset_name ="_".join(filename.split("_")[:-1])

            dest_name = f"{folder}/{dataset_name }_{str(timestamp)}.parquet"
            self.logger.debug(f"Processing: {dataset_name}...")

            self.duckdb_client.convert_csv_to_parquet(bucket=bucket,src_s3_path= key, dest_s3_path=dest_name)

            target_delta_folder  = f"s3://{bucket}/{folder}/{dataset_name}/"
            df = pl.read_parquet(f"s3://{bucket}/{dest_name}", storage_options=self.storage_options)
            write_deltalake(
                target_delta_folder,
                df,
                mode="overwrite",
                storage_options=self.storage_options
            )

            self.minio_client.delete_object(bucket, dest_name)
            self.logger.info(f"CHECK PATH: '{target_delta_folder}'")
            processed = self.duckdb_client.final_verification(f"s3://{bucket}/{key}",target_delta_folder)
            if processed:
                self.minio_client.delete_object(bucket, key)
            self.logger.debug(f"Processed: {dataset_name} {processed}...")