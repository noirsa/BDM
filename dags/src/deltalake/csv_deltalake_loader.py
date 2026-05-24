from deltalake import write_deltalake
import polars as pl
from .base_deltalakeloader import BaseDeltalakeLoader
from src.utils.time_anchor import coerce_logical_date, logical_date_suffix

class CSVDeltalakeLoader(BaseDeltalakeLoader):
    @staticmethod
    def _metadata_int(metadata, key):
        value = metadata.get(key)
        if value in (None, ""):
            return None
        return int(value)

    def _validate_expected_shape(self, df, metadata, dataset_name):
        """Stop before Delta writes when the raw file shape differs from source metadata."""
        expected_rows = self._metadata_int(metadata, "expected_rows")
        expected_columns = self._metadata_int(metadata, "expected_columns")
        actual_rows, actual_columns = df.shape

        self.logger.info(
            "Validating converted dataframe for %s: rows=%s columns=%s expected_rows=%s expected_columns=%s",
            dataset_name,
            actual_rows,
            actual_columns,
            expected_rows,
            expected_columns,
        )

        if expected_rows is not None and actual_rows != expected_rows:
            raise ValueError(
                f"{dataset_name} row count mismatch after CSV conversion: expected {expected_rows}, got {actual_rows}"
            )
        if expected_columns is not None and actual_columns != expected_columns:
            raise ValueError(
                f"{dataset_name} column count mismatch after CSV conversion: expected {expected_columns}, got {actual_columns}"
            )

    def load_and_transform(self, bucket, prefix, logical_date=None):

        pending_keys = self.minio_client.get_pending_keys(bucket, prefix=prefix)
        for key in pending_keys:
            filename = key.split("/")[-1]

            head = self.minio_client.client.head_object(Bucket=bucket, Key=key)
            metadata = head.get("Metadata", {})
            dataset_name = metadata.get("table_name") or metadata.get("dataset_name") or "_".join(filename.split("_")[:-1])
            object_logical_date = coerce_logical_date(metadata.get("logical_date") or logical_date)
            run_suffix = logical_date_suffix(object_logical_date)
            structured_root = key.split("/raw/")[0] if "/raw/" in key else "/".join(key.split("/")[:-2])

            dest_name = f"{structured_root}/_staging/{dataset_name}_{run_suffix}.parquet"
            target_delta_folder = f"s3://{bucket}/{structured_root}/{dataset_name}/"
            source_csv = f"s3://{bucket}/{key}"
            self.logger.info("Processing structured raw file %s as dataset %s", key, dataset_name)

            if self.duckdb_client.final_verification(source_csv, target_delta_folder):
                self.logger.info(
                    "Existing Delta table already matches %s; cleaning raw CSV only.",
                    key,
                )
                self.minio_client.delete_object(bucket, key)
                continue

            self.duckdb_client.convert_csv_to_parquet(bucket=bucket,src_s3_path= key, dest_s3_path=dest_name)

            df = pl.read_parquet(f"s3://{bucket}/{dest_name}", storage_options=self.storage_options)
            self._validate_expected_shape(df, metadata, dataset_name)
            write_deltalake(
                target_delta_folder,
                df,
                mode="overwrite",
                storage_options=self.storage_options
            )

            self.minio_client.delete_object(bucket, dest_name)
            self.logger.info(f"CHECK PATH: '{target_delta_folder}'")
            processed = self.duckdb_client.final_verification(source_csv,target_delta_folder)
            if processed:
                self.minio_client.delete_object(bucket, key)
                self.logger.info("Deleted raw CSV after successful verification: %s", key)
            self.logger.debug(f"Processed: {dataset_name} {processed}...")
