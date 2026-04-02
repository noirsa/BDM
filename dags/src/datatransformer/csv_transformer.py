import pandas as pd
import io
from .base_transformer import BaseTransformer
from ..utils import storage_options
import duckdb

class CSVTransformer(BaseTransformer):
    def __init__(self,minio_client,duckdb_client):
        super().__init__(minio_client,duckdb_client)
        self.storage_options = storage_options




    def load_and_transform(self, source_key, table_name):

        temp_parquet = f"{table_name}/temp_staging.parquet"

        try:
            # Using DuckDB client to execute the conversion
            self.duckdb_client.convert_to_parquet(source_key, temp_parquet)

            # Ensure the structure and data fingerprint match
            if not self.duckdb_client.verify_integrity(source_key, temp_parquet):
                raise ValueError(f"Integrity check failed for {source_key}")

            self.logger.info(f"Successfully transformed {source_key} into {table_name}")

        except Exception as e:
            self.logger.error(f"Pipeline failed for {source_key}: {str(e)}")
            raise