import pandas as pd
import io
from .base_transformer import BaseTransformer
from ..utils import storage_options


class CSVTransformer(BaseTransformer):
    def __init__(self,minio_client):
        super().__init__(minio_client)
        self.storage_options = storage_options
    def load_and_transform(self, source_key, table_name):
        bucket= "landing-zone"
        raw_data = self.minio_client.get_object(bucket, source_key)
        df = pd.read_csv(io.BytesIO(raw_data))