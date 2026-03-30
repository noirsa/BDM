import io

import boto3
from botocore.exceptions import ClientError

from src.utils import (
    get_logger,
    minio_config
)

log = get_logger(__name__)

class BaseDataLoader(object):
    def __init__(self, minio_client):
        self.logger = log
        self.minio_client = minio_client
        self.logger.info(f"{self.__class__.__name__} service initialized.")


    def upload_csv(self, df, object_name, bucket_name="landing-zone", path="temporal-landing/", metadata=None):
        """
        Converts a Pandas DataFrame to CSV and uploads it to MinIO.
        Ensures the file is written to the specified path prefix.

        param:
            df: Pandas DataFrame
            bucket_name: Name of the bucket to upload CSV to
            object_name: Name of the object to upload CSV to
            path: path to upload CSV to
            metadata: metadata of the csv file
        """
        # 1. Convert DataFrame to CSV in memory (using BytesIO for binary compatibility)
        csv_buffer = io.BytesIO()
        # We use 'utf-8' encoding to ensure special characters are handled correctly
        df.to_csv(csv_buffer, index=False, encoding='utf-8')

        # 2. Construct the full object key (the virtual path)
        # Ensure the path ends with a slash if provided, then append the name
        full_object_path = f"{path.rstrip('/')}/{object_name}"

        # 3. Metadata handling (ensure it's a dictionary)
        if metadata is None:
            metadata = {}

        self.minio_client.upload_file(bucket_name, full_object_path, csv_buffer.getvalue(),content_type='text/csv', metadata=metadata)


