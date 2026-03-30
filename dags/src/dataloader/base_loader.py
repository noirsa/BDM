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


    def upload_csv(self, df, object_name, bucket_name="landing-zone", path="temporal-landing/", metadata=None,content_type="text/csv"):
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

        self.minio_client.upload_file(bucket_name, full_object_path, csv_buffer.getvalue(),content_type=content_type, metadata=metadata)

    def upload_file(self, bucket_name, object_key, content, content_type=None, metadata=None):
        """
        Uploads raw content (bytes) to a specified MinIO bucket with metadata

        param:
            bucket_name : str
                The name of the target bucket (e.g., 'landing-zone').
            object_key : str
                The full destination path/name of the object in MinIO.
            content : bytes
                The raw binary data to be uploaded.
            content_type : str, optional
                The MIME type of the file (e.g., 'text/csv', 'image/jpeg').
            metadata : dict, optional
                A dictionary of key-value pairs to be stored as object metadata.
                Note: MinIO/S3 metadata keys are typically stored in lowercase.

        """

        self.minio_client.upload_file(bucket_name, object_key, content, content_type=content_type, metadata=metadata)



