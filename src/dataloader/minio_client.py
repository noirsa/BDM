import boto3
from botocore.exceptions import ClientError

from src.utils import get_logger

log = get_logger(__name__)
class MinioClient:
    """
    Handles interactions with MinIO storage.
    Injected into DataLoaders to manage bucket lifecycle and file uploads.
    """

    def __init__(self, config):
        """
        Initializes the Minio connection using the provided configuration.

        Param:
        config: Dictionary containing credentials and endpoint
        logger: Logger instance for tracking operations
        """
        self.logger = log
        minio_config = config["minio"]
        # Extract values from the centralized config
        self.client = (  boto3.client(
                "s3",
                endpoint_url=minio_config["endpoint"],  # MinIO API endpoint
                aws_access_key_id=minio_config["access_key"], # User name
                aws_secret_access_key=minio_config["secret_key"],  # Password
            ))
        self.logger.info("MinioClient service initialized within dataloader.")

    def ensure_bucket_exists(self, bucket_name):
        """
        Checks if the bucket exists; creates it if it does not.

        Param:
        bucket_name: Name of the bucket to verify
        """
        try:
            self.client.head_bucket(Bucket=bucket_name)
            self.logger.info(f"Bucket '{bucket_name}' is ready.")
        except ClientError as e:
            # If a 404 error occurs, the bucket does not exist
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404":
                self.logger.info(f"Bucket '{bucket_name}' not found. Creating...")
                self.client.create_bucket(Bucket=bucket_name)
                self.logger.info(f"Bucket '{bucket_name}' created successfully.")
            else:
                self.logger.error(f"Unexpected error checking bucket: {e}")
                raise