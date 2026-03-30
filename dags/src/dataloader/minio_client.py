import boto3
from botocore.exceptions import ClientError
from src.utils import (
    get_logger,
    minio_config
)

log = get_logger(__name__)
class MinioClient:
    """
    Handles interactions with MinIO storage.
    Injected into DataLoaders to manage bucket lifecycle and file uploads.
    """

    def __init__(self):
        """
        Initializes the Minio connection using the provided configuration.

        Param:
        config: Dictionary containing credentials and endpoint
        logger: Logger instance for tracking operations
        """
        self.logger = log
        self.minio_config = minio_config
        # Extract values from the centralized config
        self.client = (  boto3.client(
                "s3",
                endpoint_url=self.minio_config["endpoint"],  # MinIO API endpoint
                aws_access_key_id=self.minio_config["access_key"], # User name
                aws_secret_access_key=self.minio_config["secret_key"],  # Password
            ))
        self.logger.info("MinioClient service initialized within dataloader.")

    def ensure_bucket_exists(self):
        """
        Checks if the bucket exists; creates it if it does not.

        """

        minio_config = self.minio_config
        buckets = minio_config["buckets"]
        for bucket_key, bucket_info in buckets.items():
            bucket_name = bucket_info["name"]
            self.logger.info(f"Starting infrastructure setup for bucket: {bucket_name}")
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
            if "sub_buckets" in bucket_info:
                for subbucketkey,subbucketinfo in bucket_info["sub_buckets"].items():
                    subbucket_name = subbucketinfo["name"]

                    if "categories" in subbucketinfo:
                        for cat_key, cat_path in subbucketinfo["categories"].items():
                            full_path = f"{subbucket_name}/{cat_path}/"
                            self.create_placeholder(bucket_name, full_path)
                    else:
                        self.create_placeholder(bucket_name, f"{subbucket_name}/")

    def create_placeholder(self, bucket_name, object_key):
        """
        Creates an empty folder in MinIO if it does not exist.
        This ensures the directory structure is visible in the MinIO Browser UI.

        param:
            bucket_name: Name of the bucket to create sub-buckets in
            object_key: Sub-bucket name
        """
        try:
            # Check if object exists
            self.client.head_object(Bucket=bucket_name, Key=object_key)
            self.logger.info(
                f"Folder: {bucket_name}/{object_key} is ready."
            )
            return  # already exists

        except ClientError as e:
            error_code = e.response['Error']['Code']

            if error_code in ("404", "NoSuchKey"):
                try:
                    # Create empty object (folder placeholder)
                    self.client.put_object(
                        Bucket=bucket_name,
                        Key=object_key,
                    )
                    self.logger.info(
                        f"Successfully ensured folder: {bucket_name}/{object_key}"
                    )
                except Exception as inner_e:
                    self.logger.warning(
                        f"Failed to create directory placeholder for {object_key}: {inner_e}"
                    )
            else:
                self.logger.warning(
                    f"Error checking existence of {object_key}: {e}"
                )

        except Exception as e:
            self.logger.warning(
                f"Unexpected error for {object_key}: {e}"
            )

    def upload_file(self, bucket_name, object_key, body, content_type='application/octet-stream', metadata=None):
        """
        Uploads data to MinIO bucket.
        """
        try:
            # 4. Upload to MinIO
            # We use .getvalue() to get the byte content of the buffer
            self.client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=body,
                Metadata=metadata,
                ContentType=content_type
            )
            self.logger.info(f"Successfully uploaded CSV to {bucket_name}/{object_key}")

        except ClientError as e:
            self.logger.error(f"Failed to upload CSV '{object_key}' to bucket '{bucket_name}': {e}")
            # Re-raise the exception so the Airflow task knows it failed
            raise