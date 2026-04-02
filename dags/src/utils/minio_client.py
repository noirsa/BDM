import time
from sys import prefix

import boto3
from botocore.exceptions import ClientError
import os

from . import  get_logger, get_minio_config
minio_config = get_minio_config()['minio']

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

    def delete_object(self, bucket_name, object_key):
        self.client.delete_object(Bucket=bucket_name, Key=object_key)
        self.logger.debug(f"Deleted object '{object_key}'.")

    def verify_empty_bucket(self,bucket_name, prefix=None):
        # Use your existing logic here
        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj.get('Key', '')
                size = obj.get('Size', 0)
                # Your specific logic:
                if size == 0 and key.endswith("/"):
                    continue

                # If we find even one file that is > 0 bytes and not a keeper
                if size > 0:
                    self.logger.info(f"Found valid data: {key}")
                    return True
        return False

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
            self.logger.info(f"Successfully uploaded {content_type} file to {bucket_name}/{object_key}")

        except ClientError as e:
            self.logger.error(f"Failed to upload  {content_type} file to '{object_key}' to bucket '{bucket_name}': {e}")
            # Re-raise the exception so the Airflow task knows it failed
            raise

    def _move_file(self, source_bucket, source_key, destination_bucket, destination_key):
        """
        Moves an object from one location to another by copying then deleting.

        param:
        source_bucket : str
            The bucket name where the file currently resides.
        source_key : str
            The path of the source file.
        destination_bucket : str
            The bucket name to move the file to.
        destination_key : str
            The destination path for the file.


        """
        try:

            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.client.copy_object(
                CopySource=copy_source,
                Bucket=destination_bucket,
                Key=destination_key
            )
            self.logger.debug(f"Successfully copied {source_key} to {destination_key}")

            self.client.delete_object(Bucket=source_bucket, Key=source_key)
            self.logger.debug(f"Successfully deleted original file {source_key}")


        except ClientError as e:
            self.logger.error(f"Failed to move file from {source_key} to {destination_key}: {e}")
            raise

    def _classify_object_by_head(self, bucket, key):
        """ This function classify object by head."""

        head = self.client.head_object(Bucket=bucket, Key=key)
        ct = head.get("ContentType", "")
        if ct.startswith("image/"):
            return "image"
        else:
            return "structured"

    def move_bucket(self, source_bucket,source_prefix, destination_bucket,destination_prefix=None):
        """
            Iterates through a source prefix, classifies each object, and moves it to a structured
            or unstructured landing zone in the destination bucket.

            The method performs automated routing based on file content, renames files with
            milliseconds timestamps to avoid collisions, and organizes them into a
            Medallion-style directory structure.

            Args:
                source_bucket (str): The bucket containing the incoming raw files.
                source_prefix (str): The directory prefix to scan (e.g., 'temporal-landing/').
                destination_bucket (str): The bucket where files will be archived.
                destination_prefix (str, optional): The base path for the persistent zone
                                                    (e.g., 'persistent-landing/'). Defaults to "".

            Raises:
                ClientError: If S3 operations (listing, head, copy, or delete) fail.
                Exception: For any unexpected processing errors.
            """
        paginator = self.client.get_paginator("list_objects_v2")
        count = 1
        for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
            for obj in page.get("Contents", []):
                try:
                    src_key = obj["Key"]

                    # skip folder
                    if obj['Size'] == 0 and src_key.endswith("/"):
                        continue

                    # classify
                    category = self._classify_object_by_head(source_bucket, src_key)
                    # get file extension
                    ext = src_key.split('.')[-1].split('?')[0]
                    # new filename = timestamp + original extension
                    ts = int(time.time() * 1000)  # milliseconds
                    if category == "structured":
                        filename = os.path.splitext(os.path.basename(src_key))[0].split("_")
                        filename[-1] = str(ts)
                        filename = "_".join(filename)
                        new_filename = f"{filename}.{ext}"

                    else:
                        new_filename = f"{category}_{ts}.{ext}"

                    if category == "image":
                        category = "unstructured/" + category
                    elif category == "structured":
                        category = "structured/raw"
                    dest_key = f"{destination_prefix}{category}/{new_filename}"
                    self.logger.debug(f"Moved: {src_key} -> {dest_key}")
                    self._move_file(source_bucket, src_key,destination_bucket, dest_key)
                except ClientError as e:
                    self.logger.exception(f"S3 Error during bucket move for {src_key}: {e}")
                    raise
            self.logger.info(f"Successfully routed page {count} of {source_bucket}/{source_prefix} to {destination_bucket}/{destination_prefix}")
            count += 1

    def get_pending_keys(self, bucket, prefix):
        """
        Identifies files within a specific S3 prefix that require processing.


        Param:
            bucket (str): The name of the S3/MinIO bucket to scan.
            prefix (str): The directory path to filter objects (e.g., 'raw/users/').

        Return:
            list: A list of object keys (strings) that have a 'pending' status or
                  no status metadata defined.
        """

        paginator = self.client.get_paginator("list_objects_v2")
        keys = []
        search_prefix = prefix if prefix.endswith('/') else f"{prefix}/"
        for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
            for obj in page.get("Contents", []):

                if obj['Key'].endswith("/") or obj['Size'] == 0:
                    continue
                key = obj["Key"]

                try:
                    head = self.client.head_object(Bucket=bucket, Key=key)

                    metadata = head.get('Metadata', {})
                    status = metadata.get("status", metadata.get("Status", "pending"))
                    self.logger.info(f"Found file: {key} (metadata: {metadata})")
                    if status == "pending":
                        self.logger.info(f"Adding to queue: {key}")
                        keys.append(key)
                    else:
                        self.logger.info(f"Skipping processed file: {key} (status: {status})")

                except Exception as e:
                    self.logger.warning(f"Could not head object {key}: {e}")
                    continue

        self.logger.info(f"Scan complete. Found {len(keys)} pending files in s3://{bucket}/{prefix}")
        return keys

    def mark_as_processed(self, bucket, key):
        """
        Updates object metadata in-place to mark a file as processed.


        Param:
            bucket (str): The name of the bucket where the file resides.
            key (str): The specific object key to update.

        """
        try:
            # Define the source for the copy operation (same as target)
            copy_source = {
                'Bucket': bucket,
                'Key': key
            }

            # Perform the in-place copy to update metadata
            response = self.client.copy_object(
                Bucket=bucket,
                Key=key,
                CopySource=copy_source,
                # New metadata to be attached to the object
                Metadata={
                    'status': 'processed',
                },
                # CRITICAL: Must be 'REPLACE' to overwrite existing metadata
                MetadataDirective='REPLACE'
            )

            self.logger.debug(f"Successfully marked {key} as processed in MinIO.")

        except ClientError as e:
            self.logger.error(f"Failed to update metadata for {key}: {e}")
            raise