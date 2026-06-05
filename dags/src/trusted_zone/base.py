from __future__ import annotations

import os
import shutil
import sys
from typing import Any
from urllib.parse import urlparse

from src.utils import get_logger, get_storage_options, logical_date_iso


class BaseTrustedZoneService:
    """Base class for Trusted Zone services."""

    def __init__(self, minio_client: Any | None = None, duckdb_client: Any | None = None):
        self.logger = get_logger(self.__class__.__module__)
        self.minio_client = minio_client
        self.duckdb_client = duckdb_client
        self.logger.info("%s service initialized.", self.__class__.__name__)

    @property
    def s3_client(self) -> Any:
        if self.minio_client is None or not hasattr(self.minio_client, "client"):
            raise RuntimeError("A configured MinioClient is required for Trusted Zone S3 operations")
        return self.minio_client.client

    def parse_s3_path(self, s3_path: str, default_bucket: str | None = None) -> tuple[str, str]:
        """Return bucket and key for s3/s3a paths or bucket-relative prefixes."""
        parsed = urlparse(s3_path)
        if parsed.scheme in {"s3", "s3a"}:
            return parsed.netloc, parsed.path.lstrip("/")
        if default_bucket is None:
            raise ValueError(f"Bucket is required for non-S3 path: {s3_path}")
        return default_bucket, s3_path.lstrip("/")

    def list_object_keys(self, bucket_name: str, prefix: str) -> list[str]:
        paginator = self.s3_client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/") or key.startswith("_tmp/"):
                    continue
                keys.append(key)
        return sorted(keys)

    def list_common_prefixes(self, bucket_name: str, prefix: str) -> list[str]:
        paginator = self.s3_client.get_paginator("list_objects_v2")
        prefixes: list[str] = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
            prefixes.extend(item["Prefix"] for item in page.get("CommonPrefixes", []))
        return sorted(prefixes)

    def prefix_has_objects(self, bucket_name: str, prefix: str) -> bool:
        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        return bool(response.get("Contents"))

    def ensure_bucket(self, bucket_name: str) -> None:
        from botocore.exceptions import ClientError

        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code not in {"404", "NoSuchBucket"}:
                raise
        self.s3_client.create_bucket(Bucket=bucket_name)
        self.logger.info("Created bucket %s", bucket_name)

    def create_spark_session(self, app_name: str, include_delta: bool = True, include_clickhouse: bool = False) -> Any:
        """Create a Spark session at task runtime using the shared MinIO settings."""
        self.ensure_java_home()
        executor_python = os.getenv("SPARK_EXECUTOR_PYTHON", "/usr/bin/python3.12")
        driver_python = os.getenv("PYSPARK_DRIVER_PYTHON", sys.executable)
        os.environ["PYSPARK_PYTHON"] = executor_python
        os.environ["PYSPARK_DRIVER_PYTHON"] = driver_python
        from pyspark.sql import SparkSession

        storage_options = get_storage_options()
        endpoint = storage_options["AWS_ENDPOINT_URL"]
        packages = [
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]
        if include_delta:
            packages.append(f"io.delta:delta-spark_2.13:{os.getenv('DELTA_SPARK_VERSION', '4.1.0')}")
        if include_clickhouse:
            packages.extend(
                [
                    f"com.clickhouse.spark:clickhouse-spark-runtime-3.4_2.13:{os.getenv('CLICKHOUSE_SPARK_VERSION', '0.8.0')}",
                    f"com.clickhouse:clickhouse-jdbc:{os.getenv('CLICKHOUSE_JDBC_VERSION', '0.6.5')}",
                ]
            )

        builder = (
            SparkSession.builder.appName(app_name)
            .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
            .config("spark.jars.packages", ",".join(packages))
            .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", f"/tmp/spark-ivy-{app_name}-{os.getpid()}"))
            .config("spark.pyspark.driver.python", driver_python)
            .config("spark.pyspark.python", executor_python)
            .config("spark.executorEnv.PYSPARK_PYTHON", executor_python)
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.debug.maxToStringFields", "200")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.access.key", storage_options["AWS_ACCESS_KEY_ID"])
            .config("spark.hadoop.fs.s3a.secret.key", storage_options["AWS_SECRET_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.connection.maximum", "50")
            .config("spark.hadoop.fs.s3a.threads.max", "50")
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        )
        if include_delta:
            builder = (
                builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "ERROR"))
        self.normalize_hadoop_timeouts(spark)
        return spark

    def ensure_java_home(self) -> None:
        """Populate JAVA_HOME in task runtimes when the image has Java installed."""
        if os.getenv("JAVA_HOME"):
            return
        candidates = [
            "/usr/lib/jvm/java-17-openjdk-amd64",
            "/usr/lib/jvm/java-11-openjdk-amd64",
            "/usr/lib/jvm/default-java",
        ]
        for candidate in candidates:
            if os.path.exists(os.path.join(candidate, "bin", "java")):
                os.environ["JAVA_HOME"] = candidate
                os.environ["PATH"] = f"{candidate}/bin:{os.environ.get('PATH', '')}"
                return
        java_path = shutil.which("java")
        if java_path:
            java_home = os.path.dirname(os.path.dirname(os.path.realpath(java_path)))
            os.environ["JAVA_HOME"] = java_home

    def normalize_hadoop_timeouts(self, spark_session: Any) -> None:
        hadoop_conf = spark_session.sparkContext._jsc.hadoopConfiguration()
        for item in hadoop_conf.iterator():
            key, value = item.getKey(), item.getValue()
            if isinstance(value, str) and (value.endswith("s") or value.endswith("h")):
                digits = "".join(char for char in value if char.isdigit())
                if digits:
                    hadoop_conf.set(key, digits)

    def logical_date_string(self, logical_date: Any) -> str:
        return logical_date_iso(logical_date)
