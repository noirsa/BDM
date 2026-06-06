from __future__ import annotations

from typing import Any

from src.utils import get_storage_options

from .base import BaseTrustedZoneService
from .governance import ALLOWED_IMAGE_EXTENSIONS, CATALOGUE_POLICY_FIELDS, catalogue_policy_metadata, governance_metadata
from .quality_checks import TrustedQualityChecks


class UnstructuredTrustedCleaner(BaseTrustedZoneService):
    """Cleaner for unstructured image metadata and standardized image assets."""

    def __init__(self, minio_client: Any | None = None, duckdb_client: Any | None = None):
        super().__init__(minio_client=minio_client, duckdb_client=duckdb_client)
        self.quality_checks = TrustedQualityChecks(minio_client=minio_client, duckdb_client=duckdb_client)

    def read_image_catalog(self, catalog_path: str) -> Any:
        """Read the existing image file catalog Delta table."""
        spark_session = self.create_spark_session("trusted_zone_unstructured_processing", include_delta=True)
        storage_options = get_storage_options()
        self.logger.debug("Storage options prepared for image catalog read: %s", sorted(storage_options))
        return spark_session, spark_session.read.format("delta").load(catalog_path)

    def validate_image_objects(self, image_records: Any) -> None:
        """Validate image object paths, labels, sizes, and corruption flags."""
        missing = image_records.where("file_path IS NULL").limit(1).count()
        if missing:
            self.logger.warning("Image catalogue contains records without file_path; they will be written to rejected metadata")

    @staticmethod
    def image_metadata_schema() -> Any:
        """Schema used to parse landing image metadata blobs."""
        from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

        return StructType(
            [
                StructField("label", StringType(), True),
                StructField("url", StringType(), True),
                StructField("file_size_bytes", IntegerType(), True),
                StructField("content_type", StringType(), True),
                StructField("width", IntegerType(), True),
                StructField("height", IntegerType(), True),
                StructField("aspect_ratio", DoubleType(), True),
                StructField("image_mode", StringType(), True),
                StructField("is_corrupted", BooleanType(), True),
                StructField("md5", StringType(), True),
            ]
        )

    def clean_image_metadata(self, image_records: Any) -> Any:
        """Produce trusted unstructured metadata/features."""
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        blob_schema = self.image_metadata_schema()
        deterministic_image_choice = Window.partitionBy("md5").orderBy(F.col("file_path").asc(), F.col("file_id").asc())
        cleaned = (
            image_records.filter(F.col("file_type") == "Image")
            .withColumn("meta", F.from_json(F.col("metadata_blob"), blob_schema))
            .filter(F.col("file_path").isNotNull())
            .filter(F.col("file_id").isNotNull())
            .filter(F.col("meta.file_size_bytes") > F.lit(0))
            .filter(F.col("meta.is_corrupted") == F.lit(False))
            .filter(F.lower(F.col("file_path")).rlike(r"\.(jpg|jpeg|png|webp)$"))
            .withColumn("md5", F.col("meta.md5"))
            .filter(F.col("md5").isNotNull())
            .withColumn("dedupe_rank", F.row_number().over(deterministic_image_choice))
            .filter(F.col("dedupe_rank") == 1)
            .drop("dedupe_rank")
        )
        return cleaned

    def write_trusted_image_metadata(self, dataframe: Any, target_path: str) -> None:
        """Write trusted image metadata to Delta."""
        row_count = dataframe.count()
        dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
        self.quality_checks.validate_write_result(target_path, row_count)

    def log_image_processing_status_counts(self, results_df: Any) -> dict[str, int]:
        """Log transformation outcomes before trusted catalogue materialisation."""
        from pyspark.sql import functions as F

        counts = {
            row["status"]: int(row["count"])
            for row in results_df.groupBy("status").agg(F.count("*").alias("count")).collect()
        }
        self.logger.info(
            "Trusted unstructured processing status counts=%s source_assets=%s output_assets=%s",
            counts,
            ["s3a://landing-zone/persistent-landing/structured/file_catalog/"],
            ["s3a://trusted-zone/unstructured/image/", "s3a://trusted-zone/file_catalog/"],
        )
        return counts

    def validate_trusted_image_catalog(self, dataframe: Any, expected_success_count: int, status_counts: dict[str, int]) -> None:
        """Validate that trusted image cleaning preserved valid assets and metadata evidence."""
        from functools import reduce
        from operator import or_

        from pyspark.sql import functions as F

        row_count = int(dataframe.count())
        missing_critical_fields = [
            "id",
            "trusted_path",
            "raw_source_path",
            "source_file_path",
            "source_system",
            "ingestion_time",
            "validation_status",
            "schema_version",
            *CATALOGUE_POLICY_FIELDS,
        ]
        missing_critical = dataframe.where(
            reduce(
                or_,
                [F.col(field).isNull() | (F.trim(F.col(field).cast("string")) == "") for field in missing_critical_fields],
            )
        ).count()
        invalid_standardisation = dataframe.where(
            (F.col("current_width") != 512)
            | (F.col("current_height") != 512)
            | (F.col("current_image_mode") != "RGB")
            | (F.col("current_format") != "PNG")
            | (~F.col("trusted_path").startswith("s3a://trusted-zone/"))
            | (F.col("validation_status") != "valid")
        ).count()
        missing_lineage_metrics = dataframe.where(F.col("file_size_bytes").isNull() | F.col("md5").isNull()).count()

        validation_summary = {
            "catalogue_rows": row_count,
            "expected_success_rows": int(expected_success_count),
            "missing_critical_fields": int(missing_critical),
            "invalid_standardisation_rows": int(invalid_standardisation),
            "missing_lineage_metrics": int(missing_lineage_metrics),
            "status_counts": status_counts,
        }
        self.logger.info("Trusted unstructured validation summary=%s", validation_summary)

        if row_count != expected_success_count:
            raise ValueError(
                f"Trusted image catalogue row count mismatch: expected {expected_success_count}, observed {row_count}"
            )
        if missing_critical or invalid_standardisation or missing_lineage_metrics:
            raise ValueError(f"Trusted image catalogue validation failed: {validation_summary}")

    def rejected_image_metadata(self, image_records: Any, logical_date: Any) -> Any:
        """Build a rejected catalogue for invalid landing image metadata."""
        from pyspark.sql import functions as F

        blob_schema = self.image_metadata_schema()
        parsed = image_records.filter(F.col("file_type") == "Image").withColumn("meta", F.from_json(F.col("metadata_blob"), blob_schema))
        rejected = parsed.where(
            F.col("file_id").isNull()
            | F.col("file_path").isNull()
            | F.col("meta.file_size_bytes").isNull()
            | (F.col("meta.file_size_bytes") <= F.lit(0))
            | (F.col("meta.is_corrupted") == F.lit(True))
            | F.col("meta.md5").isNull()
            | (~F.lower(F.col("file_path")).rlike(r"\.(jpg|jpeg|png|webp)$"))
        ).withColumn(
            "reason",
            F.when(F.col("file_id").isNull(), F.lit("missing_file_id"))
            .when(F.col("file_path").isNull(), F.lit("missing_file_path"))
            .when(F.col("meta.file_size_bytes").isNull() | (F.col("meta.file_size_bytes") <= F.lit(0)), F.lit("empty_or_missing_source_file"))
            .when(F.col("meta.is_corrupted") == F.lit(True), F.lit("corrupted_image"))
            .when(F.col("meta.md5").isNull(), F.lit("missing_md5"))
            .otherwise(F.lit("unsupported_extension")),
        )
        metadata = governance_metadata(
            source_system="landing-zone",
            ingestion_time=self.logical_date_string(logical_date),
            source_file_path="landing image catalogue",
            validation_status="rejected",
            schema_version="unstructured_image_v1",
        )
        for field_name, field_value in metadata.items():
            rejected = rejected.withColumn(field_name, F.lit(field_value))
        return rejected.select(
            F.col("file_id").alias("id"),
            F.col("file_path").alias("source_file_path"),
            "reason",
            F.lit("trusted").alias("zone"),
            F.lit("unstructured_file").alias("asset_type"),
            F.lit("image").alias("dataset_name"),
            F.lit(self.logical_date_string(logical_date)).alias("rejected_at"),
            "source_system",
            "ingestion_time",
            "validation_status",
            "schema_version",
        )

    def write_rejected_image_metadata(self, dataframe: Any, target_path: str) -> None:
        """Append rejected image metadata to the Trusted Zone rejected catalogue."""
        row_count = dataframe.count()
        if not row_count:
            return
        dataframe.write.format("delta").mode("append").option("mergeSchema", "true").save(target_path)
        self.quality_checks.validate_write_result(target_path, row_count)

    def clean_all(self, logical_date: Any) -> None:
        """Run unstructured Trusted Zone cleaning."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, StructField, StructType

        self.logger.info("Run unstructured trusted cleaning logical_date=%s", logical_date)
        spark_session, catalog_df = self.read_image_catalog("s3a://landing-zone/persistent-landing/structured/file_catalog/")
        try:
            self.validate_image_objects(catalog_df)
            rejected_catalog_df = self.rejected_image_metadata(catalog_df, logical_date)
            cleaned_image_df = self.clean_image_metadata(catalog_df).cache()
            cleaned_image_count = cleaned_image_df.count()
            self.logger.info("Prepared deterministic trusted image candidates count=%s", cleaned_image_count)
            paths_df = cleaned_image_df.select(
                F.col("file_id").alias("id"),
                F.col("file_path").alias("file_path"),
                F.col("meta.image_mode").alias("image_mode"),
                F.col("meta.width").alias("width"),
                F.col("meta.height").alias("height"),
                F.col("meta.file_size_bytes").alias("file_size_bytes"),
            )
            credentials_broadcast = spark_session.sparkContext.broadcast(
                {
                    "endpoint": get_storage_options()["AWS_ENDPOINT_URL"],
                    "access_key": get_storage_options()["AWS_ACCESS_KEY_ID"],
                    "secret_key": get_storage_options()["AWS_SECRET_ACCESS_KEY"],
                }
            )
            allowed_image_extensions = tuple(sorted(ALLOWED_IMAGE_EXTENSIONS))

            def transform_and_upload_image(rows):
                import io
                import os

                import boto3
                from PIL import Image

                creds = credentials_broadcast.value
                s3_client = boto3.client(
                    "s3",
                    endpoint_url=creds["endpoint"],
                    aws_access_key_id=creds["access_key"],
                    aws_secret_access_key=creds["secret_key"],
                )
                for row in rows:
                    try:
                        image_id = row["id"]
                        src_key = row["file_path"]
                        orig_mode = row["image_mode"]
                        orig_w = row["width"]
                        orig_h = row["height"]
                        file_size_bytes = row["file_size_bytes"]
                        if not src_key or not file_size_bytes or int(file_size_bytes) <= 0:
                            yield {"id": image_id, "trusted_path": None, "status": "FAILED: empty_or_missing_source_file"}
                            continue
                        if os.path.splitext(src_key.lower())[1] not in allowed_image_extensions:
                            yield {"id": image_id, "trusted_path": None, "status": "FAILED: unsupported_extension"}
                            continue
                        orig_ext = src_key.rsplit(".", 1)[-1].lower()
                        dest_key = (src_key.rsplit(".", 1)[0] + ".png").replace("persistent-landing/", "")
                        if orig_w == 512 and orig_h == 512 and orig_mode == "RGB" and orig_ext == "png":
                            s3_client.copy_object(
                                Bucket="trusted-zone",
                                Key=dest_key,
                                CopySource={"Bucket": "landing-zone", "Key": src_key},
                                ContentType="image/png",
                            )
                            yield {"id": image_id, "trusted_path": f"s3a://trusted-zone/{dest_key}", "status": "DIRECT_COPY"}
                            continue

                        obj = s3_client.get_object(Bucket="landing-zone", Key=src_key)
                        try:
                            image_data = obj["Body"].read()
                        finally:
                            obj["Body"].close()
                        img = Image.open(io.BytesIO(image_data))
                        if orig_mode != "RGB":
                            img = img.convert("RGB")
                        img = img.resize((512, 512))
                        buffer = io.BytesIO()
                        img.save(buffer, format="PNG")
                        buffer.seek(0)
                        s3_client.put_object(
                            Bucket="trusted-zone",
                            Key=dest_key,
                            Body=buffer.getvalue(),
                            ContentType="image/png",
                        )
                        yield {"id": image_id, "trusted_path": f"s3a://trusted-zone/{dest_key}", "status": "PILLOW_TRANSFORMED"}
                    except Exception as exc:
                        yield {"id": row["id"], "trusted_path": None, "status": f"FAILED: {exc}"}

            processing_results = paths_df.repartition(8).rdd.mapPartitions(transform_and_upload_image).collect()
            result_schema = StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("trusted_path", StringType(), True),
                    StructField("status", StringType(), True),
                ]
            )
            all_results_df = spark_session.createDataFrame(processing_results, schema=result_schema)
            status_counts = self.log_image_processing_status_counts(all_results_df)
            failed_results_df = all_results_df.filter(~F.col("status").isin("DIRECT_COPY", "PILLOW_TRANSFORMED")).join(
                cleaned_image_df.select(F.col("file_id").alias("id"), F.col("file_path").alias("source_file_path")),
                on="id",
                how="left",
            )
            failed_results_df = (
                failed_results_df.withColumn("reason", F.col("status"))
                .withColumn("zone", F.lit("trusted"))
                .withColumn("asset_type", F.lit("unstructured_file"))
                .withColumn("dataset_name", F.lit("image"))
                .withColumn("rejected_at", F.lit(self.logical_date_string(logical_date)))
                .withColumn("source_system", F.lit("landing-zone"))
                .withColumn("ingestion_time", F.lit(self.logical_date_string(logical_date)))
                .withColumn("validation_status", F.lit("rejected"))
                .withColumn("schema_version", F.lit("unstructured_image_v1"))
                .select(
                    "id",
                    "source_file_path",
                    "reason",
                    "zone",
                    "asset_type",
                    "dataset_name",
                    "rejected_at",
                    "source_system",
                    "ingestion_time",
                    "validation_status",
                    "schema_version",
                )
            )
            self.write_rejected_image_metadata(
                rejected_catalog_df.unionByName(failed_results_df, allowMissingColumns=True),
                "s3a://trusted-zone/rejected/unstructured/image/",
            )
            execution_results_df = all_results_df.filter(
                F.col("status").isin("DIRECT_COPY", "PILLOW_TRANSFORMED")
            )
            success_count = execution_results_df.count()
            trusted_catalog_df = execution_results_df.join(
                cleaned_image_df.select(
                    F.col("file_id").alias("id"),
                    F.col("file_path").alias("raw_source_path"),
                    F.col("meta.label").alias("label"),
                    F.col("meta.url").alias("source_url"),
                    F.col("meta.file_size_bytes").alias("file_size_bytes"),
                    F.col("meta.md5").alias("md5"),
                ),
                on="id",
                how="inner",
            )
            policy_metadata = catalogue_policy_metadata({"source_type": "unstructured", "validation_status": "valid"})
            trusted_catalog_df = (
                trusted_catalog_df.withColumn("current_width", F.lit(512))
                .withColumn("current_height", F.lit(512))
                .withColumn("current_image_mode", F.lit("RGB"))
                .withColumn("current_format", F.lit("PNG"))
                .withColumn("processed_at", F.lit(self.logical_date_string(logical_date)))
                .withColumn("source_system", F.lit("landing-zone"))
                .withColumn("ingestion_time", F.lit(self.logical_date_string(logical_date)))
                .withColumn("source_file_path", F.col("raw_source_path"))
                .withColumn("validation_status", F.lit("valid"))
                .withColumn("schema_version", F.lit("trusted_v1"))
            )
            for field_name, field_value in policy_metadata.items():
                trusted_catalog_df = trusted_catalog_df.withColumn(field_name, F.lit(field_value))
            self.validate_trusted_image_catalog(trusted_catalog_df, success_count, status_counts)
            self.write_trusted_image_metadata(trusted_catalog_df, "s3a://trusted-zone/file_catalog/")
        finally:
            spark_session.stop()
