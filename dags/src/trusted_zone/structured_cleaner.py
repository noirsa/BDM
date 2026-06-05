from __future__ import annotations

from typing import Any

from .base import BaseTrustedZoneService
from .clickhouse_writer import ClickHouseTrustedWriter
from .governance import governance_metadata
from .quality_checks import TrustedQualityChecks
from .rejected_writer import TrustedRejectedWriter
from .schema_utils import TrustedSchemaUtils


class StructuredTrustedCleaner(BaseTrustedZoneService):
    """Cleaner for structured Delta tables in Persistent Landing."""

    NULL_TOKENS = {"", "na", "n/a", "null", "none", "nan", "-", "--"}
    DEGREE_C = chr(0x00B0) + "C"
    CORRUPTED_DEGREE_C_PATTERN = f"(?i)[{chr(0x00E2)}{chr(0x00C2)}]\\s*[{chr(0x00B0)}{chr(0x00BA)}]\\s*c"
    CLEAN_DEGREE_C_PATTERN = f"(?i){chr(0x00B0)}\\s*c"
    MOJIBAKE_DASH_PATTERN = f"{chr(0x00E2)}{chr(0x0080)}[{chr(0x0093)}{chr(0x0094)}]"
    ARRAY_LIKE_COLUMNS = {"hashtags", "emojis"}
    DATASET_RULES = {
        "co2_emission_by_vehicles": {
            "schema_version": "co2_emission_by_vehicles_v1",
            "required_columns": ["make", "model", "vehicle_class"],
            "business_keys": ["make", "model", "vehicle_class", "transmission", "fuel_type"],
            "expected_columns": [
                "make",
                "model",
                "vehicle_class",
                "engine_sizel",
                "cylinders",
                "transmission",
                "fuel_type",
                "fuel_consumption_city_l_100_km",
                "fuel_consumption_hwy_l_100_km",
                "fuel_consumption_comb_l_100_km",
                "fuel_consumption_comb_mpg",
                "co2_emissionsg_km",
            ],
            "integer_columns": ["cylinders", "fuel_consumption_comb_mpg", "co2_emissionsg_km"],
            "non_negative_columns": [
                "engine_sizel",
                "cylinders",
                "fuel_consumption_city_l_100_km",
                "fuel_consumption_hwy_l_100_km",
                "fuel_consumption_comb_l_100_km",
                "fuel_consumption_comb_mpg",
                "co2_emissionsg_km",
            ],
        },
        "global_warming_dataset": {
            "schema_version": "global_warming_dataset_v1",
            "required_columns": ["country", "year"],
            "business_keys": ["country", "year"],
            "expected_columns": ["country", "year", "temperature_anomaly", "co2_emissions", "population"],
            "integer_columns": ["year", "extreme_weather_events"],
            "non_negative_columns": ["co2_emissions", "population", "forest_area", "gdp", "methane_emissions"],
            "year_range": (1900, 2100),
        },
        "natural_disaster_tweets": {
            "schema_version": "natural_disaster_tweets_v1",
            "required_columns": ["id"],
            "business_keys": ["id"],
            "unique_keys": ["id"],
            "expected_columns": ["id", "text", "label", "hashtags", "emojis"],
        },
        "temperature_change": {
            "schema_version": "temperature_change_v1",
            "required_columns": ["area", "months", "year"],
            "business_keys": ["area", "months", "year"],
            "expected_columns": [
                "domain_code",
                "domain",
                "area_code_m49",
                "area",
                "element_code",
                "element",
                "months_code",
                "months",
                "year_code",
                "year",
                "unit",
                "value",
                "flag",
                "flag_description",
            ],
            "integer_columns": ["year", "year_code"],
            "year_range": (1961, 2100),
        },
    }

    def __init__(self, minio_client: Any | None = None, duckdb_client: Any | None = None):
        super().__init__(minio_client=minio_client, duckdb_client=duckdb_client)
        self.schema_utils = TrustedSchemaUtils(minio_client=minio_client, duckdb_client=duckdb_client)
        self.quality_checks = TrustedQualityChecks(minio_client=minio_client, duckdb_client=duckdb_client)
        self.clickhouse_writer = ClickHouseTrustedWriter(minio_client=minio_client, duckdb_client=duckdb_client)
        self.rejected_writer = TrustedRejectedWriter(minio_client=minio_client, duckdb_client=duckdb_client)

    def discover_dataset_paths(self, base_path: str) -> list[str]:
        """Discover stable structured Delta roots under persistent-landing/structured/."""
        bucket, key = self.parse_s3_path(base_path)
        prefixes = self.list_common_prefixes(bucket, key)
        dataset_paths: list[str] = []
        for prefix in prefixes:
            folder_name = prefix.rstrip("/").split("/")[-1]
            if folder_name in {"raw", "_staging", "file_catalog"} or folder_name.startswith("."):
                continue
            dataset_paths.append(f"s3a://{bucket}/{prefix}")
        dataset_paths = sorted(dataset_paths)
        self.quality_checks.validate_source_paths(dataset_paths)
        return dataset_paths

    def read_delta_or_parquet(self, spark_session: Any, s3_path: str) -> Any:
        """Read Delta when _delta_log exists; otherwise fall back to Parquet."""
        self.logger.info("Read structured source path=%s", s3_path)
        sc = spark_session.sparkContext
        delta_log_path = sc._jvm.org.apache.hadoop.fs.Path(s3_path.rstrip("/") + "/_delta_log")
        fs = delta_log_path.getFileSystem(sc._jsc.hadoopConfiguration())
        reader_format = "delta" if fs.exists(delta_log_path) else "parquet"
        return spark_session.read.format(reader_format).load(s3_path)

    def apply_column_normalization(self, dataframe: Any) -> Any:
        """Standardize headers through TrustedSchemaUtils."""
        normalized = self.schema_utils.standardize_column_names(dataframe)
        bad_columns = [column for column in normalized.columns if column != column.lower()]
        if bad_columns:
            raise ValueError(f"Structured output contains non-lowercase columns: {bad_columns}")
        return normalized

    def apply_string_cleaning(self, dataframe: Any, table_name: str) -> Any:
        """Normalize strings, null tokens, degree symbols, and mojibake dashes."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        string_columns = [field.name for field in dataframe.schema.fields if isinstance(field.dataType, StringType)]
        for column_name in string_columns:
            cleaned_col = F.regexp_replace(F.col(column_name), r"[\n\r\t]", " ")
            cleaned_col = F.regexp_replace(cleaned_col, r"\s+", " ")
            cleaned_col = F.regexp_replace(cleaned_col, self.MOJIBAKE_DASH_PATTERN, "-")
            cleaned_col = F.regexp_replace(cleaned_col, self.CORRUPTED_DEGREE_C_PATTERN, self.DEGREE_C)
            cleaned_col = F.regexp_replace(cleaned_col, self.CLEAN_DEGREE_C_PATTERN, self.DEGREE_C)
            cleaned_col = F.regexp_replace(cleaned_col, r"[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F-\u009F]", "")
            trimmed_col = F.trim(cleaned_col)
            lower_trimmed_col = F.lower(trimmed_col)

            if table_name == "temperature_change" and column_name == "unit":
                normalized_col = F.when(
                    lower_trimmed_col.isin("c", "celsius", "degree c", "degrees c"),
                    F.lit(self.DEGREE_C),
                ).otherwise(trimmed_col)
            else:
                normalized_col = lower_trimmed_col

            dataframe = dataframe.withColumn(
                column_name,
                F.when(lower_trimmed_col.isin(*sorted(self.NULL_TOKENS)), F.lit(None)).otherwise(normalized_col),
            )
        self.quality_checks.validate_string_encoding(dataframe, table_name)
        return dataframe

    def apply_array_cleaning(self, dataframe: Any) -> Any:
        """Normalize known array-like columns such as hashtags and emojis."""
        from pyspark.sql import functions as F

        dtype_lookup = dict(dataframe.dtypes)
        for column_name in self.ARRAY_LIKE_COLUMNS.intersection(dataframe.columns):
            if "string" in dtype_lookup[column_name]:
                dataframe = dataframe.withColumn(
                    column_name,
                    F.split(F.regexp_replace(F.col(column_name), r"[\[\]'\"\s]", ""), ","),
                )
            dataframe = dataframe.withColumn(
                column_name,
                F.expr(f"filter(transform({column_name}, x -> lower(trim(x))), x -> x IS NOT NULL AND x != '')"),
            )
        return dataframe

    def deduplicate(self, dataframe: Any, sorting_keys: list[str], table_name: str) -> Any:
        """Deduplicate using full-row or key-based notebook rules."""
        known_business_table = any(
            token in table_name for token in ["emission", "global_warming", "temperature_change", "tweet"]
        )
        if known_business_table:
            return dataframe.dropDuplicates()
        if sorting_keys:
            return dataframe.dropDuplicates(subset=sorting_keys)
        return dataframe.dropDuplicates()

    def drop_empty_rows(self, dataframe: Any) -> Any:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        if not dataframe.columns:
            return dataframe
        checks = []
        for field in dataframe.schema.fields:
            col_ref = F.col(field.name)
            if isinstance(field.dataType, StringType):
                checks.append(col_ref.isNotNull() & (F.trim(col_ref) != ""))
            else:
                checks.append(col_ref.isNotNull())
        condition = checks[0]
        for check in checks[1:]:
            condition = condition | check
        return dataframe.where(condition)

    def mask_invalid_numeric_values(self, dataframe: Any) -> Any:
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType, FloatType

        for field in dataframe.schema.fields:
            if isinstance(field.dataType, (FloatType, DoubleType)):
                as_text = F.lower(F.col(field.name).cast("string"))
                dataframe = dataframe.withColumn(
                    field.name,
                    F.when(
                        F.isnan(F.col(field.name))
                        | as_text.isin("infinity", "+infinity", "-infinity", "inf", "+inf", "-inf"),
                        F.lit(None),
                    ).otherwise(F.col(field.name)),
                )
        return dataframe

    def apply_table_specific_fixes(self, dataframe: Any, table_name: str) -> Any:
        from pyspark.sql import functions as F

        if "id" in dataframe.columns and "tweet" in table_name:
            dataframe = dataframe.withColumn("id", F.col("id").cast("string"))
        return dataframe

    def rules_for_table(self, table_name: str) -> dict[str, Any]:
        """Return explicit dataset cleaning rules for a normalized table name."""
        for table_hint, rules in self.DATASET_RULES.items():
            if table_hint in table_name:
                return rules
        return {
            "schema_version": "trusted_v1",
            "required_columns": [],
            "business_keys": [],
            "expected_columns": [],
        }

    def apply_dataset_specific_rules(self, dataframe: Any, table_name: str) -> Any:
        """Apply table-specific casts, standardization, and anomaly masking."""
        from pyspark.sql import functions as F

        rules = self.rules_for_table(table_name)
        for column_name in rules.get("integer_columns", []):
            if column_name in dataframe.columns:
                dataframe = dataframe.withColumn(column_name, F.col(column_name).cast("int"))
        for column_name in rules.get("non_negative_columns", []):
            if column_name in dataframe.columns:
                dataframe = dataframe.withColumn(
                    column_name,
                    F.when(F.col(column_name).cast("double") < F.lit(0), F.lit(None)).otherwise(F.col(column_name)),
                )
        if "year_range" in rules and "year" in dataframe.columns:
            min_year, max_year = rules["year_range"]
            dataframe = dataframe.withColumn(
                "year",
                F.when((F.col("year") < F.lit(min_year)) | (F.col("year") > F.lit(max_year)), F.lit(None)).otherwise(F.col("year")),
            )
        if table_name == "temperature_change" and "months" in dataframe.columns:
            dataframe = dataframe.withColumn("months", F.regexp_replace(F.col("months"), r"\s*-\s*", "-"))
        return dataframe

    def log_expected_schema_gaps(self, dataframe: Any, table_name: str) -> None:
        """Log non-blocking expected-schema gaps for reportable quality evidence."""
        expected_columns = self.rules_for_table(table_name).get("expected_columns", [])
        missing = [column for column in expected_columns if column not in dataframe.columns]
        if missing:
            self.logger.warning("Structured expected schema gaps table=%s missing_columns=%s", table_name, missing)

    def quarantine_invalid_required_rows(
        self,
        dataframe: Any,
        table_name: str,
        source_path: str,
        logical_date: Any,
        required_columns: list[str],
    ) -> Any:
        """Remove rows missing required values and write an auditable rejected summary."""
        if not required_columns:
            return dataframe
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        conditions = []
        for column_name in required_columns:
            field = dataframe.schema[column_name]
            column_ref = F.col(column_name)
            if isinstance(field.dataType, StringType):
                conditions.append(column_ref.isNull() | (F.trim(column_ref) == ""))
            else:
                conditions.append(column_ref.isNull())
        invalid_condition = conditions[0]
        for condition in conditions[1:]:
            invalid_condition = invalid_condition | condition

        invalid_df = dataframe.where(invalid_condition)
        invalid_count = invalid_df.count()
        if invalid_count:
            sample_rows = [row.asDict(recursive=True) for row in invalid_df.limit(5).collect()]
            self.rejected_writer.write_minio_event(
                domain="structured",
                dataset_name=table_name,
                logical_date=logical_date,
                event={
                    "reason": "missing_required_values",
                    "source_file_path": source_path,
                    "required_columns": required_columns,
                    "rejected_record_count": invalid_count,
                    "sample_records": sample_rows,
                },
            )
        return dataframe.where(~invalid_condition)

    def fill_sorting_key_nulls(self, dataframe: Any, sorting_keys: list[str]) -> Any:
        from pyspark.sql import functions as F

        dtype_lookup = dict(dataframe.dtypes)
        for column_name in sorting_keys:
            column_type = dtype_lookup[column_name]
            if "string" in column_type:
                dataframe = dataframe.withColumn(column_name, F.coalesce(F.col(column_name), F.lit("unknown")))
            elif any(token in column_type for token in ["int", "long", "short", "byte"]):
                dataframe = dataframe.withColumn(column_name, F.coalesce(F.col(column_name), F.lit(0)))
            elif "double" in column_type or "float" in column_type:
                dataframe = dataframe.withColumn(column_name, F.coalesce(F.col(column_name), F.lit(0.0)))
        return dataframe

    def required_columns_for_table(self, table_name: str) -> list[str]:
        """Return minimal required columns already implied by known business tables."""
        return list(self.rules_for_table(table_name).get("required_columns", []))

    def schema_version_for_table(self, table_name: str) -> str:
        return str(self.rules_for_table(table_name).get("schema_version", "trusted_v1"))

    def add_governance_metadata(self, dataframe: Any, logical_date: Any, source_path: str, table_name: str) -> Any:
        """Append minimal Trusted Zone governance fields without changing business columns."""
        from pyspark.sql import functions as F

        metadata = governance_metadata(
            source_system="landing-zone",
            ingestion_time=self.logical_date_string(logical_date),
            source_file_path=source_path,
            validation_status="valid",
            schema_version=self.schema_version_for_table(table_name),
        )
        for field_name, field_value in metadata.items():
            dataframe = dataframe.withColumn(field_name, F.lit(field_value))
        return dataframe

    def clean_dataset(self, spark_session: Any, s3_path: str, logical_date: Any, database_name: str = "bi_analytics") -> None:
        """Clean one structured dataset and write it to ClickHouse."""
        table_name = self.schema_utils.normalize_table_name(s3_path)
        if not table_name:
            self.logger.info("Skipping excluded structured path %s", s3_path)
            return
        dataframe = self.read_delta_or_parquet(spark_session, s3_path)
        dataframe = self.apply_column_normalization(dataframe)
        self.log_expected_schema_gaps(dataframe, table_name)
        dataframe = self.apply_table_specific_fixes(dataframe, table_name)
        dataframe = self.drop_empty_rows(dataframe)
        dataframe = self.apply_string_cleaning(dataframe, table_name)
        dataframe = self.mask_invalid_numeric_values(dataframe)
        dataframe = self.apply_array_cleaning(dataframe)
        required_columns = self.required_columns_for_table(table_name)
        try:
            self.quality_checks.validate_required_columns(dataframe, required_columns, table_name)
        except ValueError as exc:
            self.logger.error("Structured dataset marked invalid and skipped: table=%s source=%s error=%s", table_name, s3_path, exc)
            self.rejected_writer.write_minio_event(
                domain="structured",
                dataset_name=table_name,
                logical_date=logical_date,
                event={
                    "reason": "missing_required_columns",
                    "source_file_path": s3_path,
                    "required_columns": required_columns,
                    "error": str(exc),
                    "observed_columns": dataframe.columns,
                },
            )
            return
        dataframe = self.apply_dataset_specific_rules(dataframe, table_name)
        dataframe = self.quarantine_invalid_required_rows(dataframe, table_name, s3_path, logical_date, required_columns)
        sorting_keys = self.schema_utils.choose_sorting_keys(dataframe, table_name)
        dataframe = self.fill_sorting_key_nulls(dataframe, sorting_keys)
        dataframe = self.deduplicate(dataframe, sorting_keys, table_name)
        self.quality_checks.validate_no_duplicate_keys(dataframe, table_name, self.rules_for_table(table_name).get("unique_keys", []))
        dataframe = self.add_governance_metadata(dataframe, logical_date, s3_path, table_name)
        row_count = self.quality_checks.validate_row_count(dataframe, table_name)
        ddl_sql = self.schema_utils.generate_clickhouse_ddl(dataframe, table_name, sorting_keys, database_name)
        self.clickhouse_writer.sync_dataframe_parallel(dataframe, ddl_sql, table_name, database_name)
        self.quality_checks.validate_write_result(f"{database_name}.{table_name}", row_count)

    def clean_all(self, logical_date: Any, base_path: str = "s3a://landing-zone/persistent-landing/structured/") -> None:
        """Run structured Trusted Zone cleaning for all discovered datasets."""
        self.logger.info("Run structured trusted cleaning logical_date=%s base_path=%s", logical_date, base_path)
        spark_session = self.create_spark_session(
            "trusted_zone_structured_processing",
            include_delta=True,
            include_clickhouse=False,
        )
        try:
            for dataset_path in self.discover_dataset_paths(base_path):
                self.clean_dataset(spark_session, dataset_path, logical_date)
        finally:
            spark_session.stop()
