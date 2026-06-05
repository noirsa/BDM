from __future__ import annotations

from typing import Any, Iterable, Sequence

from .base import BaseTrustedZoneService


class TrustedQualityChecks(BaseTrustedZoneService):
    """Data quality checks shared by Trusted Zone cleaners."""

    def validate_source_paths(self, source_paths: Sequence[str]) -> None:
        """Verify expected persistent landing inputs exist before cleaning."""
        if not source_paths:
            raise ValueError("No source paths supplied for Trusted Zone validation")
        missing: list[str] = []
        for source_path in source_paths:
            bucket, key = self.parse_s3_path(source_path)
            if "/structured/raw/" in f"/{key}" or key.rstrip("/").endswith("/raw"):
                raise ValueError(f"Trusted Zone structured input cannot be a raw CSV path: {source_path}")
            prefix = key if key.endswith("/") else f"{key}/"
            if not self.prefix_has_objects(bucket, prefix):
                missing.append(source_path)
        if missing:
            raise FileNotFoundError(f"Missing Trusted Zone source paths: {missing}")

    def validate_required_columns(self, dataframe: Any, required_columns: Sequence[str], table_name: str) -> None:
        """Fail fast when a trusted source is missing mandatory columns."""
        actual = set(dataframe.columns)
        missing = [column for column in required_columns if column not in actual]
        if missing:
            raise ValueError(f"{table_name} is missing required columns: {missing}")

    def validate_row_count(self, dataframe: Any, table_name: str, expected_rows: int | None = None) -> int | None:
        """Compare cleaned row count with source expectations."""
        if hasattr(dataframe, "count") and callable(dataframe.count):
            observed = int(dataframe.count())
        else:
            observed = len(dataframe)
        if expected_rows is not None and observed != expected_rows:
            raise ValueError(f"{table_name} row count mismatch: expected {expected_rows}, observed {observed}")
        self.logger.info("%s row count: %s", table_name, observed)
        return observed

    def validate_no_duplicate_keys(self, dataframe: Any, table_name: str, key_columns: Sequence[str]) -> None:
        """Check duplicate business keys after trusted-zone deduplication."""
        if not key_columns:
            return
        from pyspark.sql import functions as F

        duplicates = dataframe.groupBy(*key_columns).count().where(F.col("count") > 1).limit(5).collect()
        if duplicates:
            raise ValueError(f"{table_name} has duplicate keys for {key_columns}: {duplicates}")

    def validate_string_encoding(self, dataframe: Any, table_name: str) -> None:
        """Check mojibake and control characters after string normalization."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        string_columns = [field.name for field in dataframe.schema.fields if isinstance(field.dataType, StringType)]
        if not string_columns:
            return
        checks = [
            F.col(column).rlike(r"[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F-\u009F\r\n\t]")
            for column in string_columns
        ]
        condition = checks[0]
        for check in checks[1:]:
            condition = condition | check
        bad_rows = dataframe.where(condition).limit(5).collect()
        if bad_rows:
            raise ValueError(f"{table_name} still contains mojibake/control characters: {bad_rows}")

    def validate_write_result(self, target_name: str, expected_count: int | None = None) -> None:
        """Log downstream write expectations.

        Concrete writer classes perform target-specific count checks when they
        own the live connection. This shared method keeps a consistent log line.
        """
        self.logger.info("Validated write target=%s expected_count=%s", target_name, expected_count)

    def validate_non_empty_records(self, records: Iterable[Any], target_name: str) -> None:
        """Validate non-dataframe record batches such as JSON or image metadata."""
        records_list = records if isinstance(records, list) else list(records)
        if not records_list:
            raise ValueError(f"{target_name} produced no records")
        first = records_list[0]
        if isinstance(first, dict):
            self.logger.info("%s first record keys: %s", target_name, sorted(first.keys()))
