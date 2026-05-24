from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetValidationSpec:
    """Notebook-derived checks that guard ingestion against source drift."""

    dataset_name: str
    source_file: str | None = None
    expected_rows: int | None = None
    expected_columns: int | None = None

    def validate_dataframe(self, df, logger):
        rows, columns = df.shape
        logger.info(
            "Validating dataset '%s': rows=%s columns=%s expected_rows=%s expected_columns=%s source_file=%s",
            self.dataset_name,
            rows,
            columns,
            self.expected_rows,
            self.expected_columns,
            self.source_file,
        )

        if self.expected_rows is not None and rows != self.expected_rows:
            raise ValueError(
                f"{self.dataset_name} row count mismatch: expected {self.expected_rows}, got {rows}"
            )

        if self.expected_columns is not None and columns != self.expected_columns:
            raise ValueError(
                f"{self.dataset_name} column count mismatch: expected {self.expected_columns}, got {columns}"
            )
