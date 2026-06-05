from __future__ import annotations

import re
from typing import Any, Sequence

from .base import BaseTrustedZoneService


class TrustedSchemaUtils(BaseTrustedZoneService):
    """Schema helpers mirroring the Trusted Zone structured notebook."""

    TABLE_SORTING_KEYS = {
        "global_warming": ["country", "year"],
        "temperature_change": ["area", "months", "year"],
        "emission": ["make", "model", "vehicle_class"],
        "tweet": ["id"],
    }

    def normalize_table_name(self, s3_path: str) -> str | None:
        """Convert a landing Delta path into a stable Trusted Zone table name."""
        folder_name = s3_path.rstrip("/").split("/")[-1]
        if folder_name in {"raw", "_staging", "file_catalog"} or folder_name.startswith("."):
            return None
        table_name = folder_name.replace("_delta", "")
        table_name = re.sub(r"_\d+$", "", table_name)
        table_name = re.sub(r"_\d{8}t\d{6}$", "", table_name, flags=re.IGNORECASE)
        table_name = re.sub(r"[^0-9A-Za-z_]+", "_", table_name)
        table_name = re.sub(r"_+", "_", table_name).strip("_").lower()
        return table_name or None

    def normalize_column_name(self, column_name: str, position: int) -> str | None:
        """Normalize source headers into lowercase ClickHouse-safe names."""
        cleaned = str(column_name).replace("\ufeff", "")
        cleaned = re.sub(r"[()]+", "", cleaned)
        cleaned = re.sub(r"[^0-9A-Za-z_]+", "_", cleaned.strip())
        cleaned = re.sub(r"_+", "_", cleaned).strip("_")
        cleaned = cleaned or f"column_{position + 1}"
        if cleaned.lower() in {"tweet_id", "id"}:
            cleaned = "id"
        if cleaned[0].isdigit():
            cleaned = f"col_{cleaned}"
        return cleaned.lower()

    def standardize_column_names(self, dataframe: Any) -> Any:
        """Apply deterministic column renaming and duplicate-name handling."""
        seen: dict[str, int] = {}
        renamed = dataframe
        mapping: dict[str, str] = {}
        for idx, original_name in enumerate(dataframe.columns):
            base_name = self.normalize_column_name(original_name, idx)
            occurrence = seen.get(base_name, 0)
            seen[base_name] = occurrence + 1
            final_name = base_name if occurrence == 0 else f"{base_name}_{occurrence + 1}"
            mapping[original_name] = final_name
            if final_name != original_name:
                renamed = renamed.withColumnRenamed(original_name, final_name)
        self.logger.info("Column normalization mapping: %s", mapping)
        return renamed

    def choose_sorting_keys(self, dataframe: Any, table_name: str) -> list[str]:
        """Choose MergeTree sorting keys using notebook business rules."""
        sorting_keys: list[str] = []
        for table_hint, candidate_keys in self.TABLE_SORTING_KEYS.items():
            if table_hint in table_name:
                sorting_keys = candidate_keys
                break
        final_keys = [key for key in sorting_keys if key in dataframe.columns]
        if not final_keys and "id" in dataframe.columns:
            final_keys = ["id"]
        if not final_keys and dataframe.columns:
            final_keys = [dataframe.columns[0]]
        return final_keys

    def clickhouse_type_for_field(self, field: Any) -> str | None:
        """Map Spark field types to ClickHouse column types."""
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            ByteType,
            DateType,
            DecimalType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            ShortType,
            StringType,
            TimestampType,
        )

        type_map = {
            StringType: "String",
            IntegerType: "Int32",
            LongType: "Int64",
            ShortType: "Int16",
            ByteType: "Int8",
            FloatType: "Float32",
            DoubleType: "Float64",
            BooleanType: "UInt8",
            DateType: "Date",
            TimestampType: "DateTime",
        }
        data_type = field.dataType
        if isinstance(data_type, ArrayType):
            element = type_map.get(type(data_type.elementType), "String")
            return f"Array({element})"
        if isinstance(data_type, DecimalType):
            return f"Decimal({data_type.precision},{data_type.scale})"
        ch_type = type_map.get(type(data_type))
        if ch_type is None:
            self.logger.warning("Mapping unsupported Spark type %s on %s to String", data_type, field.name)
            return "String"
        return ch_type

    def generate_clickhouse_ddl(
        self,
        dataframe: Any,
        table_name: str,
        sorting_keys: Sequence[str],
        database_name: str,
    ) -> str | None:
        """Generate ClickHouse DDL for a cleaned Trusted Zone dataframe."""
        from pyspark.sql.types import ArrayType

        if not dataframe.columns:
            raise ValueError(f"Cannot generate ClickHouse DDL for empty schema table {table_name}")
        sorting_key_set = set(sorting_keys)
        columns = []
        for field in dataframe.limit(0).schema.fields:
            ch_type = self.clickhouse_type_for_field(field)
            if field.name in sorting_key_set or isinstance(field.dataType, ArrayType):
                columns.append(f"    `{field.name}` {ch_type}")
            else:
                columns.append(f"    `{field.name}` Nullable({ch_type})")
        order_by = ", ".join(f"`{key}`" for key in sorting_keys) if sorting_keys else f"`{dataframe.columns[0]}`"
        column_sql = ",\n".join(columns)
        return (
            f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (\n"
            f"{column_sql}\n"
            ") ENGINE = MergeTree()\n"
            f"ORDER BY ({order_by})"
        )
