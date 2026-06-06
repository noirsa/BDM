from __future__ import annotations

import os
from hashlib import sha256
from typing import Any

from src.utils import get_logger, logical_date_iso


CONSUMPTION_SCHEMA_VERSION = "consumption_v1"


class NaturalDisasterTweetClassifierPipeline:
    """Consumption DAG implementation of the natural disaster classifier notebook."""

    SOURCE_DB = "exploitation_analytics"
    SOURCE_TABLE = "fact_tweet_features"
    SOURCE_ASSETS = "exploitation_analytics.fact_tweet_features"
    SOURCE_SYSTEM = "exploitation_zone"

    METRICS_TABLE = "model_tweet_classifier_metrics"
    CONSUMPTION_TASK = "natural_disaster_tweet_classifier"
    MODEL_NAME = "tfidf_logistic_regression"
    MODEL_VERSION = "v1"
    MODEL_BUCKET = "exploitation-zone"
    MODEL_ARTIFACT_PREFIX = "consumption/classifier/natural_disaster_tweet_classifier"
    MODEL_ARTIFACT_FORMAT = "joblib"

    REQUIRED_COLUMNS = {"tweet_text", "disaster_type", "word_count"}
    METRICS_COLUMNS = [
        "row_count",
        "accuracy",
        "precision",
        "recall",
        "f1",
        "train_size",
        "test_size",
        "source_system",
        "source_assets",
        "created_at",
        "schema_version",
        "consumption_task",
        "model_name",
        "model_version",
        "model_uri",
        "model_artifact_sha256",
        "model_artifact_bytes",
    ]

    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__module__)
        self.client: Any | None = None

    def run(self, logical_date: Any) -> dict[str, Any]:
        created_at = logical_date_iso(logical_date)
        self.logger.info(
            "Starting natural disaster tweet classifier consumption run created_at=%s source=%s",
            created_at,
            self.SOURCE_ASSETS,
        )
        client = self.get_client()
        try:
            self.client = client
            self.validate_source()
            training_frame = self.load_training_frame()
            metrics_record = self.build_metrics_record(training_frame, created_at=created_at)
            self.ensure_metrics_table()
            self.insert_metrics_record(metrics_record)
            self.logger.info(
                "Classifier metrics written table=%s.%s row_count=%s train_size=%s test_size=%s accuracy=%.6f f1=%.6f",
                self.SOURCE_DB,
                self.METRICS_TABLE,
                metrics_record["row_count"],
                metrics_record["train_size"],
                metrics_record["test_size"],
                metrics_record["accuracy"],
                metrics_record["f1"],
            )
            return metrics_record
        finally:
            client.close()
            self.client = None

    def get_client(self) -> Any:
        import clickhouse_connect

        username = os.getenv("CLICKHOUSE_CONSUMPTION_USER")
        password = os.getenv("CLICKHOUSE_CONSUMPTION_PASSWORD")
        if not username or not password:
            # NOTE: analytics is kept only as the local maintenance fallback.
            username = os.getenv("CLICKHOUSE_USER", "analytics")
            password = os.getenv("CLICKHOUSE_PASSWORD", "analytics_secret")
            self.logger.warning("CLICKHOUSE_CONSUMPTION_* is missing; using ClickHouse maintenance fallback user")

        return clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
            username=username,
            password=password,
            database=os.getenv("CLICKHOUSE_DATABASE", "default"),
        )

    @staticmethod
    def quote_identifier(identifier: str) -> str:
        return "`" + identifier.replace("`", "``") + "`"

    @staticmethod
    def sql_literal(value: str) -> str:
        return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"

    def table_ref(self, table_name: str) -> str:
        return f"{self.quote_identifier(self.SOURCE_DB)}.{self.quote_identifier(table_name)}"

    def _require_client(self) -> Any:
        if self.client is None:
            raise RuntimeError("ClickHouse client is not initialized")
        return self.client

    def validate_source(self) -> None:
        client = self._require_client()
        self.logger.info("Validating source table %s", self.SOURCE_ASSETS)

        exists = client.query(
            f"""
            SELECT count()
            FROM system.tables
            WHERE database = {self.sql_literal(self.SOURCE_DB)}
              AND name = {self.sql_literal(self.SOURCE_TABLE)}
            """
        ).first_row[0]
        if exists == 0:
            raise ValueError(f"Source table does not exist: {self.SOURCE_ASSETS}")

        columns = {
            row[0]
            for row in client.query(
                f"""
                SELECT name
                FROM system.columns
                WHERE database = {self.sql_literal(self.SOURCE_DB)}
                  AND table = {self.sql_literal(self.SOURCE_TABLE)}
                """
            ).result_rows
        }
        missing_columns = sorted(self.REQUIRED_COLUMNS - columns)
        if missing_columns:
            raise ValueError(f"Source table {self.SOURCE_ASSETS} is missing required columns: {missing_columns}")

        row_count = client.query(f"SELECT count() FROM {self.table_ref(self.SOURCE_TABLE)}").first_row[0]
        if row_count == 0:
            raise ValueError(f"Source table is empty: {self.SOURCE_ASSETS}")

        self.logger.info(
            "Validated source table %s rows=%s required_columns=%s",
            self.SOURCE_ASSETS,
            row_count,
            sorted(self.REQUIRED_COLUMNS),
        )

    def load_training_frame(self) -> Any:
        client = self._require_client()
        self.logger.info("Loading classifier training columns from %s", self.SOURCE_ASSETS)
        training_frame = client.query_df(
            f"""
            SELECT tweet_text, disaster_type, word_count
            FROM {self.table_ref(self.SOURCE_TABLE)}
            """
        )
        self.logger.info("Loaded %s raw classifier rows from %s", len(training_frame), self.SOURCE_ASSETS)
        return training_frame

    def build_metrics_record(self, dataframe: Any, *, created_at: str) -> dict[str, Any]:
        training_result = self.train_evaluate(dataframe)
        metrics = training_result["metrics"]
        model_artifact = training_result["model_artifact"]
        model_uri = self.store_model_artifact(
            model_artifact["bytes"],
            created_at=created_at,
            metrics=metrics,
        )
        record: dict[str, Any] = {
            **metrics,
            "source_system": self.SOURCE_SYSTEM,
            "source_assets": self.SOURCE_ASSETS,
            "created_at": created_at,
            "schema_version": CONSUMPTION_SCHEMA_VERSION,
            "consumption_task": self.CONSUMPTION_TASK,
            "model_name": self.MODEL_NAME,
            "model_version": self.MODEL_VERSION,
            "model_uri": model_uri,
            "model_artifact_sha256": model_artifact["sha256"],
            "model_artifact_bytes": model_artifact["byte_count"],
        }
        return record

    def train_evaluate(self, dataframe: Any) -> dict[str, Any]:
        import io

        import joblib
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import accuracy_score, precision_recall_fscore_support
        from sklearn.model_selection import train_test_split
        from sklearn.pipeline import Pipeline

        missing_columns = sorted(self.REQUIRED_COLUMNS - set(dataframe.columns))
        if missing_columns:
            raise ValueError(f"Classifier dataframe is missing required columns: {missing_columns}")

        df_clean = dataframe.copy()
        df_clean = df_clean[df_clean["word_count"] > 3]
        row_count = int(len(df_clean))
        self.logger.info("Classifier preprocessing retained rows=%s from raw_rows=%s using word_count > 3", row_count, len(dataframe))
        if row_count == 0:
            raise ValueError("Classifier training frame is empty after word_count > 3 preprocessing")

        label_counts = df_clean["disaster_type"].value_counts()
        if len(label_counts) < 2:
            raise ValueError("Classifier requires at least two disaster_type classes")
        if int(label_counts.min()) < 2:
            raise ValueError("Classifier stratified split requires at least two rows per disaster_type class")

        x_values = df_clean["tweet_text"]
        y_values = df_clean["disaster_type"]

        x_train, x_test, y_train, y_test = train_test_split(
            x_values,
            y_values,
            test_size=0.2,
            stratify=y_values,
            random_state=2026,
        )

        model_pipeline = Pipeline(
            steps=[
                ("tfidf", TfidfVectorizer(max_features=20000, ngram_range=(1, 2))),
                ("classifier", LogisticRegression(max_iter=1000)),
            ]
        )
        model_pipeline.fit(x_train, y_train)
        y_pred = model_pipeline.predict(x_test)

        precision, recall, f1, _ = precision_recall_fscore_support(
            y_test,
            y_pred,
            average="weighted",
            zero_division=0,
        )
        metrics = {
            "row_count": row_count,
            "accuracy": float(accuracy_score(y_test, y_pred)),
            "precision": float(precision),
            "recall": float(recall),
            "f1": float(f1),
            "train_size": int(len(x_train)),
            "test_size": int(len(x_test)),
        }
        self.logger.info(
            "Classifier evaluation completed classes=%s train_size=%s test_size=%s accuracy=%.6f precision=%.6f recall=%.6f f1=%.6f",
            sorted(str(label) for label in label_counts.index),
            metrics["train_size"],
            metrics["test_size"],
            metrics["accuracy"],
            metrics["precision"],
            metrics["recall"],
            metrics["f1"],
        )

        artifact_buffer = io.BytesIO()
        joblib.dump(model_pipeline, artifact_buffer)
        artifact_bytes = artifact_buffer.getvalue()
        artifact_digest = sha256(artifact_bytes).hexdigest()
        self.logger.info(
            "Classifier model artifact prepared model_name=%s model_version=%s format=%s bytes=%s sha256=%s",
            self.MODEL_NAME,
            self.MODEL_VERSION,
            self.MODEL_ARTIFACT_FORMAT,
            len(artifact_bytes),
            artifact_digest,
        )
        return {
            "metrics": metrics,
            "model_artifact": {
                "bytes": artifact_bytes,
                "sha256": artifact_digest,
                "byte_count": len(artifact_bytes),
            },
        }

    def store_model_artifact(self, artifact_bytes: bytes, *, created_at: str, metrics: dict[str, Any]) -> str:
        from src import get_minio_client

        safe_created_at = "".join(ch if ch.isalnum() else "_" for ch in created_at).strip("_")
        object_key = (
            f"{self.MODEL_ARTIFACT_PREFIX}/"
            f"model_name={self.MODEL_NAME}/"
            f"model_version={self.MODEL_VERSION}/"
            f"created_at={safe_created_at}/"
            f"model.{self.MODEL_ARTIFACT_FORMAT}"
        )
        model_uri = f"s3a://{self.MODEL_BUCKET}/{object_key}"
        minio_role = os.getenv("MINIO_CONSUMPTION_WRITER_ROLE", "writer")
        minio_client = get_minio_client(role=minio_role)
        minio_client.upload_file_atomic(
            self.MODEL_BUCKET,
            object_key,
            artifact_bytes,
            content_type="application/octet-stream",
            metadata={
                "model_name": self.MODEL_NAME,
                "model_version": self.MODEL_VERSION,
                "schema_version": CONSUMPTION_SCHEMA_VERSION,
                "created_at": created_at,
                "accuracy": f"{metrics['accuracy']:.6f}",
                "f1": f"{metrics['f1']:.6f}",
            },
        )
        self.logger.info(
            "Classifier model artifact stored model_uri=%s bytes=%s",
            model_uri,
            len(artifact_bytes),
        )
        return model_uri

    def ensure_metrics_table(self) -> None:
        client = self._require_client()
        self.logger.info("Ensuring classifier metrics table %s.%s", self.SOURCE_DB, self.METRICS_TABLE)
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_ref(self.METRICS_TABLE)}
            (
                row_count UInt64,
                accuracy Float64,
                precision Float64,
                recall Float64,
                f1 Float64,
                train_size UInt64,
                test_size UInt64,
                source_system String,
                source_assets String,
                created_at String,
                schema_version String,
                consumption_task String,
                model_name String,
                model_version String,
                model_uri String,
                model_artifact_sha256 String,
                model_artifact_bytes UInt64
            )
            ENGINE = MergeTree()
            ORDER BY (created_at, consumption_task, model_name, model_version)
            """
        )
        for column_name, column_type in (
            ("model_uri", "String"),
            ("model_artifact_sha256", "String"),
            ("model_artifact_bytes", "UInt64"),
        ):
            client.command(
                f"ALTER TABLE {self.table_ref(self.METRICS_TABLE)} ADD COLUMN IF NOT EXISTS {self.quote_identifier(column_name)} {column_type}"
            )

    def insert_metrics_record(self, record: dict[str, Any]) -> None:
        client = self._require_client()
        payload = [[record[column] for column in self.METRICS_COLUMNS]]
        client.insert(
            table=self.METRICS_TABLE,
            data=payload,
            column_names=self.METRICS_COLUMNS,
            database=self.SOURCE_DB,
        )
        self.logger.info("Inserted classifier metrics row columns=%s", self.METRICS_COLUMNS)
