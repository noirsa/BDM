"""Long-running Spark Structured Streaming job for the live weather dashboard."""

from __future__ import annotations

import os
import signal
import sys
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


APP_NAME = os.getenv("DASHBOARD_SPARK_APP_NAME", "consumption_streaming_dashboard")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPICS = os.getenv("DASHBOARD_KAFKA_TOPICS", "weather-barcelona,airquality-barcelona")
STARTING_OFFSETS = os.getenv("DASHBOARD_KAFKA_STARTING_OFFSETS", "earliest")
CHECKPOINT_LOCATION = os.getenv(
    "CONSUMPTION_DASHBOARD_CHECKPOINT_LOCATION",
    "/opt/airflow/logs/checkpoints/consumption_streaming_dashboard/weather",
)
POSTGRES_URL = os.getenv(
    "DASHBOARD_POSTGRES_JDBC_URL",
    "jdbc:postgresql://postgres-analytics:5432/analytics",
)
POSTGRES_TABLE = os.getenv("DASHBOARD_POSTGRES_TABLE", "weather_events")
POSTGRES_USER = os.getenv("DASHBOARD_POSTGRES_USER", "superset")
POSTGRES_PASSWORD = os.getenv("DASHBOARD_POSTGRES_PASSWORD", "superset")
ENABLE_CONSOLE_QUERIES = os.getenv("DASHBOARD_ENABLE_CONSOLE_QUERIES", "false").lower() == "true"


def build_spark_session() -> SparkSession:
    packages = os.getenv(
        "DASHBOARD_SPARK_PACKAGES",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.3",
    )
    executor_python = os.getenv("PYSPARK_EXECUTOR_PYTHON", os.getenv("PYSPARK_PYTHON", "/usr/bin/python3.13"))
    builder = (
        SparkSession.builder.appName(APP_NAME)
        .master(SPARK_MASTER_URL)
        .config("spark.jars.packages", packages)
        .config("spark.pyspark.python", executor_python)
        .config("spark.executorEnv.PYSPARK_PYTHON", executor_python)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.debug.maxToStringFields", "200")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def weather_schema() -> StructType:
    return StructType(
        [
            StructField("interval", IntegerType()),
            StructField("is_day", IntegerType()),
            StructField("temperature", DoubleType()),
            StructField("time", TimestampType()),
            StructField("weathercode", IntegerType()),
            StructField("winddirection", IntegerType()),
            StructField("windspeed", DoubleType()),
        ]
    )


def air_schema() -> ArrayType:
    return ArrayType(
        StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("locality", StringType()),
                StructField("timezone", StringType()),
                StructField(
                    "country",
                    StructType(
                        [
                            StructField("id", IntegerType()),
                            StructField("code", StringType()),
                            StructField("name", StringType()),
                        ]
                    ),
                ),
                StructField(
                    "owner",
                    StructType(
                        [
                            StructField("id", IntegerType()),
                            StructField("name", StringType()),
                        ]
                    ),
                ),
                StructField(
                    "provider",
                    StructType(
                        [
                            StructField("id", IntegerType()),
                            StructField("name", StringType()),
                        ]
                    ),
                ),
                StructField("isMobile", BooleanType()),
                StructField("isMonitor", BooleanType()),
                StructField(
                    "coordinates",
                    StructType(
                        [
                            StructField("latitude", DoubleType()),
                            StructField("longitude", DoubleType()),
                        ]
                    ),
                ),
                StructField("instruments", ArrayType(StringType())),
                StructField("sensors", ArrayType(StringType())),
                StructField("licenses", ArrayType(StringType())),
                StructField("bounds", ArrayType(DoubleType())),
                StructField(
                    "datetimeFirst",
                    StructType(
                        [
                            StructField("utc", StringType()),
                            StructField("local", StringType()),
                        ]
                    ),
                ),
                StructField(
                    "datetimeLast",
                    StructType(
                        [
                            StructField("utc", StringType()),
                            StructField("local", StringType()),
                        ]
                    ),
                ),
            ]
        )
    )


def build_streams(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPICS)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    weather_stream = raw_stream.filter(col("topic") == "weather-barcelona")
    air_stream = raw_stream.filter(col("topic") == "airquality-barcelona")

    weather_events = (
        weather_stream.select(from_json(col("value").cast("string"), weather_schema()).alias("data"))
        .select("data.*")
    )

    air_rows = air_stream.select(from_json(col("value").cast("string"), air_schema()).alias("data")).select(
        explode(col("data")).alias("station")
    )
    air_events = air_rows.select(
        col("station.id").alias("station_id"),
        col("station.name").alias("station_name"),
        col("station.locality"),
        col("station.timezone"),
        col("station.isMobile"),
        col("station.isMonitor"),
        col("station.coordinates.latitude").alias("latitude"),
        col("station.coordinates.longitude").alias("longitude"),
        col("station.country.name").alias("country"),
        col("station.provider.name").alias("provider"),
        col("station.owner.name").alias("owner"),
        col("station.datetimeFirst.local").alias("datetimeFirst"),
        col("station.datetimeLast.local").alias("datetimeLast"),
        col("station.sensors"),
    )
    return weather_events, air_events


def write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    raw_count = batch_df.count()
    print(f"[micro-batch {batch_id}] received rows={raw_count}", flush=True)

    cleaned_df = batch_df.filter(
        (col("temperature").between(-50, 60))
        & (col("windspeed").between(0, 150))
        & (col("winddirection").between(0, 360))
        & (col("interval") > 0)
        & (col("time").isNotNull())
    )
    cleaned_count = cleaned_df.count()
    print(
        f"[micro-batch {batch_id}] cleaned rows={cleaned_count}; rejected={raw_count - cleaned_count}",
        flush=True,
    )

    if cleaned_count == 0:
        print(f"[micro-batch {batch_id}] skipped PostgreSQL write because the cleaned batch is empty", flush=True)
        return

    (
        cleaned_df.write.format("jdbc")
        .mode("append")
        .option("url", POSTGRES_URL)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", POSTGRES_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .save()
    )
    print(f"[micro-batch {batch_id}] wrote {cleaned_count} rows to PostgreSQL table {POSTGRES_TABLE}", flush=True)


def stop_queries(queries: list[Any], spark: SparkSession | None) -> None:
    for query in queries:
        try:
            if query.isActive:
                query.stop()
        except Exception as exc:  # pragma: no cover - defensive shutdown logging
            print(f"[shutdown] failed to stop query: {exc}", flush=True)
    if spark is not None:
        try:
            spark.stop()
        except Exception as exc:  # pragma: no cover
            print(f"[shutdown] failed to stop Spark session: {exc}", flush=True)


def main() -> int:
    spark: SparkSession | None = None
    queries: list[Any] = []

    def handle_shutdown(signum: int, _frame: Any) -> None:
        print(f"[shutdown] received signal={signum}", flush=True)
        stop_queries(queries, spark)
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    spark = build_spark_session()
    weather_events, air_events = build_streams(spark)

    if ENABLE_CONSOLE_QUERIES:
        queries.append(
            weather_events.writeStream.format("console")
            .option("truncate", False)
            .option("numRows", 10)
            .queryName(f"{APP_NAME}_weather_console")
            .start()
        )
        queries.append(
            air_events.writeStream.format("console")
            .option("truncate", False)
            .option("numRows", 10)
            .queryName(f"{APP_NAME}_air_console")
            .start()
        )

    dashboard_query = (
        weather_events.writeStream.foreachBatch(write_to_postgres)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .outputMode("append")
        .queryName(f"{APP_NAME}_weather_postgres")
        .start()
    )
    queries.append(dashboard_query)
    print(
        f"[startup] {APP_NAME} started topics={KAFKA_TOPICS} checkpoint={CHECKPOINT_LOCATION} sink={POSTGRES_TABLE}",
        flush=True,
    )
    dashboard_query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
