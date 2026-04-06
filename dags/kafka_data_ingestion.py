from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
import os
from src.utils import kafka_config

KAFKA_BROKERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

@dag(
    dag_id='ingest_kafka_dataset',
    is_paused_upon_creation=False,

    schedule=timedelta(minutes=5),
    start_date=datetime.now(tz=timezone.utc) - timedelta(hours=1),
    catchup=False,
    tags=["ingest", "kafka", "delta-lake", "aggregation"]
)
def ingest_kafka_dataset():
    @task()
    def consume_and_store(config) -> dict:

        import json

        import pyarrow as pa
        from deltalake import write_deltalake
        from kafka import KafkaConsumer
        from src.utils import get_logger
        from src import minio_client
        topic_name = config["name"]
        group_id = config["group_id"]
        logger = get_logger(f"consume_and_store_{topic_name}")
        import time
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset="earliest",  # first run reads from beginning
            enable_auto_commit=False,  # commit manually after Delta write
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1_000,  # stop after 1 s with no new messages (== we are up to date)
        )
        records = consumer.poll(timeout_ms=3000)
        raw_records = []
        for tp, msgs in records.items():
            for msg in msgs:
                raw_records.append(msg.value)
        if not raw_records:
            logger.info("No new messages in Kafka — nothing to write.")
            consumer.close()
            return {"rows_written": 0}
        now = datetime.now()
        year_month_day = now.strftime("%Y_%m_%d")
        file_timestamp = str(int(time.time()))
        key=f"persistent-landing/semistructured/{topic_name}/{year_month_day}/{file_timestamp}.json"

        minio_client.client.put_object(Bucket="landing-zone",Key= key, Body=json.dumps(raw_records).encode("utf-8"))

        logger.info(f"Successfully saved {len(raw_records)} records to {key}")

        consumer.commit()
        consumer.close()
        return {"rows_written": len(raw_records)}
    if kafka_config:
        # Using .override() is the SDK way to set dynamic task IDs
        ingest_instance_kafka = consume_and_store.expand(config=kafka_config)


ingest_kafka_dataset()


