from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
import os
from src.utils import load_kafka_config, load_stream_config

KAFKA_BROKERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
stream_config = load_stream_config()
kafka_config = load_kafka_config()
schedule_time = stream_config.get("batch_aggregation_schedule_minutes",5)
@dag(
    dag_id='ingest_kafka_dataset',
    is_paused_upon_creation=False,

    schedule=timedelta(minutes=schedule_time),
    start_date=datetime(2026, 4, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ingest", "kafka", "delta-lake", "aggregation"]
)
def ingest_kafka_dataset():

    @task(retries=60, retry_delay=timedelta(seconds=60))
    def wait_for_infra_ready_task(**context):
        from src import get_minio_client
        from src.utils import get_logger

        minio_client = get_minio_client(role="writer")
        logger = get_logger(context.get("task_id"))

        try:
            logger.info("Checking MinIO bucket existence...")

            minio_client.client.head_bucket(Bucket="landing-zone")

            logger.info("Infra ready (bucket exists).")
            return True

        except Exception as e:
            logger.exception("Infra not ready")
            raise
    wait_for_infra = wait_for_infra_ready_task()
    @task()
    def consume_and_store(config, **context) -> dict:

        import hashlib
        import json

        from kafka import KafkaConsumer
        from src.utils import get_logger
        from src.utils.time_anchor import compact_date_partition, logical_date_from_context, logical_date_iso
        from src import get_minio_client
        minio_client = get_minio_client(role="writer")
        topic_name = config["name"]
        group_id = config["group_id"]
        logical_date = logical_date_from_context(context)
        logger = get_logger(f"consume_and_store_{topic_name}")
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
        offsets_by_partition = {}
        for tp, msgs in records.items():
            for msg in msgs:
                raw_records.append(msg.value)
                offsets_by_partition.setdefault(tp.partition, []).append(msg.offset)
        if not raw_records:
            logger.info("No new messages in Kafka; nothing to write.")
            consumer.close()
            return {"rows_written": 0}
        date_folder = compact_date_partition(logical_date)
        offset_ranges = {
            str(partition): {
                "start": min(offsets),
                "end": max(offsets),
            }
            for partition, offsets in sorted(offsets_by_partition.items())
        }
        offset_fingerprint = hashlib.md5(
            json.dumps(offset_ranges, sort_keys=True).encode("utf-8")
        ).hexdigest()[:12]
        partition_label = "-".join(offset_ranges.keys())
        key = (
            f"persistent-landing/semistructured/{topic_name}/{date_folder}/"
            f"p{partition_label}_{offset_fingerprint}.json"
        )

        minio_client.upload_file_atomic(
            bucket_name="landing-zone",
            object_key=key,
            body=json.dumps(raw_records).encode("utf-8"),
            content_type="application/json",
            metadata={
                "source": "kafka",
                "topic": topic_name,
                "group_id": group_id,
                "logical_date": logical_date_iso(logical_date),
                "offset_ranges": json.dumps(offset_ranges, sort_keys=True),
            },
        )

        logger.info(f"Successfully saved {len(raw_records)} records to {key}")

        consumer.commit()
        consumer.close()
        return {"rows_written": len(raw_records)}
    if kafka_config:
        # Using .override() is the SDK way to set dynamic task IDs
        ingest_instance_kafka = consume_and_store.expand(config=kafka_config)
        wait_for_infra >> ingest_instance_kafka

        trigger_catalog_update = TriggerDagRunOperator(
            task_id="trigger_kafka_catalog_update",
            trigger_dag_id="daily_kafka_data_catalog_update",
            conf={
                "trigger_source": "kafka_ingest",
                "source_logical_date": "{{ logical_date.isoformat() }}",
            },
            wait_for_completion=False,
        )
        ingest_instance_kafka >> trigger_catalog_update

ingest_kafka_dataset()


