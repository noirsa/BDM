"""
Utility Package Initialization.
This module ensures that the environment, logging, and configuration 
are set up in the correct sequence before any data processing tasks begin.
"""

from .env_utils import setup_environment
from .logging_util import configure_logger, get_logger
from .load_config import get_minio_config, get_config
from .helper import get_deep_keys
from .time_anchor import compact_date_partition, date_partition, logical_date_from_context, logical_date_iso, logical_date_suffix
# 1. Load environment variables first (No logging dependencies here)
setup_environment()

# 2. Configure the global logging system
configure_logger()

# 3. Create a primary logger for the package
log = get_logger(__name__)

log.info("Utility package initialized successfully.")

def load_kaggle_config():
    return get_config("kaggle_dataset.yaml", "kaggle_dataset")["kaggle_dataset"]


def load_huggingface_config():
    return get_config("huggingface_dataset.yaml", "huggingface_dataset")["huggingface_dataset"]


def load_kafka_config():
    return get_config("kafka.yaml", "kafka_consumer")["kafka_consumer"]


def load_stream_config():
    return get_config("stream_ingestion_config.yaml", "ingestion_strategy")["ingestion_strategy"]


def get_storage_options():
    minio_config = get_minio_config()['minio']
    return {
        "AWS_ACCESS_KEY_ID": minio_config["access_key"],
        "AWS_SECRET_ACCESS_KEY": minio_config["secret_key"],
        "AWS_ENDPOINT_URL": minio_config["endpoint"],
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_S3_ADDRESSING_STYLE": "path",
        "AWS_ALLOW_HTTP": "true",
        "region": "us-east-1"
    }
