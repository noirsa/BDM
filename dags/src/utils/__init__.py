"""
Utility Package Initialization.
This module ensures that the environment, logging, and configuration 
are set up in the correct sequence before any data processing tasks begin.
"""

from .env_utils import setup_environment
from .logging_util import configure_logger, get_logger
from .load_config import get_minio_config, get_dataset_config
from .minio_client import MinioClient
from .duckdb_client import MinioDuckDB
from .parsers import ImageParser
# 1. Load environment variables first (No logging dependencies here)
setup_environment()

# 2. Configure the global logging system
configure_logger()

# 3. Create a primary logger for the package
log = get_logger(__name__)

# 4. Load the configuration and pass the logger for tracking
# The @lru_cache in get_minio_config ensures this only runs once
minio_config = get_minio_config()['minio']
kaggle_config = get_dataset_config("kaggle_dataset.yaml","kaggle_dataset")['kaggle_dataset']
huggingface_config = get_dataset_config("huggingface_dataset.yaml","huggingface_dataset")['huggingface_dataset']

log.info("Utility package initialized successfully.")

storage_options = {
    "AWS_ACCESS_KEY_ID": minio_config["access_key"],
    "AWS_SECRET_ACCESS_KEY": minio_config["secret_key"],
    "AWS_ENDPOINT_URL": minio_config["endpoint"],
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "AWS_ALLOW_HTTP": "true",
    "region": "us-east-1"
}
