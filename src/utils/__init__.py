# 1. Initialize environment variables first
from .env_utils import setup_environment
setup_environment()

from .load_config import get_minio_config