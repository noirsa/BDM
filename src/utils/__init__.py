"""
Utility Package Initialization.
This module ensures that the environment, logging, and configuration 
are set up in the correct sequence before any data processing tasks begin.
"""

from .env_utils import setup_environment
from .logging_util import configure_logger, get_logger
from .load_config import get_minio_config

# 1. Load environment variables first (No logging dependencies here)
setup_environment()

# 2. Configure the global logging system
configure_logger()

# 3. Create a primary logger for the package
log = get_logger(__name__)

# 4. Load the configuration and pass the logger for tracking
# The @lru_cache in get_minio_config ensures this only runs once
minio_config = get_minio_config()

log.info("Utility package initialized successfully.")