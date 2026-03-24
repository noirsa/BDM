import os
import logging
import sys


def configure_logger():
    """
    Sets up the global logging configuration.
    This should be called only once during application startup.
    Mapping:
        1: DEBUG | 2: INFO | 3: WARNING | 4: ERROR | 5: CRITICAL
    """
    # Map integer keys to Python logging level constants
    level_mapping = {
        1: logging.DEBUG,
        2: logging.INFO,
        3: logging.WARNING,
        4: logging.ERROR,
        5: logging.CRITICAL
    }

    # Retrieve LOG_LEVEL from .env (defaults to 2/INFO if missing or invalid)
    try:
        user_val = int(os.getenv("LOG_LEVEL", 2))
    except ValueError:
        user_val = 2

    # Get the corresponding logging level, defaulting to INFO if key not in map
    log_level = level_mapping.get(user_val, logging.INFO)

    # Define the log format
    log_format = "%(asctime)s - %(filename)s:%(lineno)d - %(name)s - %(levelname)s - %(message)s"

    # Configure the root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)  # Ensure logs go to Airflow/Docker console
        ]
    )


def get_logger(name):
    """
    Returns a logger instance for a specific module.
    """
    return logging.getLogger(name)