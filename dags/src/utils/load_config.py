import os
from pathlib import Path
import yaml
import re
from functools import lru_cache
from .logging_util import get_logger
logger = get_logger(__name__)



def _substitute_env_vars(content: str) -> str:
    """
    Helper to replace ${VAR_NAME} with environment variable values.
    """
    def replace_env_var(match):
        var_name = match.group(1)
        value = os.getenv(var_name)
        if value is None:
            logger.error(f"Environment variable '${{{var_name}}}' is not set!")
            return f"MISSING_{var_name}"
        return value

    return re.sub(r"\${(.*?)}", replace_env_var, content)

@lru_cache(maxsize=1)
def get_minio_config():
    """
        Loads MinIO configuration from a YAML file and performs environment variable substitution.

        Returns:
            dict: The processed configuration dictionary containing connection details and bucket definitions.

        Raises:
            FileNotFoundError: If the 'minio.yaml' file is not found at the expected project path.
            yaml.YAMLError: If the configuration file contains invalid YAML syntax.
            KeyError: If mandatory environment variables required by the YAML are not set in the system.
        """
    # Resolve the absolute path to the configuration directory
    root = Path(__file__).resolve().parents[3]
    path = root / "config" / "minio.yaml"

    if not path.exists():
        raise FileNotFoundError(f"MinIO configuration not found at: {path}")

    logger.info(f"Loading MinIO configuration from: {path}")

    with open(path, "r") as f:
        processed_yaml = _substitute_env_vars(f.read())
        config = yaml.safe_load(processed_yaml)

    buckets = list(config['minio'].get("buckets", {}).keys())
    logger.info(f"MinIO configuration ready. Buckets: {buckets}")
    return config


@lru_cache(maxsize=10)
def get_config(file_name, key):
    """
    Generic configuration loader that reads YAML files from the project's config directory.
    It supports environment variable substitution and caches results for performance.

    Param:
        file_name (str): The name of the YAML file (e.g., 'kafka_consumer.yaml').
        key (str): The top-level key in the YAML to extract data from.

    Returns:
        list|dict: The data associated with the specified key, or an empty list if not found.

    Raises:
        FileNotFoundError: If the configuration file does not exist at the expected path.
    """
    # Navigate up 3 levels from this file to locate the project root, then enter the 'config' folder
    root = Path(__file__).resolve().parents[3]
    path = root / "config" / file_name

    if not path.exists():
        logger.error(f"Configuration file missing at: {path}")
        raise FileNotFoundError(f"Configuration not found at: {path}")

    logger.info(f"Accessing config file: {path}")

    with open(path, "r") as f:
        # Step 1: Replace ${VAR} placeholders with actual environment variable values
        processed_yaml = _substitute_env_vars(f.read())
        # Step 2: Parse the processed string into a Python dictionary
        config = yaml.safe_load(processed_yaml)

    # Extract the specific configuration data (e.g., list of topics or datasets)
    data = config.get(key, [])
    logger.info(f"Successfully retrieved {len(data)} items from {file_name} using key '{key}'.")

    return config