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


@lru_cache(maxsize=1)
def get_dataset_config(path,key):
    """
    Retrieves Kaggle dataset configurations from a YAML file.

    Returns:
        dict: The processed configuration dictionary containing dataset details.

    Raises:
        FileNotFoundError: If the kaggle_dataset.yaml file is not found at the expected project path.
        yaml.YAMLError: If the configuration file contains invalid YAML syntax.
    """

    root = Path(__file__).resolve().parents[3]
    path = root / "config" / path

    if not path.exists():
        raise FileNotFoundError(f"dataset configuration not found at: {path}")

    logger.info(f"Loading dataset configuration from: {path}")

    with open(path, "r") as f:
        # Reusing the substitution logic in case your Kaggle YAML uses env vars too!
        processed_yaml = _substitute_env_vars(f.read())
        config = yaml.safe_load(processed_yaml)

    datasets = config.get(key, [])
    logger.info(f"Successfully loaded {len(datasets)} datasets configurations from {path}.")
    return config

@lru_cache(maxsize=1)
def get_kafka_config(path,key):
    """
    Retrieves Kaggle dataset configurations from a YAML file.

    Returns:
        dict: The processed configuration dictionary containing dataset details.

    Raises:
        FileNotFoundError: If the kaggle_dataset.yaml file is not found at the expected project path.
        yaml.YAMLError: If the configuration file contains invalid YAML syntax.
    """

    root = Path(__file__).resolve().parents[3]
    path = root / "config" / path

    if not path.exists():
        raise FileNotFoundError(f"kafka configuration not found at: {path}")

    logger.info(f"Loading dataset configuration from: {path}")

    with open(path, "r") as f:
        # Reusing the substitution logic in case your Kaggle YAML uses env vars too!
        processed_yaml = _substitute_env_vars(f.read())
        config = yaml.safe_load(processed_yaml)

    datasets = config.get(key, [])
    logger.info(f"Successfully loaded {len(datasets)} configurations of kafka consumer from {path}.")
    return config