import os
from pathlib import Path
import yaml
import re
from functools import lru_cache
from .logging_util import get_logger
logger = get_logger(__name__)
@lru_cache(maxsize=1)
def get_minio_config():
    """
    Loads MinIO configuration from a YAML file and performs environment variable substitution.


    Returns:
        dict: The processed configuration dictionary.
    """
    # Resolve the absolute path to the configuration directory
    root = Path(__file__).resolve().parent.parent.parent
    path = root / "configuration" / "minio.yaml"

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {path}")

    # Log the action only if a logger is provided
    if logger:
        logger.info(f"Loading MinIO configuration from: {path}")

    with open(path, "r") as f:
        raw_yaml = f.read()

        # Regex helper to replace ${VAR} with environment values
        def replace_env_var(match):
            var_name = match.group(1)
            value = os.getenv(var_name)
            if value is None:
                logger.error(f"Environment variable '${{{var_name}}}' is not set!")
                return f"MISSING_{var_name}"
            return value

        processed_yaml = re.sub(r"\${(.*?)}", replace_env_var, raw_yaml)
        config = yaml.safe_load(processed_yaml)

        # Safely report initialized buckets
        buckets = list(config.get("buckets", {}).keys())
        logger.info(f"MinIO configuration ready. Buckets: {buckets}")

        return config