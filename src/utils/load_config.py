import os
from pathlib import Path
import yaml
import re
from dotenv import load_dotenv
from functools import lru_cache

@lru_cache(maxsize=1)
def get_minio_config(path=None):
    if path is None:
        # This makes it robust regardless of where the script runs
        path = Path(__file__).resolve().parent.parent.parent / "configuration" / "minio.yaml"
    with open(path, "r") as f:
        # Load the raw text
        raw_yaml = f.read()
        # Simple Regex to find ${VARIABLE_NAME} and replace with os.environ
        def replace_env_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, f"MISSING_{var_name}")

        processed_yaml = re.sub(r"\${(.*?)}", replace_env_var, raw_yaml)
        return yaml.safe_load(processed_yaml)
