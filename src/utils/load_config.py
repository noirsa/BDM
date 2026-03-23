import os
import yaml
import re
from dotenv import load_dotenv

load_dotenv()
def get_minio_config(path="../../configuration/minio.yaml"):
    with open(path, "r") as f:
        # Load the raw text
        raw_yaml = f.read()
        # Simple Regex to find ${VARIABLE_NAME} and replace with os.environ
        def replace_env_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, f"MISSING_{var_name}")

        processed_yaml = re.sub(r"\${(.*?)}", replace_env_var, raw_yaml)
        return yaml.safe_load(processed_yaml)

# Usage in your DAG:
minio_settings = get_minio_config()["minio"]
print(f"Connecting to: {minio_settings}")