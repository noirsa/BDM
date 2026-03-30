from utils import get_minio_config
from utils import kaggle_config

# Usage in your DAG:
minio_settings = get_minio_config()["minio"]
print(f"Connecting to: {minio_settings}")
print(f"Connecting to: {kaggle_config}")