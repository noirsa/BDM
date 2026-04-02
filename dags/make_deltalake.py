from airflow.sdk import dag, task
from datetime import datetime
from airflow.sensors.python import PythonSensor
from src.utils import get_logger
from src import minio_client,duckdb_client
logger = get_logger(__name__)


