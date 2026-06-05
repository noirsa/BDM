from ..utils import get_storage_options

from ..utils import get_logger

log = get_logger(__name__)


class BaseDeltalakeLoader:
    def __init__(self,minio_client,duckdb_client):
        self.logger = log
        self.minio_client = minio_client
        self.logger.info(f"{self.__class__.__name__} service initialized.")
        self.duckdb_client= duckdb_client
        self.storage_options = get_storage_options(role=getattr(minio_client, "role", "admin"))
