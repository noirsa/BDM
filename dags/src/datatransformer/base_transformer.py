import pandas as pd
import io


log = get_logger(__name__)


class BaseTransformer:
    def __init__(self,minio_client):
        self.logger = log
        self.minio_client = minio_client
        self.logger.info(f"{self.__class__.__name__} service initialized.")