def get_minio_client():
    from .utils.minio_client import MinioClient

    return MinioClient()


def get_duckdb_client():
    from .utils.duckdb_client import MinioDuckDB

    return MinioDuckDB()
