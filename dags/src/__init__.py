def get_minio_client(role="admin"):
    from .utils.minio_client import MinioClient

    return MinioClient(role=role)


def get_duckdb_client(role="admin"):
    from .utils.duckdb_client import MinioDuckDB

    return MinioDuckDB(role=role)
