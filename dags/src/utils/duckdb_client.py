import duckdb
from .logging_util import get_logger
from .load_config import get_minio_config, get_minio_credentials

logger = get_logger(__name__)

class MinioDuckDB:
    def __init__(self, role="admin"):
        self.role = role
        minio_config = get_minio_config()['minio']
        minio_credentials = get_minio_credentials(role)
        self.endpoint = minio_config["endpoint"]
        self.access_key = minio_credentials["access_key"]
        self.secret_key = minio_credentials["secret_key"]

        logger.info(f"Initializing DuckDB with MinIO endpoint: {self.endpoint} and role '{self.role}'")

        self.con = duckdb.connect(database=':memory:')

        # Ensure HTTPFS is available
        try:
            self.con.execute("INSTALL httpfs; LOAD httpfs;")
            self.con.execute("INSTALL delta; LOAD delta;")
            # Configure MinIO connection
            clean_endpoint = self.endpoint.replace("http://", "").replace("https://", "")
            use_ssl = 'true' if 'https' in self.endpoint else 'false'

            self.con.execute(f"""
                CREATE OR REPLACE SECRET minio_secret (
                    TYPE S3,
                    KEY_ID '{self.access_key}',
                    SECRET '{self.secret_key}',
                    ENDPOINT '{clean_endpoint}',
                    URL_STYLE 'path',
                    USE_SSL {use_ssl}
                );
            """)
            logger.info("Successfully configured DuckDB S3 secret.")
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB/HTTPFS: {e}")
            raise

        # 3. Optimization for Large Files
        # Use more threads for processing but limit to prevent OOM on Airflow Workers
        self.con.execute("SET threads TO 2;")
        # Increase memory limit if your CSVs are very large (e.g., '4GB')
        self.con.execute("SET memory_limit = '2GB';")

    def convert_csv_to_parquet(self,bucket: str, src_s3_path: str, dest_s3_path: str):
        """
        Converts CSV to Parquet without specifying any columns or schema.
        DuckDB will auto-detect everything.
        """
        logger.info(f"Converting {src_s3_path} using full auto-inference.")


        sql = f"""
            COPY (
                SELECT * FROM read_csv_auto(
                    's3://{bucket}/{src_s3_path}',
                    sample_size=200000,
                    hive_partitioning=false
                )
            ) TO 's3://{bucket}/{dest_s3_path}' (FORMAT 'PARQUET', COMPRESSION 'SNAPPY');
        """

        self.con.execute(sql)

    def final_verification(self, original_csv, delta_table_path):
        """
        Verify raw CSV and Delta contents before the raw file is deleted.
        """
        logger.info("--- Starting CSV to Delta verification ---")

        try:
            self.con.execute("INSTALL delta; LOAD delta;")
        except Exception as e:
            logger.warning(f"Delta extension might already be loaded: {e}")

        try:
            from src.deltalake.integrity import DataIntegrityVerifier

            verifier = DataIntegrityVerifier(self.con)
            return verifier.verify_csv_matches_delta(original_csv, delta_table_path)

        except Exception as e:
            error_message = str(e)
            if "MissingVersionError" in error_message or "No table version found" in error_message:
                logger.info("Delta table is not readable yet; conversion is required. Details: %s", error_message)
            else:
                logger.error(f"Verification process crashed: {error_message}")
            return False
