import duckdb
from . import  get_logger, get_minio_config

minio_config = get_minio_config()['minio']

logger = get_logger(__name__)

class MinioDuckDB:
    def __init__(self):
        self.endpoint = minio_config["endpoint"]
        self.access_key = minio_config["access_key"]
        self.secret_key = minio_config["secret_key"]

        logger.info(f"Initializing DuckDB with MinIO endpoint: {self.endpoint}")

        self.con = duckdb.connect(database=':memory:')

        # Ensure HTTPFS is available
        try:
            self.con.execute("INSTALL httpfs; LOAD httpfs;")

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

    def convert_csv_to_parquet_no_schema(self, src_s3_path: str, dest_s3_path: str):
        """
        Converts CSV to Parquet without specifying any columns or schema.
        DuckDB will auto-detect everything.
        """
        logger.info(f"Converting {src_s3_path} using full auto-inference.")


        sql = f"""
            COPY (
                SELECT * FROM read_csv_auto('{src_s3_path}', sample_size=200000)
            ) TO '{dest_s3_path}' (FORMAT 'PARQUET', COMPRESSION 'SNAPPY');
        """

        self.con.execute(sql)

    def final_verification(original_csv, delta_table_path, con):
        """
        Verification without knowing column names.
        Checks Row Count + Global Data Hash.
        """
        logger.info("--- Starting Ultimate Verification (Zero-Schema) ---")

        try:
            # 1. Row Count & Column Count Check
            # Ensures the structure hasn't collapsed or split
            res = con.execute(f"""
                SELECT 
                    (SELECT count(*) FROM read_csv_auto('{original_csv}')) as csv_count,
                    (SELECT count(*) FROM read_parquet('{delta_table_path}/*.parquet')) as delta_count,
                    (SELECT len(columns(*)) FROM read_csv_auto('{original_csv}') LIMIT 1) as csv_cols,
                    (SELECT len(columns(*)) FROM read_parquet('{delta_table_path}/*.parquet') LIMIT 1) as delta_cols
            """).fetchone()

            csv_count, delta_count, csv_cols, delta_cols = res

            if csv_count != delta_count or csv_cols != delta_cols:
                logger.error(
                    f"Structural Mismatch! Rows: {csv_count} vs {delta_count}, Cols: {csv_cols} vs {delta_cols}")
                return False

            # 2. Global Checksum (The 'Fingerprint')
            # We cast every column to VARCHAR and hash the concatenation of the first 10,000 rows.
            # This catches '1' vs '1.0' because string representations differ.
            fingerprint_sql = f"""
                WITH csv_data AS (
                    SELECT md5(group_concat(columns(*)::VARCHAR)) as hash 
                    FROM (SELECT * FROM read_csv_auto('{original_csv}') LIMIT 10000)
                ),
                delta_data AS (
                    SELECT md5(group_concat(columns(*)::VARCHAR)) as hash 
                    FROM (SELECT * FROM read_parquet('{delta_table_path}/*.parquet') LIMIT 10000)
                )
                SELECT csv_data.hash == delta_data.hash FROM csv_data, delta_data
            """

            is_identical = con.execute(fingerprint_sql).fetchone()[0]

            if not is_identical:
                logger.error("Data Integrity Failed! Fingerprints do not match (possible type drift).")
                return False

            logger.info(f" All checks passed! Verified {csv_count} rows across {csv_cols} columns.")
            return True

        except Exception as e:
            logger.error(f"Verification crashed: {str(e)}")
            return False