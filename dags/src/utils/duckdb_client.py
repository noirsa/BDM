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
                SELECT * FROM read_csv_auto('s3://{bucket}/{src_s3_path}', sample_size=200000)
            ) TO 's3://{bucket}/{dest_s3_path}' (FORMAT 'PARQUET', COMPRESSION 'SNAPPY');
        """

        self.con.execute(sql)

    def final_verification(self, original_csv, delta_table_path):
        """
        Ultimate Zero-Schema Data Integrity Verification.

        This method is 'Order-Agnostic' (works regardless of row order) and
        'Type-Agnostic' (standardizes data to VARCHAR for comparison).
        """
        logger.info("--- Starting Ultimate Verification (Zero-Schema) ---")

        # Ensure the Delta extension is ready for DuckDB
        try:
            self.con.execute("INSTALL delta; LOAD delta;")
        except Exception as e:
            logger.warning(f"Delta extension might already be loaded: {e}")

        try:
            # STEP 1: Structural Validation
            # We use DESCRIBE to count columns to avoid issues with specific data types.
            # We use delta_scan() to ensure we only count 'active' data in the Lakehouse.
            structure_sql = f"""
                SELECT 
                    (SELECT count(*) FROM read_csv_auto('{original_csv}')) as csv_rows,
                    (SELECT count(*) FROM delta_scan('{delta_table_path}')) as delta_rows,
                    (SELECT count(*) FROM (DESCRIBE SELECT * FROM read_csv_auto('{original_csv}'))) as csv_cols,
                    (SELECT count(*) FROM (DESCRIBE SELECT * FROM delta_scan('{delta_table_path}'))) as delta_cols
            """
            res = self.con.execute(structure_sql).fetchone()
            csv_rows, delta_rows, csv_cols, delta_cols = res

            if csv_rows != delta_rows or csv_cols != delta_cols:
                logger.error(
                    f"Structural Mismatch! [Rows] CSV: {csv_rows} vs Delta: {delta_rows} | "
                    f"[Cols] CSV: {csv_cols} vs Delta: {delta_cols}"
                )
                return False

            # STEP 2: Deep Content Inspection (Fingerprinting)
            # Logic:
            # a) Cast all columns to VARCHAR to standardize (removes storage format differences).
            # b) Hash each row to create a 'digital signature'.
            # c) Use BIT_XOR to aggregate all hashes. XOR is commutative (A^B = B^A),
            #    so the result is identical even if the rows are in a different order.
            # d) COALESCE handles NULL values so they don't break the string concatenation.

            fingerprint_sql = f"""
                WITH csv_signature AS (
                    SELECT bit_xor(hash(coalesce(columns(*)::VARCHAR, 'NULL'))) as sign
                    FROM read_csv_auto('{original_csv}')
                ),
                delta_signature AS (
                    SELECT bit_xor(hash(coalesce(columns(*)::VARCHAR, 'NULL'))) as sign
                    FROM delta_scan('{delta_table_path}')
                )
                SELECT csv_signature.sign == delta_signature.sign 
                FROM csv_signature, delta_signature
            """

            is_identical = self.con.execute(fingerprint_sql).fetchone()[0]

            if not is_identical:
                logger.error("Data Integrity Failed, Content fingerprints do not match (data corruption or drift).")
                # Pro tip: If this fails, it's often due to floating point precision (0.666 vs 0.6666667)
                return False

            logger.info(f"Verification Passed, Confirmed {csv_rows} rows and {csv_cols} columns are identical.")
            return True

        except Exception as e:
            logger.error(f"Verification process crashed: {str(e)}")
            return False