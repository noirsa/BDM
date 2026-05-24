from src.utils import get_logger


class DataIntegrityVerifier:
    """Verifies that raw CSV content and Delta content are row-for-row equivalent."""

    def __init__(self, duckdb_connection):
        self.con = duckdb_connection
        self.logger = get_logger(__name__)

    @staticmethod
    def _quote_identifier(identifier):
        return '"' + identifier.replace('"', '""') + '"'

    @staticmethod
    def _quote_literal(value):
        return "'" + value.replace("'", "''") + "'"

    def _columns_for_query(self, query):
        rows = self.con.execute(f"DESCRIBE SELECT * FROM {query}").fetchall()
        return [row[0] for row in rows]

    def _row_fingerprint_expr(self, columns):
        parts = []
        for column in columns:
            quoted = self._quote_identifier(column)
            parts.append(
                f"coalesce(length({quoted}::VARCHAR)::VARCHAR || ':' || {quoted}::VARCHAR, '<NULL>')"
            )
        return "md5(" + " || chr(31) || ".join(parts) + ")"

    def verify_csv_matches_delta(self, original_csv, delta_table_path):
        # Keep Hive partition inference disabled for raw CSVs. Older runs may still
        # contain year=/month=/day= paths, and DuckDB would otherwise add those path
        # fields or collide with source fields such as global_warming_dataset.year.
        csv_query = f"read_csv_auto({self._quote_literal(original_csv)}, hive_partitioning=false)"
        delta_query = f"delta_scan({self._quote_literal(delta_table_path)})"

        csv_columns = self._columns_for_query(csv_query)
        delta_columns = self._columns_for_query(delta_query)

        csv_rows = self.con.execute(f"SELECT count(*) FROM {csv_query}").fetchone()[0]
        delta_rows = self.con.execute(f"SELECT count(*) FROM {delta_query}").fetchone()[0]

        if csv_rows != delta_rows or csv_columns != delta_columns:
            self.logger.error(
                "Structural mismatch. CSV rows=%s Delta rows=%s CSV columns=%s Delta columns=%s",
                csv_rows,
                delta_rows,
                csv_columns,
                delta_columns,
            )
            return False

        row_expr = self._row_fingerprint_expr(csv_columns)
        diff_sql = f"""
            WITH csv_counts AS (
                SELECT {row_expr} AS row_hash, count(*) AS row_count
                FROM {csv_query}
                GROUP BY row_hash
            ),
            delta_counts AS (
                SELECT {row_expr} AS row_hash, count(*) AS row_count
                FROM {delta_query}
                GROUP BY row_hash
            ),
            diff AS (
                (SELECT * FROM csv_counts EXCEPT ALL SELECT * FROM delta_counts)
                UNION ALL
                (SELECT * FROM delta_counts EXCEPT ALL SELECT * FROM csv_counts)
            )
            SELECT count(*) FROM diff
        """
        diff_count = self.con.execute(diff_sql).fetchone()[0]

        if diff_count:
            self.logger.error("Content mismatch. Found %s row-hash count differences.", diff_count)
            return False

        self.logger.info("Verification passed. Confirmed %s rows and %s columns.", csv_rows, len(csv_columns))
        return True
