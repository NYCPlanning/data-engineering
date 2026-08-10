import os
from pathlib import Path

import duckdb  # type: ignore
import pandas as pd

from dcpy.utils.logging import logger


def setup_s3_secret(conn: duckdb.DuckDBPyConnection | None = None) -> None:
    cmd = conn.sql if conn else duckdb.sql
    cmd("INSTALL httpfs; LOAD httpfs;")
    cmd(
        f"""
            CREATE SECRET IF NOT EXISTS s3_secret (
                TYPE S3,
                KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
                SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
                ENDPOINT '{os.environ["AWS_S3_ENDPOINT"].split("://")[1]}'
            );
        """
    )


def sanitize_name(name: str) -> str:
    """Sanitize table/schema names for DuckDB (same as postgres sanitization)."""
    return name.strip().replace("-", "_").replace("'", "_").replace(" ", "_").lower()


class DuckDBClient:
    """Client for managing DuckDB databases with schema support."""

    def __init__(self, db_path: Path, schema: str):
        """Initialize DuckDB client.

        Args:
            db_path: Path to DuckDB database file
            schema: Schema name to use (will be created if doesn't exist)
        """
        self.db_path = db_path
        self.schema = sanitize_name(schema)

        # Connect with storage_compatibility_version set to ensure we get v1.5.0+
        # storage format which is required for geometry columns with CRS
        config: dict[str, str | bool | int | float | list[str]] = {
            "storage_compatibility_version": "v1.5.0"
        }
        self.conn = duckdb.connect(str(db_path), config=config)

        # Install and load spatial extension for geometry support
        # This needs to happen on every connection
        self.conn.execute("INSTALL spatial;")
        self.conn.execute("LOAD spatial;")

        self.create_schema()

    def create_schema(self) -> None:
        """Create schema if it doesn't exist."""
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        logger.info(f"Created/verified schema: {self.schema}")

    def execute_query(self, query: str) -> None:
        """Execute a SQL query."""
        self.conn.execute(query)

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the schema."""
        result = self.conn.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [self.schema, sanitize_name(table_name)],
        ).fetchone()
        return result[0] > 0 if result else False

    def load_csv(
        self, csv_path: Path, table_name: str, include_ogc_fid_col: bool = True
    ) -> str:
        """Load CSV file into DuckDB table.

        Args:
            csv_path: Path to CSV file
            table_name: Name of table to create
            include_ogc_fid_col: Whether to add ogc_fid primary key column

        Returns:
            Fully qualified table name (schema.table)
        """
        sanitized_table = sanitize_name(table_name)
        full_table_name = f"{self.schema}.{sanitized_table}"

        # Drop table if exists
        self.conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")

        # Create table from CSV
        if include_ogc_fid_col:
            # Use row_number() as ogc_fid
            self.conn.execute(
                f"""
                CREATE TABLE {full_table_name} AS
                SELECT row_number() OVER () AS ogc_fid, *
                FROM read_csv_auto('{csv_path}')
                """
            )
        else:
            self.conn.execute(
                f"""
                CREATE TABLE {full_table_name} AS
                SELECT * FROM read_csv_auto('{csv_path}')
                """
            )

        logger.info(f"Loaded CSV {csv_path} into {full_table_name}")
        return full_table_name

    def load_parquet(
        self, parquet_path: Path, table_name: str, include_ogc_fid_col: bool = True
    ) -> str:
        """Load Parquet file into DuckDB table.

        Args:
            parquet_path: Path to Parquet file
            table_name: Name of table to create
            include_ogc_fid_col: Whether to add ogc_fid primary key column

        Returns:
            Fully qualified table name (schema.table)
        """
        sanitized_table = sanitize_name(table_name)
        full_table_name = f"{self.schema}.{sanitized_table}"

        # Drop table if exists
        self.conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")

        # Create table from Parquet
        if include_ogc_fid_col:
            # Use row_number() as ogc_fid
            self.conn.execute(
                f"""
                CREATE TABLE {full_table_name} AS
                SELECT row_number() OVER () AS ogc_fid, *
                FROM read_parquet('{parquet_path}')
                """
            )
        else:
            self.conn.execute(
                f"""
                CREATE TABLE {full_table_name} AS
                SELECT * FROM read_parquet('{parquet_path}')
                """
            )

        logger.info(f"Loaded Parquet {parquet_path} into {full_table_name}")
        return full_table_name

    def load_spatial(
        self,
        source_path: Path,
        table_name: str,
        include_ogc_fid_col: bool = True,
        layer_name: str | None = None,
    ) -> str:
        """Load a GDAL/OGR-readable spatial dataset (e.g. shapefile) into a DuckDB table.

        Uses the spatial extension's ST_Read, which reads zipped shapefiles
        (e.g. `foo.shp.zip`) directly without needing to unzip first.

        Args:
            source_path: Path to the spatial dataset (e.g. a .shp or .shp.zip file)
            table_name: Name of table to create
            include_ogc_fid_col: Whether to add ogc_fid primary key column
            layer_name: Optional layer to read, for multi-layer sources

        Returns:
            Fully qualified table name (schema.table)
        """
        sanitized_table = sanitize_name(table_name)
        full_table_name = f"{self.schema}.{sanitized_table}"

        # Drop table if exists
        self.conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")

        st_read_call = (
            f"ST_Read('{source_path}', layer='{layer_name}')"
            if layer_name
            else f"ST_Read('{source_path}')"
        )

        # ST_Read includes its own OGC_FID column; exclude it so our row_number()
        # based ogc_fid (consistent with the CSV/Parquet loaders) doesn't collide with it.
        if include_ogc_fid_col:
            self.conn.execute(
                f"""
                CREATE TABLE {full_table_name} AS
                SELECT row_number() OVER () AS ogc_fid, * EXCLUDE (OGC_FID)
                FROM {st_read_call}
                """
            )
        else:
            self.conn.execute(
                f"""
                CREATE TABLE {full_table_name} AS
                SELECT * EXCLUDE (OGC_FID) FROM {st_read_call}
                """
            )

        logger.info(f"Loaded spatial dataset {source_path} into {full_table_name}")
        return full_table_name

    def export_to_csv(
        self,
        table_name: str,
        output_path: Path,
        *,
        query: str | None = None,
        include_header: bool = True,
    ) -> None:
        """Export a table (or query) to a CSV file.

        Args:
            table_name: Name of table to export (ignored if `query` is given)
            output_path: Path to write the CSV file to
            query: Optional SQL query to export instead of the full table
            include_header: Whether to write a header row
        """
        select = query or f"SELECT * FROM {self.schema}.{sanitize_name(table_name)}"
        header = "true" if include_header else "false"
        self.conn.execute(
            f"COPY ({select}) TO '{output_path}' (FORMAT CSV, HEADER {header})"
        )
        logger.info(f"Exported {table_name} to {output_path}")

    def export_to_parquet(
        self,
        table_name: str,
        output_path: Path,
        *,
        query: str | None = None,
    ) -> None:
        """Export a table (or query) to a Parquet file.

        Args:
            table_name: Name of table to export (ignored if `query` is given)
            output_path: Path to write the Parquet file to
            query: Optional SQL query to export instead of the full table
        """
        select = query or f"SELECT * FROM {self.schema}.{sanitize_name(table_name)}"
        self.conn.execute(f"COPY ({select}) TO '{output_path}' (FORMAT PARQUET)")
        logger.info(f"Exported {table_name} to {output_path}")

    def add_table_column(
        self, table_name: str, col_name: str, col_type: str, default_value: str
    ) -> None:
        """Add a column to a table with a default value.

        Args:
            table_name: Name of table
            col_name: Column name to add
            col_type: SQL type of column
            default_value: Default value for the column
        """
        sanitized_table = sanitize_name(table_name)
        full_table_name = f"{self.schema}.{sanitized_table}"

        self.conn.execute(
            f"""
            ALTER TABLE {full_table_name}
            ADD COLUMN {col_name} {col_type} DEFAULT '{default_value}'
            """
        )
        logger.info(f"Added column {col_name} to {full_table_name}")

    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table.

        Args:
            table_name: Name of table

        Returns:
            Number of rows in table
        """
        sanitized_table = sanitize_name(table_name)
        full_table_name = f"{self.schema}.{sanitized_table}"

        result = self.conn.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()
        return result[0] if result else 0

    def query_to_df(self, query: str) -> pd.DataFrame:
        """Execute query and return results as DataFrame.

        Args:
            query: SQL query to execute

        Returns:
            Query results as DataFrame
        """
        return self.conn.execute(query).df()

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()
