import os
from pathlib import Path
from typing import cast

import duckdb  # type: ignore
import pandas as pd
import pyarrow as pa

from dcpy.utils import datastores
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

    def __init__(self, db_path: Path, schema: str, clear_schema: bool = False):
        """Initialize DuckDB client.

        Args:
            db_path: Path to DuckDB database file
            schema: Schema name to use (will be created if doesn't exist)
            clear_schema: Drop the schema (and everything in it - source tables,
                any dbt-built views/tables/seeds sharing it) before creating it fresh.
                Off by default since it's destructive; the recipe loader only ever
                drops+recreates the individual source tables it's re-inserting, so a
                build directory reused across runs (same product/version) can
                otherwise accumulate stale objects (e.g. a dbt seed) that a plain
                reload won't clear.
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

        if clear_schema:
            self.drop_schema()
        self.create_schema()

    def create_schema(self) -> None:
        """Create schema if it doesn't exist."""
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        logger.info(f"Created/verified schema: {self.schema}")

    def drop_schema(self) -> None:
        """Drop schema (and everything in it) if it exists."""
        self.conn.execute(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE")
        logger.info(f"Dropped schema: {self.schema}")

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
        self,
        csv_path: Path,
        table_name: str,
        include_ogc_fid_col: bool = True,
        all_varchar: bool = False,
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

        varchar = ", all_varchar = true" if all_varchar else ""
        # Create table from CSV
        if include_ogc_fid_col:
            # Use row_number() as ogc_fid
            self.conn.execute(
                f"""
                CREATE TABLE {full_table_name} AS
                SELECT row_number() OVER () AS ogc_fid, *
                FROM read_csv_auto('{csv_path}'{varchar})
                """
            )
        else:
            self.conn.execute(
                f"""
                CREATE TABLE {full_table_name} AS
                SELECT * FROM read_csv_auto('{csv_path}'{varchar})
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
        select = (
            "SELECT row_number() OVER () AS ogc_fid, * EXCLUDE (OGC_FID)"
            if include_ogc_fid_col
            else "SELECT * EXCLUDE (OGC_FID)"
        )

        # SHAPE_RESTORE_SHX lets GDAL rebuild a missing .shx from the .shp. GDAL only reads
        # it as a config option (the environment), not an ST_Read open option, and it makes
        # zipped shapefiles fail to open, so it's set only for a bare .shp missing its .shx.
        restore_shx = (
            source_path.suffix.lower() == ".shp"
            and not source_path.with_suffix(".shx").exists()
        )
        previous = os.environ.get("SHAPE_RESTORE_SHX")
        if restore_shx:
            os.environ["SHAPE_RESTORE_SHX"] = "YES"
        try:
            # ST_Read includes its own OGC_FID column; exclude it so our row_number()
            # based ogc_fid (consistent with the CSV/Parquet loaders) doesn't collide with it.
            self.conn.execute(
                f"CREATE TABLE {full_table_name} AS {select} FROM {st_read_call}"
            )
        finally:
            if restore_shx:
                if previous is None:
                    del os.environ["SHAPE_RESTORE_SHX"]
                else:
                    os.environ["SHAPE_RESTORE_SHX"] = previous

        logger.info(f"Loaded spatial dataset {source_path} into {full_table_name}")
        return full_table_name

    def load_geoparquet(
        self, parquet_path: Path, table_name: str, geom_col: str | None = None
    ) -> str:
        """Load a GeoParquet file as-is, renaming its geometry column to ``geom``.

        The file is expected to already carry its own ``ogc_fid``.
        """
        full_table_name = f"{self.schema}.{sanitize_name(table_name)}"
        self.conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")
        src = f"read_parquet('{parquet_path}')"
        if geom_col is None:
            geom_col = self._geometry_column(src)
        rename = f" RENAME ({geom_col} AS geom)" if geom_col.lower() != "geom" else ""
        self.conn.execute(
            f"CREATE TABLE {full_table_name} AS SELECT *{rename} FROM {src}"
        )
        self._lowercase_columns(full_table_name)
        logger.info(f"Loaded GeoParquet {parquet_path} into {full_table_name}")
        return full_table_name

    def _lowercase_columns(self, full_table_name: str) -> None:
        """ogr2ogr lowercases GDB field names when loading into Postgres; match that."""
        cols = self.conn.execute(
            f"SELECT column_name FROM (DESCRIBE {full_table_name})"
        ).fetchall()
        for (col,) in cols:
            if col != col.lower():
                self.conn.execute(
                    f'ALTER TABLE {full_table_name} RENAME COLUMN "{col}" TO "{col.lower()}"'
                )

    def _geometry_column(self, relation: str) -> str:
        cols = self.conn.execute(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM {relation}) "
            "WHERE column_type LIKE 'GEOMETRY%'"
        ).fetchall()
        assert len(cols) == 1, f"expected one geometry column, found {cols}"
        return cols[0][0]

    def load_arrow(self, table: pa.Table, table_name: str) -> str:
        """Load an arrow table (already in FID order), adding ``ogc_fid`` = 1..N.

        Timestamps are cast to plain TIMESTAMP, as ST_Read loads them. Dictionary
        columns (GDAL's coded-value-domain fields) keep their indices, the stored codes
        that ogr2ogr writes, not the domain descriptions.
        """
        full_table_name = f"{self.schema}.{sanitize_name(table_name)}"
        self.conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")
        for i, field in enumerate(table.schema):
            if pa.types.is_dictionary(field.type):
                codes = pa.chunked_array(
                    [
                        cast(pa.DictionaryArray, chunk).indices
                        for chunk in table.column(i).chunks
                    ],
                    type=field.type.index_type,
                )
                table = table.set_column(
                    i, pa.field(field.name, field.type.index_type), codes
                )
        table = table.add_column(
            0, "ogc_fid", pa.array(range(1, table.num_rows + 1), pa.int32())
        )
        casts = [
            f'"{f.name}"::TIMESTAMP AS "{f.name}"'
            for f in table.schema
            if pa.types.is_timestamp(f.type)
        ]
        replace = f" REPLACE ({', '.join(casts)})" if casts else ""
        self.conn.register("_arrow_to_load", table)
        try:
            self.conn.execute(
                f"CREATE TABLE {full_table_name} AS SELECT *{replace} FROM _arrow_to_load"
            )
        finally:
            self.conn.unregister("_arrow_to_load")
        self._lowercase_columns(full_table_name)
        return full_table_name

    def load_gdb_layer(
        self, source_path: Path, table_name: str, layer_name: str
    ) -> str:
        """Load one File Geodatabase layer through ST_Read, matching an ogr2ogr-to-PG load.

        ``ogc_fid`` is numbered 1..N in OBJECTID order (the GDB's FID, which ogr2ogr
        consumes rather than emitting as a column), and the geometry column is ``geom``.
        """
        full_table_name = f"{self.schema}.{sanitize_name(table_name)}"
        self.conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")
        src = f"ST_Read('{source_path}', layer='{layer_name}')"
        described = self.conn.execute(
            f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM {src})"
        ).fetchall()
        fid = next((c for c, _ in described if c.lower() == "objectid"), None)
        geom = next((c for c, t in described if t.startswith("GEOMETRY")), None)
        star = "*"
        if fid:
            star += f" EXCLUDE ({fid})"
        if geom and geom != "geom":
            star += f" RENAME ({geom} AS geom)"
        order = f"ORDER BY {fid}" if fid else ""
        self.conn.execute(
            f"CREATE TABLE {full_table_name} AS "
            f"SELECT row_number() OVER ({order}) AS ogc_fid, {star} FROM {src} {order}"
        )
        self._lowercase_columns(full_table_name)
        logger.info(f"Loaded GDB layer {layer_name} into {full_table_name}")
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

    def read_table_df(self, table_name: str) -> pd.DataFrame:
        """Read a full table into a DataFrame. Mirrors postgres.PostgresClient.read_table_df."""
        sanitized_table = sanitize_name(table_name)
        return self.query_to_df(f"SELECT * FROM {self.schema}.{sanitized_table}")

    def read_table_gdf(self, table_name: str, geom_column: str = "geom"):
        """Read a table into a GeoDataFrame. Mirrors postgres.PostgresClient.read_table_gdf.

        Duckdb's spatial GEOMETRY type doesn't convert to shapely via .df() the way
        postgis geometry does through geopandas' read_postgis, so this goes through WKB
        explicitly instead.
        """
        import geopandas as gpd
        import shapely.wkb

        sanitized_table = sanitize_name(table_name)
        full_table_name = f"{self.schema}.{sanitized_table}"

        crs_row = self.conn.execute(
            f"""
            SELECT ST_CRS({geom_column}) FROM {full_table_name}
            WHERE {geom_column} IS NOT NULL LIMIT 1
            """
        ).fetchone()
        crs = crs_row[0] if crs_row and crs_row[0] else "EPSG:2263"

        df = self.conn.execute(
            f"""
            SELECT * EXCLUDE ({geom_column}), ST_AsWKB({geom_column}) AS {geom_column}
            FROM {full_table_name}
            """
        ).df()
        df[geom_column] = df[geom_column].apply(
            lambda wkb: shapely.wkb.loads(bytes(wkb)) if pd.notna(wkb) else None
        )
        return gpd.GeoDataFrame(df, geometry=geom_column, crs=crs)

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()


def export_geodataset_from_duckdb(
    table_name: str,
    file_path: Path,
    format: str,
    duckdb_client: DuckDBClient,
    *,
    geom_column: str = "geom",
    geometry_type: str
    | None = None,  # "points" | "polygons" | "lines" | None (no filter)
    layer: str | None = None,
) -> None:
    """Export a geospatial table from duckdb as a zipped shapefile or FGDB, a
    GeoPackage, or GeoParquet.

    `format` takes a plain string ("shp", "gdb", "gpkg", or "geoparquet") rather than
    dcpy.lifecycle.builds.models.ExportFormat -- utils can't import from lifecycle -- but
    ExportFormat is a StrEnum, so callers can pass its members here directly.
    """
    import tempfile

    logger.info(
        f"Exporting geospatial table {table_name} to {file_path} in format {format}"
    )
    gdf = duckdb_client.read_table_gdf(table_name, geom_column=geom_column)
    gdf = datastores.filter_geometry_type(gdf, geometry_type, table_name)

    if format == "gpkg":
        datastores.write_gpkg([(layer or table_name, gdf)], file_path)
        return
    if format == "geoparquet":
        datastores.write_geoparquet(gdf, table_name, file_path)
        return
    with tempfile.TemporaryDirectory() as tmp_str:
        tmp_dir = Path(tmp_str)
        if format == "shp":
            datastores.write_shapefile_zip(gdf, table_name, file_path, tmp_dir)
        elif format == "gdb":
            datastores.write_gdb_zip([(layer or table_name, gdf)], file_path, tmp_dir)
