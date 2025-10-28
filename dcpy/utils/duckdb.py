import duckdb  # type: ignore
import os
from pathlib import Path

from dcpy.utils.postgres import PostgresClient


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


def setup_postgres(database: str | None = None):
    database = database or os.environ["BUILD_ENGINE_DB"]
    duckdb.sql("INSTALL spatial;")
    duckdb.sql("LOAD spatial;")
    duckdb.sql("INSTALL postgres;")
    duckdb.sql("LOAD postgres;")
    duckdb.sql(f"""
        CREATE SECRET postgres_build_engine (
        TYPE POSTGRES,
        HOST '{os.environ["BUILD_ENGINE_HOST"]}',
        PORT '{os.environ["BUILD_ENGINE_PORT"]}',
        DATABASE '{database}',
        USER '{os.environ["BUILD_ENGINE_USER"]}',
        PASSWORD '{os.environ["BUILD_ENGINE_PASSWORD"]}'
    );""")
    duckdb.sql("ATTACH '' AS pg (TYPE POSTGRES, SECRET postgres_build_engine);")


def copy_file_to_table(
    filepath: Path, table_name: str, pg_client: PostgresClient | None = None
):
    if pg_client:
        table_name = f"pg.{pg_client.schema}.{table_name}"
    duckdb.sql(f"CREATE TABLE {table_name} AS SELECT * FROM '{filepath}'")


setup_s3_secret()
setup_postgres()
