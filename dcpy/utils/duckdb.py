import duckdb
import os


def setup_s3_secret(conn: duckdb.DuckDBPyConnection | None = None) -> None:
    print("this line isn't tested!")
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
