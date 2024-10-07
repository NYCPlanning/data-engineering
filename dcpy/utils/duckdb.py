import duckdb
from pathlib import Path
import os


def setup_s3_secret(db_path: Path):
    with duckdb.connect(str(db_path)) as connection:
        connection.sql("INSTALL httpfs; LOAD httpfs;")
        connection.sql(
            f"""
                CREATE SECRET IF NOT EXISTS s3_secret (
                    TYPE S3,
                    KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
                    SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
                    ENDPOINT '{os.environ["AWS_S3_ENDPOINT"].split('://')[1]}'
                );
            """
        )
