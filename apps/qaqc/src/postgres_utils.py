import os
import subprocess
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from psycopg2.extensions import AsIs
import pandas as pd
from src.constants import SQL_FILE_DIRECTORY


load_dotenv()
BUILD_ENGINE = os.getenv("SQL_ENGINE_EDM_DATA")

QAQC_DB_SCHEMA_SOURCE_DATA = "source_data"


def load_data_from_sql_dump(
    table_schema: str,
    dataset_by_version: str,
    dataset_name: str,
) -> list:
    print(f"Loading data into table {table_schema}.{dataset_name} ...")
    file_name = f"{dataset_by_version}.sql"
    # run sql dump file to create initial table
    execute_sql_file(filename=file_name)
    # copy inital data to a new table in the dataset-specific schema
    execute_sql_query(
        """
        CREATE TABLE :table_schema.:dataset_by_version AS TABLE :dataset_name
        """,
        {
            "table_schema": AsIs(table_schema),
            "dataset_by_version": AsIs(dataset_by_version),
            "dataset_name": AsIs(dataset_name),
        },
    )
    # copy inital data to a new table in the dataset-specific schema
    execute_sql_query(
        """
        DROP TABLE IF EXISTS :dataset_name CASCADE
        """,
        {
            "dataset_name": AsIs(dataset_name),
        },
    )
    vacuum_database()


def get_schemas() -> list:
    schema_names = execute_sql_select_query(
        """
        SELECT schema_name FROM information_schema.schemata
        """
    )
    return sorted(schema_names["schema_name"].to_list())


def get_schema_tables(table_schema: str) -> list:
    table_names = execute_sql_select_query(
        """
        SELECT table_name FROM information_schema.tables WHERE table_schema = :table_schema
        """,
        {"table_schema": table_schema},
    )

    return sorted(table_names["table_name"].to_list())


def get_table_columns(table_schema: str, table_name: str) -> list:
    column_names = execute_sql_select_query(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = ':table_schema'
        AND table_name   = ':table_name';
        """,
        {"table_schema": AsIs(table_schema), "table_name": AsIs(table_name)},
    )
    return sorted(column_names["column_name"])


def get_table_row_count(table_schema: str, table_name: str) -> int:
    row_counts = execute_sql_select_query(
        """
        SELECT c.reltuples::bigint AS row_count
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = ':table_name'
        AND n.nspname = ':table_schema';
        """,
        {"table_schema": AsIs(table_schema), "table_name": AsIs(table_name)},
    )
    return int(row_counts["row_count"][0])


def create_postigs_extension() -> None:
    query = "CREATE EXTENSION POSTGIS"
    execute_sql_query(query)


def vacuum_database() -> None:
    query = "VACUUM (ANALYZE)"
    execute_sql_query(query)


def create_sql_schema(table_schema: str) -> pd.DataFrame:
    execute_sql_query(
        "DROP SCHEMA IF EXISTS :table_schema CASCADE",
        {"table_schema": AsIs(table_schema)},
    )
    execute_sql_query(
        "CREATE SCHEMA :table_schema", {"table_schema": AsIs(table_schema)}
    )
    table_schema_name = execute_sql_select_query(
        """
        SELECT schema_name
        FROM :table_schema.schemata;
        """,
        {"table_schema": AsIs("information_schema")},
    )
    return table_schema_name


def execute_sql_select_query(query: str, placeholders: dict = {}) -> pd.DataFrame:
    sql_engine = create_engine(BUILD_ENGINE)
    with sql_engine.connect() as sql_conn:
        select_records = pd.read_sql(sql=text(query), con=sql_conn, params=placeholders)
    sql_engine.dispose()
    return select_records


def execute_sql_query(query: str, placeholders: dict = {}) -> None:
    sql_engine = create_engine(BUILD_ENGINE, isolation_level="AUTOCOMMIT")
    with sql_engine.connect() as connection:
        connection.execute(statement=text(query), params=placeholders)
    sql_engine.dispose()


def execute_sql_file(filename: str) -> None:
    subprocess.run(
        [
            f"psql {BUILD_ENGINE} --set ON_ERROR_STOP=1 --file {SQL_FILE_DIRECTORY / filename}"
        ],
        shell=True,
        check=True,
    )
