from io import StringIO
from pathlib import Path
import csv
import os
import pandas as pd
from psycopg2.extensions import AsIs
from sqlalchemy import create_engine, text
from typing import Optional


BUILD_ENGINE_RAW = os.environ["BUILD_ENGINE"]
build_engine = create_engine(BUILD_ENGINE_RAW)


def execute_query(query: str, placeholders: Optional[dict] = None) -> None:
    if placeholders is None:
        placeholders = {}
    with build_engine.connect() as connection:
        connection.execute(statement=text(query), parameters=placeholders)


def execute_select_query(
    query: str, placeholders: Optional[dict] = None
) -> pd.DataFrame:
    if placeholders is None:
        placeholders = {}
    with build_engine.connect() as sql_conn:
        select_records = pd.read_sql(sql=text(query), con=sql_conn, params=placeholders)
    return select_records


def execute_file_via_shell(build_engine: str, path: Path):
    """Execute .sql script at given path."""
    cmd = f"psql {build_engine} -v ON_ERROR_STOP=1 -f {path}"
    if os.system(cmd) != 0:
        raise Exception(f"{path} has errors!")


def execute_via_shell(build_engine: str, sql_statement):
    """Execute sql via psql shell."""
    cmd = f"psql {build_engine} -v ON_ERROR_STOP=1 {sql_statement}"
    if os.system(cmd) != 0:
        raise Exception(f"Command has errors! {cmd}")


def insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data.
    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = "{}.{}".format(table.schema, table.name)
        else:
            table_name = table.name

        sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def create_postigs_extension() -> None:
    execute_query("CREATE EXTENSION POSTGIS")


def vacuum_database() -> None:
    execute_query("VACUUM (ANALYZE)")


def get_schemas() -> list:
    schema_names = execute_select_query(
        """
        SELECT schema_name FROM information_schema.schemata
        """
    )
    return sorted(schema_names["schema_name"].to_list())


def get_schema_tables(table_schema: str) -> list:
    table_names = execute_select_query(
        """
        SELECT table_name FROM information_schema.tables WHERE table_schema = :table_schema
        """,
        {"table_schema": table_schema},
    )

    return sorted(table_names["table_name"].to_list())


def create_sql_schema(table_schema: str) -> pd.DataFrame:
    execute_query(
        "DROP SCHEMA IF EXISTS :table_schema CASCADE",
        {"table_schema": AsIs(table_schema)},
    )
    execute_query("CREATE SCHEMA :table_schema", {"table_schema": AsIs(table_schema)})
    table_schema_name = execute_select_query(
        """
        SELECT schema_name
        FROM :table_schema.schemata;
        """,
        {"table_schema": AsIs("information_schema")},
    )
    return table_schema_name


def get_table_columns(table_schema: str, table_name: str) -> list:
    column_names = execute_select_query(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = ':table_schema'
        AND table_name   = ':table_name';
        """,
        {"table_schema": AsIs(table_schema), "table_name": AsIs(table_name)},
    )
    return sorted(column_names["column_name"])


def get_table_row_count(table_schema: str, table_name: str) -> int:
    row_counts = execute_select_query(
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


def load_data_from_sql_dump(
    table_schema: str,
    dataset_by_version: str,
    dataset_name: str,
):
    print(f"Loading data into table {table_schema}.{dataset_name} ...")
    file_name = Path(f"{dataset_by_version}.sql")
    # run sql dump file to create initial table
    execute_file_via_shell(build_engine=BUILD_ENGINE_RAW, path=file_name)
    # copy inital data to a new table in the dataset-specific schema
    execute_query(
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
    execute_query(
        """
        DROP TABLE IF EXISTS :dataset_name CASCADE
        """,
        {
            "dataset_name": AsIs(dataset_name),
        },
    )
    vacuum_database()
