from io import StringIO
from pathlib import Path
import csv
import os
import pandas as pd
from psycopg2.extensions import AsIs
from sqlalchemy import create_engine, text

DEFAULT_POSTGRES_SCHEMA = "public"
PROTECTED_POSTGRES_SCHEMAS = [
    DEFAULT_POSTGRES_SCHEMA,
    "information_schema",
    "pg_catalog",
]


def generate_engine_uri(
    server_url: str, database: str, schema: str = DEFAULT_POSTGRES_SCHEMA
) -> str:
    # the default postgres schema must always be in the search_path
    schemas = (
        schema
        if schema == DEFAULT_POSTGRES_SCHEMA
        else f"{schema},{DEFAULT_POSTGRES_SCHEMA}"
    )
    options = f"?options=--search_path%3D{schemas}"
    return server_url + "/" + database + options


def execute_file_via_shell(engine_uri: str, path: Path) -> None:
    """Execute .sql script at given path."""
    cmd = f"psql {engine_uri} -v ON_ERROR_STOP=1 -f {path}"
    if os.system(cmd) != 0:
        raise Exception(f"{path} has errors!")


def execute_query_via_shell(engine_uri: str, sql_statement) -> None:
    """Execute sql via psql shell."""
    cmd = f"psql {engine_uri} -v ON_ERROR_STOP=1 {sql_statement}"
    if os.system(cmd) != 0:
        raise Exception(f"Command has errors! {cmd}")


class PostgresClient:
    def __init__(
        self,
        schema: str,
        *,
        database: str | None = None,
        server_url: str | None = None,
    ):
        self.schema = schema
        self.database = database if database else os.environ["BUILD_ENGINE_DB"]
        self.engine_uri = generate_engine_uri(
            server_url if server_url else os.environ["BUILD_ENGINE_SERVER"],
            self.database,
            schema,
        )
        self.engine = create_engine(
            self.engine_uri,
            isolation_level="AUTOCOMMIT",
        )
        self.create_schema()

    def execute_query(self, query: str, placeholders: dict | None = None) -> None:
        with self.engine.connect() as connection:
            connection.execute(statement=text(query), parameters=placeholders)

    def execute_select_query(
        self,
        query: str,
        placeholders: dict | None = None,
    ) -> pd.DataFrame:
        with self.engine.connect() as sql_conn:
            select_records = pd.read_sql(
                sql=text(query), con=sql_conn, params=placeholders
            )
        return select_records

    def create_postigs_extension(self) -> None:
        self.execute_query("CREATE EXTENSION POSTGIS")

    def vacuum_database(self) -> None:
        self.execute_query("VACUUM (ANALYZE)")

    def drop_schema(self, schema_name: str | None = None) -> None:
        schema_name = self.schema if not schema_name else schema_name
        if schema_name in PROTECTED_POSTGRES_SCHEMAS:
            raise ValueError(
                f"Cannot delete the protected postgres schema '{schema_name}'"
            )
        self.execute_query(
            "DROP SCHEMA IF EXISTS :schema_name CASCADE",
            {"schema_name": AsIs(schema_name)},
        )

    def create_schema(self) -> None:
        self.execute_query(
            "CREATE SCHEMA IF NOT EXISTS :schema_name",
            {"schema_name": AsIs(self.schema)},
        )

    def get_build_schemas(self) -> list[str]:
        schema_names = self.execute_select_query(
            """
            SELECT schema_name FROM information_schema.schemata
            """
        )
        return sorted(
            [
                schema
                for schema in schema_names["schema_name"].to_list()
                if schema not in PROTECTED_POSTGRES_SCHEMAS
            ]
        )

    def get_schema_tables(self) -> list[str]:
        select_table_names = self.execute_select_query(
            """
            SELECT table_name FROM information_schema.tables WHERE table_schema = :table_schema
            """,
            {"table_schema": self.schema},
        )
        all_table_names = sorted(select_table_names["table_name"].to_list())
        postgis_tables = [
            "spatial_ref_sys",
            "geography_columns",
            "geometry_columns",
        ]
        table_names = [
            table_name
            for table_name in all_table_names
            if table_name not in postgis_tables
        ]
        return table_names

    def get_table(self, table_name: str) -> pd.DataFrame:
        return self.execute_select_query(
            """
            SELECT * FROM :table_name;
            """,
            {"table_name": AsIs(table_name)},
        )

    def get_table_columns(self, table_name: str) -> list[str]:
        column_names = self.execute_select_query(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = ':table_schema'
            AND table_name   = ':table_name';
            """,
            {"table_schema": AsIs(self.schema), "table_name": AsIs(table_name)},
        )
        return sorted(column_names["column_name"])

    def get_table_row_count(self, table_name: str) -> int:
        row_counts = self.execute_select_query(
            """
            SELECT c.reltuples::bigint AS row_count
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = ':table_name'
            AND n.nspname = ':table_schema';
            """,
            {"table_schema": AsIs(self.schema), "table_name": AsIs(table_name)},
        )
        return int(row_counts["row_count"][0])

    def create_table_from_csv(self, table_name: str, file_path: Path) -> None:
        pd.read_csv(file_path).to_sql(
            name=table_name,
            con=self.engine,
            index=False,
            if_exists="replace",
            method=insert_copy,
        )


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
