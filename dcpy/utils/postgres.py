import csv
import os
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd
import typer
from psycopg2.extensions import AsIs
from sqlalchemy import create_engine, dialects, text

from dcpy.utils.logging import logger


class TableType(Enum):
    VIEW = "VIEW"
    BASE_TABLE = "BASE TABLE"
    OTHER = "OTHER"  # Encompasses FOREIGN TABLE, LOCAL TEMPORARY, etc. - we don't expect to encounter these often


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


def generate_schema_tests_name(schema: str):
    return schema + "__tests"


def execute_file_via_shell(engine_uri: str, path: Path, **kwargs) -> None:
    """Execute a .sql script at given path using psql CLI and kwargs to set variables."""
    psql_vars = [f"--variable {name}={value}" for (name, value) in kwargs.items()]
    psql_vars_text = " ".join(psql_vars)
    cmd = f"psql {engine_uri} --variable ON_ERROR_STOP=1 {psql_vars_text} -f {path}"
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
        *,
        schema: str | None = None,
        database: str | None = None,
        server_url: str | None = None,
    ):
        self.schema = schema if schema else os.environ["BUILD_ENGINE_SCHEMA"]
        self.schema_tests = generate_schema_tests_name(self.schema)
        self.database = database if database else os.environ["BUILD_ENGINE_DB"]
        self.engine_uri = generate_engine_uri(
            server_url if server_url else os.environ["BUILD_ENGINE_SERVER"],
            self.database,
            self.schema,
        )
        self.engine = create_engine(
            self.engine_uri,
            isolation_level="AUTOCOMMIT",
        )
        self.create_schema()

    def connect(self):
        return self.engine.connect()

    def execute_query(self, query: str, *, conn=None, **kwargs) -> None:
        if conn is None:
            with self.connect() as conn:
                conn.execute(statement=text(query), parameters=kwargs)
        else:
            conn.execute(statement=text(query), parameters=kwargs)

    def execute_file(self, path: Path, *, conn=None, **kwargs) -> None:
        """Execute a .sql script at given path using sqlalchemy and kwargs to set variables."""
        with open(path) as query_file:
            query = query_file.read()
        self.execute_query(query, conn=conn, **kwargs)

    def execute_file_via_shell(self, path: Path, **kwargs) -> None:
        """Execute a .sql script at given path using psql CLI and kwargs to set variables."""
        execute_file_via_shell(self.engine_uri, path, **kwargs)

    def execute_select_query(
        self,
        query: str,
        *,
        conn=None,
        **kwargs,
    ) -> pd.DataFrame:
        if conn is None:
            with self.engine.connect() as conn:
                select_records = pd.read_sql(sql=text(query), con=conn, params=kwargs)
        else:
            select_records = pd.read_sql(sql=text(query), con=conn, params=kwargs)
        return select_records

    def read_table_df(
        self,
        table_name: str,
        *,
        conn=None,
        **kwargs,
    ) -> pd.DataFrame:
        if conn is None:
            with self.engine.connect() as conn:
                return pd.read_sql_table(table_name=table_name, con=conn, **kwargs)
        else:
            return pd.read_sql_table(table_name=table_name, con=conn, **kwargs)

    def read_table_gdf(
        self,
        table_name: str,
        geom_column: str = "geom",
        *,
        conn=None,
        **kwargs,
    ) -> gpd.GeoDataFrame:
        if conn is None:
            with self.engine.connect() as conn:
                return gpd.read_postgis(
                    table_name, conn, geom_col=geom_column, **kwargs
                )
        else:
            return gpd.read_postgis(table_name, conn, geom_col=geom_column, **kwargs)

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
        logger.info(f"Dropping schema: {schema_name}")
        self.execute_query(
            "DROP SCHEMA IF EXISTS :schema_name CASCADE", schema_name=AsIs(schema_name)
        )

    def create_schema(self) -> None:
        self.execute_query(
            "CREATE SCHEMA IF NOT EXISTS :schema_name", schema_name=AsIs(self.schema)
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
            table_schema=self.schema,
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

    def get_table_type(self, table_name: str, schema: str | None = None) -> TableType:
        """Get the type of a table (VIEW, BASE TABLE, etc.)"""
        schema_to_check = schema or self.schema
        result = self.execute_select_query(
            "SELECT table_type FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table_name",
            schema=schema_to_check,
            table_name=table_name,
        )
        if result.empty:
            raise ValueError(
                f"Table '{table_name}' not found in schema '{schema_to_check}'"
            )
        raw_type = result.iloc[0]["table_type"]

        # Map raw PostgreSQL table types to our enum
        if raw_type == "VIEW":
            return TableType.VIEW
        elif raw_type == "BASE TABLE":
            return TableType.BASE_TABLE
        else:
            return TableType.OTHER

    def is_view(self, table_name: str, schema: str | None = None) -> bool:
        """Check if a table is a view"""
        return self.get_table_type(table_name, schema) == TableType.VIEW

    def table_or_view_exists(self, table_name: str, schema: str | None = None) -> bool:
        """Check if a table or view exists in the schema"""
        result = self.execute_select_query(
            """
            SELECT COUNT(*) AS count FROM information_schema.tables
            WHERE table_schema = :schema AND table_name = :table_name
            """,
            schema=schema or self.schema,
            table_name=table_name,
        )
        return result.iloc[0]["count"] > 0

    def set_table_schema(self, table, *, old_schema, new_schema):
        """Set the schema for a table."""
        self.execute_query(
            "ALTER TABLE :table_with_schema SET SCHEMA :new_schema;",
            table_with_schema=AsIs(f"{old_schema}.{table}"),
            new_schema=AsIs(new_schema),
        )

    def rename_table(self, *, old_name: str, new_name: str):
        return self.execute_query(
            """
            ALTER TABLE :old_name RENAME TO :new_name;
            """,
            old_name=AsIs(old_name),
            new_name=AsIs(new_name),
        )

    def get_table_columns(self, table_name: str) -> list[str]:
        column_names = self.execute_select_query(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = ':table_schema'
            AND table_name   = ':table_name';
            """,
            table_schema=AsIs(self.schema),
            table_name=AsIs(table_name),
        )
        return sorted(column_names["column_name"])

    def get_column_types(self, table_name: str) -> dict[str, str]:
        columns = self.execute_select_query(
            """
            SELECT
                column_name,
                CASE
                    WHEN data_type = 'USER-DEFINED' THEN udt_name
                    ELSE data_type
                END AS data_type
            FROM information_schema.columns
            WHERE table_schema = ':table_schema'
            AND table_name = ':table_name';
            """,
            table_schema=AsIs(self.schema),
            table_name=AsIs(table_name),
        )
        return {r["column_name"]: r["data_type"] for _, r in columns.iterrows()}

    def get_geometry_columns(self, table_name: str) -> set[str]:
        columns = self.execute_select_query(
            """
            SELECT
                column_name
            FROM information_schema.columns
            WHERE
                table_schema = ':table_schema'
                AND table_name = ':table_name'
                AND data_type = 'USER-DEFINED'
                AND udt_name = 'geometry';
            """,
            table_schema=AsIs(self.schema),
            table_name=AsIs(table_name),
        )
        return set(columns["column_name"])

    def add_pk(self, table: str, id_column: str = "id"):
        self.execute_query(
            'ALTER TABLE ":schema".":table" ADD COLUMN ":id_column" SERIAL CONSTRAINT ":constraint" PRIMARY KEY;',
            schema=AsIs(self.schema),
            table=AsIs(table),
            id_column=AsIs(id_column),
            constraint=AsIs(f"{table}_pk"),
        )

    def drop_table(self, table):
        self.execute_query(
            'DROP TABLE IF EXISTS ":schema".":table" CASCADE;',
            schema=AsIs(self.schema),
            table=AsIs(table),
        )

    def add_table_column(
        self,
        table_name: str,
        *,
        col_name: str,
        col_type: str,
        default_value: str = "",
    ):
        q = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
        if default_value:
            q += f" DEFAULT '{default_value}'"
        self.execute_query(q + ";")

    def get_table_row_count(self, table_name: str) -> int:
        row_counts = self.execute_select_query(
            """
            SELECT c.reltuples::bigint AS row_count
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = ':table_name'
            AND n.nspname = ':table_schema';
            """,
            table_schema=AsIs(self.schema),
            table_name=AsIs(table_name),
        )
        return int(row_counts["row_count"][0])

    def create_table_from_csv(
        self, file_path: Path, table_name: str | None = None
    ) -> None:
        table_name = table_name or file_path.stem
        pd.read_csv(file_path).to_sql(
            name=table_name,
            con=self.engine,
            index=False,
            if_exists="replace",
            method=insert_copy,
        )

    def import_pg_dump(
        self,
        pg_dump_path: Path,
        *,
        pg_dump_table_name: str,
        target_table_name: str | None = None,
    ):
        """Import a pg_dump file into the specified schema.

        NOTE: supplying the pg_dump_table_name is a really unfortunate hack.
        We could potentially parse the pg_dump for table_name.
        ----------
        pg_dump_table_name : name of the table, as specified pg_dump file
        """
        execute_file_via_shell(self.engine_uri, pg_dump_path)
        if self.schema != DEFAULT_POSTGRES_SCHEMA:
            self.set_table_schema(
                pg_dump_table_name,
                old_schema=DEFAULT_POSTGRES_SCHEMA,
                new_schema=self.schema,
            )

        if target_table_name is not None and target_table_name != pg_dump_table_name:
            self.rename_table(old_name=pg_dump_table_name, new_name=target_table_name)
            self.execute_query(
                f"ALTER TABLE {target_table_name} RENAME CONSTRAINT {pg_dump_table_name}_pk TO {target_table_name}_pk"
            )
            self.execute_query(
                f"ALTER INDEX IF EXISTS {pg_dump_table_name}_wkb_geometry_geom_idx RENAME TO {target_table_name}_wkb_geometry_geom_idx"
            )
            self.execute_query(
                f"ALTER SEQUENCE IF EXISTS {pg_dump_table_name}_ogc_fid_seq RENAME TO {target_table_name}_ogc_fid_seq"
            )

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str | None = None,
        if_exists: Literal["fail", "replace", "append"] = "replace",
    ):
        # our custom insert method seems to make this not work properly, so need to manually drop first
        with self.connect() as conn:
            with conn.begin():
                if if_exists == "replace":
                    self.execute_query(
                        'DROP TABLE IF EXISTS ":table_name" CASCADE;',
                        table_name=AsIs(table_name),
                        conn=conn,
                    )
                if isinstance(df, gpd.GeoDataFrame):
                    df.to_postgis(
                        name=table_name,
                        schema=schema or self.schema,
                        con=conn,
                        if_exists=if_exists,
                        index=False,
                    )
                else:
                    df.to_sql(
                        table_name,
                        schema=schema or self.schema,
                        con=conn,
                        if_exists=if_exists,
                        index=False,
                        dtype={
                            "geo_1b": dialects.postgresql.JSON,
                            "geo_bl": dialects.postgresql.JSON,
                            "geo_bn": dialects.postgresql.JSON,
                        },
                        method=insert_copy,
                    )

    def export_to_csv(
        self,
        table_name: str,
        output_path: Path,
        *,
        query: str | None = None,
        columns: list[str] | None = None,
        include_header: bool = True,
    ) -> None:
        dbapi_conn = self.engine.raw_connection()
        try:
            with dbapi_conn.cursor() as cur:
                header_clause = "WITH CSV HEADER" if include_header else "WITH CSV"

                if query:
                    copy_sql = f"COPY ({query}) TO STDOUT {header_clause}"
                else:
                    full_table_name = f'"{self.schema}"."{table_name}"'
                    if columns:
                        columns_str = ", ".join(f'"{col}"' for col in columns)
                        copy_sql = f"COPY {full_table_name} ({columns_str}) TO STDOUT {header_clause}"
                    else:
                        copy_sql = f"COPY {full_table_name} TO STDOUT {header_clause}"

                with open(output_path, "w") as f:
                    cur.copy_expert(copy_sql, f)
        finally:
            dbapi_conn.close()

    def create_view(self, table_name: str, from_schema: str) -> str:
        """Create a view in the current schema."""
        self.execute_query(f'''
            CREATE VIEW "{self.schema}"."{table_name}" AS
            SELECT * FROM "{from_schema}"."{table_name}"
            ''')
        logger.info(
            f"Created view {table_name} in schema {self.schema} referencing {from_schema}.{table_name}"
        )

        return table_name

    def copy_table(self, from_table_name: str, from_schema: str) -> str:
        """Copy a table."""
        self.execute_query(f'''
        CREATE TABLE "{self.schema}"."{from_table_name}" AS
        SELECT * FROM "{from_schema}"."{from_table_name}"
        ''')
        logger.info(
            f"Copied table {from_table_name} from schema {from_schema} to schema {self.schema}"
        )

        return from_table_name


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


app = typer.Typer(add_completion=False)


@app.command("import_pg_dump")
def _cli_wrapper_import_pg_dump(
    file_path: Path = typer.Argument(),
    table_name: str = typer.Argument(),
) -> None:
    PostgresClient().import_pg_dump(file_path, pg_dump_table_name=table_name)


@app.command("import_csv")
def _cli_wrapper_create_table_from_csv(
    file_path: Path = typer.Argument(),
    table_name: str = typer.Option(None, "-t", "--table-name"),
) -> None:
    PostgresClient().create_table_from_csv(file_path, table_name)


if __name__ == "__main__":
    app()
