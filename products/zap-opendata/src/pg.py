import pandas as pd
import string
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from psycopg2.extensions import AsIs


def format_sql_query_parameters(parameters: dict) -> dict:
    formatted_parameters = {}
    for key, value in parameters.items():
        formatted_parameters[key] = AsIs(value)
    return formatted_parameters


class PG:
    def __init__(self, url: str, schema: str):
        self.url = url
        self.schema = schema.replace("-", "_")  # no dashes in postgres schema names
        self.engine = create_engine(
            url,
            isolation_level="AUTOCOMMIT",
            connect_args={"options": f"-csearch_path={self.schema}"},
        )
        self.create_schema()

    def create_schema(self) -> None:
        self.execute_sql_query(
            "CREATE SCHEMA IF NOT EXISTS :schema", {"schema": AsIs(self.schema)}
        )
        print(f"Schema '{self.schema}' created or already exists")

    def execute_select_query(self, base_query: str, parameters: dict) -> pd.DataFrame:
        with self.engine.begin() as sql_connection:
            records = pd.read_sql(
                sql=text(base_query),
                con=sql_connection,
                params=format_sql_query_parameters(parameters),
            )
        return records

    def execute_sql_query(self, base_query: str, parameters: dict) -> None:
        with self.engine.connect() as connection:
            connection.execute(
                statement=text(base_query),
                parameters=format_sql_query_parameters(parameters),
            )
