from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os
from datetime import date

EDM_DATA_SQL_ENGINE = create_engine(os.environ["EDM_DATA"])
DATE = date.today().strftime("%Y-%m-%d")

def execute_sql_query(statement: str) -> None:
    with EDM_DATA_SQL_ENGINE.begin() as sql_conn:
        sql_conn.execute(statement=text(statement))
