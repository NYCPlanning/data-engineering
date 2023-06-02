from sqlalchemy import create_engine, text
import pandas as pd
import os
from utils import psql_insert_copy

# connect to postgres db
engine = create_engine(os.environ.get("BUILD_ENGINE", ""))

with engine.begin() as conn:
    # makes selection
    doitt_buildingfootprints_source = pd.read_sql_query(
        text("SELECT * FROM doitt_buildingfootprints_source"), conn
    )
    # bin cleaning
    doitt_buildingfootprints_source_clean = doitt_buildingfootprints_source[doitt_buildingfootprints_source["bin"].astype(str).str.isnumeric()]

    # write new table to postgres
    doitt_buildingfootprints_source_clean.to_sql(
        "doitt_buildingfootprints", conn, if_exists="replace", index=False, method=psql_insert_copy
    )
