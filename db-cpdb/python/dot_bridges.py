from sqlalchemy import create_engine, text
import pandas as pd
import os
from utils import psql_insert_copy

# connect to postgres db
engine = create_engine(os.environ.get("BUILD_ENGINE", ""))

# helper function
def fms_parse(x):
    # case 1: comma separated fms ids
    if "," in x:
        fms_ids = x.split(",")
        fms_ids = [i.strip() for i in fms_ids]
    # case 2: / delimited fms ids
    elif "/" in x:
        fms_ids = x.split("/")
        fms_ids[1:] = [fms_ids[0][0:-1] + i for i in fms_ids[1:]]
    else:
        fms_ids = x
    return fms_ids

with engine.begin() as conn:
    # makes selection
    bridges = pd.read_sql_query(
        text("SELECT * FROM dot_projects_bridges WHERE fms_id is not NULL"), conn
    )
    bridges["fms_id"] = bridges["fms_id"].apply(fms_parse)
    # fms_id cleaning
    bridges_cleaned = bridges.explode("fms_id")

    bridges_cleaned["ogc_fid"] = bridges_cleaned["ogc_fid"].astype("str")

    # write new table to postgres
    bridges_cleaned.to_sql(
        "dot_projects_bridges_byfms", conn, if_exists="replace", index=False, method=psql_insert_copy
    )
