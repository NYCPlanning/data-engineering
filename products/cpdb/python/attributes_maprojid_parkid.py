import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from utils import psql_insert_copy

load_dotenv()


def parkid_parse(x):
    # comma separated parkids
    if "," in x.park_id:
        park_ids = x.park_id.split(",")
        park_ids = [i.strip() for i in park_ids]
        row = x.to_frame().T
        rows = x.to_frame().T
        while len(rows) < len(park_ids):
            rows = rows.append(row)
        rows["park_id"] = park_ids
    else:
        rows = x.to_frame().T
    return rows


# connect to postgres db
engine = create_engine(os.getenv("BUILD_ENGINE", ""))

# makes selection
with engine.begin() as conn:
    parkproj = pd.read_sql_query(
        text("SELECT * FROM dpr_capitalprojects WHERE park_id is not NULL"), conn
    )

    # park_id cleaning
    parkproj_cleaned = pd.DataFrame()
    for i in range(len(parkproj)):
        parkproj_cleaned = pd.concat(
            [parkproj_cleaned, parkid_parse(parkproj.iloc[i, :])]
        )
    parkproj_cleaned = parkproj_cleaned[["fmsid", "park_id"]]

    parkproj_cleaned.to_sql(
        "dpr_capitalprojects_fms_parkid",
        conn,
        if_exists="replace",
        index=False,
        method=psql_insert_copy,
    )
