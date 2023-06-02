from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv
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
parkproj = pd.read_sql_query(
    "SELECT * FROM dpr_capitalprojects WHERE park_id is not NULL", engine
)

# park_id cleaning
parkproj_cleaned = pd.DataFrame()
for i in range(len(parkproj)):
    parkproj_cleaned = pd.concat([parkproj_cleaned, parkid_parse(parkproj.iloc[i, :])])
parkproj_cleaned = parkproj_cleaned[["fmsid", "park_id"]]

parkproj_cleaned.to_sql(
    "dpr_capitalprojects_fms_parkid",
    engine,
    if_exists="replace",
    index=False,
    method=psql_insert_copy,
)
