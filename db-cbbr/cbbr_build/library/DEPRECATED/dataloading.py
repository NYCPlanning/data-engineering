# DEPRECATED
from cook import Importer
import os
from sqlalchemy import create_engine
import pandas as pd

RECIPE_ENGINE = os.environ.get("RECIPE_ENGINE", "")
BUILD_ENGINE = os.environ.get("BUILD_ENGINE", "")
EDM_DATA = os.environ.get("EDM_DATA", "")


def ETL():
    importer = Importer(RECIPE_ENGINE, BUILD_ENGINE)
    importer.import_table(schema_name="cbbr_submissions")
    importer.import_table(schema_name="cbbr_agency_updates")
    importer.import_table(schema_name="dpr_parksproperties")
    importer.import_table(schema_name="doitt_buildingfootprints")


def FACDB():
    importer = Importer(EDM_DATA, BUILD_ENGINE)
    importer.import_table(schema_name="facilities")


def old_cbbr_submissions():
    df = pd.read_sql(
        '''SELECT * FROM cbbr_submissions."2018/12/11"''', con=RECIPE_ENGINE
    )
    df.to_sql(
        "old_cbbr_submissions",
        BUILD_ENGINE,
        if_exists="replace",
        chunksize=500,
        index=False,
    )


if __name__ == "__main__":
    ETL()
    FACDB()
    old_cbbr_submissions()
