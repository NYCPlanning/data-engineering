import os
from io import StringIO
import re
import csv
import hashlib
from sqlalchemy import create_engine, text
import pandas as pd
import geopandas as gpd
from functools import wraps

from . import BUILD_ENGINE, DATE, PROCESSED_DATA_PATH

engine = create_engine(os.environ.get("BUILD_ENGINE"))


def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data
    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
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


def hash_each_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    e.g. df = hash_each_row(df)
    this function will create a "uid" column with hashed row values
    ----------
    df: input dataframe
    """
    df["temp_column"] = df.astype(str).values.sum(axis=1)

    def hash_helper(x):
        return hashlib.md5(x.encode("utf-8")).hexdigest()

    df["uid"] = df["temp_column"].apply(hash_helper)
    del df["temp_column"]
    cols = list(df.columns)
    cols.remove("uid")
    cols = ["uid"] + cols
    return df[cols]


def format_field_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Change field name to lower case
    and replace all spaces with underscore
    """

    def format_func(x):
        return re.sub(r"\W+", "", x.lower().strip().replace("-", "_").replace(" ", "_"))

    df.columns = df.columns.map(format_func)
    return df


def add_version_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adding today's date as the version of the file
    -----
    note that this function is not implemented yet
    because we might want to get the file creation date
    directly from the file instead of assigning a date
    """
    df["version"] = DATE
    return df


def ETL(func) -> callable:
    """
    Decorator for extractor functions that does the following:
    1. extracts data
    2. adds md5 uid field
    3. format field names
    4. if geopandas dataframe, convert geometry to string type
    5. copy table to postgres
    6. alter geometry field type to geometry with SRID 4326 if needed
    6. save data to /processed/
    """

    @wraps(func)
    def wrapper(*args, **kwargs) -> None:
        name = func.__name__
        print(f"ingesting\t{name} ...")
        df = func()

        # Adding uid
        df = hash_each_row(df)

        # Formating field names
        df = format_field_names(df)

        # Write to postgres database
        # If it's a geopandas dataframe, we will have to
        # Convert geometry column to text first
        if isinstance(df, gpd.geodataframe.GeoDataFrame):
            df = pd.DataFrame(df, dtype=str)

        print(f"export\t{name} to postgres ...")
        df.to_sql(
            name,
            con=engine,
            if_exists="replace",
            index=False,
            method=psql_insert_copy,
        )

        with engine.begin() as conn:
            if "geometry" in list(df.columns):
                conn.execute(
                    text(
                        """
                BEGIN; 
                ALTER TABLE %(name)s 
                ALTER COLUMN geometry type Geometry 
                    USING ST_SetSRID(ST_GeomFromText(geometry), 4326);
                COMMIT;
                """
                        % {"name": name}
                    )
                )
            # df.to_csv(f"{output_dir}/{name}.csv", index=False)
        print("🎉 done!")
        return None

    return wrapper
