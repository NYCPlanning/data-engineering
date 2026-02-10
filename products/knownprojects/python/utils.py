import csv
import hashlib
import re
from functools import wraps
from io import StringIO

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text

from . import BUILD_ENGINE, DCP_HOUSING_DATA_FILENAMES

engine = create_engine(BUILD_ENGINE)


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


def _hash_each_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    e.g. df = hash_each_row(df)
    this function will create a "uid" column with hashed row values
    ----------
    df: input dataframe
    """
    df["temp_column"] = df.astype(str).values.sum(axis=1)

    def _hash_helper(x):
        return hashlib.md5(x.encode("utf-8")).hexdigest()

    df["uid"] = df["temp_column"].apply(_hash_helper)
    del df["temp_column"]
    cols = list(df.columns)
    cols.remove("uid")
    cols = ["uid"] + cols
    return df[cols]


def _format_field_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Change field name to lower case
    and replace all spaces with underscore
    """

    def _format_func(x):
        return re.sub(r"\W+", "", x.lower().strip().replace("-", "_").replace(" ", "_"))

    df.columns = df.columns.map(_format_func)
    return df


def ETL(extractor_func) -> callable:
    """
    Decorator for extractor functions that does the following:
    1. extracts data
    2. adds md5 uid field
    3. format field names
    4. if geopandas dataframe, convert geometry to string type
    5. copy table to postgres
    6. alter geometry field type to geometry with SRID 4326 if needed
    7. update source_data_versions table
    """

    @wraps(extractor_func)
    def wrapper(*args, **kwargs) -> None:
        source_dataset_name = extractor_func.__name__
        filename = DCP_HOUSING_DATA_FILENAMES[source_dataset_name]
        df = extractor_func(filename)

        # Adding uid
        df = _hash_each_row(df)

        # Formating field names
        df = _format_field_names(df)

        # Write to postgres database
        # If it's a geopandas dataframe, we will have to
        # Convert geometry column to text first
        if isinstance(df, gpd.geodataframe.GeoDataFrame):
            df = pd.DataFrame(df, dtype=str)

        print(f"export\t{source_dataset_name} to postgres ...")
        df.to_sql(
            source_dataset_name,
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
                        % {"name": source_dataset_name}
                    )
                )
            conn.execute(
                text(
                    """INSERT INTO source_data_versions VALUES ('%(name)s','%(version)s');"""
                    % {"name": source_dataset_name, "version": filename}
                )
            )
        print("🎉 done!")
        return None

    return wrapper
