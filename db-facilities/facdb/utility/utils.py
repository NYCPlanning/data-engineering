import csv
import hashlib
import os
import re
from functools import wraps
from io import StringIO

import pandas as pd


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
