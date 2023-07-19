import os
from pathlib import Path
import csv
from io import StringIO


def exec_file_via_shell(build_engine: str, path: Path):
    """Execute .sql script at given path."""
    cmd = f"psql {build_engine} -v ON_ERROR_STOP=1 -f {path}"
    if os.system(cmd) != 0:
        raise Exception(f"{path} has errors!")


def exec_via_shell(build_engine: str, sql_statement):
    """Execute sql via psql shell."""
    cmd = f"psql {build_engine} -v ON_ERROR_STOP=1 {sql_statement}"
    if os.system(cmd) != 0:
        raise Exception(f"Command has errors! {cmd}")


def insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data.
    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str Column names
    data_iter : Iterable that iterates the values to be inserted
    """
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
