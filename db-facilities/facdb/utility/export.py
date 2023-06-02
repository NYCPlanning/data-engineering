from functools import wraps

import pandas as pd
from sqlalchemy import dialects

from . import ENGINE
from .utils import psql_insert_copy


def Export(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        df = func()
        name = df.loc[df.index.min(), "source"]
        df.to_sql(
            name,
            con=ENGINE,
            if_exists="replace",
            index=False,
            dtype={
                "geo_1b": dialects.postgresql.JSON,
                "geo_bl": dialects.postgresql.JSON,
                "geo_bn": dialects.postgresql.JSON,
            },
            method=psql_insert_copy,
        )

    return wrapper
