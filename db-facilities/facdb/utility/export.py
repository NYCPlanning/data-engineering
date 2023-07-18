from functools import wraps
from sqlalchemy import dialects
from dcpy.connectors import psql
from .. import BUILD_ENGINE
from sqlalchemy import create_engine

ENGINE = create_engine(BUILD_ENGINE)

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
            method=psql.insert_copy,
        )

    return wrapper
