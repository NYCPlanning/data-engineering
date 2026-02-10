from facdb import BUILD_ENGINE
from sqlalchemy import create_engine, dialects

from dcpy.utils import postgres

ENGINE = create_engine(BUILD_ENGINE)


def export(df):
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
        method=postgres.insert_copy,
    )
