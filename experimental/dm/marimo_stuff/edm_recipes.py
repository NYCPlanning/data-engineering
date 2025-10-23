import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import marimo as mo
    import duckdb
    import os


@app.cell(hide_code=True)
def _():
    mo.md(r"""# `edm-recipes` Data Catalog""")
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    This notebook is for exploring Data Engineering's source data stored in the `edm-recipes` S3 bucket in Digital Ocean.

    Every day, a duckdb database in `edm-reicpes` is refreshed to have up-to-date views of all versions of all datasets. This is limited to the versions that have a parquet files in them.
    """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Setup""")
    return


@app.cell
def _():
    DATABASE_URL = "s3://edm-recipes/datasets/catalog.duckdb"
    return (DATABASE_URL,)


@app.function
def setup_s3_secret(conn: duckdb.DuckDBPyConnection | None = None) -> None:
    cmd = conn.sql if conn else duckdb.sql
    cmd("INSTALL httpfs; LOAD httpfs;")
    cmd("INSTALL spatial; LOAD spatial;")
    cmd(
        f"""
            CREATE SECRET IF NOT EXISTS s3_secret (
                TYPE S3,
                KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
                SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
                ENDPOINT '{os.environ["AWS_S3_ENDPOINT"].split("://")[1]}'
            );
        """
    )


@app.cell
def _():
    conn = duckdb.connect()
    setup_s3_secret(conn)
    return (conn,)


@app.cell
def _(DATABASE_URL, conn):
    conn.sql(f"ATTACH DATABASE '{DATABASE_URL}' AS recipes_catelog;")
    conn.sql("USE recipes_catelog")
    return


@app.cell
def _(conn):
    _df = mo.sql(
        f"""
        SHOW ALL TABLES
        """,
        engine=conn,
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Explore an example dataset""")
    return


@app.cell
def _():
    example_dataset_name = "dpr_parksproperties"
    return (example_dataset_name,)


@app.cell
def _(conn, example_dataset_name):
    example_dataset_versions = mo.sql(
        f"""
        select
            *
        from
            (show all tables)
        where
            schema = '{example_dataset_name}'
        """,
        engine=conn,
    )
    return (example_dataset_versions,)


@app.cell
def _(example_dataset_versions):
    sorted_versions = sorted(list(example_dataset_versions["name"]))
    sorted_versions
    return (sorted_versions,)


@app.cell
def _(conn, example_dataset_name, sorted_versions):
    latest_data = mo.sql(
        f"""
        select
            *
        from
            -- "dpr_parksproperties"."20250920"
            "{example_dataset_name}"."{sorted_versions[-1]}"
        """,
        engine=conn,
    )
    return (latest_data,)


@app.cell
def _(latest_data):
    latest_data["subcategory"].value_counts(sort=True)
    return


if __name__ == "__main__":
    app.run()
