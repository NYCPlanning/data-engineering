import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import os

    import duckdb
    import marimo as mo


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # `edm-recipes` Data Catalog
    """)
    return


@app.cell
def _():
    mo.md(r"""
    This notebook is for exploring Data Engineering's source data stored in the `edm-recipes` S3 bucket in Digital Ocean.

    Every day, a duckdb database in `edm-reicpes` is refreshed to have up-to-date views of all versions of all datasets. **This is limited to the versions that have a parquet file in them.**
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Setup
    """)
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
        """
        SHOW ALL TABLES
        """,
        engine=conn,
    )
    return


@app.cell(hide_code=True)
def _(conn):
    _df = mo.sql(
        """
        select
            *
        from duckdb_views()
        """,
        engine=conn,
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Explore a dataset
    """)
    return


@app.cell
def _(conn):
    dataset_names = (
        conn.sql("select distinct schema from (show all tables) order by schema")
        .df()["schema"]
        .to_list()
    )
    return (dataset_names,)


@app.cell
def _(dataset_names):
    dropdown_dataset_names = mo.ui.dropdown(
        label="Choose a dataset", options=dataset_names
    )
    dropdown_dataset_names
    return (dropdown_dataset_names,)


@app.cell
def _(conn, dropdown_dataset_names):
    _df = mo.sql(
        f"""
        select
            *
        from
            (show all tables)
        where
            schema = '{dropdown_dataset_names.value}'
        """,
        engine=conn,
    )
    return


@app.cell
def _(conn, dropdown_dataset_names):
    _target_schema = dropdown_dataset_names.value

    df_dataset_versions_details = conn.sql(
        f"SELECT * FROM duckdb_views() WHERE schema_name = '{_target_schema}'"
    ).df()

    _row_counts = []

    # 5. Fast loop to query parquet metadata for each view
    for view_name in df_dataset_versions_details["view_name"]:
        # Safely wrap names in double quotes to handle spaces/special characters
        query = f'SELECT COUNT(*) FROM "{_target_schema}"."{view_name}"'

        # fetchone()[0] extracts the integer count directly
        _count = conn.sql(query).fetchone()[0]
        _row_counts.append(_count)

    df_dataset_versions_details["row_count"] = _row_counts
    return (df_dataset_versions_details,)


@app.cell
def _(df_dataset_versions_details):
    df_dataset_versions_details[
        ["schema_name", "view_name", "column_count", "row_count"]
    ]
    return


@app.cell
def _(conn, dropdown_dataset_names):
    dataset_versions = (
        conn.sql(
            f"select name from (show all tables) where schema = '{dropdown_dataset_names.value}' order by name"
        )
        .df()["name"]
        .to_list()
    )
    dropdown_dataset_versions = mo.ui.dropdown(
        label="Choose a version: ", options=dataset_versions
    )
    dropdown_dataset_versions
    return (dropdown_dataset_versions,)


@app.cell(hide_code=True)
def _(conn, dropdown_dataset_names, dropdown_dataset_versions):
    _df = mo.sql(
        f"""
        select
            *
        from
            "{dropdown_dataset_names.value}"."{dropdown_dataset_versions.value}"
        """,
        engine=conn,
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Explore an example dataset
    """)
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
