import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
def _():
    import os

    import marimo as mo
    import sqlalchemy

    return mo, os, sqlalchemy


@app.cell
def _():
    default_database_name = "db-template"
    return (default_database_name,)


@app.cell
def _(default_database_name, os, sqlalchemy):
    _password = os.environ.get("BUILD_ENGINE_PASSWORD")
    _username = os.environ.get("BUILD_ENGINE_USER")
    _host = os.environ.get("BUILD_ENGINE_HOST")
    _port = os.environ.get("BUILD_ENGINE_PORT")
    DATABASE_URL = (
        f"postgresql://{_username}:{_password}@{_host}:{_port}/{default_database_name}"
    )
    engine = sqlalchemy.create_engine(DATABASE_URL)
    return (engine,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Choose a database
    """)
    return


@app.cell
def _(engine, sqlalchemy):
    with engine.connect() as _conn:
        database_names = sorted(
            row[0]
            for row in _conn.execute(
                sqlalchemy.text(
                    "SELECT datname FROM pg_database WHERE datistemplate = false"
                )
            )
        )
    return (database_names,)


@app.cell
def _(database_names, default_database_name, mo):
    dropdown_database_name = mo.ui.dropdown(
        label="Choose a database",
        options=database_names,
        value=default_database_name,
    )
    dropdown_database_name
    return (dropdown_database_name,)


@app.cell
def _(dropdown_database_name, os, sqlalchemy):
    _password = os.environ.get("BUILD_ENGINE_PASSWORD")
    _username = os.environ.get("BUILD_ENGINE_USER")
    _host = os.environ.get("BUILD_ENGINE_HOST")
    _port = os.environ.get("BUILD_ENGINE_PORT")
    SELECTED_DATABASE_URL = f"postgresql://{_username}:{_password}@{_host}:{_port}/{dropdown_database_name.value}"
    selected_engine = sqlalchemy.create_engine(SELECTED_DATABASE_URL)
    return (selected_engine,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Choose a schema
    """)
    return


@app.cell
def _(selected_engine, sqlalchemy):
    with selected_engine.connect() as _conn:
        schema_names = sorted(
            row[0]
            for row in _conn.execute(
                sqlalchemy.text(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')"
                )
            )
        )
    return (schema_names,)


@app.cell
def _(mo, schema_names):
    dropdown_schema_name = mo.ui.dropdown(label="Choose a schema", options=schema_names)
    dropdown_schema_name
    return (dropdown_schema_name,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Choose a table
    """)
    return


@app.cell
def _(dropdown_schema_name, selected_engine, sqlalchemy):
    with selected_engine.connect() as _conn:
        table_names = sorted(
            row[0]
            for row in _conn.execute(
                sqlalchemy.text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = :schema"
                ),
                {"schema": dropdown_schema_name.value},
            )
        )
    return (table_names,)


@app.cell
def _(mo, table_names):
    dropdown_table_name = mo.ui.dropdown(label="Choose a table", options=table_names)
    dropdown_table_name
    return (dropdown_table_name,)


@app.cell(hide_code=True)
def _(dropdown_schema_name, dropdown_table_name, mo, selected_engine):
    mo.stop(
        dropdown_schema_name.value is None or dropdown_table_name.value is None,
        mo.md("_Select a schema and table above to preview data._"),
    )

    _df = mo.sql(
        f"""
        select * from "{dropdown_schema_name.value}"."{dropdown_table_name.value}"
        """,
        engine=selected_engine,
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## SQL sandbox
    """)
    return


@app.cell
def _(dropdown_schema_name, dropdown_table_name, mo, selected_engine):
    mo.stop(
        dropdown_schema_name.value is None or dropdown_table_name.value is None,
        mo.md("_Select a schema and table above to run the sandbox query._"),
    )

    _df = mo.sql(
        f"""
        SELECT
            *
        FROM "{dropdown_schema_name.value}"."{dropdown_table_name.value}"
        order by 1 asc, 2 asc
        """,
        engine=selected_engine,
    )
    return


if __name__ == "__main__":
    app.run()
