import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import leafmap.maplibregl as leafmapgl


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    # Using leafmap and DuckDB

    This is based on an example in the leafmap docs: https://leafmap.org/maplibre/duckdb_layer/.

    The maplibregl module generates DuckDB tile servers to vizualize geospatial vector data.
    """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Example 1: Load data from vector file (creates in-memory database)""")
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""Download a sample GeoParquet dataset from [Source Coop](https://source.coop/giswqs/nwi/wetlands/RI_Wetlands.parquet)."""
    )
    return


@app.cell
def _():
    url_wetlands = "https://data.source.coop/giswqs/nwi/wetlands/RI_Wetlands.parquet"
    fileapth_wetlands = leafmapgl.download_file(
        url=url_wetlands, output=".data/RI_Wetlands.parquet"
    )
    return (fileapth_wetlands,)


@app.cell
def _(fileapth_wetlands):
    m_wetlands = leafmapgl.Map(style="liberty", height="600px")
    m_wetlands.add_basemap("Esri.WorldImagery")
    m_wetlands.add_duckdb_layer(
        data=fileapth_wetlands,  # Supports GeoParquet, GeoJSON, and GeoPackage, etc.
        layer_name="wetlands",
        layer_type="fill",
        paint={"fill-color": "#3388ff"},
        opacity=0.5,
        fit_bounds=True,
        use_view=False,  # For remote datasets, set use_view to True to avoid copying data to local
        min_zoom=None,  # For large datasets, set min_zoom to None to avoid zooming out too much
        quiet=False,
    )
    m_wetlands
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Example 2: Load from DuckDB database file""")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""Download a database of NYC data.""")
    return


@app.cell
def _():
    url_nyc = "https://data.gishub.org/duckdb/nyc_data.db.zip"
    leafmapgl.download_file(url=url_nyc, output=".data/nyc_data.db.zip")
    return


@app.cell
def _():
    db_path = ".data/nyc_data.db"
    return (db_path,)


@app.cell
def _():
    import duckdb

    DATABASE_URL = "nyc_data_2.db"
    engine = duckdb.connect(DATABASE_URL, read_only=False)
    return (engine,)


@app.cell
def _(engine):
    _df = mo.sql(
        f"""
        show all tables
        """,
        engine=engine,
    )
    return


@app.cell
def _(engine, nyc_neighborhoods):
    _df = mo.sql(
        f"""
        SELECT * FROM nyc_neighborhoods
        """,
        engine=engine,
    )
    return


@app.cell
def _(db_path):
    m_nyc = leafmapgl.Map(
        center=[-73.9031255, 40.7127753], zoom=9, style="liberty", height="600px"
    )
    m_nyc.add_duckdb_layer(
        database_path=db_path,
        table_name="nyc_neighborhoods",
        layer_type="fill",
        paint={"fill-color": "#ff0000"},
        opacity=0.5,
        fit_bounds=False,
        src_crs="EPSG:26918",
        quiet=True,
    )
    m_nyc
    return


if __name__ == "__main__":
    app.run()
