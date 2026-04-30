import marimo

__generated_with = "0.23.1"
app = marimo.App(width="full")

with app.setup:
    import marimo as mo
    import pandas as pd

    from dcpy.utils import s3


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # S3 Build QA
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Directory Comparison

    Explore and compare files across two S3 paths (e.g. a published draft vs. a new build).
    """)
    return


@app.cell
def _():
    bucket_input = mo.ui.text(value="edm-publishing", label="Bucket")
    path_a_input = mo.ui.text(
        value="db-cpdb/draft/26prelim/2/",
        label="Path A — baseline (e.g. published draft)",
        full_width=True,
    )
    path_b_input = mo.ui.text(
        value="db-cpdb/build/dm-cpdb-26prelim/",
        label="Path B — new build",
        full_width=True,
    )
    mo.vstack(
        [
            bucket_input,
            mo.hstack([path_a_input, path_b_input], gap=2),
        ],
        align="center",
    )
    return bucket_input, path_a_input, path_b_input


@app.cell
def _(bucket_input, path_a_input, path_b_input):
    def _fetch(prefix: str) -> pd.DataFrame:
        objs = s3.list_objects(bucket_input.value, prefix)
        if not objs:
            return pd.DataFrame(columns=["filename", "size_bytes", "last_modified"])
        return pd.DataFrame(
            [
                {
                    "filename": o["Key"].removeprefix(prefix),
                    "size_bytes": o["Size"],
                    "last_modified": o["LastModified"],
                }
                for o in objs
                if not o["Key"].endswith("/")
            ]
        )

    with mo.status.spinner(title="Fetching objects from S3…"):
        df_a = _fetch(path_a_input.value)
        df_b = _fetch(path_b_input.value)
    return df_a, df_b


@app.cell
def _(df_a, df_b, path_a_input, path_b_input):
    _merged = pd.merge(
        df_a[["filename", "size_bytes"]].rename(columns={"size_bytes": "size_a"}),
        df_b[["filename", "size_bytes"]].rename(columns={"size_bytes": "size_b"}),
        on="filename",
        how="outer",
        indicator=True,
    )
    _merged["in_a"] = _merged["_merge"].isin(["left_only", "both"])
    _merged["in_b"] = _merged["_merge"].isin(["right_only", "both"])
    _merged["size_diff_bytes"] = _merged["size_b"] - _merged["size_a"]
    _merged["size_diff_pct"] = (
        _merged["size_diff_bytes"] / _merged["size_a"] * 100
    ).round(1)
    comparison = _merged.drop(columns=["_merge"])

    _n_only_a = comparison[~comparison["in_b"]].shape[0]
    _n_only_b = comparison[~comparison["in_a"]].shape[0]
    _n_both = comparison[comparison["in_a"] & comparison["in_b"]].shape[0]

    mo.md(
        f"""
        ## Summary

        | | Count |
        |---|---|
        | Files in **A** (`{path_a_input.value}`) | {len(df_a)} |
        | Files in **B** (`{path_b_input.value}`) | {len(df_b)} |
        | Only in A | {_n_only_a} |
        | Only in B | {_n_only_b} |
        | In both | {_n_both} |
        """
    )
    return (comparison,)


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Only in A — missing from new build
    """)
    return


@app.cell
def _(comparison):
    _df = comparison[~comparison["in_b"]][["filename", "size_a"]].reset_index(drop=True)
    mo.ui.table(_df, selection=None) if len(_df) else mo.md(
        "_None — all files from A are present in B._"
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Only in B — new files
    """)
    return


@app.cell
def _(comparison):
    _df = comparison[~comparison["in_a"]][["filename", "size_b"]].reset_index(drop=True)
    mo.ui.table(_df, selection=None) if len(_df) else mo.md(
        "_None — no new files in B._"
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Files in both — size comparison
    """)
    return


@app.cell
def _(comparison):
    _df = (
        comparison[comparison["in_a"] & comparison["in_b"]][
            ["filename", "size_a", "size_b", "size_diff_bytes", "size_diff_pct"]
        ]
        .sort_values("size_diff_pct", key=abs, ascending=False, na_position="last")
        .reset_index(drop=True)
    )
    mo.ui.table(_df, selection=None)
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Table Comparison
    """)
    return


@app.cell(hide_code=True)
def _(comparison):
    _files_in_both = sorted(
        comparison[comparison["in_a"] & comparison["in_b"]]["filename"].tolist()
    )
    table_selector = mo.ui.dropdown(
        options=_files_in_both,
        label="Select a file to compare",
        searchable=True,
    )
    mo.vstack([table_selector], align="center")
    return (table_selector,)


@app.cell(hide_code=True)
def _(bucket_input, path_a_input, path_b_input, table_selector):
    from io import BytesIO

    def _load_file(prefix: str, filename: str) -> pd.DataFrame:
        key = prefix + filename
        body = s3.get_file(bucket_input.value, key)
        data = BytesIO(body.read())
        ext = filename.rsplit(".", 1)[-1].lower()
        if ext == "csv":
            return pd.read_csv(data)
        elif ext == "parquet":
            return pd.read_parquet(data)
        elif ext == "json":
            return pd.read_json(data)
        else:
            raise ValueError(f"Unsupported file extension: .{ext}")

    if table_selector.value:
        with mo.status.spinner(title=f"Loading {table_selector.value}…"):
            tbl_a = _load_file(path_a_input.value, table_selector.value)
            tbl_b = _load_file(path_b_input.value, table_selector.value)
    else:
        tbl_a = tbl_b = None
    return tbl_a, tbl_b


@app.cell(hide_code=True)
def _(table_selector, tbl_a, tbl_b):
    mo.stop(
        tbl_a is None or tbl_b is None,
        mo.md("_Select a file above to load and compare._"),
    )
    _cols_only_a = sorted(set(tbl_a.columns) - set(tbl_b.columns))
    _cols_only_b = sorted(set(tbl_b.columns) - set(tbl_a.columns))
    _col_status = (
        "Column sets match."
        if not _cols_only_a and not _cols_only_b
        else f"Only in A: `{'`, `'.join(_cols_only_a) or '—'}`  ·  Only in B: `{'`, `'.join(_cols_only_b) or '—'}`"
    )
    mo.vstack(
        [
            mo.md(f"""
    | | A | B |
    |---|---|---|
    | Rows | {len(tbl_a):,} | {len(tbl_b):,} |
    | Columns | {len(tbl_a.columns)} | {len(tbl_b.columns)} |

    **Columns:** {_col_status}
    """),
            mo.ui.tabs(
                {
                    f"A — {table_selector.value}": mo.ui.table(tbl_a, selection=None),
                    f"B — {table_selector.value}": mo.ui.table(tbl_b, selection=None),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _():
    _df = mo.sql(
        f"""
        select * from 
        """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Geospatial Comparison
    """)
    return


@app.cell
def _():
    import openlayers as ol
    from openlayers.basemaps import CartoBasemapLayer, Carto

    return Carto, CartoBasemapLayer, ol


@app.cell(hide_code=True)
def _(comparison):
    _zip_files = sorted(
        comparison.loc[
            comparison["in_a"]
            & comparison["in_b"]
            & comparison["filename"].str.endswith(".zip"),
            "filename",
        ].tolist()
    )
    mo.stop(not _zip_files, mo.md("_No shared `.zip` files found in both paths._"))
    geo_selector = mo.ui.dropdown(
        options=_zip_files,
        label="Select a shapefile (.zip) to compare",
        searchable=True,
    )
    mo.vstack([geo_selector], align="center")
    return (geo_selector,)


@app.cell(hide_code=True)
def _(bucket_input, geo_selector, path_a_input, path_b_input):
    import geopandas as gpd
    import tempfile
    import os

    mo.stop(geo_selector.value is None, mo.md("_Select a shapefile above._"))

    def _load_shapefile(prefix: str, filename: str) -> gpd.GeoDataFrame:
        body = s3.get_file(bucket_input.value, prefix + filename)
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as _tmp:
            _tmp.write(body.read())
            _tmp_path = _tmp.name
        try:
            _gdf = gpd.read_file(f"zip://{_tmp_path}")
        finally:
            os.unlink(_tmp_path)
        return _gdf

    with mo.status.spinner(title=f"Loading {geo_selector.value} from both paths…"):
        geo_gdf_a = (
            _load_shapefile(path_a_input.value, geo_selector.value)
            .to_crs(4326)
            .reset_index(drop=True)
        )
        geo_gdf_b = (
            _load_shapefile(path_b_input.value, geo_selector.value)
            .to_crs(4326)
            .reset_index(drop=True)
        )
    return geo_gdf_a, geo_gdf_b


@app.cell(hide_code=True)
def _(geo_gdf_a, geo_selector, path_a_input):
    _geom_col = geo_gdf_a.geometry.name
    _attr_cols = [c for c in geo_gdf_a.columns if c != _geom_col]
    geo_table_a = mo.ui.table(
        geo_gdf_a[_attr_cols],
        selection="multi",
        page_size=20,
        label=f"A — {path_a_input.value}{geo_selector.value} ({len(geo_gdf_a):,} features) · check rows to highlight on map",
    )
    geo_table_a
    return (geo_table_a,)


@app.cell(hide_code=True)
def _(Carto, CartoBasemapLayer, geo_gdf_a, geo_table_a, ol):
    _sel_idx = geo_table_a.value.index.tolist()
    _plot_a = geo_gdf_a.loc[_sel_idx] if _sel_idx else geo_gdf_a
    _style_a = ol.FlatStyle(
        fill_color="rgba(70, 130, 180, 0.35)",
        stroke_color="#4682b4",
        stroke_width=2,
        circle_radius=6,
        circle_fill_color="rgba(70, 130, 180, 0.7)",
        circle_stroke_color="#4682b4",
        circle_stroke_width=1.5,
    )
    _layer_a = _plot_a.ol.to_layer(style=_style_a, fit_bounds=False)
    _map_a = ol.MapWidget(
        view=ol.View(center=[0, 0], zoom=1),
        layers=[CartoBasemapLayer(Carto.LIGHT_ALL), _layer_a],
    )
    _map_a.fit_bounds(tuple(_plot_a.geometry.total_bounds))
    _map_a.add_tooltip()
    _map_a
    return


@app.cell(hide_code=True)
def _(geo_gdf_b, geo_selector, path_b_input):
    _geom_col = geo_gdf_b.geometry.name
    _attr_cols = [c for c in geo_gdf_b.columns if c != _geom_col]
    geo_table_b = mo.ui.table(
        geo_gdf_b[_attr_cols],
        selection="multi",
        page_size=20,
        label=f"B — {path_b_input.value}{geo_selector.value} ({len(geo_gdf_b):,} features) · check rows to highlight on map",
    )
    geo_table_b
    return (geo_table_b,)


@app.cell(hide_code=True)
def _(Carto, CartoBasemapLayer, geo_gdf_b, geo_table_b, ol):
    _sel_idx = geo_table_b.value.index.tolist()
    _plot_b = geo_gdf_b.loc[_sel_idx] if _sel_idx else geo_gdf_b
    _style_b = ol.FlatStyle(
        fill_color="rgba(230, 57, 70, 0.35)",
        stroke_color="#e63946",
        stroke_width=2,
        circle_radius=6,
        circle_fill_color="rgba(230, 57, 70, 0.7)",
        circle_stroke_color="#e63946",
        circle_stroke_width=1.5,
    )
    _layer_b = _plot_b.ol.to_layer(style=_style_b, fit_bounds=False)
    _map_b = ol.MapWidget(
        view=ol.View(center=[0, 0], zoom=1),
        layers=[CartoBasemapLayer(Carto.LIGHT_ALL), _layer_b],
    )
    _map_b.fit_bounds(tuple(_plot_b.geometry.total_bounds))
    _map_b.add_tooltip()
    _map_b
    return


if __name__ == "__main__":
    app.run()
