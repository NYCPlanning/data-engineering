import marimo

__generated_with = "0.23.3"
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

    def list_dirs(bucket: str, prefix: str) -> list[str]:
        try:
            c = s3.client()
            p = (prefix.rstrip("/") + "/") if prefix else ""
            resp = c.list_objects_v2(Bucket=bucket, Prefix=p, Delimiter="/")
            return sorted(
                cp["Prefix"].removeprefix(p).rstrip("/")
                for cp in resp.get("CommonPrefixes", [])
            )
        except Exception:
            return []

    bucket_input
    return bucket_input, list_dirs


@app.cell(hide_code=True)
def _(bucket_input, list_dirs):
    with mo.status.spinner(title="Listing bucket…"):
        _root = [x for x in list_dirs(bucket_input.value, "") if x]
    mo.md(
        "**Path A — baseline** &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; **Path B — new build**"
    )
    path_a_seg1 = mo.ui.dropdown(options=_root, label="A", searchable=True)
    path_b_seg1 = mo.ui.dropdown(options=_root, label="B", searchable=True)
    mo.hstack([path_a_seg1, path_b_seg1], gap=2)
    return path_a_seg1, path_b_seg1


@app.cell(hide_code=True)
def _(bucket_input, list_dirs, path_a_seg1, path_b_seg1):
    _a2 = (
        [x for x in list_dirs(bucket_input.value, path_a_seg1.value) if x]
        if path_a_seg1.value
        else []
    )
    _b2 = (
        [x for x in list_dirs(bucket_input.value, path_b_seg1.value) if x]
        if path_b_seg1.value
        else []
    )
    path_a_seg2 = mo.ui.dropdown(options=_a2, label="A", searchable=True)
    path_b_seg2 = mo.ui.dropdown(options=_b2, label="B", searchable=True)
    mo.hstack([path_a_seg2, path_b_seg2], gap=2) if _a2 or _b2 else None
    return path_a_seg2, path_b_seg2


@app.cell(hide_code=True)
def _(
    bucket_input,
    list_dirs,
    path_a_seg1,
    path_a_seg2,
    path_b_seg1,
    path_b_seg2,
):
    _pfx_a = "/".join(x for x in [path_a_seg1.value, path_a_seg2.value] if x)
    _pfx_b = "/".join(x for x in [path_b_seg1.value, path_b_seg2.value] if x)
    _a3 = (
        [x for x in list_dirs(bucket_input.value, _pfx_a) if x]
        if path_a_seg2.value
        else []
    )
    _b3 = (
        [x for x in list_dirs(bucket_input.value, _pfx_b) if x]
        if path_b_seg2.value
        else []
    )
    path_a_seg3 = mo.ui.dropdown(options=_a3, label="A", searchable=True)
    path_b_seg3 = mo.ui.dropdown(options=_b3, label="B", searchable=True)
    mo.hstack([path_a_seg3, path_b_seg3], gap=2) if _a3 or _b3 else None
    return path_a_seg3, path_b_seg3


@app.cell(hide_code=True)
def _(
    bucket_input,
    list_dirs,
    path_a_seg1,
    path_a_seg2,
    path_a_seg3,
    path_b_seg1,
    path_b_seg2,
    path_b_seg3,
):
    _pfx_a = "/".join(
        x for x in [path_a_seg1.value, path_a_seg2.value, path_a_seg3.value] if x
    )
    _pfx_b = "/".join(
        x for x in [path_b_seg1.value, path_b_seg2.value, path_b_seg3.value] if x
    )
    _a4 = (
        [x for x in list_dirs(bucket_input.value, _pfx_a) if x]
        if path_a_seg3.value
        else []
    )
    _b4 = (
        [x for x in list_dirs(bucket_input.value, _pfx_b) if x]
        if path_b_seg3.value
        else []
    )
    path_a_seg4 = mo.ui.dropdown(options=_a4, label="A", searchable=True)
    path_b_seg4 = mo.ui.dropdown(options=_b4, label="B", searchable=True)
    mo.hstack([path_a_seg4, path_b_seg4], gap=2) if _a4 or _b4 else None
    return path_a_seg4, path_b_seg4


@app.cell(hide_code=True)
def _(
    path_a_seg1,
    path_a_seg2,
    path_a_seg3,
    path_a_seg4,
    path_b_seg1,
    path_b_seg2,
    path_b_seg3,
    path_b_seg4,
):
    def _join(*segs):
        parts = [s for s in segs if s]
        return "/".join(parts) + "/" if parts else ""

    path_a = _join(
        path_a_seg1.value, path_a_seg2.value, path_a_seg3.value, path_a_seg4.value
    )
    path_b = _join(
        path_b_seg1.value, path_b_seg2.value, path_b_seg3.value, path_b_seg4.value
    )
    mo.hstack(
        [
            mo.callout(mo.md(f"**Path A:** `{path_a or '—'}`"), kind="neutral"),
            mo.callout(mo.md(f"**Path B:** `{path_b or '—'}`"), kind="neutral"),
        ],
        gap=2,
    )
    return path_a, path_b


@app.cell
def _(bucket_input, path_a, path_b):
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

    mo.stop(
        not path_a or not path_b,
        mo.md("_Select paths above to begin comparison._"),
    )

    _err = None
    try:
        with mo.status.spinner(title="Fetching objects from S3…"):
            df_a = _fetch(path_a)
            df_b = _fetch(path_b)
    except Exception as e:
        _err = str(e)
        df_a = df_b = pd.DataFrame(columns=["filename", "size_bytes", "last_modified"])

    mo.stop(
        _err is not None,
        mo.callout(mo.md(f"**S3 fetch failed:** `{_err}`"), kind="danger"),
    )
    return df_a, df_b


@app.cell
def _(df_a, df_b, path_a, path_b):
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
        | Files in **A** (`{path_a}`) | {len(df_a)} |
        | Files in **B** (`{path_b}`) | {len(df_b)} |
        | Only in A | {_n_only_a} |
        | Only in B | {_n_only_b} |
        | In both | {_n_both} |
        """
    )
    return (comparison,)


@app.cell(hide_code=True)
def _(comparison):
    _only_a = comparison[~comparison["in_b"]].shape[0]
    _only_b = comparison[~comparison["in_a"]].shape[0]
    _shared = comparison[comparison["in_a"] & comparison["in_b"]]
    _sizes_equal = (
        (_shared["size_a"] == _shared["size_b"]).all() if len(_shared) else True
    )
    _equal = _only_a == 0 and _only_b == 0 and _sizes_equal
    if _equal:
        _msg = "**Directory equal:** ✅ Yes — same files and sizes."
    elif _only_a == 0 and _only_b == 0:
        _msg = "**Directory equal:** ❌ No — same file set but sizes differ for some files."
    else:
        _msg = (
            f"**Directory equal:** ❌ No — {_only_a} file(s) only in A, {_only_b} file(s) only in B"
            + ("." if _sizes_equal else ", and sizes differ.")
        )
    mo.callout(
        mo.md(_msg),
        kind="success" if _equal else "danger",
    )
    return


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
    ## File Comparison
    """)
    return


@app.cell(hide_code=True)
def _(comparison):
    _files_in_both = sorted(
        comparison[comparison["in_a"] & comparison["in_b"]]["filename"].tolist()
    )
    file_selector = mo.ui.dropdown(
        options=_files_in_both,
        label="Select a file to compare",
        searchable=True,
    )
    mo.vstack([file_selector], align="center")
    return (file_selector,)


@app.cell(hide_code=True)
def _(bucket_input, file_selector, path_a, path_b):
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

    if file_selector.value:
        with mo.status.spinner(title=f"Loading {file_selector.value}…"):
            file_df_a = _load_file(path_a, file_selector.value)
            file_df_b = _load_file(path_b, file_selector.value)
    else:
        file_df_a = file_df_b = None
    return file_df_a, file_df_b


@app.cell(hide_code=True)
def _(file_df_a, file_df_b):
    mo.stop(file_df_a is None or file_df_b is None)
    _equal = file_df_a.reset_index(drop=True).equals(file_df_b.reset_index(drop=True))
    mo.callout(
        mo.md(
            "**File equal:** ✅ Yes — contents are identical."
            if _equal
            else "**File equal:** ❌ No — contents differ."
        ),
        kind="success" if _equal else "danger",
    )
    return


@app.cell(hide_code=True)
def _(file_df_a, file_df_b):
    mo.stop(
        file_df_a is None or file_df_b is None,
    )
    row_focus = mo.ui.number(
        label="Jump to row",
        value=-1,
        step=1,
        start=-1,
        stop=max(len(file_df_a), len(file_df_b)) - 1,
    )
    context_rows = mo.ui.number(
        label="Context rows (\u00b1)",
        value=0,
        step=1,
        start=0,
        stop=50,
    )
    show_diff_rows = mo.ui.checkbox(label="Show diff rows only")
    mo.hstack(
        [
            row_focus,
            context_rows,
            show_diff_rows,
            mo.md("_Set \u2018Jump to row\u2019 to \u22121 to show all rows._"),
        ],
        gap=2,
        align="center",
    )
    return context_rows, row_focus, show_diff_rows


@app.cell(hide_code=True)
def _(file_df_a, file_df_b):
    mo.stop(file_df_a is None or file_df_b is None)

    _di_shared = [c for c in file_df_a.columns if c in file_df_b.columns]
    _di_a = file_df_a[_di_shared].reset_index(drop=True)
    _di_b = file_df_b[_di_shared].reset_index(drop=True)
    _di_min = min(len(_di_a), len(_di_b))
    diff_row_indices: set[int] = set()
    for _di_col in _di_shared:
        _ca = _di_a.iloc[:_di_min][_di_col]
        _cb = _di_b.iloc[:_di_min][_di_col]
        _ch = ~((_ca == _cb) | (_ca.isna() & _cb.isna()))
        diff_row_indices.update(int(i) for i in _ch[_ch].index)
    return (diff_row_indices,)


@app.cell(hide_code=True)
def _(
    context_rows,
    diff_row_indices: set[int],
    file_df_a,
    file_df_b,
    file_selector,
    row_focus,
    show_diff_rows,
):
    mo.stop(
        file_df_a is None or file_df_b is None,
        mo.md("_Select a file above to load and compare._"),
    )
    _cols_only_a = sorted(set(file_df_a.columns) - set(file_df_b.columns))
    _cols_only_b = sorted(set(file_df_b.columns) - set(file_df_a.columns))
    _col_status = (
        "Column sets match."
        if not _cols_only_a and not _cols_only_b
        else (
            ("Only in A: " + ", ".join(_cols_only_a) if _cols_only_a else "")
            + (
                "  \u00b7  Only in B: " + ", ".join(_cols_only_b)
                if _cols_only_b
                else ""
            )
        )
    )

    def _with_row_num(df):
        out = df.reset_index(drop=True)
        out.insert(0, "row", range(len(out)))
        return out

    def _maybe_focus(df_with_row):
        r = row_focus.value
        ctx_n = context_rows.value
        if show_diff_rows.value and diff_row_indices:
            rows_to_show: set[int] = set()
            max_row = len(df_with_row) - 1
            for dr in diff_row_indices:
                lo = max(0, dr - ctx_n)
                hi = min(max_row, dr + ctx_n)
                rows_to_show.update(range(lo, hi + 1))
            return df_with_row[df_with_row["row"].isin(rows_to_show)].reset_index(
                drop=True
            )
        if r < 0:
            return df_with_row
        lo = max(0, r - ctx_n)
        hi = min(len(df_with_row) - 1, r + ctx_n)
        return df_with_row.iloc[lo : hi + 1].reset_index(drop=True)

    _summary = (
        f"| | A | B |\n"
        f"|---|---|---|\n"
        f"| Rows | {len(file_df_a):,} | {len(file_df_b):,} |\n"
        f"| Columns | {len(file_df_a.columns)} | {len(file_df_b.columns)} |\n"
        f"\n**Columns:** {_col_status}"
    )

    mo.vstack(
        [
            mo.md(_summary),
            mo.vstack(
                [
                    mo.md(f"**A \u2014 {file_selector.value}**"),
                    mo.ui.table(
                        _maybe_focus(_with_row_num(file_df_a)),
                        selection=None,
                    ),
                ]
            ),
            mo.vstack(
                [
                    mo.md(f"**B \u2014 {file_selector.value}**"),
                    mo.ui.table(
                        _maybe_focus(_with_row_num(file_df_b)),
                        selection=None,
                    ),
                ]
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(file_df_a, file_df_b):
    mo.stop(
        file_df_a is None or file_df_b is None,
        mo.md("_Select a file above to load and compare._"),
    )

    _shared_cols = [c for c in file_df_a.columns if c in file_df_b.columns]
    _a = file_df_a[_shared_cols].reset_index(drop=True)
    _b = file_df_b[_shared_cols].reset_index(drop=True)
    _min_len = min(len(_a), len(_b))
    _a_cmp = _a.iloc[:_min_len]
    _b_cmp = _b.iloc[:_min_len]

    _records = []
    for col in _shared_cols:
        _col_a = _a_cmp[col]
        _col_b = _b_cmp[col]
        _changed = ~((_col_a == _col_b) | (_col_a.isna() & _col_b.isna()))
        for idx in _changed[_changed].index:
            _records.append(
                {
                    "row": int(idx),
                    "column": col,
                    "value_a": _col_a.at[idx],
                    "value_b": _col_b.at[idx],
                }
            )

    _row_count_note = (
        f"\n\n> \u26a0\ufe0f Row counts differ (A: {len(_a):,}, B: {len(_b):,}) \u2014 only first {_min_len:,} rows compared."
        if len(_a) != len(_b)
        else ""
    )

    if not _records and len(_a) == len(_b):
        _diff_out = mo.md("_No differences found in shared columns._")
    elif not _records:
        _diff_out = mo.md(
            f"_Shared columns match across first {_min_len:,} rows.{_row_count_note}_"
        )
    else:
        _n_diff_rows = len({r["row"] for r in _records})
        _n_diff_cols = len({r["column"] for r in _records})
        _diff_out = mo.vstack(
            [
                mo.md(
                    f"**{_n_diff_rows:,} row(s)** with differences across **{_n_diff_cols:,}** column(s).{_row_count_note}"
                ),
                mo.ui.table(pd.DataFrame(_records), selection=None),
            ]
        )
    _diff_out
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
    from openlayers.basemaps import Carto, CartoBasemapLayer

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
def _(bucket_input, geo_selector, path_a, path_b):
    import os
    import tempfile

    import geopandas as gpd

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
            _load_shapefile(path_a, geo_selector.value)
            .to_crs(4326)
            .reset_index(drop=True)
        )
        geo_gdf_b = (
            _load_shapefile(path_b, geo_selector.value)
            .to_crs(4326)
            .reset_index(drop=True)
        )
    return geo_gdf_a, geo_gdf_b


@app.cell(hide_code=True)
def _(geo_gdf_a, geo_gdf_b):
    _geom_col = geo_gdf_a.geometry.name
    _attr_cols = [c for c in geo_gdf_a.columns if c != _geom_col]
    _attrs_equal = (
        geo_gdf_a[_attr_cols]
        .reset_index(drop=True)
        .equals(geo_gdf_b[_attr_cols].reset_index(drop=True))
    )
    _geom_equal = geo_gdf_a.geometry.reset_index(drop=True).equals(
        geo_gdf_b.geometry.reset_index(drop=True)
    )
    _equal = _attrs_equal and _geom_equal
    mo.callout(
        mo.md(
            "**GeoDataFrame equal:** ✅ Yes — features and geometries match."
            if _equal
            else "**GeoDataFrame equal:** ❌ No — "
            + ("" if _attrs_equal else "attributes differ")
            + (" and " if not _attrs_equal and not _geom_equal else "")
            + ("" if _geom_equal else "geometries differ")
            + "."
        ),
        kind="success" if _equal else "danger",
    )
    return


@app.cell(hide_code=True)
def _(geo_gdf_a, geo_selector, path_a):
    _geom_col = geo_gdf_a.geometry.name
    _attr_cols = [c for c in geo_gdf_a.columns if c != _geom_col]
    geo_table_a = mo.ui.table(
        geo_gdf_a[_attr_cols],
        selection="multi",
        page_size=20,
        label=f"A — {path_a}{geo_selector.value} ({len(geo_gdf_a):,} features) · check rows to highlight on map",
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
def _(geo_gdf_b, geo_selector, path_b):
    _geom_col = geo_gdf_b.geometry.name
    _attr_cols = [c for c in geo_gdf_b.columns if c != _geom_col]
    geo_table_b = mo.ui.table(
        geo_gdf_b[_attr_cols],
        selection="multi",
        page_size=20,
        label=f"B — {path_b}{geo_selector.value} ({len(geo_gdf_b):,} features) · check rows to highlight on map",
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
