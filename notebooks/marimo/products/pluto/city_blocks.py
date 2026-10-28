import marimo

__generated_with = "0.25.1"
app = marimo.App(width="full")

with app.setup:
    import folium
    import geopandas as gpd
    import lonboard
    import marimo as mo
    import matplotlib
    import numpy as np
    import pandas as pd
    import xyzservices.providers as xyz

    from dcpy.configuration import PUBLISHING_BUCKET
    from dcpy.connectors.edm import publishing
    from dcpy.utils import s3

    PRODUCT = "db-pluto"
    CITY_BLOCKS = "city_blocks.parquet"
    SUB_BLOCKS = "tax_sub_blocks.parquet"


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # PLUTO city blocks

    Reads `city_blocks.parquet` and `tax_sub_blocks.parquet` from a PLUTO build in
    edm-publishing, to look at the unusual cases:

    - city blocks made of more than one tax block
    - city blocks that span boroughs
    - lots over 90% water, which are their own rows (`is_mostly_water`)
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Open threads

    City blocks should match the blocks people know: land divided by streets.

    - Stop relying on the shoreline clip. The 90% water rule
      (`int_blocks__lot_water_share`) still uses it.
    - Coastal lots that are mostly land still join neighbors in the water
      (Fort Wadsworth, Staten Island).
    - Lots that cross streets (parks, rail yards) join blocks across them. Try
      checking for lots split across 2020 census blocks (`dcp_cb2020_wi`, already
      in PLUTO's recipe).
    - Add `city_block_id` to lots, so PLUTO joins to city blocks without unnesting
      `bbls` (see the list column rule in `docs/data-modeling.md`).
    - Define tax block, tax sub-block, and city block in a glossary.
    - Rebuild the branch so `city_blocks.parquet` comes from CI. The current file
      was uploaded from a local dbt run.
    - Check whether the Custom QAQC failures in the branch build also fail on `main`.
    - Decide whether `apps/notebook-server` needs lonboard to run this notebook.
    - Maybe move the block exports from `06_export.sh` to `recipe.yml`.
    """)
    return


@app.cell
def _():
    builds = publishing.get_builds(PRODUCT)
    build = mo.ui.dropdown(
        builds,
        value="dm-pluto-blocks" if "dm-pluto-blocks" in builds else None,
        label="Build",
        searchable=True,
    )
    build
    return (build,)


@app.cell
def _(build):
    mo.stop(not build.value)
    key = publishing.BuildKey(PRODUCT, build.value)
    mo.stop(
        not publishing.file_exists(key, CITY_BLOCKS),
        mo.callout(f"{key} has no `{CITY_BLOCKS}`.", kind="warn"),
    )
    key
    return (key,)


@app.cell
def _(key):
    # keyed on each file's upload time, so a re-uploaded file is read again instead of
    # served from cache
    @mo.cache
    def read_parquet(
        key_path: str, filename: str, uploaded_at: str
    ) -> gpd.GeoDataFrame:
        return gpd.read_parquet(publishing.get_file(key, filename)).rename_geometry(
            "geometry"
        )

    def uploaded_at(filename: str) -> str:
        return str(
            s3.get_metadata(PUBLISHING_BUCKET, f"{key.path}/{filename}").last_modified
        )

    with mo.status.spinner(title="Reading parquet files…"):
        city_blocks = read_parquet(key.path, CITY_BLOCKS, uploaded_at(CITY_BLOCKS))
        sub_blocks = read_parquet(key.path, SUB_BLOCKS, uploaded_at(SUB_BLOCKS))
    return city_blocks, sub_blocks


@app.cell
def _(city_blocks):
    # the first digit of a BBL is its borough
    boroughs = city_blocks["bbls"].map(lambda bbls: sorted({b[0] for b in bbls}))
    blocks = city_blocks.assign(
        lot_count=city_blocks["bbls"].map(len),
        borough_codes=boroughs.map(",".join),
        spans_boroughs=boroughs.map(len) > 1,
    )
    cases = {
        "multiple tax blocks": blocks["has_multiple_tax_blocks"],
        "spans boroughs": blocks["spans_boroughs"],
        "mostly water": blocks["is_mostly_water"],
    }
    mo.ui.table(
        pd.DataFrame(
            {
                "case": list(cases),
                "rows": [int(mask.sum()) for mask in cases.values()],
            }
        ),
        selection=None,
        show_column_summaries=False,
        label=f"{len(blocks):,} rows in {CITY_BLOCKS}",
    )
    return blocks, cases


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## All city blocks

    City blocks with more than one tax block are darker gray, and lots over 90% water are blue.
    Switch on the tax sub-blocks to see what each city block is made of.
    Click a city block to see its details.
    """)
    return


@app.cell
def _():
    show_sub_blocks = mo.ui.switch(label="Show tax sub-blocks")
    show_sub_blocks
    return (show_sub_blocks,)


@app.cell
def _(blocks, sub_blocks):
    LIGHT_GRAY, DARK_GRAY, BLUE = (
        [225, 225, 225],
        [170, 170, 170],
        [80, 150, 220],
    )
    # 1 ft keeps the shapes at street zoom and cuts the vertices to about a quarter
    _city = (
        blocks.drop(columns=["bbls", "borough_codes"])
        .assign(geometry=blocks.geometry.simplify(1, preserve_topology=True))
        .to_crs(4326)
    )
    city_fill = lonboard.PolygonLayer.from_geopandas(
        _city,
        get_fill_color=np.select(
            [
                _city["is_mostly_water"].to_numpy()[:, None],
                _city["has_multiple_tax_blocks"].to_numpy()[:, None],
            ],
            [BLUE, DARK_GRAY],
            LIGHT_GRAY,
        ).astype("uint8"),
        stroked=False,
        opacity=0.35,
    )
    city_outline = lonboard.PolygonLayer.from_geopandas(
        _city,
        filled=False,
        get_line_color=[0, 0, 0],
        line_width_min_pixels=1,
        pickable=False,
    )

    _subs = (
        sub_blocks.drop(columns="bbls")
        .assign(geometry=sub_blocks.geometry.simplify(1, preserve_topology=True))
        .to_crs(4326)
    )
    # tab20 by sub-block, so neighbors in the same city block are usually different colors
    _codes = pd.factorize(_subs["tax_sub_block_id"])[0] % 20
    sub_layer = lonboard.PolygonLayer.from_geopandas(
        _subs,
        get_fill_color=(
            np.array(matplotlib.colormaps["tab20"].colors)[_codes] * 255
        ).astype("uint8"),
        get_line_color=[90, 90, 90],
        line_width_min_pixels=0.5,
        opacity=0.7,
        pickable=False,
        visible=False,
    )
    # clicks pick from the fill layer, since an unfilled layer only picks its edges
    all_blocks_map = lonboard.Map([city_fill, sub_layer, city_outline], height=600)
    all_blocks_map
    return (sub_layer,)


@app.cell
def _(show_sub_blocks, sub_layer):
    # flips the layer in place, so the map isn't rebuilt on every toggle
    sub_layer.visible = show_sub_blocks.value
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Cases
    """)
    return


@app.cell
def _(cases):
    case = mo.ui.radio(list(cases), value="multiple tax blocks", inline=True)
    case
    return (case,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### All rows in this case

    Simplified to about 5 m so the map can render. Colored by number of tax blocks.
    """)
    return


@app.cell
def _(case_blocks):
    case_map = (
        case_blocks.drop(columns="bbls")
        .assign(geometry=case_blocks.geometry.simplify(15, preserve_topology=True))
        .explore(column="tax_block_count", cmap="viridis", tooltip=True, tiles=None)
    )
    mo.iframe(with_basemaps(case_map).get_root().render(), height="570px")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### One city block and its sub-blocks

    The city block outline in black, over the tax sub-blocks that have lots in it.
    """)
    return


@app.cell
def _(blocks, case, cases):
    case_blocks = blocks[cases[case.value]].sort_values(
        ["tax_block_count", "lot_count"], ascending=False
    )
    case_table = mo.ui.table(
        pd.DataFrame(case_blocks.drop(columns=["geometry", "bbls"])),
        selection="single",
        page_size=10,
        show_column_summaries=False,
        label="Pick a row to map it below",
    )
    case_table
    return case_blocks, case_table


@app.cell
def _(case_blocks, case_table, sub_blocks):
    mo.stop(case_blocks.empty, mo.md("No rows in this case."))
    picked = case_table.value
    city_block_id = (
        picked["city_block_id"].iloc[0]
        if len(picked)
        else case_blocks["city_block_id"].iloc[0]
    )
    city_block = case_blocks[case_blocks["city_block_id"] == city_block_id]
    bbls = set(city_block["bbls"].iloc[0])
    members = sub_blocks[sub_blocks["bbls"].map(lambda b: not bbls.isdisjoint(b))]

    block_map = (
        members.drop(columns="bbls")
        .assign(lot_count=members["bbls"].map(len))
        .explore(column="tax_sub_block_id", tooltip=True, tiles=None, legend=False)
    )
    city_block.drop(columns="bbls").explore(
        m=block_map,
        style_kwds={"fill": False, "color": "black", "weight": 3},
        tooltip=True,
    )
    mo.vstack(
        [
            mo.md(
                f"**{city_block_id}**: {len(bbls):,} lots in "
                f"{len(members):,} tax sub-blocks"
            ),
            mo.iframe(with_basemaps(block_map).get_root().render(), height="570px"),
        ]
    )
    return


@app.function
def with_basemaps(m: folium.Map) -> folium.Map:
    # added here rather than through explore's tiles, which would cap every basemap at
    # the first one's max zoom. past a provider's own max zoom, its tiles are stretched
    for name, provider in {
        "light gray": "Esri.WorldGrayCanvas",
        "OpenStreetMap": "OpenStreetMap.Mapnik",
        "satellite": "Esri.WorldImagery",
    }.items():
        folium.TileLayer(
            provider,
            name=name,
            show=name == "light gray",
            max_zoom=22,
            max_native_zoom=xyz.query_name(provider).get("max_zoom", 18),
        ).add_to(m)
    folium.LayerControl().add_to(m)
    return m


if __name__ == "__main__":
    app.run()
