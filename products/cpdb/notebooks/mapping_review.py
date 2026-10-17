import marimo

__generated_with = "0.23.3"
app = marimo.App(width="full")

with app.setup:
    import tempfile
    from pathlib import Path

    import geopandas as gpd
    import marimo as mo
    import pandas as pd

    from dcpy.connectors.edm import publishing

    PRODUCT = "db-cpdb"
    GPKG = "cpdb_mapping_review.gpkg"


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # CPDB mapping review

    How a set of CPDB files mapped its projects: which geometry source won for each project,
    what was removed, and where projects land by borough and community district. Reads
    `cpdb_mapping_review.gpkg` from a build, draft, or published version in edm-publishing.
    """)
    return


@app.cell
def _():
    stage = mo.ui.dropdown(["build", "draft", "publish"], value="build", label="Stage")
    stage
    return (stage,)


@app.cell
def _(stage):
    versions = {
        "build": publishing.get_builds(PRODUCT),
        "draft": publishing.get_draft_versions(PRODUCT),
        "publish": publishing.get_published_versions(PRODUCT, exclude_latest=False),
    }[stage.value]
    version = mo.ui.dropdown(
        versions,
        value="dm-cpdb-mapping" if "dm-cpdb-mapping" in versions else None,
        label="Build or version",
        searchable=True,
    )
    version
    return (version,)


@app.cell
def _(stage, version):
    revision = mo.ui.dropdown(
        publishing.get_draft_version_revisions(PRODUCT, version.value)
        if stage.value == "draft" and version.value
        else [],
        label="Draft revision",
    )
    revision
    return (revision,)


@app.cell
def _(revision, stage, version):
    mo.stop(not version.value or (stage.value == "draft" and not revision.value))
    key = {
        "build": lambda: publishing.BuildKey(PRODUCT, version.value),
        "draft": lambda: publishing.DraftKey(PRODUCT, version.value, revision.value),
        "publish": lambda: publishing.PublishKey(PRODUCT, version.value),
    }[stage.value]()
    mo.stop(
        not publishing.file_exists(key, GPKG),
        mo.callout(
            f"{key} has no `{GPKG}`. It's only exported by builds from the `dm-cpdb-mapping` branch or later.",
            kind="warn",
        ),
    )
    key
    return (key,)


@app.cell
def _(key):
    @mo.cache
    def read_layers(key_path: str) -> dict[str, pd.DataFrame]:
        # GDAL wants the .gpkg extension, so read from a file instead of bytes
        path = Path(tempfile.gettempdir()) / f"{key_path.replace('/', '_')}.gpkg"
        path.write_bytes(publishing.get_file(key, GPKG).read())
        return {
            layer: gpd.read_file(path, layer=layer)
            for layer in gpd.list_layers(path)["name"]
        }

    with mo.status.spinner(title=f"Reading {GPKG}…"):
        layers = read_layers(key.path)
    return (layers,)


@app.cell(hide_code=True)
def _(layers):
    total = (
        layers["cpdb_mapping_summary_by_source"]
        .set_index("geometry_source")
        .loc["total"]
    )
    mo.md(f"""
    ## Summary

    | projects | geometry found | removed | mapped | points | polygons | no geometry found |
    |---:|---:|---:|---:|---:|---:|---:|
    | {total.projects_total:,} | {total.projects_geometry_found:,} | {total.projects_geometry_removed:,} | {total.projects_mapped:,} | {total.projects_mapped_as_points:,} | {total.projects_mapped_as_polygons:,} | {total.projects_no_geometry_found:,} |

    Planned commitments on mapped projects: {total.planned_commitments_mapped / total.planned_commitments_total:.0%}
    """)
    return


@app.cell
def _(layers):
    mo.ui.table(
        pd.DataFrame(layers["cpdb_mapping_summary_by_source"]),
        label="By geometry source",
        page_size=25,
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## By managing agency

    Sorted by planned commitments on unmapped projects, the biggest gaps first.
    """)
    return


@app.cell
def _(layers):
    outcomes = pd.DataFrame(layers["int__project_mapping_outcomes"]).assign(
        mapped=lambda df: df["mapped_as"].notna(),
        plannedcommit_not_mapped=lambda df: df["plannedcommit_total"].where(
            ~df["mapped"], 0
        ),
    )
    by_agency = (
        outcomes.groupby("magencyacro")
        .agg(
            projects=("maprojid", "count"),
            projects_mapped=("mapped", "sum"),
            planned_commitments=("plannedcommit_total", "sum"),
            planned_commitments_not_mapped=("plannedcommit_not_mapped", "sum"),
        )
        .assign(
            share_mapped=lambda df: (df["projects_mapped"] / df["projects"]).round(2)
        )
        .sort_values("planned_commitments_not_mapped", ascending=False)
        .reset_index()
    )
    mo.ui.table(by_agency, page_size=15)
    return (outcomes,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Borough and community district

    Where each value comes from: the project's geometry, else the Capital Projects Dashboard.
    """)
    return


@app.cell
def _(layers):
    boundaries = pd.DataFrame(layers["int__project_boundaries"]).fillna(
        {"borough_source": "none", "community_district_source": "none"}
    )
    mo.hstack(
        [
            pd.crosstab(
                boundaries["borough_source"],
                boundaries["borough"].where(
                    boundaries["borough"] == "multiple", "one borough"
                ),
                margins=True,
            ),
            pd.crosstab(
                boundaries["community_district_source"],
                boundaries["community_district"].where(
                    boundaries["community_district"] == "multiple", "one district"
                ),
                margins=True,
            ),
            pd.crosstab(
                boundaries["dashboard_citywide"],
                boundaries["borough_source"],
                margins=True,
            ),
        ],
        justify="start",
        gap=2,
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Map
    """)
    return


@app.cell
def _(layers):
    sources = sorted(
        layers["int__project_mapping_outcomes"]["geometry_source"].dropna().unique()
    )
    source_filter = mo.ui.multiselect(
        sources, label="Geometry sources (all when empty)"
    )
    source_filter
    return (source_filter,)


@app.cell
def _(layers, outcomes, source_filter):
    mapped = (
        layers["cpdb_dcpattributes"][["maprojid", "description", "geometry"]]
        .dropna(subset=["geometry"])
        .merge(
            outcomes[["maprojid", "magencyacro", "geometry_method", "geometry_source"]],
            on="maprojid",
        )
    )
    shown = (
        mapped[mapped["geometry_source"].isin(source_filter.value)]
        if source_filter.value
        else mapped
    )
    shown.explore(column="geometry_method", tooltip=True)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## One project's candidates

    Every geometry found for a project, ranked. Priority 1 is the one CPDB uses.
    """)
    return


@app.cell
def _():
    maprojid = mo.ui.text(label="maprojid")
    maprojid
    return (maprojid,)


@app.cell
def _(layers, maprojid):
    candidates = layers["int__project_geom_candidates"].query(
        "maprojid == @maprojid.value"
    )
    mo.vstack(
        [
            mo.ui.table(
                pd.DataFrame(candidates.drop(columns="geometry")), selection=None
            ),
            candidates.explore(column="candidate")
            if len(candidates)
            else mo.md("No candidates."),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
