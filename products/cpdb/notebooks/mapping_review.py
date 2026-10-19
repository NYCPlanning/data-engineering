import marimo

__generated_with = "0.25.1"
app = marimo.App(width="full")

with app.setup:
    import tempfile
    from pathlib import Path

    import folium
    import geopandas as gpd
    import marimo as mo
    import pandas as pd
    import plotly.graph_objects as go

    from dcpy.connectors.edm import publishing

    PRODUCT = "db-cpdb"
    GPKG = "cpdb_mapping_review.gpkg"
    SHARED_GPKG = "cpdb_projects_boundaries.gpkg"


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # CPDB mapping review

    How a set of CPDB files mapped its projects. Reads `cpdb_mapping_review.gpkg` from a build,
    draft, or published version in edm-publishing, and answers:

    - How many projects are mapped, and from which sources?
    - Which sources can map which projects?
    - What do we know about where unmapped projects are?
    - Where could agencies help map more?

    The [CPDB wiki page](https://github.com/NYCPlanning/data-engineering/wiki/Product:-CPDB)
    has a diagram of the mapping logic.
    """)
    return


@app.cell
def _():
    # select an output to point a connected notebook agent at it
    from marimo_lens import Lens

    lens = Lens()
    lens
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Version
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
    # keyed on the build time too, so a rebuilt build is read again instead of served from cache
    @mo.cache
    def read_layers(
        key_path: str, filename: str, built_at: str
    ) -> dict[str, pd.DataFrame]:
        # GDAL wants the .gpkg extension, so read from a file instead of bytes
        path = Path(tempfile.gettempdir()) / f"{key_path.replace('/', '_')}_{filename}"
        path.write_bytes(publishing.get_file(key, filename).read())
        return {
            layer: gpd.read_file(path, layer=layer)
            for layer in gpd.list_layers(path)["name"]
        }

    with mo.status.spinner(title=f"Reading {GPKG}…"):
        layers = read_layers(
            key.path, GPKG, str(publishing.get_build_metadata(key).timestamp)
        )
    return layers, read_layers


@app.cell
def _(layers):
    # one row per project with everything the sections below group by
    boundaries = pd.DataFrame(layers["int__project_boundaries"]).fillna(
        {"borough_source": "none", "community_district_source": "none"}
    )
    # files exported before OMB's climate data was added have no asset category layer
    asset_categories = pd.DataFrame(
        layers.get("int__project_asset_categories"),
        columns=[
            "maprojid",
            "asset_category",
            "heat_vulnerability_index",
            "flood_vulnerability_index",
        ],
    )
    projects = (
        pd.DataFrame(layers["int__project_mapping_outcomes"])
        .merge(boundaries, on="maprojid")
        .merge(
            pd.DataFrame(layers["cpdb_dcpattributes"])[
                ["maprojid", "description", "typecategory"]
            ],
            on="maprojid",
        )
        .merge(asset_categories, on="maprojid", how="left")
        .assign(
            mapped=lambda df: df["mapped_as"].notna(),
            typecategory=lambda df: df["typecategory"].fillna("unknown"),
            asset_category=lambda df: df["asset_category"].fillna(
                "not in climate data"
            ),
            omb=lambda df: (
                df["asset_category"]
                .eq("not in climate data")
                .map({True: "not in data", False: "in data, not flagged"})
                .mask(
                    df["heat_vulnerability_index"].eq(True)
                    | df["flood_vulnerability_index"].eq(True),
                    "flagged high heat or flood",
                )
            ),
            location_evidence=lambda df: df.apply(location_evidence, axis=1),
        )
    )
    return boundaries, projects


@app.function
def location_evidence(project: pd.Series) -> str:
    """The best evidence of where a project is, strongest first."""
    if project["mapped"]:
        return "mapped"
    if pd.notna(project["geom_removed_reason"]):
        return "geometry found but removed"
    if project["community_district_source"] == "dashboard":
        return "dashboard: community district"
    if project["borough_source"] == "dashboard":
        return "dashboard: borough"
    if project["dashboard_citywide"]:
        return "dashboard: citywide"
    return "no location evidence"


@app.cell
def _(layers):
    total = (
        layers["cpdb_mapping_summary_by_source"]
        .set_index("geometry_source")
        .loc["total"]
    )
    mo.vstack(
        [
            mo.md("## Summary"),
            mo.hstack(
                [
                    mo.stat(
                        f"{total.projects_total:,}",
                        label="projects",
                        # a blank caption keeps this card as tall as the others
                        caption="\u00a0",
                        bordered=True,
                    ),
                    mo.stat(
                        f"{total.projects_mapped:,}",
                        label="projects mapped",
                        caption=f"{total.projects_mapped / total.projects_total:.0%} of projects ({total.projects_mapped_as_points:,} points, {total.projects_mapped_as_polygons:,} polygons)",
                        bordered=True,
                    ),
                    mo.stat(
                        f"{total.projects_no_geometry_found:,}",
                        label="projects unmapped",
                        caption=f"{total.projects_no_geometry_found / total.projects_total:.0%} of projects",
                        bordered=True,
                    ),
                ],
                wrap=True,
                justify="center",
            ),
            mo.hstack(
                [
                    mo.stat(
                        f"{total.projects_geometry_found:,}",
                        label="geometries found",
                        caption=f"{total.projects_geometry_found / total.projects_total:.0%} of projects",
                        bordered=True,
                    ),
                    mo.stat(
                        f"{total.projects_geometry_removed:,}",
                        label="geometries removed",
                        caption="by DCP or project agency",
                        bordered=True,
                    ),
                    mo.stat(
                        f"{total.planned_commitments_mapped / total.planned_commitments_total:.0%}",
                        label="commitment dollars mapped",
                        caption=f"{total.planned_commitment_records_mapped / total.planned_commitment_records_total:.0%} of records",
                        bordered=True,
                    ),
                ],
                wrap=True,
                justify="center",
            ),
        ]
    )
    return (total,)


@app.cell(hide_code=True)
def _(projects, total):
    evidence = projects.loc[~projects["mapped"], "location_evidence"].value_counts()
    dashboard_located = evidence.reindex(
        ["dashboard: community district", "dashboard: borough"], fill_value=0
    ).sum()
    no_evidence = projects[projects["location_evidence"] == "no location evidence"]
    no_evidence_categories = no_evidence["asset_category"].value_counts()
    equipment_it_vehicles = no_evidence_categories.reindex(
        ["Equipment", "IT & Technology", "Vehicles"], fill_value=0
    ).sum()
    unmapped_heat_or_flood = (
        ~projects["mapped"]
        & (
            projects["heat_vulnerability_index"].eq(True)
            | projects["flood_vulnerability_index"].eq(True)
        )
    ).sum()
    mo.md(f"""
    **Which projects did CPDB try to map?** All of them.

    - Every project is checked against every geometry source, and none are screened out first.
    - A source had a geometry for {total.projects_geometry_found:,} projects.
    - {total.projects_geometry_removed:,} of those geometries were removed:
        - {total.projects_geometry_removed_flagged_bad:,} flagged as bad by DCP
        - {total.projects_geometry_removed_agency_never_mappable:,} the agency said can never be mapped
        - {total.projects_geometry_removed_agency_mappable_in_future:,} the agency said can be mapped in future
    - That leaves {total.projects_mapped:,} mapped.

    **Are the unmapped projects citywide?** Mostly we don't know. Counting each of the
    {evidence.sum():,} unmapped projects once, by its strongest location evidence:

    - The Mayor's Office of Operations (MOO) [Capital Projects Dashboard data](https://data.cityofnewyork.us/City-Government/Capital-Projects-Dashboard-Citywide-Budget-and-Sch/fb86-vt7u/about_data) puts {evidence.get("dashboard: community district", 0):,} in a
      community district and {evidence.get("dashboard: borough", 0):,} in a borough.
    - The dashboard calls {evidence.get("dashboard: citywide", 0):,} citywide.
    - {len(no_evidence):,} have no location evidence: no geometry, and no borough, community
      district, or citywide flag from the dashboard. By OMB's asset category:
        - {equipment_it_vehicles:,} are equipment, IT, or vehicles
        - {no_evidence_categories.get("Buildings & Facilities", 0):,} are buildings and facilities
        - {no_evidence_categories.get("not in climate data", 0):,} aren't in OMB's
          [Climate Alignment Assessment data](https://data.cityofnewyork.us/City-Government/NYC-Climate-Budgeting-Report-Climate-Alignment-Ass/c99a-c5ux/about_data)

    **How many couldn't be mapped, and would more information from agencies help?**
    {evidence.sum():,} couldn't be mapped:

    - {total.projects_no_geometry_found:,} had no geometry from any source.
    - {total.projects_geometry_removed:,} had theirs removed.

    Agencies could help most with the projects that show some sign of a place:

    - the {dashboard_located:,} the dashboard puts in a borough or community district
    - the {total.projects_geometry_removed_agency_mappable_in_future:,} the agency said can be mapped in future
    - the {no_evidence_categories.get("Buildings & Facilities", 0):,} buildings and facilities with no location evidence

    Separately, OMB's climate data flags {unmapped_heat_or_flood:,} unmapped projects as in a high
    heat area or high flood zone, so OMB has some location for them. OMB doesn't flag any unmapped
    project the dashboard only places in a borough or calls citywide, so a project without the flag
    isn't necessarily outside those areas. These overlap the groups above. The agency breakdown is under Unmapped
    projects, in "Where could agencies help map more?"
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### How projects move through mapping

    Every project, from the method that found its geometry (if any) to whether CPDB mapped it
    (DCP). Projects that weren't mapped then go through how OMB's climate budgeting data treats
    them, and the most specific location the Capital Projects Dashboard (MOO) gives them, the
    same groups as the "DCP, OMB, and the dashboard" table. OMB flags projects in a high heat
    area or high flood zone. Unlike the location evidence counts
    above, projects with a removed geometry go through OMB and MOO too.
    """)
    return


@app.cell
def _():
    flow_measure = mo.ui.radio(
        {"projects": "maprojid", "planned commitments": "plannedcommit_total"},
        value="projects",
        label="Count",
        inline=True,
    )
    flow_measure
    return (flow_measure,)


@app.cell
def _(flow_measure, projects):
    _value = flow_measure.value
    _agg = "count" if _value == "maprojid" else "sum"
    _methods = (
        projects.groupby("geometry_method")["geometry_source_priority"]
        .min()
        .sort_values()
        .index.tolist()
    )
    # stage titles and their nodes, top to bottom
    _stages = {
        "projects": ["all projects"],
        "DCP: geometry found by": [*_methods, "no geometry found"],
        "DCP: mapped": ["mapped as polygons", "mapped as points", "geometry removed"],
        "OMB: climate data": [
            "OMB: flagged",
            "OMB: no area flag",
            "OMB: not in data",
        ],
        "MOO: dashboard": [
            "MOO: community district",
            "MOO: borough",
            "MOO: citywide",
            "MOO: not located",
        ],
    }
    _no_location = {
        "no geometry found",
        "geometry removed",
        "OMB: no area flag",
        "OMB: not in data",
        "MOO: not located",
    }
    # shorter than the table's labels so they fit between stages
    _omb_labels = {
        "flagged high heat or flood": "OMB: flagged",
        "in data, not flagged": "OMB: no area flag",
        "not in data": "OMB: not in data",
    }
    _paths = projects.assign(
        found=lambda df: df["geometry_method"].fillna("no geometry found"),
        outcome=lambda df: (
            ("mapped as " + df["mapped_as"])
            .fillna("geometry removed")
            .where(df["geometry_method"].notna())
        ),
        omb_stage=lambda df: df["omb"].map(_omb_labels).where(~df["mapped"]),
        moo_stage=lambda df: (
            ("MOO: " + df["dashboard_location"])
            .fillna("MOO: not located")
            .where(~df["mapped"])
        ),
    )
    _unmapped = _paths[~_paths["mapped"]]
    _links = (
        pd.concat(
            [
                _paths.assign(source="all projects", target=_paths["found"]),
                _paths.dropna(subset=["outcome"]).assign(
                    source=lambda df: df["found"], target=lambda df: df["outcome"]
                ),
                _unmapped.assign(
                    source=lambda df: df["outcome"].fillna(df["found"]),
                    target=lambda df: df["omb_stage"],
                ),
                _unmapped.assign(
                    source=lambda df: df["omb_stage"],
                    target=lambda df: df["moo_stage"],
                ),
            ]
        )
        .groupby(["source", "target"])[_value]
        .agg(_agg)
        .reset_index()
    )
    _size = (
        pd.concat(
            [
                _links.groupby("source")[_value].sum(),
                _links.groupby("target")[_value].sum(),
            ]
        )
        .groupby(level=0)
        .max()
    )

    # plotly puts every node with no outgoing links in the last column, so place nodes by
    # stage instead
    _nodes, _x, _y, _titles = [], [], [], []
    _pad = 0.03
    for _i, (_title, _column) in enumerate(_stages.items()):
        _column = [n for n in _column if n in _size]
        _scale = (1 - _pad * (len(_column) - 1)) / _size["all projects"]
        _height = sum(_size[n] for n in _column) * _scale + _pad * (len(_column) - 1)
        # OMB sits left of even spacing so its labels clear MOO's
        _stage_x = [0.001, 0.25, 0.5, 0.71, 0.999][_i]
        _titles.append(
            {
                "x": _stage_x,
                "y": 1.06,
                "text": f"<b>{_title}</b>",
                "showarrow": False,
                "xanchor": "left"
                if _i == 0
                else "right"
                if _i == len(_stages) - 1
                else "center",
            }
        )
        # the OMB and MOO stages only hold unmapped projects, so they sit level with them
        _top = 1 - _height if _title.startswith(("OMB", "MOO")) else 0
        for _node in _column:
            _nodes.append(_node)
            _x.append(_stage_x)
            _y.append(max(_top + _size[_node] * _scale / 2, 0.001))
            _top += _size[_node] * _scale + _pad

    def _color(node: str, alpha: float) -> str:
        if node.startswith("mapped"):
            return f"rgba(46, 125, 50, {alpha})"
        if node in _no_location:
            return f"rgba(140, 140, 140, {alpha})"
        return f"rgba(74, 111, 165, {alpha})"

    def _format(value: float) -> str:
        return f"{value:,.0f}" if _agg == "count" else f"${value / 1e9:,.1f}B"

    # plotly stacks a node's links in the order they're given, so order them by where their
    # nodes sit to keep bands from crossing
    _links = _links.assign(
        source=lambda df: df["source"].map(_nodes.index),
        target=lambda df: df["target"].map(_nodes.index),
    ).sort_values(["source", "target"])

    go.Figure(
        go.Sankey(
            arrangement="fixed",
            node={
                "label": [f"{n} ({_format(_size[n])})" for n in _nodes],
                "x": _x,
                "y": _y,
                "pad": 15,
                "color": [_color(n, 1) for n in _nodes],
            },
            link={
                "source": _links["source"],
                "target": _links["target"],
                "value": _links[_value],
                "color": [_color(_nodes[t], 0.3) for t in _links["target"]],
            },
        ),
        layout={
            "height": 560,
            "margin": {"l": 10, "r": 10, "t": 40, "b": 10},
            "annotations": _titles,
        },
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Mapping by source
    """)
    return


@app.cell
def _(layers):
    mo.ui.table(
        pd.DataFrame(layers["cpdb_mapping_summary_by_source"]),
        label="By geometry source",
        selection=None,
        page_size=25,
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Which sources can map which projects?

    Every project is checked against every source, so there's no separate list of projects
    CPDB tried to map. Agency datasets and DCP's curated geometries match on the project ID.
    String matches only run for some agencies. A source can find a geometry for a project and
    still lose to a higher-priority source.
    """)
    return


@app.cell
def _(layers, projects):
    candidates_by_source = (
        pd.DataFrame(layers["int__project_geom_candidates"])
        .merge(
            projects[["maprojid", "magencyacro", "geometry_source"]],
            on="maprojid",
        )
        .assign(won=lambda df: df["candidate"] == df["geometry_source"])
        .groupby(["priority", "method", "candidate"])
        .agg(
            projects_with_a_geometry=("maprojid", "count"),
            projects_won=("won", "sum"),
            agencies=(
                "magencyacro",
                lambda s: ", ".join(s.value_counts().index[:8]),
            ),
            agency_count=("magencyacro", "nunique"),
        )
        .reset_index()
    )
    mo.ui.table(
        candidates_by_source,
        label="Agencies are listed by how many of their projects the source matched, up to 8",
        page_size=25,
        selection=None,
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Unmapped projects
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### What do we know about where unmapped projects are?

    Each project's strongest location evidence, by category. Not mapped doesn't mean citywide:
    the Capital Projects Dashboard puts many unmapped projects in a borough or community
    district.

    - **OMB asset category** is OMB's, from its climate budgeting data, for the project's budget
      lines with the most planned dollars. Projects missing from that data are `not in climate data`.
    - **Type category** comes from keywords in the project description. The CPDB wiki says it's
      for prioritizing manual mapping, not for reports.
    """)
    return


@app.cell
def _():
    measure = mo.ui.radio(
        {"projects": "maprojid", "planned commitments": "plannedcommit_total"},
        value="projects",
        label="Count",
        inline=True,
    )
    category = mo.ui.radio(
        {
            "OMB asset category": "asset_category",
            "type category": "typecategory",
        },
        value="OMB asset category",
        label="By",
        inline=True,
    )
    mo.hstack([category, measure], justify="start", gap=2)
    return category, measure


@app.cell
def _(category, measure, projects):
    location_evidence_table = (
        pd.pivot_table(
            projects,
            index=category.value,
            columns="location_evidence",
            values=measure.value,
            aggfunc="count" if measure.value == "maprojid" else "sum",
            fill_value=0,
            margins=True,
            margins_name="total",
        )
        .round(0)
        .assign(unmapped=lambda df: df["total"] - df["mapped"])
    )
    flagged_unmapped = projects[
        ~projects["mapped"]
        & (
            projects["heat_vulnerability_index"].eq(True)
            | projects["flood_vulnerability_index"].eq(True)
        )
    ].groupby(category.value)[measure.value]
    flagged_unmapped = (
        flagged_unmapped.count()
        if measure.value == "maprojid"
        else flagged_unmapped.sum()
    )
    location_evidence_table["unmapped, flagged by omb climate"] = (
        flagged_unmapped.reindex(location_evidence_table.index, fill_value=0)
    )
    location_evidence_table.loc["total", "unmapped, flagged by omb climate"] = (
        flagged_unmapped.sum()
    )
    # mapped, then unmapped and the evidence it breaks down into. Every version may not have
    # every kind of evidence, so missing ones are zero
    location_evidence_table = location_evidence_table.reindex(
        columns=[
            "mapped",
            "unmapped",
            "unmapped, flagged by omb climate",
            "dashboard: community district",
            "dashboard: borough",
            "dashboard: citywide",
            "geometry found but removed",
            "no location evidence",
            "total",
        ],
        fill_value=0,
    )
    if category.value == "asset_category":
        # subtotal of the OMB asset categories, placed before the projects OMB doesn't have
        in_climate_data = (
            location_evidence_table.loc["total"]
            - location_evidence_table.loc["not in climate data"]
        )
        location_evidence_table = pd.concat(
            [
                location_evidence_table.drop(index=["not in climate data", "total"]),
                in_climate_data.to_frame("in climate data").T,
                location_evidence_table.loc[["not in climate data", "total"]],
            ]
        ).rename_axis(category.value)
    mo.vstack(
        [
            mo.ui.table(
                location_evidence_table.astype(int).reset_index(),
                page_size=25,
                selection=None,
                show_column_summaries=False,
                show_data_types=False,
            ),
            mo.md(
                "`unmapped, flagged by omb climate` counts unmapped projects that OMB's climate "
                "data marks as in a high heat area or high flood zone. OMB doesn't flag any "
                "unmapped project the dashboard only places in a borough or calls citywide, so a "
                "project without the flag isn't necessarily outside those areas."
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### DCP, OMB, and the dashboard

    Each project by whether CPDB maps it (DCP), how OMB's climate budgeting data treats it, and the
    most specific location the Capital Projects Dashboard (Mayor's Office of Operations) gives it.
    The dashboard columns come from the dashboard alone, even for projects CPDB maps.
    """)
    return


@app.cell(hide_code=True)
def _(measure, projects):
    sources_table = pd.pivot_table(
        projects.assign(
            dcp=lambda df: df["mapped"].map({True: "mapped", False: "not mapped"}),
            dashboard=lambda df: df["dashboard_location"].fillna("not located"),
        ),
        index=["dcp", "omb"],
        columns="dashboard",
        values=measure.value,
        aggfunc="count" if measure.value == "maprojid" else "sum",
        fill_value=0,
        margins=True,
        margins_name="total",
    ).reindex(
        columns=["community district", "borough", "citywide", "not located", "total"],
        fill_value=0,
    )
    mo.ui.table(
        sources_table.round(0)
        .astype(int)
        .add_prefix("dashboard: ")
        .rename(columns={"dashboard: total": "total"})
        .reset_index(),
        page_size=25,
        selection=None,
        show_column_summaries=False,
        show_data_types=False,
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Where could agencies help map more?

    Unmapped projects by managing agency, with the most planned commitments on unmapped
    projects first. `unmapped_with_dashboard_location` already have a borough or community
    district in the Capital Projects Dashboard, so the agency knows where they are.
    `removed_agency_mappable_in_future` had a geometry that was removed because the agency
    said it could be mapped later.
    The `omb_*` columns count unmapped projects OMB's climate budgeting data flags as in a
    high heat area or high flood zone, so OMB has some idea where they are. A project without the
    flag may be outside those areas or not located by OMB at all.
    """)
    return


@app.cell
def _(projects):
    unmapped = projects[~projects["mapped"]]
    by_agency = (
        unmapped.assign(
            with_dashboard_location=lambda df: df["location_evidence"].isin(
                ["dashboard: community district", "dashboard: borough"]
            ),
            fixed_asset=lambda df: df["typecategory"] == "Fixed Asset",
            mappable_in_future=lambda df: (
                df["geom_removed_reason"] == "agency: can be mapped in future"
            ),
            # null for projects that aren't in OMB's climate data
            omb_heat=lambda df: df["heat_vulnerability_index"].eq(True),
            omb_flood=lambda df: df["flood_vulnerability_index"].eq(True),
            omb_heat_or_flood=lambda df: df["omb_heat"] | df["omb_flood"],
        )
        .groupby("magencyacro")
        .agg(
            unmapped_projects=("maprojid", "count"),
            unmapped_planned_commitments=("plannedcommit_total", "sum"),
            unmapped_omb_high_heat_area=("omb_heat", "sum"),
            unmapped_omb_high_flood_zone=("omb_flood", "sum"),
            unmapped_omb_heat_or_flood=("omb_heat_or_flood", "sum"),
            unmapped_fixed_assets=("fixed_asset", "sum"),
            removed_agency_mappable_in_future=("mappable_in_future", "sum"),
            unmapped_with_dashboard_location=(
                "with_dashboard_location",
                "sum",
            ),
        )
        .join(
            projects.groupby("magencyacro").agg(
                projects=("maprojid", "count"), mapped=("mapped", "sum")
            )
        )
        .assign(share_mapped=lambda df: (df["mapped"] / df["projects"]).round(2))
        .sort_values("unmapped_planned_commitments", ascending=False)
        .reset_index()
    )
    summed_columns = by_agency.columns.drop(["magencyacro", "share_mapped"])
    by_agency_total = (
        by_agency[summed_columns]
        .sum()
        .to_frame()
        .T.astype(by_agency[summed_columns].dtypes)
        .assign(
            magencyacro="total",
            share_mapped=lambda df: (df["mapped"] / df["projects"]).round(2),
        )
    )
    # one page so the total row is always visible at the bottom
    mo.ui.table(
        pd.concat([by_agency, by_agency_total], ignore_index=True),
        page_size=len(by_agency) + 1,
        selection=None,
        show_column_summaries=False,
        show_data_types=False,
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Geography
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Borough and community district

    Where each value comes from: the project's geometry, else the Capital Projects Dashboard.
    """)
    return


@app.cell
def _(boundaries):
    def _counts(source: str, value: str) -> pd.Series:
        """Projects by where the value came from and whether it's one or multiple."""
        one_or_multiple = (
            boundaries[value].eq("multiple").map({True: "multiple", False: "one"})
        )
        return (
            (boundaries[source] + ": " + one_or_multiple)
            .where(boundaries[source] != "none", "unknown")
            .value_counts()
        )

    boundary_counts = (
        pd.DataFrame(
            {
                "borough": _counts("borough_source", "borough"),
                "community district": _counts(
                    "community_district_source", "community_district"
                ),
            }
        )
        .T.reindex(
            columns=[
                "geometry: one",
                "geometry: multiple",
                "dashboard: one",
                "dashboard: multiple",
                "unknown",
            ]
        )
        .fillna(0)
        .astype(int)
        .assign(total=lambda df: df.sum(axis=1))
        .rename_axis("boundary")
    )
    citywide = boundaries[boundaries["dashboard_citywide"]]
    mo.vstack(
        [
            boundary_counts,
            mo.md(
                f"The dashboard calls {len(citywide):,} projects citywide. "
                f"{(citywide['borough_source'] == 'geometry').sum():,} of them have a borough "
                f"from their geometry and {(citywide['borough_source'] == 'none').sum():,} have none."
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(boundaries, key, read_layers, total):
    mo.stop(
        not publishing.file_exists(key, SHARED_GPKG),
        mo.md(f"This version has no `{SHARED_GPKG}`."),
    )
    shared_layers = read_layers(
        key.path, SHARED_GPKG, str(publishing.get_build_metadata(key).timestamp)
    )
    shared_projects = shared_layers["cpdb_projects_boundaries"]
    shared_checks = pd.DataFrame(
        [
            (
                "point projects",
                len(shared_layers["cpdb_projects_boundaries_pts"]),
                total.projects_mapped_as_points,
            ),
            (
                "polygon projects",
                len(shared_layers["cpdb_projects_boundaries_poly"]),
                total.projects_mapped_as_polygons,
            ),
            ("all projects", len(shared_projects), total.projects_total),
            (
                "all projects with a geometry",
                shared_projects.geometry.notna().sum(),
                total.projects_mapped,
            ),
            (
                "in more than one borough",
                shared_projects["multi_boro"].eq(True).sum(),
                boundaries["borough"].eq("multiple").sum(),
            ),
            (
                "in more than one community district",
                shared_projects["multi_cd"].eq(True).sum(),
                boundaries["community_district"].eq("multiple").sum(),
            ),
            (
                "dashboard calls citywide",
                shared_projects["citywide"].eq(True).sum(),
                boundaries["dashboard_citywide"].sum(),
            ),
        ],
        columns=["check", "shared file", "this notebook"],
    ).assign(matches=lambda df: df["shared file"] == df["this notebook"])
    mo.vstack(
        [
            mo.md(
                f"**Shared file.** `{SHARED_GPKG}` has the points and polygons shapefiles with "
                "borough and community district, and every project in one layer. It should "
                "agree with the numbers above before it goes to anyone."
            ),
            mo.ui.table(
                shared_checks,
                selection=None,
                show_column_summaries=False,
                show_data_types=False,
            ),
            mo.download(
                data=lambda: publishing.get_file(key, SHARED_GPKG).read(),
                filename=SHARED_GPKG,
                label=f"Download {SHARED_GPKG}",
            ),
        ]
    )
    return (shared_projects,)


@app.cell(hide_code=True)
def _():
    shared_rows = mo.ui.radio(
        ["unmapped", "mapped", "all"], value="unmapped", label="Projects", inline=True
    )
    shared_rows
    return (shared_rows,)


@app.cell(hide_code=True)
def _(shared_projects, shared_rows):
    shared_table = pd.DataFrame(shared_projects.drop(columns="geometry"))
    shared_table = {
        "unmapped": shared_table[shared_table["mapped_as"].isna()],
        "mapped": shared_table[shared_table["mapped_as"].notna()],
        "all": shared_table,
    }[shared_rows.value]
    # who and what first, then where, then the published budget columns
    first_columns = [
        "maprojid",
        "magenacro",
        "descript",
        "mapped_as",
        "borough",
        "multi_boro",
        "boros",
        "boro_src",
        "cd",
        "multi_cd",
        "cds",
        "cd_src",
        "citywide",
    ]
    mo.ui.table(
        shared_table[
            first_columns + [c for c in shared_table.columns if c not in first_columns]
        ],
        page_size=25,
        selection=None,
        freeze_columns_left=["maprojid"],
        show_column_summaries=False,
        show_data_types=False,
        label=f"{len(shared_table):,} projects in the all-projects layer of {SHARED_GPKG}",
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Map

    Polygons are simplified to about 5 m so the map can render. The GeoPackage has the full
    geometries.
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
def _(layers, projects, source_filter):
    mapped = (
        layers["cpdb_dcpattributes"][["maprojid", "description", "geometry"]]
        .dropna(subset=["geometry"])
        .merge(
            projects[
                [
                    "maprojid",
                    "magencyacro",
                    "geometry_method",
                    "geometry_source",
                    "borough",
                    "community_district",
                ]
            ],
            on="maprojid",
        )
    )
    shown = (
        mapped[mapped["geometry_source"].isin(source_filter.value)]
        if source_filter.value
        else mapped
    )
    # Leaflet's tooltips throw on multipoints, which stops the basemaps loading, so map single parts
    shown_parts = shown.explode(index_parts=False)
    # full geometries are too large for marimo to render, so simplify to about 5 m for display
    shown_map = shown_parts.assign(
        geometry=shown_parts.geometry.simplify(0.00005, preserve_topology=True)
    ).explore(column="geometry_method", tooltip=True, height=550, tiles=None)
    # basemaps that don't need an API key, picked from the layer control. Added here rather
    # than through explore's tiles, which would cap every basemap at the first one's max zoom
    for basemap_name, provider in {
        "light gray": "Esri.WorldGrayCanvas",
        "OpenStreetMap": "OpenStreetMap.Mapnik",
        "satellite": "Esri.WorldImagery",
    }.items():
        folium.TileLayer(
            provider, name=basemap_name, show=basemap_name == "light gray"
        ).add_to(shown_map)
    folium.LayerControl().add_to(shown_map)
    # folium's own output is sized by aspect ratio and cuts off the legend, so fix the height
    mo.iframe(shown_map.get_root().render(), height="570px")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## One project's candidates

    Every geometry found for a project, ranked. Priority 1 is the one CPDB uses.
    """)
    return


@app.cell(hide_code=True)
def _(projects):
    project_agency = mo.ui.dropdown(
        sorted(projects["magencyacro"].dropna().unique()),
        label="Managing agency (all when empty)",
        searchable=True,
    )
    project_agency
    return (project_agency,)


@app.cell
def _(project_agency, projects):
    project_options = (
        projects[projects["magencyacro"] == project_agency.value]
        if project_agency.value
        else projects
    ).sort_values("maprojid")
    maprojid = mo.ui.dropdown(
        dict(
            zip(
                project_options["maprojid"] + ": " + project_options["description"],
                project_options["maprojid"],
            )
        ),
        label="Project",
        searchable=True,
        full_width=True,
    )
    maprojid
    return (maprojid,)


@app.cell
def _(layers, maprojid, projects):
    mo.stop(maprojid.value is None, mo.md("Pick a project."))
    candidates = layers["int__project_geom_candidates"].query(
        "maprojid == @maprojid.value"
    )
    project = projects.set_index("maprojid").loc[maprojid.value]

    def _located(value, source: str) -> str:
        return "unknown" if source == "none" else f"{value} (from {source})"

    borough = _located(project["borough"], project["borough_source"])
    community_district = _located(
        project["community_district"], project["community_district_source"]
    )
    boroughs = project["boroughs"] if pd.notna(project["boroughs"]) else "unknown"
    community_districts = (
        project["community_districts"]
        if pd.notna(project["community_districts"])
        else "unknown"
    )
    dashboard_location = (
        project["dashboard_location"]
        if pd.notna(project["dashboard_location"])
        else "not located"
    )
    mo.vstack(
        [
            mo.md(
                f"**Borough:** {borough} · **Boroughs:** {boroughs} · "
                f"**Community district:** {community_district} · "
                f"**Community districts:** {community_districts} · "
                f"**Dashboard location:** {dashboard_location}"
            ),
            mo.ui.table(
                pd.DataFrame(candidates.drop(columns="geometry")),
                selection=None,
            ),
            candidates.explore(column="candidate")
            if len(candidates)
            else mo.md("No candidates."),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
