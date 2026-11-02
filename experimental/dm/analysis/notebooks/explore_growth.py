import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
    import warnings

    import altair as alt
    import duckdb
    import marimo as mo

    # a hover selection shared by two charts is the intended use, not a mistake
    warnings.filterwarnings("ignore", message="Automatically deduplicated selection parameter")
    return alt, duckdb, json, mo


@app.cell
def _(duckdb, mo):
    db_path = mo.notebook_dir().parent / "data" / "analysis_growth.duckdb"
    core_tables = ["dim_community_district", "dim_kpdb_record_community_district", "mart_community_district_date", "mart_borough_survey_year"]
    # copy the core tables into memory and detach, so the notebook doesn't hold a lock that
    # blocks `dbt build`. Rerun this cell to pick up a new build.
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    for _statement in [
        f"ATTACH '{db_path}' AS build (READ_ONLY)",
        *(f"CREATE TABLE {table} AS SELECT * FROM build.main.{table}" for table in core_tables),
        "DETACH build",
    ]:
        con.execute(_statement)
    return (con,)


@app.cell
def _(mo):
    mo.md("""
    # Community district growth

    Where has population grown faster than housing? This notebook compares population and housing
    growth in New York City's 59 community districts: past (2010 to 2020), present (the end of 2024), and future
    (KPDB's projection through 2034). It's built by the dbt project in this folder, and the column docs
    are in `models/core/_core_models.yml`.

    Housing growth is measured as **units built**, the net units completed according to the Housing
    Database. That's the measure the Fair Housing Growth Strategy uses. The census change in housing
    units (**units counted**) is shown for comparison. It also picks up differences in how the 2010 and
    2020 censuses counted units.

    The **population vs housing growth gap** is population growth minus housing growth, each as a
    percent of the district's 2010 total, in percentage points. A positive gap means the district added
    people faster than housing units.

    - **Housing Database** (HDB): DCP's database of DOB jobs that add or remove housing units. Housing
      units after the 2020 census are the census count plus units built, from Housing Database 26Q2.
    - **KPDB**: DCP's Known Projects Database, housing expected in the pipeline.
    - **FHGS**: the Fair Housing Growth Strategy (October 2026), with 2030 housing targets by district.
    - **AHFT**: the Affordable Housing Fair Share tracker. The 12 AHFT districts have the lowest rate
      of affordable housing development.
    - **CDTA**: community district tabulation area, built from census tracts to match or approximate
      each district's boundary. Community districts are numbered, not named, so the charts and tables
      label each district by its CDTA's code and name, e.g. "BK10 Bay Ridge-Dyker Heights".
    """)
    return


@app.cell
def _(con):
    districts = con.sql("""
        SELECT * EXCLUDE (geom) FROM dim_community_district
    """).df()
    # dark gray reads against every fill the charts use (blues, reds, and white) and leaves black
    # for the hovered district
    ahft_color = "#555555"
    hover_color = "#000000"
    districts["ahft"] = districts["is_ahft_district"].map(
        {True: "AHFT district", False: "Not AHFT"}
    ).fillna("Not ranked")
    return ahft_color, districts, hover_color


@app.cell
def _(con, json):
    # simplified so the map stays light; analysis uses the full geometry in the database
    shapes = con.sql("""
        SELECT borocd, ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, 0.0002)) AS geojson
        FROM dim_community_district
    """).fetchall()
    features = [
        {"type": "Feature", "geometry": json.loads(geojson), "properties": {"borocd": borocd}}
        for borocd, geojson in shapes
    ]
    return (features,)


@app.cell
def _(mo):
    mo.md("""
    ## Past housing and population growth

    The 59 community districts, April 2010 to April 2020 (the two census dates) unless a name says
    otherwise. Units built is a percent of 2010 units worked back from the 2020
    census. Units counted is a percent of the 2010 census.
    """)
    return


@app.cell
def _(districts):
    growth = districts[~districts["is_joint_interest_area"]].copy()
    # ordered, so rank correlations can use it
    growth["fhgs_growth_cohort_rank"] = growth["fhgs_growth_cohort"].map({"Low": 1, "Medium": 2, "High": 3})
    # the measures that tell the population and housing growth story most directly; bold in the matrix
    focus_columns = [
        "population_change_2010_2020_pct",
        "units_built_2010_2020_pct",
        "population_vs_housing_growth_gap_2010_2020_pts",
    ]
    key_corr_columns = {
        "population_change_2010_2020_pct": "Population change 2010-2020 (%)",
        "units_built_2010_2020_pct": "Units built 2010-2020 (%)",
        "population_vs_housing_growth_gap_2010_2020_pts": "Population vs housing growth gap 2010-2020 (pts)",
        "units_counted_change_2010_2020_pct": "Units counted 2010-2020 (%)",
        "fhgs_annual_growth_rate_2016_2025_pct": "FHGS annual growth rate 2016-2025 (%)",
        "fhgs_target_growth_rate_pct": "FHGS 2030 target growth rate (%)",
        "projected_net_units_2025_2034_pct": "Projected net units 2025-2034 (%)",
    }
    other_corr_columns = {
        "population_change_2010_2020": "Population change 2010-2020",
        "units_built_2010_2020": "Units built 2010-2020",
        "units_counted_change_2010_2020": "Units counted 2010-2020",
        "units_built_2015_2024": "Units built 2015-2024",
        "fhgs_growth_cohort_rank": "FHGS growth cohort (Low 1 to High 3)",
        "fhgs_homes_added_2021_2025": "FHGS homes added 2021-2025",
        "fhgs_target_new_homes_2030": "FHGS 2030 target new homes",
        "ahft_rank": "AHFT rank",
    }
    return focus_columns, growth, key_corr_columns, other_corr_columns


@app.cell
def _(mo):
    mo.md("""
    ### Population vs housing growth

    The dashed line is equal growth. Districts above it added people faster than housing units.
    """)
    return


@app.cell
def _(ahft_color, alt, growth):
    _ends = growth[["units_built_2010_2020_pct", "population_change_2010_2020_pct"]]
    alt.Chart(growth).mark_circle(size=70, opacity=0.8).encode(
        x=alt.X("units_built_2010_2020_pct:Q", title="Units built 2010-2020 (%)"),
        y=alt.Y("population_change_2010_2020_pct:Q", title="Population change 2010-2020 (%)"),
        color=alt.Color("ahft:N", title=None, scale=alt.Scale(domain=["AHFT district", "Not AHFT"], range=[ahft_color, "#bbbbbb"])),
        tooltip=[
            alt.Tooltip("cdta_name:N", title="District"),
            alt.Tooltip("units_built_2010_2020_pct:Q", title="Units built (%)", format=".2f"),
            alt.Tooltip("population_change_2010_2020_pct:Q", title="Population change (%)", format=".2f"),
        ],
    ).properties(width=500, height=380, title="Population vs housing growth, 2010-2020") + alt.Chart(
        alt.Data(values=[{"v": _ends.min().min()}, {"v": _ends.max().max()}])
    ).mark_line(color="gray", strokeDash=[4, 4]).encode(x="v:Q", y="v:Q")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Growth pressure

    Each district's population vs housing growth gap, 2010 to 2020. Red districts added people faster than
    housing units, and blue ones added housing faster than people.
    """)
    return


@app.cell
def _(ahft_color, alt, features, growth, hover_color):
    pressure_column = "population_vs_housing_growth_gap_2010_2020_pts"
    gap_values = growth[["borocd", "cdta_name", "is_ahft_district", pressure_column]]
    # symmetric around 0, so a shade of red and the same shade of blue mean the same size gap
    pressure_extent = float(growth[pressure_column].abs().max())
    pressure_color = alt.Color(
        f"{pressure_column}:Q",
        # the dot plot reads as the legend
        legend=None,
        scale=alt.Scale(
            domain=[-pressure_extent, 0, pressure_extent],
            range=["#2166ac", "#f7f7f7", "#b2182b"],
        ),
    )
    pressure_tooltip = [
        alt.Tooltip("cdta_name:N", title="District"),
        alt.Tooltip(f"{pressure_column}:Q", title="Population vs housing growth gap (pts)", format=".2f"),
    ]
    # hovering a district in either chart outlines it in both, rather than dimming the rest, which
    # flickers as the pointer crosses other districts
    pressure_hover = alt.selection_point(on="pointerover", clear="pointerout", fields=["borocd"])
    pressure_base = alt.Chart(alt.Data(values=features)).mark_geoshape(
        stroke="white", strokeWidth=0.5
    ).transform_lookup(
        lookup="properties.borocd",
        from_=alt.LookupData(gap_values, "borocd", list(gap_values.columns)),
    ).transform_filter(
        alt.datum.cdta_name != None  # noqa: E711 (vega expression, not python)
    ).project(type="mercator")
    pressure_choropleth = pressure_base.encode(color=pressure_color, tooltip=pressure_tooltip)
    pressure_ahft_outline = pressure_base.transform_filter(alt.datum.is_ahft_district).mark_geoshape(
        fill=None, stroke=ahft_color, strokeWidth=2
    )
    pressure_hovered = pressure_base.transform_filter(
        {"param": pressure_hover.name, "empty": False}
    ).mark_geoshape(fill=None, stroke=hover_color, strokeWidth=3)
    # circles default to 70% opacity, which washes the colors out next to the map's
    pressure_dots = alt.Chart(gap_values).mark_circle(opacity=1).encode(
        x=alt.X(f"{pressure_column}:Q", title="Population vs housing growth gap (pts)"),
        y=alt.Y("cdta_name:N", sort="-x", title=None, axis=alt.Axis(labelLimit=240)),
        color=pressure_color,
        size=alt.condition(pressure_hover, alt.value(220), alt.value(70), empty=False),
        stroke=alt.when(pressure_hover, empty=False).then(alt.value(hover_color))
        .when(alt.datum.is_ahft_district).then(alt.value(ahft_color))
        .otherwise(alt.value(None)),
        strokeWidth=alt.condition(pressure_hover, alt.value(3), alt.value(2), empty=False),
        tooltip=pressure_tooltip,
    )
    pressure_zero = alt.Chart(alt.Data(values=[{"x": 0}])).mark_rule(color="gray").encode(x="x:Q")
    alt.hconcat(
        (pressure_choropleth + pressure_ahft_outline + pressure_hovered).properties(width=460, height=620),
        (pressure_zero + pressure_dots).properties(width=200, height=620),
    ).add_params(pressure_hover).properties(
        title="Population vs housing growth gap, 2010-2020 (gray outline: AHFT districts)"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Units counted vs units built

    Each district's census change in housing units against the net units the Housing Database says
    were built over the same years. The dashed line is where they agree. Districts below it counted
    fewer new units than were built, which construction and demolition don't explain. Color is the
    district's population change: if the same districts are far from the line and have extreme
    population changes, counting differences may affect the population figures too.
    """)
    return


@app.cell
def _(alt, growth, mo):
    units_compare = alt.Chart(growth).mark_circle(size=70, opacity=0.85).encode(
        x=alt.X("units_built_2010_2020:Q", title="Units built, April 2010 to April 2020"),
        y=alt.Y("units_counted_change_2010_2020:Q", title="Units counted 2010-2020"),
        color=alt.Color(
            "population_change_2010_2020_pct:Q",
            title="Population change (%)",
            scale=alt.Scale(domainMid=0, range=["#5e3c99", "#f7f7f7", "#e66101"]),
        ),
        tooltip=[
            alt.Tooltip("cdta_name:N", title="District"),
            alt.Tooltip("units_built_2010_2020:Q", title="Units built", format=",.0f"),
            alt.Tooltip("units_counted_change_2010_2020:Q", title="Units counted", format=",.0f"),
            alt.Tooltip("units_counted_minus_built_2010_2020:Q", title="Counted minus built", format=",.0f"),
            alt.Tooltip("population_change_2010_2020_pct:Q", title="Population change (%)", format=".2f"),
        ],
    ).properties(width=500, height=380)
    agreement_ends = growth[["units_built_2010_2020", "units_counted_change_2010_2020"]]
    agreement_line = alt.Chart(alt.Data(values=[
        {"v": agreement_ends.min().min()}, {"v": agreement_ends.max().max()}
    ])).mark_line(
        color="gray", strokeDash=[4, 4]
    ).encode(x="v:Q", y="v:Q")
    largest_gaps = (
        growth.reindex(
            growth["units_counted_minus_built_2010_2020"].abs().sort_values(ascending=False).index
        )
        .head(10)
        .rename(columns={
            "cdta_name": "District",
            "units_counted_change_2010_2020": "Counted",
            "units_built_2010_2020": "Built",
            "units_counted_minus_built_2010_2020": "Counted minus built",
        })[["District", "Counted", "Built", "Counted minus built"]]
        .reset_index(drop=True)
    )
    mo.vstack([
        units_compare + agreement_line,
        mo.ui.table(largest_gaps, selection=None, show_data_types=False, show_column_summaries=False, label="Largest gaps"),
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ### Where population change went

    Population in households is housing units x the share occupied x people per household, so each
    district's 2010-2020 population change splits into parts that add up to it. The parts are
    accounting, not causes: each one is how much the population would have changed if only that one
    thing had.

    - **Units built**: people the units built would hold if they filled like the district's existing
      units. Where new units hold smaller households than that, this part is too high.
    - **Occupied units not built**: people in occupied units construction doesn't explain. It's the
      rest of the census change in units plus the change in the share occupied. Those two often
      offset each other (a unit one census counted and the other didn't moves both), so they're
      shown together.
    - **Household size**: bigger or smaller households. It also takes up whatever units built gets
      wrong, so where a lot was built, a negative value doesn't by itself mean existing households
      shrank. It isn't crowding either: the decennial census doesn't count rooms. See the data notes at
      the end.
    - **Group quarters**: dorms, nursing homes, prisons, shelters, and the like. A direct count.

    Each part depends on the order the factors are changed in, so each is the average over every
    order. Each is in percentage points of the district's 2010 population, so the parts add up to its
    population change (%). The map shades the picked part in its bar color where it added people
    and gray where it lost them, on one scale for every part, capped at 10 points.
    """)
    return


@app.cell
def _(mo):
    # dark enough to fill a map from white, and none gray, since gray is negative on the map
    component_parts = {
        "population_change_from_units_built_2010_2020_pts": ("Units built", "#2166ac"),
        "population_change_from_occupied_units_not_built_2010_2020_pts": ("Occupied units not built", "#e08214"),
        "population_change_from_household_size_2010_2020_pts": ("Household size", "#b2182b"),
        "population_change_from_group_quarters_2010_2020_pts": ("Group quarters", "#6a51a3"),
    }
    component_pick = mo.ui.radio(
        {label: column for column, (label, _color) in component_parts.items()},
        value="Household size",
        label="Map",
        inline=True,
    )
    component_pick
    return component_parts, component_pick


@app.cell
def _(
    ahft_color,
    alt,
    component_parts,
    component_pick,
    features,
    growth,
    hover_color,
):
    component_points = growth[
        ["borocd", "cdta_name", "is_ahft_district", "population_change_2010_2020_pct", *component_parts]
    ].rename(columns={column: label for column, (label, _color) in component_parts.items()})
    component_labels = [label for label, _color in component_parts.values()]
    component_map_label = component_parts[component_pick.value][0]
    component_map_color = component_parts[component_pick.value][1]
    # one scale for every part, so a shade means the same size change whichever part is mapped. Most
    # parts stay within 10 points either way; the few beyond show at full color.
    component_scale_limit = 10
    # an explicit order, since a sort by another field is dropped when two layers share the axis
    component_order = list(
        component_points.sort_values("population_change_2010_2020_pct", ascending=False)["cdta_name"]
    )
    component_hover = alt.selection_point(on="pointerover", clear="pointerout", fields=["borocd"])
    component_map = alt.Chart(alt.Data(values=features)).mark_geoshape(
        stroke="white", strokeWidth=0.5
    ).transform_lookup(
        lookup="properties.borocd",
        from_=alt.LookupData(component_points, "borocd", list(component_points.columns)),
    ).transform_filter(
        alt.datum.cdta_name != None  # noqa: E711 (vega expression, not python)
    ).project(type="mercator")
    component_choropleth = component_map.encode(
        color=alt.Color(
            f"{component_map_label}:Q",
            title="pts of 2010 population",
            legend=alt.Legend(orient="bottom-left", direction="horizontal", gradientLength=200),
            scale=alt.Scale(
                domain=[-component_scale_limit, 0, component_scale_limit],
                range=["#8c8c8c", "#f7f7f7", component_map_color],
                clamp=True,
            ),
        ),
        tooltip=[
            alt.Tooltip("cdta_name:N", title="District"),
            alt.Tooltip("population_change_2010_2020_pct:Q", title="Population change (%)", format=".2f"),
            *[alt.Tooltip(f"{label}:Q", title=f"{label} (pts)", format=".2f") for label in component_labels],
        ],
    )
    component_ahft_outline = component_map.transform_filter(alt.datum.is_ahft_district).mark_geoshape(
        fill=None, stroke=ahft_color, strokeWidth=2
    )
    component_bars = alt.Chart(component_points).transform_fold(
        component_labels, as_=["part", "pts"]
    ).mark_bar().encode(
        x=alt.X("pts:Q", stack="zero", title="Population change (pts of 2010 population)"),
        y=alt.Y(
            "cdta_name:N",
            title=None,
            sort=component_order,
            axis=alt.Axis(labelLimit=240),
        ),
        color=alt.Color(
            "part:N",
            title=None,
            sort=component_labels,
            scale=alt.Scale(
                domain=component_labels,
                range=[color for _label, color in component_parts.values()],
            ),
            # below the chart, so it doesn't crowd the first district
            legend=alt.Legend(orient="bottom", columns=2),
        ),
        order=alt.Order("part_order:Q"),
        tooltip=[
            alt.Tooltip("cdta_name:N", title="District"),
            alt.Tooltip("part:N", title="Part"),
            alt.Tooltip("pts:Q", title="Points", format=".2f"),
            alt.Tooltip("population_change_2010_2020_pct:Q", title="Population change (%)", format=".2f"),
        ],
    ).transform_calculate(
        part_order=f"indexof({component_labels}, datum.part)"
    )
    component_hovered = component_map.transform_filter(
        {"param": component_hover.name, "empty": False}
    ).mark_geoshape(fill=None, stroke=hover_color, strokeWidth=3)
    # the outline spans each district's whole stacked bar, from its negative parts to its positive ones
    component_extents = component_points.assign(
        low=component_points[component_labels].clip(upper=0).sum(axis=1),
        high=component_points[component_labels].clip(lower=0).sum(axis=1),
    )
    component_bar_outline = alt.Chart(component_extents).mark_bar(
        fill=None, stroke=hover_color, strokeWidth=3
    ).encode(
        x="low:Q",
        x2="high:Q",
        y=alt.Y(
            "cdta_name:N",
            sort=component_order,
        ),
    ).transform_filter({"param": component_hover.name, "empty": False})
    alt.hconcat(
        (component_choropleth + component_ahft_outline + component_hovered).properties(
            width=460, height=620, title=f"{component_map_label}, 2010-2020 (pts of 2010 population)"
        ),
        (component_bars + component_bar_outline).properties(width=260, height=620),
    ).add_params(component_hover).resolve_scale(color="independent")
    return


@app.cell
def _(mo):
    mo.md("""
    ## AHFT districts compared to the rest

    Joint interest areas are left out because AHFT doesn't rank them.

    Units built per person added is only shown for these two groups. For a single district whose
    population barely changed or fell, the ratio gets huge or turns negative.
    """)
    return


@app.cell
def _(districts, mo):
    ranked = districts[~districts["is_joint_interest_area"]]
    by_group = ranked.groupby("ahft")[
        [
            "population_2010_census",
            "population_2020_census",
            "units_built_2010_2020",
            "housing_units_2010_backcast",
            "housing_units_2024_end",
            "projected_net_units_2025_2034",
        ]
    ].sum()
    # same 0-100 scale, rounding, and 2010 housing base as the percentages in dim_community_district
    by_group["population_change_2010_2020_pct"] = (
        100 * (by_group["population_2020_census"] / by_group["population_2010_census"] - 1)
    ).round(2)
    by_group["units_built_2010_2020_pct"] = (
        100 * by_group["units_built_2010_2020"] / by_group["housing_units_2010_backcast"]
    ).round(2)
    by_group["population_vs_housing_growth_gap_2010_2020_pts"] = (
        by_group["population_change_2010_2020_pct"] - by_group["units_built_2010_2020_pct"]
    ).round(2)
    by_group["population_change_2010_2020"] = (
        by_group["population_2020_census"] - by_group["population_2010_census"]
    )
    by_group["units_built_per_person_added_2010_2020"] = (
        by_group["units_built_2010_2020"] / by_group["population_change_2010_2020"]
    ).round(2)
    by_group["units_built_2010_2024_pct"] = (
        100 * (by_group["housing_units_2024_end"] / by_group["housing_units_2010_backcast"] - 1)
    ).round(2)
    by_group["projected_net_units_2025_2034_pct"] = (
        100 * by_group["projected_net_units_2025_2034"] / by_group["housing_units_2024_end"]
    ).round(2)
    group_sizes = ranked["ahft"].value_counts()
    group_labels = {
        "AHFT district": f"AHFT districts ({group_sizes['AHFT district']})",
        "Not AHFT": f"Other districts ({group_sizes['Not AHFT']})",
    }
    group_columns = {
        "population_change_2010_2020_pct": "Population change 2010-2020 (%)",
        "units_built_2010_2020_pct": "Units built 2010-2020 (%)",
        "population_vs_housing_growth_gap_2010_2020_pts": "Population vs housing growth gap 2010-2020 (pts)",
        "units_built_per_person_added_2010_2020": "Units built per person added 2010-2020",
        "units_built_2010_2024_pct": "Units built 2010-2024 (%)",
        "projected_net_units_2025_2034_pct": "Projected net units 2025-2034 (%)",
        "population_2010_census": "Population 2010",
        "population_2020_census": "Population 2020",
        "population_change_2010_2020": "People added 2010-2020",
        "units_built_2010_2020": "Units built 2010-2020",
        "housing_units_2010_backcast": "Housing units 2010 (2020 census minus units built)",
        "housing_units_2024_end": "Housing units end of 2024",
        "projected_net_units_2025_2034": "Projected net units 2025-2034",
    }
    takeaway_columns = list(group_columns)[:4]
    # the window is in the table label, so the row names drop it
    group_takeaway = (
        by_group[takeaway_columns]
        .rename(index=group_labels, columns=lambda column: group_columns[column].replace(" 2010-2020", ""))
        .T.rename_axis(index="Measure", columns=None)
        .reset_index()
    )
    group_totals = (
        by_group[list(group_columns)]
        .rename(index=group_labels, columns=group_columns)
        .rename_axis("Group")
        .reset_index()
    )
    mo.vstack([
        mo.ui.table(group_takeaway, selection=None, show_data_types=False, show_column_summaries=False, label="Growth, 2010 to 2020"),
        mo.ui.table(group_totals, selection=None, show_data_types=False, show_column_summaries=False, label="All measures"),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Map

    "FHGS 2030 target vs homes added 2021-2025" is the 2026-2030 target as a percent of the homes added
    in the five years before. 200 means the target asks for twice the recent pace.
    """)
    return


@app.cell(hide_code=True)
def _(districts, mo):
    boroughs = mo.ui.multiselect(
        sorted(districts["boro_name"].unique()), label="Boroughs"
    )
    hide_jias = mo.ui.checkbox(value=True, label="Hide joint interest areas")
    show_all_metrics = mo.ui.checkbox(label="Show all metrics")
    return boroughs, hide_jias, show_all_metrics


@app.cell
def _(boroughs, hide_jias, mo, show_all_metrics):
    # the measures behind the growth pressure question; the checkbox adds the rest
    key_metric_options = {
        "Units built 2010-2020 (%)": "units_built_2010_2020_pct",
        "Population change 2010-2020 (%)": "population_change_2010_2020_pct",
        "Population vs housing growth gap 2010-2020 (pts)": "population_vs_housing_growth_gap_2010_2020_pts",
        "FHGS 2030 target growth rate (%)": "fhgs_target_growth_rate_pct",
        "Projected net units 2025-2034 (%)": "projected_net_units_2025_2034_pct",
    }
    other_metric_options = {
        "Units built 2015-2024": "units_built_2015_2024",
        "FHGS growth cohort": "fhgs_growth_cohort",
        "FHGS annual growth rate 2016-2025 (%)": "fhgs_annual_growth_rate_2016_2025_pct",
        "FHGS total homes 2025": "fhgs_total_homes_2025",
        "FHGS 2030 target new homes": "fhgs_target_new_homes_2030",
        "FHGS 2030 target vs homes added 2021-2025 (%)": "fhgs_target_pct_of_homes_added_2021_2025",
        "Projected net units 2025-2034": "projected_net_units_2025_2034",
        "Population 2020 (census)": "population_2020_census",
        "Units counted 2010-2020 (%)": "units_counted_change_2010_2020_pct",
        "AHFT rank": "ahft_rank",
    }
    metric_options = (
        key_metric_options | other_metric_options if show_all_metrics.value else key_metric_options
    )
    metric = mo.ui.dropdown(
        metric_options, value="Units built 2010-2020 (%)", label="Map metric"
    )
    mo.hstack([metric, show_all_metrics, boroughs, hide_jias], justify="start")
    return (metric,)


@app.cell
def _(boroughs, districts, hide_jias):
    filtered = districts
    if boroughs.value:
        filtered = filtered[filtered["boro_name"].isin(boroughs.value)]
    if hide_jias.value:
        filtered = filtered[~filtered["is_joint_interest_area"]]
    return (filtered,)


@app.cell
def _(ahft_color, alt, features, filtered, metric):
    metric_column = metric.value
    is_decimal = metric_column.endswith(("_pct", "_pts"))
    number_format = ",.2f" if is_decimal else ",.0f"
    map_values = filtered[["borocd", "cdta_name", "ahft", metric_column]]
    base = alt.Chart(
        alt.Data(values=features)
    ).mark_geoshape(stroke="white", strokeWidth=0.5).transform_lookup(
        lookup="properties.borocd",
        from_=alt.LookupData(map_values, "borocd", list(map_values.columns)),
    ).transform_filter(
        alt.datum.cdta_name != None  # noqa: E711 (vega expression, not python)
    ).project(type="mercator")
    if metric_column == "fhgs_growth_cohort":
        metric_color = alt.Color(
            f"{metric_column}:N",
            title=metric.selected_key,
            scale=alt.Scale(domain=["Low", "Medium", "High"], scheme="blues"),
        )
        metric_tooltip = alt.Tooltip(f"{metric_column}:N", title=metric.selected_key)
    else:
        metric_color = alt.Color(
            f"{metric_column}:Q",
            title=metric.selected_key,
            scale=alt.Scale(scheme="blues"),
            legend=alt.Legend(format=number_format),
        )
        metric_tooltip = alt.Tooltip(f"{metric_column}:Q", title=metric.selected_key, format=number_format)
    choropleth = base.encode(
        color=metric_color,
        tooltip=[alt.Tooltip("cdta_name:N", title="District"), metric_tooltip],
    )
    ahft_outline = base.transform_filter(alt.datum.ahft == "AHFT district").mark_geoshape(
        fill=None, stroke=ahft_color, strokeWidth=2
    )
    (choropleth + ahft_outline).properties(
        width=650, height=650, title=f"{metric.selected_key} (gray outline: AHFT districts)"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Districts

    The districts on the map, with the same borough and joint interest area filters.
    """)
    return


@app.cell
def _(filtered, mo):
    district_table_columns = {
        "borocd": "borocd",
        "cdta_name": "District",
        "ahft": "AHFT",
        "population_2010_census": "Population 2010",
        "population_2020_census": "Population 2020",
        "population_change_2010_2020_pct": "Population change 2010-2020 (%)",
        "units_built_2010_2020": "Units built 2010-2020",
        "units_built_2010_2020_pct": "Units built 2010-2020 (%)",
        "population_vs_housing_growth_gap_2010_2020_pts": "Population vs housing growth gap 2010-2020 (pts)",
        "housing_units_2010_backcast": "Housing units 2010 (2020 census minus units built)",
        "housing_units_2010_census": "Housing units 2010 census",
        "housing_units_2020_census": "Housing units 2020 census",
        "housing_units_2024_end": "Housing units end of 2024",
        "projected_net_units_2025_2034": "Projected net units 2025-2034",
        "projected_net_units_2025_2034_pct": "Projected net units 2025-2034 (%)",
        "ahft_rank": "AHFT rank",
        "fhgs_growth_cohort": "FHGS growth cohort",
        "fhgs_annual_growth_rate_2016_2025_pct": "FHGS annual growth rate 2016-2025 (%)",
        "fhgs_total_homes_2025": "FHGS total homes 2025",
        "fhgs_target_growth_rate_pct": "FHGS 2030 target growth rate (%)",
        "fhgs_target_new_homes_2030": "FHGS 2030 target new homes",
    }
    mo.ui.table(
        filtered[list(district_table_columns)].rename(columns=district_table_columns),
        selection=None, show_data_types=False,
        label="Districts",
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## Districts comparison
    """)
    return


@app.cell
def _(districts, mo):
    district_options = dict(zip(districts["cdta_name"], districts["borocd"]))
    selected_districts = mo.ui.multiselect(
        district_options,
        value=[
            "BK10 Bay Ridge-Dyker Heights",
            "MN05 Midtown-Flatiron-Union Square",
        ],
        label="Districts",
    )
    selected_districts
    return (selected_districts,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Housing units over time

    - **2020 census ± units built** (solid): the 2020 census count, plus net units built after it and
      minus net units built before it, from Housing Database 26Q2. One point per year end through 2025.
    - **KPDB projection** (dashed): starts from the solid line's end-of-2024 count and adds the units
      KPDB expects within 5 years (the 2029 dot), then within 5 to 10 years (the 2034 dot). KPDB phases
      projects by status, not by date, so those years are labels rather than forecast dates. Units it
      expects after 10 years aren't shown.
    - **2010 census** (hollow gray dot): the 2010 count. Where it's off the line, the
      two censuses counted units differently than construction explains.
    """)
    return


@app.cell
def _(alt, con, districts, selected_districts):
    built_series = "2020 census ± units built"
    projected_series = "KPDB projection"
    series_order = [built_series, projected_series]
    # Censuses count as of April 1, everything else is as of year end. The solid line counts from the
    # 2020 census, so a census counting difference doesn't show up as a change in units.
    housing_series = con.execute(
        """
        SELECT borocd, strftime(as_of_date, '%Y-%m-%d') AS date, housing_units_from_2020_census AS units
        FROM mart_community_district_date
        WHERE borocd IN (SELECT unnest(?::integer[]))
        ORDER BY as_of_date
        """,
        [list(selected_districts.value)],
    ).df()
    housing_points = []
    census_2010_points = []
    for _, _row in districts[districts["borocd"].isin(selected_districts.value)].iterrows():
        _label = _row["cdta_name"]
        _series = housing_series[housing_series["borocd"] == _row["borocd"]]
        census_2010_points.append(
            {"district": _label, "date": "2010-04-01", "units": _row["housing_units_2010_census"]}
        )
        for _date, _units in zip(_series["date"], _series["units"]):
            housing_points.append({"district": _label, "date": _date, "units": _units, "series": built_series})
        _start = _row["housing_units_2024_end"]
        for _date, _units in [
            ("2024-12-31", _start),
            ("2029-12-31", _start + _row["projected_net_units_2025_2029"]),
            ("2034-12-31", _start + _row["projected_net_units_2025_2029"] + _row["projected_net_units_2030_2034"]),
        ]:
            housing_points.append({"district": _label, "date": _date, "units": _units, "series": projected_series})
    year_ticks = [alt.DateTime(year=_year) for _year in range(2010, 2036)]
    five_year_ticks = [alt.DateTime(year=_year) for _year in range(2010, 2036, 5)]
    housing_lines = alt.Chart(alt.Data(values=housing_points)).mark_line(point=True).encode(
        x=alt.X(
            "date:T",
            title=None,
            axis=alt.Axis(
                values=five_year_ticks,
                format="%Y",
                tickSize=8,
                # only the first layer's axis gets a grid by default
                grid=True,
                gridColor="#bbb",
                orient="bottom",
            ),
            # whole years, so the first and last five-year ticks are inside the chart
            scale=alt.Scale(domain=[five_year_ticks[0], five_year_ticks[-1]]),
        ),
        y=alt.Y("units:Q", title="Housing units", scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", title=None, sort=series_order, scale=alt.Scale(domain=series_order)),
        strokeDash=alt.StrokeDash(
            "series:N",
            title=None,
            sort=series_order,
            scale=alt.Scale(domain=series_order, range=[[1, 0], [6, 4]]),
        ),
        detail="district:N",
        tooltip=[
            "district:N",
            "series:N",
            alt.Tooltip("date:T", format="%b %-d, %Y"),
            alt.Tooltip("units:Q", format=",.0f"),
        ],
    )
    # a second, unlabeled axis on the same scale draws the yearly minor ticks. Its invisible points
    # go in the bottom layer so they don't block the lines' tooltips.
    minor_ticks = alt.Chart(alt.Data(values=housing_points)).mark_point(opacity=0).encode(
        x=alt.X(
            "date:T",
            title=None,
            axis=alt.Axis(
                values=year_ticks, labels=False, tickSize=4, gridOpacity=0.4, domain=False, orient="bottom"
            ),
        ),
        y="units:Q",
    )
    census_2010_dots = alt.Chart(alt.Data(values=census_2010_points)).mark_point(
        filled=False, color="gray", size=60
    ).encode(
        x=alt.X("date:T", axis=None),
        y="units:Q",
        tooltip=[
            "district:N",
            alt.Tooltip("units:Q", title="2010 census", format=",.0f"),
        ],
    )
    # Vega-Lite parses dates before filters run, so the end points are picked here
    district_labels = alt.Chart(
        alt.Data(values=[_point for _point in housing_points if _point["date"] == "2034-12-31"])
    ).mark_text(align="left", dx=6, fontSize=11).encode(
        x=alt.X("date:T", axis=None),
        y="units:Q",
        text="district:N",
    )
    (minor_ticks + housing_lines + census_2010_dots + district_labels).resolve_axis(
        x="independent"
    ).properties(width=600, height=320).configure_view(continuousWidth=600).configure_legend(
        orient="bottom"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### KPDB records

    Records in the selected districts with units KPDB expects in the future, largest first. A record that
    spans districts shows only its share in each.
    """)
    return


@app.cell
def _(con, mo, selected_districts):
    records = con.execute(
        """
        SELECT
            dim.cdta2020 AS district,
            rec.record_id, rec.source, rec.record_name, rec.status,
            round(rec.within_5_years_in_cd + rec.from_5_to_10_years_in_cd) AS projected_units_2025_2034,
            round(rec.after_10_years_in_cd) AS projected_units_after_2034,
            round(rec.proportion_in_cd, 2) AS proportion_in_cd
        FROM main.dim_kpdb_record_community_district AS rec
        INNER JOIN dim_community_district AS dim ON rec.borocd = dim.borocd
        WHERE rec.borocd IN (SELECT unnest(?::integer[]))
            AND coalesce(rec.within_5_years_in_cd, 0) + coalesce(rec.from_5_to_10_years_in_cd, 0)
                + coalesce(rec.after_10_years_in_cd, 0) > 0
        ORDER BY projected_units_2025_2034 DESC NULLS LAST
        """,
        [list(selected_districts.value)],
    ).df()
    mo.ui.table(records, selection=None, show_data_types=False, label=f"KPDB records with future units ({len(records)})")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Correlation matrix

    Spearman ranks the 59 community districts first, so a few outliers can't dominate it. Pearson is
    the plain linear correlation.

    **Bold labels** are the focus: population change, units built, and the gap between them, all over
    April 2010 to April 2020 as a percent of each district's 2010 base. The rest are context. Units
    counted carries census counting differences, and the FHGS and KPDB columns cover other years. "Show
    all measures" adds absolute counts, which mostly track how big a district is.
    """)
    return


@app.cell
def _(mo):
    corr_method = mo.ui.radio(
        {"Spearman (rank)": "spearman", "Pearson": "pearson"},
        value="Spearman (rank)",
        label="Correlation",
        inline=True,
    )
    corr_show_all = mo.ui.checkbox(label="Show all measures")
    mo.hstack([corr_method, corr_show_all], justify="start")
    return corr_method, corr_show_all


@app.cell
def _(
    alt,
    corr_method,
    corr_show_all,
    focus_columns,
    growth,
    json,
    key_corr_columns,
    other_corr_columns,
):
    corr_columns = key_corr_columns | other_corr_columns if corr_show_all.value else key_corr_columns
    corr = growth[list(corr_columns)].corr(method=corr_method.value).round(2)
    corr_long = (
        corr.rename(index=corr_columns, columns=corr_columns)
        .rename_axis("x")
        .reset_index()
        .melt(id_vars="x", var_name="y", value_name="r")
    )
    labels = list(corr_columns.values())
    focus_labels = json.dumps([corr_columns[column] for column in focus_columns])
    focus_weight = {"expr": f"indexof({focus_labels}, datum.value) >= 0 ? 'bold' : 'normal'"}
    heatmap = alt.Chart(corr_long).mark_rect().encode(
        x=alt.X(
            "x:N",
            sort=labels,
            title=None,
            axis=alt.Axis(labelAngle=-40, labelLimit=260, labelFontWeight=focus_weight),
        ),
        y=alt.Y("y:N", sort=labels, title=None, axis=alt.Axis(labelLimit=260, labelFontWeight=focus_weight)),
        color=alt.Color("r:Q", scale=alt.Scale(scheme="redblue", domain=[-1, 1]), title="r"),
        tooltip=["x:N", "y:N", alt.Tooltip("r:Q", format=".2f")],
    )
    heatmap_labels = heatmap.mark_text(fontSize=10).encode(
        text=alt.Text("r:Q", format=".2f"),
        color=alt.condition("abs(datum.r) > 0.6", alt.value("white"), alt.value("black")),
    )
    (heatmap + heatmap_labels).properties(
        width=420 + 25 * len(corr_columns), height=420 + 25 * len(corr_columns), title=f"{corr_method.value.title()} correlation, 59 community districts"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Data notes

    ### Household size in new buildings

    The units built part of where population change went assumes new units fill like a district's
    existing units. HPD's Housing and Vacancy Survey (HVS) says households in buildings built 2010 or
    later are smaller. Its public files only go down to borough, so this can't adjust the district
    numbers. Read it as a sign that units built runs high, and household size runs low by the same
    amount, wherever a lot was built.

    HVS is a sample. The ± is one standard error from HPD's replicate weights. HPD cautions that
    differences between survey cycles may not reflect real change, and the jump in older buildings'
    household size from 2021 to 2023 in the Bronx and Brooklyn is bigger than seems plausible. Staten
    Island has under 10 sampled households in newer buildings, so it's left out.
    """)
    return


@app.cell(hide_code=True)
def _(con, mo):
    household_size_by_building_age = con.sql("""
        SELECT
            boro_name AS "Borough",
            survey_year AS "HVS year",
            average_household_size_built_before_2010_hvs AS "Built before 2010",
            average_household_size_built_2010_or_later_hvs AS "Built 2010 or later",
            printf('%.2f ± %.2f', household_size_gap_built_2010_or_later_hvs, household_size_gap_built_2010_or_later_hvs_se)
                AS "Difference",
            hvs_records_built_2010_or_later AS "Sampled households, built 2010 or later"
        FROM mart_borough_survey_year
        WHERE boro_name != 'Staten Island'
        ORDER BY boro_code, survey_year
    """).df()
    mo.ui.table(
        household_size_by_building_age,
        selection=None,
        show_data_types=False,
        show_column_summaries=False,
        label="Average household size by building age (HVS)",
    )
    return


if __name__ == "__main__":
    app.run()
