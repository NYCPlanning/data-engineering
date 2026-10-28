import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import json

    import altair as alt
    import duckdb
    import marimo as mo

    return alt, duckdb, json, mo


@app.cell
def _(duckdb, mo):
    db_path = mo.notebook_dir().parent / "data" / "analysis_growth.duckdb"
    # copy the core tables into memory and detach, so the notebook doesn't hold a lock that
    # blocks `dbt build`. Rerun this cell to pick up a new build.
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    con.execute(f"ATTACH '{db_path}' AS build (READ_ONLY)")
    for table in ["dim_community_district", "dim_kpdb_record_community_district"]:
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM build.main.{table}")
    con.execute("DETACH build")
    return (con,)


@app.cell
def _(mo):
    mo.md("""
    # Community district growth

    Population and housing by community district: past (2010 and 2020 censuses), present
    (ACS 2020-2024, KPDB's end-of-2024 estimate), and future (KPDB's projection through 2034).
    Built by the dbt project in this folder. Column docs are in `models/core/_core_models.yml`.

    ACS values come from each district's CDTA, which approximates its boundary.
    """)
    return


@app.cell
def _(con):
    districts = con.sql("""
        SELECT * EXCLUDE (geom) FROM dim_community_district
    """).df()
    districts["ahft"] = districts["is_ahft_district"].map(
        {True: "AHFT district", False: "Not AHFT"}
    ).fillna("Not ranked")
    return (districts,)


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


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Map

    Pick a measure to color each district. Orange outlines mark the 12 AHFT districts, the ones with
    the lowest rate of affordable housing development.
    """)
    return


@app.cell
def _(districts, mo):
    metric_options = {
        "Units built 2010-2020 (%)": "hdb_housing_growth_2010_2020_pct",
        "Population minus units built 2010-2020 (pts)": "population_minus_hdb_growth_2010_2020_pts",
        "Net completed units 2015-2024": "net_completed_units_2015_2024",
        "FHGS growth cohort": "fhgs_growth_cohort",
        "FHGS annual growth rate 2016-2025 (%)": "fhgs_annual_growth_rate_2016_2025_pct",
        "FHGS total homes 2025": "fhgs_total_homes_2025",
        "FHGS 2030 target growth rate (%)": "fhgs_target_growth_rate_pct",
        "FHGS 2030 target new homes": "fhgs_target_new_homes_2030",
        "FHGS 2030 target vs homes added 2021-2025 (%)": "fhgs_target_pct_of_homes_added_2021_2025",
        "Projected housing growth 2024-2034 (%)": "projected_housing_growth_2024_2034_pct",
        "Projected net units 2025-2034": "projected_net_units_2025_2034",
        "Population change 2010-2020 (%)": "population_change_2010_2020_pct",
        "Population 2020 (census)": "population_2020_census",
        "Units counted, census change 2010-2020 (%)": "housing_change_2010_2020_pct",
        "AHFT rank": "ahft_rank",
    }
    metric = mo.ui.dropdown(
        metric_options, value="Units built 2010-2020 (%)", label="Map metric"
    )
    boroughs = mo.ui.multiselect(
        sorted(districts["boro_name"].unique()), label="Boroughs"
    )
    hide_jias = mo.ui.checkbox(value=True, label="Hide joint interest areas")
    mo.hstack([metric, boroughs, hide_jias], justify="start")
    return boroughs, hide_jias, metric


@app.cell
def _(boroughs, districts, hide_jias):
    filtered = districts
    if boroughs.value:
        filtered = filtered[filtered["boro_name"].isin(boroughs.value)]
    if hide_jias.value:
        filtered = filtered[~filtered["is_joint_interest_area"]]
    return (filtered,)


@app.cell
def _(alt, features, filtered, metric):
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
        fill=None, stroke="#d95f02", strokeWidth=2
    )
    (choropleth + ahft_outline).properties(
        width=650, height=650, title=f"{metric.selected_key} (orange outline: AHFT districts)"
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
    mo.ui.table(
        filtered[
            [
                "borocd",
                "cdta_name",
                "ahft_rank",
                "ahft",
                "fhgs_growth_cohort",
                "fhgs_annual_growth_rate_2016_2025_pct",
                "fhgs_total_homes_2025",
                "fhgs_target_growth_rate_pct",
                "fhgs_target_new_homes_2030",
                "population_2010_census",
                "population_2020_census",
                "population_acs_2020_2024",
                "hdb_net_completed_units_2010q2_2020q1",
                "hdb_housing_growth_2010_2020_pct",
                "housing_units_2010_census",
                "housing_units_2020_census",
                "housing_units_2024_end",
                "projected_net_units_2025_2034",
                "projected_housing_units_2034_end",
                "projected_housing_growth_2024_2034_pct",
            ]
        ],
        selection=None,
        label="Districts",
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## AHFT districts compared to the rest

    Joint interest areas are left out: AHFT doesn't rank them.
    """)
    return


@app.cell
def _(districts):
    ranked = districts[~districts["is_joint_interest_area"]]
    by_group = ranked.groupby("ahft")[
        [
            "population_2010_census",
            "population_2020_census",
            "housing_units_2010_census",
            "housing_units_2024_end",
            "projected_net_units_2025_2034",
        ]
    ].sum()
    # same 0-100 scale and rounding as the percentages in dim_community_district
    by_group["population_change_2010_2020_pct"] = (
        100 * (by_group["population_2020_census"] / by_group["population_2010_census"] - 1)
    ).round(2)
    by_group["housing_change_2010_2024_pct"] = (
        100 * (by_group["housing_units_2024_end"] / by_group["housing_units_2010_census"] - 1)
    ).round(2)
    by_group["projected_housing_growth_2024_2034_pct"] = (
        100 * by_group["projected_net_units_2025_2034"] / by_group["housing_units_2024_end"]
    ).round(2)
    by_group.reset_index()
    return


@app.cell
def _(mo):
    mo.md("""
    ## Districts comparison

    Pick one or more districts to compare.
    """)
    return


@app.cell
def _(districts, mo):
    district_options = dict(zip(districts["cdta_name"], districts["borocd"]))
    selected_districts = mo.ui.multiselect(
        district_options,
        value=[
            "BK10 Bay Ridge-Dyker Heights (CD 10 Approximation)",
            "MN05 Midtown-Flatiron-Union Square (CD 5 Approximation)",
        ],
        label="Districts",
    )
    selected_districts
    return (selected_districts,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Housing units over time

    Each district is labeled at the end of its line. Color and line style show what each segment measures:

    - **Census count** (solid): decennial housing units as of April 1. The line between censuses
      is only a connector. Nothing measures the years in between.
    - **Census + net completions** (solid): the 2020 census plus net units completed since, from
      the Housing Database (dcp_housing 24Q4). A count, not an estimate.
    - **KPDB projection** (dashed): end-of-2024 units plus units KPDB phases within 10 years. The
      2034 end point is a labeling convention. KPDB phases by project status, not by date.
    """)
    return


@app.cell
def _(alt, districts, selected_districts):
    census_series = "Census count"
    counted_series = "Census + net completions"
    projected_series = "KPDB projection"
    series_order = [census_series, counted_series, projected_series]
    # Dates are the reference date of each figure: censuses count as of April 1, KPDB's
    # figures are as of year end. The counted series starts at the 2020 census so the lines connect.
    housing_points = []
    for _, _row in districts[districts["borocd"].isin(selected_districts.value)].iterrows():
        _label = _row["cdta_name"].split(" (")[0]
        for _date, _units, _series in [
            ("2010-04-01", _row["housing_units_2010_census"], census_series),
            ("2020-04-01", _row["housing_units_2020_census"], census_series),
            ("2020-04-01", _row["housing_units_2020_census"], counted_series),
            ("2020-12-31", _row["housing_units_2020_end"], counted_series),
            ("2024-12-31", _row["housing_units_2024_end"], counted_series),
            ("2024-12-31", _row["housing_units_2024_end"], projected_series),
            ("2034-12-31", _row["projected_housing_units_2034_end"], projected_series),
        ]:
            housing_points.append({"district": _label, "date": _date, "units": _units, "series": _series})
    housing_lines = alt.Chart(alt.Data(values=housing_points)).mark_line(point=True).encode(
        x=alt.X("date:T", title=None, axis=alt.Axis(format="%Y")),
        y=alt.Y("units:Q", title="Housing units", scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", title=None, sort=series_order, scale=alt.Scale(domain=series_order)),
        strokeDash=alt.StrokeDash(
            "series:N",
            title=None,
            sort=series_order,
            scale=alt.Scale(domain=series_order, range=[[1, 0], [1, 0], [6, 4]]),
        ),
        detail="district:N",
        tooltip=[
            "district:N",
            "series:N",
            alt.Tooltip("date:T", format="%b %-d, %Y"),
            alt.Tooltip("units:Q", format=",.0f"),
        ],
    )
    district_labels = alt.Chart(alt.Data(values=housing_points)).mark_text(
        align="left", dx=6, fontSize=11
    ).encode(
        x="date:T",
        y="units:Q",
        text="district:N",
    ).transform_filter(alt.datum.date == "2034-12-31")
    (housing_lines + district_labels).properties(width=600, height=320).configure_view(
        continuousWidth=600
    ).configure_legend(orient="bottom")
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
    mo.ui.table(records, selection=None, label=f"KPDB records with future units ({len(records)})")
    return


@app.cell
def _(mo):
    mo.md("""
    ## Past housing and population growth

    How past population and housing growth move together across the 59 community districts. Joint
    interest areas are left out. All changes run April 2010 to April 2020, the two census dates, unless
    the name says otherwise. Percentages use the same 0-100 scale as the dbt models.

    Housing growth is measured two ways:

    - **Units counted**: the change in census housing units.
    - **Units built**: net completions from the Housing Database (dcp_housing 24Q4), 2010Q2 through
      2020Q1, as a percent of 2010 census units.
    """)
    return


@app.cell
def _(districts):
    growth = districts[~districts["is_joint_interest_area"]].copy()
    growth["population_change_2010_2020"] = (
        growth["population_2020_census"] - growth["population_2010_census"]
    )
    growth["housing_change_2010_2020"] = (
        growth["housing_units_2020_census"] - growth["housing_units_2010_census"]
    )
    # ordered, so rank correlations can use it
    growth["fhgs_growth_cohort_rank"] = growth["fhgs_growth_cohort"].map({"Low": 1, "Medium": 2, "High": 3})
    corr_columns = {
        "population_change_2010_2020_pct": "Population change 2010-2020 (%)",
        "hdb_housing_growth_2010_2020_pct": "Units built 2010-2020 (%)",
        "population_minus_hdb_growth_2010_2020_pts": "Population minus units built 2010-2020 (pts)",
        "housing_change_2010_2020_pct": "Units counted, change 2010-2020 (%)",
        "population_change_2010_2020": "Population change 2010-2020",
        "hdb_net_completed_units_2010q2_2020q1": "Units built 2010-2020",
        "housing_change_2010_2020": "Units counted, change 2010-2020",
        "net_completed_units_2015_2024": "Units built 2015-2024",
        "fhgs_growth_cohort_rank": "FHGS growth cohort (Low 1 to High 3)",
        "fhgs_annual_growth_rate_2016_2025_pct": "FHGS annual growth rate 2016-2025 (%)",
        "fhgs_homes_added_2021_2025": "FHGS homes added 2021-2025",
        "fhgs_target_growth_rate_pct": "FHGS 2030 target growth rate (%)",
        "fhgs_target_new_homes_2030": "FHGS 2030 target new homes",
        "projected_housing_growth_2024_2034_pct": "Projected housing growth 2024-2034 (%)",
        "ahft_rank": "AHFT rank",
    }
    # the measures that tell the population and housing growth story most directly; bold in the matrix
    focus_columns = [
        "population_change_2010_2020_pct",
        "hdb_housing_growth_2010_2020_pct",
        "population_minus_hdb_growth_2010_2020_pts",
    ]
    return corr_columns, focus_columns, growth


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Correlation matrix

    Each cell is the correlation between two district measures. Spearman ranks the districts first, so a
    few outliers can't dominate it. Pearson is the plain linear correlation.

    **Bold labels** are the focus: population change, units built, and the gap between them. All three
    cover the same April 2010 to April 2020 window as a percent of each district's 2010 count, and units
    built is the Housing Database measure the Fair Housing Growth Strategy uses. The rest are context:

    - Absolute counts mostly track how big a district is.
    - Units counted carries census counting differences.
    - The FHGS columns cover 2016-2025, so they don't line up with the census population window.
    """)
    return


@app.cell
def _(mo):
    corr_method = mo.ui.radio(
        {"Pearson": "pearson", "Spearman (rank)": "spearman"},
        value="Spearman (rank)",
        label="Correlation",
        inline=True,
    )
    corr_method
    return (corr_method,)


@app.cell
def _(alt, corr_columns, corr_method, focus_columns, growth, json):
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
        width=620, height=620, title=f"{corr_method.value.title()} correlation, 59 community districts"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Housing measure

    Used by the scatter plot and the growth pressure map below. Units built is the default: it's the same
    Housing Database measure the Fair Housing Growth Strategy uses for its growth rates.

    - **Units built** only counts DOB jobs, net of demolitions, so it measures new supply the same way in
      every district. It misses units that appear or disappear without a DOB job.
    - **Units counted** covers every change in the housing stock, including units added or lost without a
      DOB job. It also picks up any difference in how the 2010 and 2020 censuses counted units.
    """)
    return


@app.cell
def _(mo):
    housing_measures = {
        "Units built (Housing Database)": {
            "growth": "hdb_housing_growth_2010_2020_pct",
            "pressure": "population_minus_hdb_growth_2010_2020_pts",
        },
        "Units counted (census)": {
            "growth": "housing_change_2010_2020_pct",
            "pressure": "population_minus_housing_change_2010_2020_pts",
        },
    }
    housing_measure = mo.ui.radio(
        list(housing_measures), value="Units built (Housing Database)", inline=True
    )
    housing_measure
    return housing_measure, housing_measures


@app.cell
def _(mo):
    mo.md("""
    ### Population vs housing growth

    The dashed line is equal growth. Districts above it added people faster than housing units.
    """)
    return


@app.cell
def _(alt, growth, housing_measure, housing_measures):
    growth_column = housing_measures[housing_measure.value]["growth"]
    alt.Chart(growth).mark_circle(size=70, opacity=0.8).encode(
        x=alt.X(f"{growth_column}:Q", title=f"{housing_measure.value}, 2010-2020 (%)"),
        y=alt.Y("population_change_2010_2020_pct:Q", title="Population change 2010-2020 (%)"),
        color=alt.Color("ahft:N", title=None, scale=alt.Scale(domain=["AHFT district", "Not AHFT"], range=["#d95f02", "#7570b3"])),
        tooltip=[
            alt.Tooltip("cdta_name:N", title="District"),
            alt.Tooltip(f"{growth_column}:Q", title="Housing growth (%)", format=".2f"),
            alt.Tooltip("population_change_2010_2020_pct:Q", title="Population change (%)", format=".2f"),
        ],
    ).properties(width=500, height=380, title=f"Population vs housing growth: {housing_measure.value}") + alt.Chart(
        alt.Data(values=[{"v": -10}, {"v": 40}])
    ).mark_line(color="gray", strokeDash=[4, 4]).encode(x="v:Q", y="v:Q")
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
        x=alt.X("hdb_net_completed_units_2010q2_2020q1:Q", title="Units built, 2010Q2-2020Q1"),
        y=alt.Y("housing_change_2010_2020:Q", title="Units counted, census change 2010-2020"),
        color=alt.Color(
            "population_change_2010_2020_pct:Q",
            title="Population change (%)",
            scale=alt.Scale(domainMid=0, range=["#5e3c99", "#f7f7f7", "#e66101"]),
        ),
        tooltip=[
            alt.Tooltip("cdta_name:N", title="District"),
            alt.Tooltip("hdb_net_completed_units_2010q2_2020q1:Q", title="Units built", format=",.0f"),
            alt.Tooltip("housing_change_2010_2020:Q", title="Units counted", format=",.0f"),
            alt.Tooltip("census_minus_hdb_housing_change_2010_2020:Q", title="Counted minus built", format=",.0f"),
            alt.Tooltip("population_change_2010_2020_pct:Q", title="Population change (%)", format=".2f"),
        ],
    ).properties(width=500, height=380)
    agreement_line = alt.Chart(alt.Data(values=[{"v": -5000}, {"v": 20000}])).mark_line(
        color="gray", strokeDash=[4, 4]
    ).encode(x="v:Q", y="v:Q")
    largest_gaps = (
        growth.reindex(
            growth["census_minus_hdb_housing_change_2010_2020"].abs().sort_values(ascending=False).index
        )
        .head(10)
        .rename(columns={
            "cdta_name": "District",
            "housing_change_2010_2020": "Counted",
            "hdb_net_completed_units_2010q2_2020q1": "Built",
            "census_minus_hdb_housing_change_2010_2020": "Counted minus built",
        })[["District", "Counted", "Built", "Counted minus built"]]
        .reset_index(drop=True)
    )
    mo.vstack([
        units_compare + agreement_line,
        mo.ui.table(largest_gaps, selection=None, show_column_summaries=False, label="Largest gaps"),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Growth pressure

    Each district's population growth minus its housing growth, 2010 to 2020, in percentage points, using
    the housing measure picked above. Red districts added people faster than housing units, blue ones
    added housing faster than people.
    """)
    return


@app.cell
def _(alt, features, growth, housing_measure, housing_measures):
    pressure_column = housing_measures[housing_measure.value]["pressure"]
    gap_values = growth[["borocd", "cdta_name", pressure_column]]
    alt.Chart(alt.Data(values=features)).mark_geoshape(stroke="white", strokeWidth=0.5).transform_lookup(
        lookup="properties.borocd",
        from_=alt.LookupData(gap_values, "borocd", list(gap_values.columns)),
    ).transform_filter(
        alt.datum.cdta_name != None  # noqa: E711 (vega expression, not python)
    ).encode(
        color=alt.Color(
            f"{pressure_column}:Q",
            title="Pop. minus housing growth (pts)",
            scale=alt.Scale(domainMid=0, range=["#2166ac", "#f7f7f7", "#b2182b"]),
        ),
        tooltip=[
            alt.Tooltip("cdta_name:N", title="District"),
            alt.Tooltip(f"{pressure_column}:Q", title="Pop. minus housing growth (pts)", format=".2f"),
        ],
    ).project(type="mercator").properties(
        width=650,
        height=650,
        title=f"Population growth minus housing growth, 2010-2020: {housing_measure.value}",
    )
    return


if __name__ == "__main__":
    app.run()
