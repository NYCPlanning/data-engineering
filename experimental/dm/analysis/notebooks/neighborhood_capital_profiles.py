import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import json

    import altair as alt
    import branca.colormap
    import duckdb
    import folium
    import marimo as mo
    import pandas as pd

    return alt, branca, duckdb, folium, json, mo, pd


@app.cell(hide_code=True)
def _(mo):
    def source_note(text):
        """A small source line under a chart or map, which stays visible in a printout."""
        return mo.md(text).style({"font-size": "0.75rem", "color": "#55554f"})

    return (source_note,)


@app.cell
def _(duckdb, mo):
    db_path = mo.notebook_dir().parent / "data" / "analysis_growth.duckdb"
    core_tables = [
        "dim_community_district",
        "dim_nta",
        "dim_census_tract",
        "mart_community_district_date",
        "mart_nta_date",
        "dim_transit_zone",
        "dim_nta_neighbor",
        "dim_community_district_transit_zone",
        "dim_ms4_drainage_area",
        "dim_ms4_outfall",
        "dim_community_district_ms4_drainage",
        "dim_wastewater_facility",
        "dim_community_district_stormwater_flooding",
        "dim_community_district_stormwater_flooding_frame",
        "dim_community_district_floodplain_2080",
        "dim_borough",
        "dim_tcol_family_type",
        "dim_school_subdistrict",
        "dim_community_district_school_subdistrict",
        "dim_vision_zero_priority_corridor",
        "dim_vision_zero_priority_intersection",
        "dim_street_improvement_project",
    ]
    # copy the core tables into memory and detach, so the notebook doesn't hold a lock that
    # blocks `dbt build`. Rerun this cell to pick up a new build.
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    con.execute("SET geometry_always_xy = true")
    for _statement in [
        f"ATTACH '{db_path}' AS build (READ_ONLY)",
        *(
            f"CREATE TABLE {table} AS SELECT * FROM build.main.{table}"
            for table in core_tables
        ),
        "DETACH build",
    ]:
        con.execute(_statement)
    # every map's GeoJSON: simplified to the given tolerance (degrees), then coordinates rounded to 6 decimals (about
    # 10 cm), which cuts the size by about a third with no visible change
    con.execute("""
        CREATE MACRO map_geojson(geom, tolerance) AS
            ST_AsGeoJSON(ST_ReducePrecision(ST_SimplifyPreserveTopology(geom, tolerance), 0.000001))
    """)
    return (con,)


@app.cell
def _(mo):
    mo.md("""
    # Neighborhood Capital Profiles

    Infrastructure conditions in each AHFT neighborhood, for capital planning.
    """)
    return


@app.cell
def _(con, mo):
    districts = con.sql("""
        SELECT borocd, cdta_name, is_ahft_district, ahft_rank, housing_units_2010_census, housing_units_2024_end,
            projected_net_units_2025_2029, projected_net_units_2030_2034, population_change_2010_2020_pct,
            units_built_2010_2020_pct, land_acres,
            cdta_median_household_income_acs, citywide_median_household_income_acs
        FROM dim_community_district
        WHERE NOT is_joint_interest_area
        ORDER BY is_ahft_district DESC, CASE WHEN is_ahft_district THEN ahft_rank END, borocd
    """).df()
    district_options = {
        f"{row.cdta_name} (AHFT {row.ahft_rank})"
        if row.is_ahft_district
        else row.cdta_name: row.borocd
        for row in districts.itertuples()
    }
    # one selected district, shared by the dropdown and a click on the city map
    get_selected_district, set_selected_district = mo.state(
        next(iter(district_options.values()))
    )
    return (
        district_options,
        districts,
        get_selected_district,
        set_selected_district,
    )


@app.cell
def _(get_selected_district):
    selected_district = get_selected_district()
    return (selected_district,)


@app.cell
def _(district_options, mo, selected_district, set_selected_district):
    # in its own cell: marimo won't rerun a cell from a state change made by its own dropdown
    district_picker = mo.ui.dropdown(
        options=district_options,
        value=next(
            label
            for label, borocd in district_options.items()
            if borocd == selected_district
        ),
        label="Community district",
        full_width=True,
        searchable=True,
        on_change=set_selected_district,
    )
    return (district_picker,)


@app.cell
def _(con, districts, selected_district):
    focus = districts[districts["borocd"] == selected_district].iloc[0]
    # the CDTA name starts with the district's code, e.g. BK18 Canarsie-Flatlands
    district_code = focus["cdta_name"].split(" ", 1)[0]
    # NTAs touching the district's NTAs, corners included, since they're nearby to someone living there
    nearby_nta_codes = [
        code
        for (code,) in con.execute(
            """
            WITH district_ntas AS (
                SELECT nta.nta2020
                FROM dim_nta AS nta
                INNER JOIN dim_community_district AS district ON nta.cdta2020 = district.cdta2020
                WHERE district.borocd = ?
            )

            SELECT DISTINCT neighbor.neighbor_nta2020
            FROM dim_nta_neighbor AS neighbor
            INNER JOIN district_ntas ON neighbor.nta2020 = district_ntas.nta2020
            WHERE neighbor.neighbor_nta2020 NOT IN (SELECT nta2020 FROM district_ntas)
            ORDER BY neighbor.neighbor_nta2020
            """,
            [selected_district],
        ).fetchall()
    ]
    map_area_tracts = con.execute(
        """
        WITH district AS (
            SELECT cdta2020 FROM dim_community_district WHERE borocd = ?
        )

        SELECT
            tract.cdta2020 = district.cdta2020 AS in_district,
            tract.boroct2020,
            tract.tract_label,
            tract.nta_name,
            tract.population_2020_census,
            tract.housing_units_2020_census,
            tract.population_in_households_2020_census,
            tract.occupied_housing_units_2020_census,
            tract.land_acres,
            COLUMNS('^(population_age_|households_|median_household_income_|people_below_poverty_|people_poverty_status_|families_)'),
            tract.people_per_acre,
            tract.average_household_size_2020_census,
            map_geojson(tract.geom, 0.00005) AS geojson
        FROM dim_census_tract AS tract
        CROSS JOIN district
        WHERE tract.cdta2020 = district.cdta2020 OR list_contains(?, tract.nta2020)
        ORDER BY tract.nta_name, tract.boroct2020
        """,
        [selected_district, nearby_nta_codes],
    ).df()
    tracts = map_area_tracts[map_area_tracts["in_district"]].reset_index(drop=True)
    all_ntas = con.execute(
        """
        SELECT nta.* EXCLUDE (geom)
        FROM dim_nta AS nta
        INNER JOIN dim_community_district AS district ON nta.cdta2020 = district.cdta2020
        WHERE district.borocd = ?
        ORDER BY nta.nta_name
        """,
        [selected_district],
    ).df()
    # parks, cemeteries, and airports have almost no housing, so the growth chart leaves them out unless they have projects planned
    timeline_ntas = all_ntas[
        (all_ntas["nta_type_name"] == "Residential")
        | (all_ntas["projected_net_units_2025_2034"] > 0)
    ]
    return (
        all_ntas,
        district_code,
        focus,
        map_area_tracts,
        nearby_nta_codes,
        timeline_ntas,
        tracts,
    )


@app.cell
def _(con):
    # citywide reference values for the charts, summed from every tract
    city_totals = (
        con.sql("""
        SELECT
            sum(COLUMNS('^population_age_')) ,
            sum(population_2020_census) AS population_2020_census,
            sum(COLUMNS('^households_income_')),
            sum(households_acs) AS households_acs
        FROM dim_census_tract
    """)
        .df()
        .iloc[0]
    )
    age_groups = {
        "population_age_under_18_2020_census": "Under 18",
        "population_age_18_24_2020_census": "18-24",
        "population_age_25_44_2020_census": "25-44",
        "population_age_45_64_2020_census": "45-64",
        "population_age_65_plus_2020_census": "65+",
    }
    income_groups = {
        "households_income_under_25k_acs": "Under $25k",
        "households_income_25k_50k_acs": "$25k-$50k",
        "households_income_50k_100k_acs": "$50k-$100k",
        "households_income_100k_200k_acs": "$100k-$200k",
        "households_income_200k_plus_acs": "$200k+",
    }
    return age_groups, city_totals, income_groups


@app.cell
def _(
    alt,
    con,
    district_options,
    district_picker,
    json,
    mo,
    selected_district,
    set_selected_district,
    source_note,
    timeline_ntas,
):
    # simplified so the map stays light, same tolerance as the growth notebook
    city_shapes = con.sql("""
        SELECT
            borocd,
            cdta_name,
            coalesce(is_ahft_district, false) AS is_ahft_district,
            map_geojson(geom, 0.0002) AS geojson
        FROM dim_community_district
    """).fetchall()
    city_features = [
        {
            "type": "Feature",
            "geometry": json.loads(geojson),
            "properties": {
                "borocd": int(borocd),
                "cdta_name": cdta_name,
                "is_ahft_district": is_ahft_district,
                "is_focus": borocd == selected_district,
            },
        }
        for borocd, cdta_name, is_ahft_district, geojson in city_shapes
    ]
    city_base = alt.Chart(alt.Data(values=city_features)).project(type="mercator")
    district_click = alt.selection_point(
        name="district_click", fields=["borocd"], on="click", toggle=False, empty=False
    )
    # Vega's tooltip shows a field titled "title" as a heading, with no label in front of it
    city_tooltip = [alt.Tooltip("properties.cdta_name:N", title="title")]
    city_map_chart = (
        city_base.transform_calculate(borocd="datum.properties.borocd")
        .mark_geoshape(
            fill="#e4e4e0", stroke="white", strokeWidth=0.5, cursor="pointer"
        )
        .encode(tooltip=city_tooltip)
        .add_params(district_click)
        + city_base.transform_filter("datum.properties.is_ahft_district").mark_geoshape(
            fill=None, stroke="#555555", strokeWidth=0.75, strokeJoin="round"
        )
        + city_base.transform_filter("datum.properties.is_focus")
        .mark_geoshape(
            fill="#256abf", stroke="#222222", strokeWidth=1.5, strokeJoin="round"
        )
        .encode(tooltip=city_tooltip)
    ).properties(width=380, height=340)

    def select_clicked_district(_value):
        clicked = (city_map.selections.get("district_click") or {}).get("borocd") or []
        # joint interest areas are on the map but not in the dropdown
        if clicked and clicked[0] in district_options.values():
            set_selected_district(clicked[0])

    city_map = mo.ui.altair_chart(
        city_map_chart,
        chart_selection=False,
        legend_selection=False,
        on_change=select_clicked_district,
    )
    nta_shapes = con.execute(
        """
        SELECT
            nta.nta_name,
            nta.nta_type_name,
            map_geojson(nta.geom, 0.00005) AS geojson,
            ST_X(ST_PointOnSurface(nta.geom)) AS point_lon,
            ST_Y(ST_PointOnSurface(nta.geom)) AS point_lat,
            ST_XMin(nta.geom) AS min_lon,
            ST_XMax(nta.geom) AS max_lon,
            ST_YMin(nta.geom) AS min_lat,
            ST_YMax(nta.geom) AS max_lat
        FROM dim_nta AS nta
        INNER JOIN dim_community_district AS district ON nta.cdta2020 = district.cdta2020
        WHERE district.borocd = ?
        ORDER BY nta.nta_name
        """,
        [selected_district],
    ).df()
    timeline_nta_names = timeline_ntas["nta_name"].tolist()
    nta_features = [
        {
            "type": "Feature",
            "geometry": json.loads(row.geojson),
            "properties": {
                "nta_name": row.nta_name,
                "nta_type_name": row.nta_type_name,
                "on_timeline": row.nta_name in timeline_nta_names,
            },
        }
        for row in nta_shapes.itertuples()
    ]
    # labels stack in a column to the right, ordered north to south so their lines rarely cross
    callouts = (
        nta_shapes[nta_shapes["nta_name"].isin(timeline_nta_names)]
        .sort_values("point_lat", ascending=False)
        .reset_index(drop=True)
    )
    nta_lat_span = nta_shapes["max_lat"].max() - nta_shapes["min_lat"].min()
    callouts["label_lon"] = nta_shapes["max_lon"].max() + 0.15 * (
        nta_shapes["max_lon"].max() - nta_shapes["min_lon"].min()
    )
    callouts["label_lat"] = nta_shapes["max_lat"].max() - nta_lat_span * (
        callouts.index + 0.5
    ) / len(callouts)
    callout_lines = [
        {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [row.point_lon, row.point_lat],
                    [row.label_lon, row.label_lat],
                ],
            },
            "properties": {},
        }
        for row in callouts.itertuples()
    ]
    nta_base = alt.Chart(alt.Data(values=nta_features)).project(type="mercator")
    nta_tooltip = [
        alt.Tooltip("properties.nta_name:N", title="NTA"),
        alt.Tooltip("properties.nta_type_name:N", title="Type"),
    ]
    # same names in the same order as the timeline, so an NTA has the same color in both
    nta_map = (
        nta_base.transform_filter("!datum.properties.on_timeline")
        .mark_geoshape(fill="#e4e4e0", stroke="white", strokeWidth=0.75)
        .encode(tooltip=nta_tooltip)
        + nta_base.transform_filter("datum.properties.on_timeline")
        .mark_geoshape(stroke="white", strokeWidth=0.75)
        .encode(
            color=alt.Color(
                "properties.nta_name:N",
                scale=alt.Scale(domain=timeline_nta_names),
                legend=None,
            ),
            tooltip=nta_tooltip,
        )
        + alt.Chart(alt.Data(values=callout_lines)).mark_geoshape(
            filled=False, stroke="#55554f", strokeWidth=0.75
        )
        + alt.Chart(callouts[["nta_name", "label_lon", "label_lat"]])
        .mark_text(align="left", dx=3, fontSize=11, color="#222222", limit=240)
        .encode(
            longitude="label_lon:Q",
            latitude="label_lat:Q",
            text="nta_name:N",
        )
    ).properties(width=380, height=340)
    # fixed widths, so the maps don't shift when the district name or NTA labels change length
    mo.vstack(
        [
            mo.vstack(
                [
                    mo.vstack([district_picker]).style({"width": "360px"}),
                    mo.hstack(
                        [
                            mo.as_html(city_map).style({"flex": "0 0 400px"}),
                            mo.as_html(nta_map).style(
                                {"flex": "1 1 0", "min-width": "0"}
                            ),
                        ],
                        justify="start",
                        align="start",
                        gap=1,
                    ),
                ]
            ),
            source_note(
                "Sources: DCP community district, CDTA, and NTA boundaries (26c); DCP AHFT ranks (October 2026)"
            ),
        ]
    )
    return


@app.cell
def _(mo):
    mo.vstack(
        [
            mo.md("## Residents"),
            mo.accordion(
                {
                    "Map colors": mo.md("""
    - **Citywide:** a tract's rank among all NYC tracts. Colors compare across districts.
    - **District:** the district's lowest tract to its highest. Colors don't compare across districts.
    - **Nearby:** adds the NTAs touching the district's, with a halo around the district. Always citywide colors.
    - **Gray:** no value (no residents, households, or families, or no estimate).
    - **Lines:** black for NTAs, gray for the district.
    """),
                    "Data notes": mo.md("""
    - Age and density: 2020 census. Income and poverty: 2020-2024 ACS.
    - Poverty rate uses the federal poverty line, the same everywhere in the country.
    - Tracts and NTAs are the ones in the district's CDTA, which can differ a little from the district.
    - Percentages swing where few people live.
    - People per household leaves out group housing, like dorms and nursing homes.
    - Tract income is rough. Hover for the median's margin of error, or use the NTA bars.
    - Taller NTA bars have more people.
    """),
                }
            ),
        ]
    )
    return


@app.cell
def _(age_groups, income_groups, mo):
    map_measures = {
        "People per acre, 2020": "people_per_acre",
        "People per household, 2020": "average_household_size_2020_census",
        "Population, 2020": "population_2020_census",
        "Median household income": "median_household_income_acs",
        "Poverty rate (% of people)": "people_below_poverty_acs",
        "Families earning under $159,197 (%)": "families_income_under_tcol_acs",
        **{
            f"Age {label} (% of residents)": column
            for column, label in age_groups.items()
        },
        **{
            f"Household income {label} (% of households)": column
            for column, label in income_groups.items()
        },
    }
    # each percent measure's base
    rate_denominators = {
        **{column: "population_2020_census" for column in age_groups},
        **{column: "households_acs" for column in income_groups},
        "people_below_poverty_acs": "people_poverty_status_determined_acs",
        "families_income_under_tcol_acs": "families_acs",
    }
    map_measure = mo.ui.dropdown(
        options=map_measures, value="People per acre, 2020", label="Map"
    )
    map_extent = mo.ui.tabs({"District": "", "Nearby": ""})
    map_scale_basis = mo.ui.tabs({"Citywide": "", "District": ""})
    return (
        map_extent,
        map_measure,
        map_measures,
        map_scale_basis,
        rate_denominators,
    )


@app.cell(hide_code=True)
def _(map_extent, map_measure, map_scale_basis, mo):
    # shown here rather than where they're made, since Nearby is always citywide and hides the color scale
    map_controls = [
        map_measure,
        mo.md("Show").style({"margin-left": "1.5rem"}),
        map_extent,
    ]
    if map_extent.value == "District":
        map_controls += [
            mo.md("Color scale").style({"margin-left": "1.5rem"}),
            map_scale_basis,
        ]
    mo.hstack(map_controls, justify="start", align="center", gap=0.5)
    return


@app.cell
def _(con, map_measures, rate_denominators):
    def map_values_for(df, column):
        if column in rate_denominators:
            base = df[rate_denominators[column]]
            return 100 * df[column] / base.where(base > 0)
        population = df["population_2020_census"]
        return df[column].where(population > 0)

    def overall_value(df, column):
        """A place's own figure from its tract totals, so every person counts once however its tracts are drawn."""
        if column in rate_denominators:
            return 100 * df[column].sum() / df[rate_denominators[column]].sum()
        if column == "people_per_acre":
            return df["population_2020_census"].sum() / df["land_acres"].sum()
        if column == "average_household_size_2020_census":
            return (
                df["population_in_households_2020_census"].sum()
                / df["occupied_housing_units_2020_census"].sum()
            )
        raise ValueError(f"{column} has no overall value")

    citywide_tracts = con.sql("""
        SELECT * EXCLUDE (geom) FROM dim_census_tract
    """).df()
    # Citywide colors follow rank, not value: a few tracts (such as college housing and nursing homes) sit far
    # above the rest and would wash out every other tract on a value scale.
    map_citywide_values = {
        column: map_values_for(citywide_tracts, column).dropna().sort_values()
        for column in map_measures.values()
    }
    map_ranges = {
        column: {"min": values.min(), "median": values.median(), "max": values.max()}
        for column, values in map_citywide_values.items()
    }
    return (
        citywide_tracts,
        map_citywide_values,
        map_ranges,
        map_values_for,
        overall_value,
    )


@app.cell
def _(
    age_groups,
    branca,
    citywide_tracts,
    con,
    district_code,
    focus,
    folium,
    income_groups,
    json,
    map_area_tracts,
    map_citywide_values,
    map_extent,
    map_measure,
    map_ranges,
    map_scale_basis,
    map_values_for,
    mo,
    nearby_nta_codes,
    overall_value,
    pd,
    rate_denominators,
    selected_district,
    source_note,
    tracts,
):
    map_column = map_measure.value
    map_values = map_values_for(tracts, map_column)
    is_nearby = map_extent.value == "Nearby"
    is_district_scale = map_scale_basis.value == "District" and not is_nearby
    map_citywide = map_citywide_values[map_column]

    def citywide_rank(values):
        """Share of NYC tracts at or below each value. searchsorted puts missing values last, so put them back."""
        return pd.Series(
            100 * map_citywide.searchsorted(values, side="right") / len(map_citywide),
            index=values.index,
        ).where(values.notna())

    map_ranks = citywide_rank(map_values)
    shown_tracts = map_area_tracts if is_nearby else tracts
    shown_values = map_values_for(shown_tracts, map_column)
    if map_column in rate_denominators:
        map_format = "{:.1f}%"
    elif map_column == "median_household_income_acs":
        map_format = "${:,.0f}"
    elif map_column == "average_household_size_2020_census":
        map_format = "{:,.2f}"
    elif map_column == "population_2020_census":
        map_format = "{:,.0f}"
    else:
        map_format = "{:,.1f}"
    # orange for age, blue for everything else, matching the charts
    map_colors = (
        ["#fbdcc9", "#6b2f0e"] if map_column in age_groups else ["#cde2fb", "#0d366b"]
    )
    map_scale = branca.colormap.LinearColormap(
        map_colors,
        vmin=float(map_values.min()) if is_district_scale else 0,
        vmax=float(map_values.max()) if is_district_scale else 100,
    )

    # one axis for both settings, spaced by rank among NYC tracts, so the district setting shows where its
    # tracts sit in the city. Only the part the colors span is colored, so one color range shows at a time.
    # a district holding the city's lowest value starts at the bar's end, even when other tracts tie that value
    legend_from = (
        float(map_ranks.min())
        if is_district_scale and map_values.min() > map_citywide.min()
        else 0
    )
    legend_to = float(map_ranks.max()) if is_district_scale else 100
    # district colors follow value but this axis follows rank, so sample the gradient to match the map
    legend_gradient = (
        ", ".join(
            map_scale(
                map_citywide.quantile(
                    (legend_from + step * (legend_to - legend_from) / 10) / 100
                )
            )
            for step in range(11)
        )
        if is_district_scale
        else ", ".join(map_colors)
    )
    if map_column == "median_household_income_acs":
        # medians can't be built from tracts, so these are PFF's own
        tick_meaning = "Whole area"
        district_label, district_value = "", focus["cdta_median_household_income_acs"]
        nyc_label, nyc_value = "", focus["citywide_median_household_income_acs"]
    elif map_column == "population_2020_census":
        # a place's total doesn't fit on a scale of tract populations
        tick_meaning = None
        district_label, district_value = "Average tract", map_values.mean()
        nyc_label, nyc_value = "Average tract", map_citywide.mean()
    else:
        tick_meaning = "Whole area"
        district_label, district_value = "", overall_value(tracts, map_column)
        nyc_label, nyc_value = "", overall_value(citywide_tracts, map_column)
    legend_district = (
        100
        * map_citywide.searchsorted(float(district_value), side="right")
        / len(map_citywide)
    )
    legend_nyc = (
        100
        * map_citywide.searchsorted(float(nyc_value), side="right")
        / len(map_citywide)
    )

    def legend_label(position, text):
        """A label over a spot on the axis, shifted just enough to stay within the bar."""
        return (
            f'<span style="position:absolute;left:{position:.2f}%;transform:translateX(-{position:.2f}%);'
            f'white-space:nowrap">{text}</span>'
        )

    def legend_key(text):
        return f'<span style="position:absolute;right:100%;margin-right:30px;font-weight:700">{text}</span>'

    legend_low_text = map_format.format(map_scale.vmin)
    legend_high_text = map_format.format(map_scale.vmax)
    # far enough apart for the text (rough % of the axis)
    legend_min_gap = len(legend_low_text) + len(legend_high_text) + 2
    legend_low = min(legend_from, 100 - legend_min_gap)
    legend_high = max(legend_to, legend_low + legend_min_gap)
    district_ends = (
        f"""
          <span style="position:absolute;left:{legend_low:.2f}%;transform:translateX(-50%);white-space:nowrap;color:#55554f">{legend_low_text}</span>
          <span style="position:absolute;left:{legend_high:.2f}%;transform:translateX(-50%);white-space:nowrap;color:#55554f">{legend_high_text}</span>"""
        if is_district_scale
        else ""
    )

    def legend_symbol(symbol_style, text):
        """A symbol and what it means."""
        return (
            '<div style="display:flex;align-items:center;gap:5px;white-space:nowrap;color:#55554f">'
            f'<span style="{symbol_style}"></span>{text}</div>'
        )

    legend_symbols = legend_symbol(
        f"width:16px;height:8px;background:linear-gradient(to right, {', '.join(map_colors)})",
        "Tract",
    )
    if tick_meaning:
        legend_symbols += legend_symbol(
            "width:2px;height:12px;background:#222222", tick_meaning
        )
    map_legend = f"""
    <div style="position:fixed;bottom:10px;left:10px;z-index:1000;pointer-events:none;background:rgba(255,255,255,0.92);
      padding:6px 10px;border-radius:4px;font:11px sans-serif;color:#222222;box-shadow:0 1px 4px rgba(0,0,0,0.3)">
      <div style="display:flex;justify-content:space-between;gap:16px;margin-bottom:4px">
        <span style="font-weight:600;white-space:nowrap">{map_measure.selected_key} · {district_code}</span>
        <div style="display:flex;gap:10px">{legend_symbols}</div>
      </div>
      <div style="width:280px;margin:0 26px 0 74px">
        <div style="position:relative;height:15px">
          {legend_label(legend_district, f"{district_label} <b>{map_format.format(district_value)}</b>")}
        </div>
        <div style="position:relative;height:15px">
          {legend_key("District")}{district_ends}
        </div>
        <div style="position:relative;height:12px;background:#d8d8d4">
          <div style="position:absolute;left:{legend_from:.2f}%;width:{legend_to - legend_from:.2f}%;height:100%;
            background:linear-gradient(to right, {legend_gradient})"></div>
          <div style="position:absolute;left:{legend_nyc:.2f}%;top:-2px;bottom:-12px;width:2px;margin-left:-1px;background:#222222"></div>
          <div style="position:absolute;left:{legend_district:.2f}%;top:-12px;bottom:-2px;width:2px;margin-left:-1px;background:#222222"></div>
        </div>
        <div style="position:relative;height:30px">
          {legend_key("NYC")}
          <span style="position:absolute;left:0;transform:translateX(-50%);white-space:nowrap;color:#55554f">{map_format.format(map_ranges[map_column]["min"])}</span>
          <span style="position:absolute;left:100%;transform:translateX(-50%);white-space:nowrap;color:#55554f">{map_format.format(map_ranges[map_column]["max"])}</span>
          <span style="position:absolute;left:0;right:0;top:16px;height:15px">{legend_label(legend_nyc, f"{nyc_label} <b>{map_format.format(nyc_value)}</b>")}</span>
        </div>
      </div>
    </div>
    """

    def tract_style(feature):
        map_value = feature["properties"]["color_value"]
        return {
            "fillColor": "#8c8c88" if map_value is None else map_scale(map_value),
            "fillOpacity": 0.3 if map_value is None else 0.65,
            "color": "white",
            "weight": 0.75,
        }

    tract_geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": json.loads(row.geojson),
                "properties": {
                    "tract": row.tract_label,
                    "nta": row.nta_name,
                    "color_value": None
                    if map_value != map_value
                    else float(map_value if is_district_scale else map_rank),
                    "map_label": "no value"
                    if map_value != map_value
                    else map_format.format(map_value)
                    + (
                        ""
                        if is_district_scale
                        else f" (at or above {map_rank:.0f}% of NYC tracts)"
                    ),
                    "population_label": f"{row.population_2020_census:,.0f}",
                    "housing_units_label": f"{row.housing_units_2020_census:,.0f}",
                    "people_per_acre_label": f"{row.people_per_acre:,.1f}",
                    "people_per_household_label": "no value"
                    if row.average_household_size_2020_census
                    != row.average_household_size_2020_census
                    else f"{row.average_household_size_2020_census:,.2f}",
                    "income_label": "no estimate"
                    if row.median_household_income_acs
                    != row.median_household_income_acs
                    else f"${row.median_household_income_acs:,.0f} ± ${row.median_household_income_acs_moe:,.0f}",
                },
            }
            for row, map_value, map_rank in zip(
                shown_tracts.itertuples(), shown_values, citywide_rank(shown_values)
            )
        ],
    }
    # same tolerance as the tracts, so the two line up where they share a boundary
    focus_outline = con.execute(
        """
        SELECT map_geojson(geom, 0.00005)
        FROM dim_community_district
        WHERE borocd = ?
        """,
        [selected_district],
    ).fetchall()[0][0]
    # the edge of the tracts being profiled, which can differ a little from the district's
    cdta_outline = con.execute(
        """
        SELECT map_geojson(ST_Union_Agg(nta.geom), 0.00005)
        FROM dim_nta AS nta
        INNER JOIN dim_community_district AS district ON nta.cdta2020 = district.cdta2020
        WHERE district.borocd = ?
        """,
        [selected_district],
    ).fetchall()[0][0]
    nta_outlines = con.execute(
        """
        SELECT map_geojson(nta.geom, 0.00005)
        FROM dim_nta AS nta
        INNER JOIN dim_community_district AS district ON district.borocd = ?
        WHERE nta.cdta2020 = district.cdta2020 OR list_contains(?, nta.nta2020)
        """,
        [selected_district, nearby_nta_codes if is_nearby else []],
    ).fetchall()

    tract_layer = folium.GeoJson(
        tract_geojson,
        name="Tracts",
        style_function=tract_style,
        tooltip=folium.GeoJsonTooltip(
            fields=[
                "tract",
                "nta",
                "map_label",
                "population_label",
                "housing_units_label",
                "people_per_acre_label",
                "people_per_household_label",
                "income_label",
            ],
            aliases=[
                "Tract",
                "NTA",
                map_measure.selected_key,
                "Population, 2020",
                "Housing units, 2020",
                "People per acre, 2020",
                "People per household, 2020",
                "Median household income",
            ],
        ),
    )
    # quarter zoom steps, so fitting the tracts doesn't stop a whole step short
    focus_map = folium.Map(tiles=None, zoom_snap=0.25)
    # basemaps that don't need an API key, picked from the layer control. Added here rather than
    # through Map's tiles, which would cap every basemap at the first one's max zoom
    for basemap_name, provider in {
        "light gray": "Esri.WorldGrayCanvas",
        "OpenStreetMap": "OpenStreetMap.Mapnik",
        "satellite": "Esri.WorldImagery",
    }.items():
        folium.TileLayer(
            provider, name=basemap_name, show=basemap_name == "light gray"
        ).add_to(focus_map)
    tract_layer.add_to(focus_map)
    # Nearby outlines the CDTA with a halo and a line on top, since every CDTA edge is also an NTA line
    # there. The tracts keep their real colors, so values still compare across it.
    if is_nearby:
        folium.GeoJson(
            json.loads(cdta_outline),
            control=False,
            interactive=False,
            style_function=lambda _feature: {
                "fill": False,
                "color": "#222222",
                "weight": 6,
                "opacity": 0.45,
            },
        ).add_to(focus_map)
    else:
        folium.GeoJson(
            json.loads(focus_outline),
            name="District boundary",
            interactive=False,
            style_function=lambda _feature: {
                "fill": False,
                "color": "#bdbdb8",
                "weight": 2,
            },
        ).add_to(focus_map)
    folium.GeoJson(
        {
            "type": "GeometryCollection",
            "geometries": [json.loads(geojson) for (geojson,) in nta_outlines],
        },
        name="NTA boundaries",
        interactive=False,
        style_function=lambda _feature: {
            "fill": False,
            "color": "#222222",
            "weight": 1.5,
        },
    ).add_to(focus_map)
    if is_nearby:
        folium.GeoJson(
            json.loads(cdta_outline),
            name="CDTA boundary",
            interactive=False,
            style_function=lambda _feature: {
                "fill": False,
                "color": "#222222",
                "weight": 3,
            },
        ).add_to(focus_map)
    focus_map.get_root().html.add_child(folium.Element(map_legend))
    folium.LayerControl().add_to(focus_map)
    focus_map.fit_bounds(tract_layer.get_bounds())

    is_acs_measure = map_column.endswith("_acs") or map_column in income_groups
    is_tcol_measure = map_column == "families_income_under_tcol_acs"
    map_notes = []
    if is_tcol_measure:
        tcol_borough = con.execute(
            "SELECT boro_name, tcol_pct_not_meeting FROM dim_borough WHERE boro_code = ?",
            [selected_district // 100],
        ).fetchall()[0]
        tcol_lowest, tcol_highest = con.execute(
            "SELECT min(median_cost), max(median_cost) FROM dim_tcol_family_type"
        ).fetchall()[0]
        map_notes = [
            mo.accordion(
                {
                    "True Cost of Living": mo.md(f"""
    - $159,197 is NYC's TCOL threshold for families with children and adults under 65 (2022 dollars).
    - Used here for every family. Thresholds by family type run from ${tcol_lowest:,} to ${tcol_highest:,}.
    - Families only: people living alone or with non-relatives aren't counted.
    - ACS income is before taxes. TCOL compares resources after taxes, including benefits.
    - ACS income is in 2024 dollars and the threshold in 2022 dollars, so this share runs a little low.
    - {tcol_borough[0]}: {tcol_borough[1]:.1f}% of residents fall short of their TCOL threshold. The report stops at boroughs.
    """)
                }
            )
        ]

    # folium's own output is sized by aspect ratio and cuts off the legend, so fix the height
    mo.vstack(
        [
            mo.iframe(focus_map.get_root().render(), height="600px"),
            source_note(
                ("Sources: 2020-2024 ACS" if is_acs_measure else "Sources: 2020 Census")
                + ", via DCP Population FactFinder; "
                + ("NYC True Cost of Living report (2026); " if is_tcol_measure else "")
                + "DCP census tract and NTA boundaries (26c)"
            ),
            *map_notes,
        ]
    )
    return (focus_outline,)


@app.cell
def _(all_ntas, alt, city_totals, pd, tracts):
    def place_mix_chart(
        group_labels, total_column, colors, group_title, people, title, show_places=True
    ):
        """Stacked shares for NYC, the district, and every NTA in it. An NTA with nobody to share out gets a labeled empty row."""
        group_columns = list(group_labels)
        columns = [*group_columns, total_column]
        rows = pd.DataFrame(
            [
                {
                    "place": "New York City",
                    "section": "city",
                    "note": None,
                    **city_totals[columns].to_dict(),
                },
                {
                    "place": "District (CDTA)",
                    "section": "district",
                    "note": None,
                    **tracts[columns].sum().to_dict(),
                },
                *[
                    {
                        "place": row["nta_name"],
                        "section": "nta",
                        "note": f"No {people} · {row['nta_type_name']}",
                        **row[columns].to_dict(),
                    }
                    for _, row in all_ntas.iterrows()
                ],
            ]
        )
        mix = (
            rows[rows[total_column] > 0]
            .drop(columns="note")
            .melt(
                id_vars=["place", "section", total_column],
                value_vars=group_columns,
                var_name="column",
                value_name="count",
            )
        )
        mix["group"] = mix["column"].map(group_labels)
        mix["share"] = 100 * mix["count"] / mix[total_column]
        mix["group_order"] = mix["column"].map(
            {column: i for i, column in enumerate(group_columns)}
        )
        # taller means more people. Square root rather than proportional, and with a minimum, so the mix in a
        # small neighborhood stays readable. Heights compare NTAs, so NYC and the district stay full height.
        largest_nta = rows.loc[rows["section"] == "nta", total_column].max()
        mix["bar_height"] = (
            (24 * (mix[total_column] / largest_nta) ** 0.5)
            .clip(lower=4)
            .where(mix["section"] == "nta", 24)
        )
        mix["share_label"] = mix["share"].map("{:.1f}%".format)
        mix["count_label"] = [
            f"{row.count:,.0f} of {getattr(row, total_column):,.0f}"
            for row in mix.itertuples()
        ]
        empty_rows = rows[rows[total_column] == 0][["place", "section", "note"]].assign(
            start=0, end=100, middle=50
        )
        # each borrows a real place, since a row with no place would add a blank row to its panel
        gridlines = pd.DataFrame(
            [
                {"section": section, "place": place, "gridline": x}
                for section, place in rows.groupby("section")["place"].first().items()
                for x in [25, 50, 75]
            ]
        )
        share_title = f"Share of {people} (%)"
        y = alt.Y(
            "place:N",
            title=None,
            sort=rows["place"].tolist(),
            axis=alt.Axis(
                labels=show_places,
                ticks=False,
                domain=False,
                labelLimit=220,
                labelFontWeight=alt.expr(
                    "indexof(['New York City', 'District (CDTA)'], datum.value) >= 0 ? 'normal' : 'bold'"
                ),
            ),
        )
        bars = (
            alt.Chart()
            .transform_filter("isValid(datum.share)")
            .mark_bar(stroke="white", strokeWidth=1)
            .encode(
                x=alt.X(
                    "share:Q",
                    stack="zero",
                    title=share_title,
                    scale=alt.Scale(domain=[0, 100]),
                    # labels and long ticks every 25, short ticks every 5, the finest spacing that lands on every 25
                    axis=alt.Axis(
                        values=list(range(0, 101, 5)),
                        # Vega otherwise thins the values above to fit its default tick count
                        tickCount=21,
                        labelExpr="datum.value % 25 === 0 ? format(datum.value, 'd') : ''",
                        labelOverlap=False,
                        tickSize=alt.expr("datum.value % 25 === 0 ? 6 : 3"),
                        grid=False,
                    ),
                ),
                y=y,
                size=alt.Size("bar_height:Q", scale=None, legend=None),
                color=alt.Color(
                    "group:N",
                    title=group_title,
                    sort=list(group_labels.values()),
                    scale=alt.Scale(domain=list(group_labels.values()), range=colors),
                    legend=alt.Legend(orient="bottom", direction="horizontal"),
                ),
                order=alt.Order("group_order:Q"),
            )
        )
        # thin bars are hard to hover, so each segment gets an invisible full-height target
        hover_targets = (
            alt.Chart()
            .transform_filter("isValid(datum.share)")
            .mark_bar(opacity=0, size=24)
            .encode(
                x=alt.X("share:Q", stack="zero", title=share_title),
                y=y,
                detail="group:N",
                order=alt.Order("group_order:Q"),
                # Vega's tooltip shows a field titled "title" as a bold heading, and a
                # blank title leaves the count with no label in front of it
                tooltip=[
                    alt.Tooltip("share_label:N", title="title"),
                    alt.Tooltip("count_label:N", title=" "),
                ],
            )
        )
        empty_track = (
            alt.Chart()
            .transform_filter("isValid(datum.note)")
            .mark_bar(color="#f0f0ec")
            .encode(x=alt.X("start:Q", title=share_title), x2="end:Q", y=y)
        )
        empty_note = (
            alt.Chart()
            .transform_filter("isValid(datum.note)")
            .mark_text(fontStyle="italic", color="#6b6b67")
            .encode(x=alt.X("middle:Q", title=share_title), y=y, text="note:N")
        )
        grid = (
            alt.Chart()
            .transform_filter("isValid(datum.gridline)")
            .mark_rule(color="#d8d8d4", strokeWidth=1)
            .encode(x=alt.X("gridline:Q", title=share_title))
        )
        # separate panels so the gaps show the city, the district, and the NTAs are different kinds of rows
        return (
            alt.layer(
                empty_track,
                grid,
                bars,
                hover_targets,
                empty_note,
                data=pd.concat([mix, empty_rows, gridlines]),
            )
            .properties(width=340, height=alt.Step(26))
            .facet(
                row=alt.Row(
                    "section:N",
                    sort=["city", "district", "nta"],
                    title=None,
                    header=alt.Header(labels=False),
                ),
                spacing=8,
                title=title,
            )
            .resolve_scale(y="independent")
            .configure_view(stroke=None)
        )

    return (place_mix_chart,)


@app.cell
def _(age_groups, income_groups, mo, place_mix_chart, source_note):
    # side by side, with the same rows in the same order, so only the left chart needs the place names
    mo.vstack(
        [
            mo.hstack(
                [
                    place_mix_chart(
                        age_groups,
                        "population_2020_census",
                        ["#f5a06c", "#e8793f", "#c95f24", "#9e4818", "#6b2f0e"],
                        "Age",
                        "residents",
                        "Residents by age",
                    ),
                    place_mix_chart(
                        income_groups,
                        "households_acs",
                        ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#0d366b"],
                        "Household income",
                        "households",
                        "Households by income",
                        show_places=False,
                    ),
                ],
                justify="start",
                align="start",
                gap=1,
            ),
            source_note(
                "Sources: 2020 Census (age) and 2020-2024 ACS (income), via DCP Population FactFinder"
            ),
        ]
    )
    return


@app.cell
def _(focus, mo):
    mo.vstack(
        [
            mo.md(f"""
    ## Growth

    - Population change, 2010-2020: **{focus["population_change_2010_2020_pct"]:+.1f}%**
    - Housing built 2010-2020, added to the stock: **{focus["units_built_2010_2020_pct"]:.1f}%**
    """),
            mo.accordion(
                {
                    "Notes": mo.md("""
    - **Solid lines:** the 2020 census count, plus units built after it and minus units built before it (Housing Database).
    - **Hollow dots:** 2010 census counts. A dot off its line means the 2010 count doesn't match construction.
    - **Net units added:** new units minus units removed.
    - **Dashed lines:** KPDB's projected units added to the end-of-2024 count. KPDB groups projects by status, not date, so 2029 and 2034 are rough labels.
    - An NTA's dashed line is often mostly one project.
    - The NTAs tab shows a non-residential NTA only if it has projected units.
    """),
                }
            ),
        ]
    )
    return


@app.cell
def _(alt, con, focus, mo, pd, selected_district, source_note, timeline_ntas):
    import warnings

    # a hover selection shared by the three panels is the intended use, not a mistake
    warnings.filterwarnings(
        "ignore", message="Automatically deduplicated selection parameter"
    )
    built_series = "2020 census ± units built"
    projected_series = "KPDB projection"
    district_built = (
        con.execute(
            """
        SELECT strftime(as_of_date, '%Y-%m-%d') AS date, housing_units_from_2020_census AS units
        FROM mart_community_district_date
        WHERE borocd = ?
        ORDER BY as_of_date
        """,
            [selected_district],
        )
        .df()
        .assign(series=focus["cdta_name"], kind=built_series)
    )

    def projection_points(series, units_2024_end, first_phase, second_phase):
        return pd.DataFrame(
            {
                "date": ["2024-12-31", "2029-12-31", "2034-12-31"],
                "units": [
                    units_2024_end,
                    units_2024_end + first_phase,
                    units_2024_end + first_phase + second_phase,
                ],
                "series": series,
                "kind": projected_series,
            }
        )

    district_projection = projection_points(
        focus["cdta_name"],
        focus["housing_units_2024_end"],
        focus["projected_net_units_2025_2029"],
        focus["projected_net_units_2030_2034"],
    )
    nta_built = (
        con.execute(
            """
        SELECT nta_name AS series, strftime(as_of_date, '%Y-%m-%d') AS date, housing_units_from_2020_census AS units
        FROM mart_nta_date
        WHERE nta2020 IN (SELECT unnest(?::varchar[]))
        ORDER BY nta_name, as_of_date
        """,
            [timeline_ntas["nta2020"].tolist()],
        )
        .df()
        .assign(kind=built_series)
    )
    nta_lines = pd.concat(
        [
            nta_built,
            *[
                projection_points(
                    row.nta_name,
                    row.housing_units_2024_end,
                    row.projected_net_units_2025_2029,
                    row.projected_net_units_2030_2034,
                )
                for row in timeline_ntas.itertuples()
            ],
        ]
    )

    def with_growth_measures(lines, start_units, land_acres):
        return lines.assign(
            # its own text column, since the chart parses date into a timestamp, which can also show as the day before
            as_of=pd.to_datetime(lines["date"]).dt.strftime("%b %-d, %Y"),
            units_added=lines["units"] - start_units,
            pct_change=100 * (lines["units"] / start_units - 1),
            # not a percent change, which would match pct_change because land area is fixed
            units_per_acre_added=(lines["units"] - start_units) / land_acres,
        )

    district_housing = with_growth_measures(
        pd.concat([district_built, district_projection]),
        district_built["units"].iloc[0],
        focus["land_acres"],
    )
    nta_housing = with_growth_measures(
        nta_lines,
        nta_lines["series"].map(nta_built.groupby("series")["units"].first()),
        nta_lines["series"].map(timeline_ntas.set_index("nta_name")["land_acres"]),
    )

    # a percent of a count that goes to zero or below means nothing
    nta_housing["pct_change"] = nta_housing["pct_change"].where(
        nta_housing.groupby("series")["units"].transform("min") > 0
    )

    def off_scale_series(lines):
        """Lines to let run off the top: they more than double, and reach over 10 times the next-highest line."""
        peaks = (
            lines.groupby("series")["pct_change"]
            .max()
            .dropna()
            .sort_values(ascending=False)
        )
        off_scale = {}
        while (
            len(peaks) > 1
            and peaks.iloc[0] > 100
            and peaks.iloc[0] > 10 * max(peaks.iloc[1], 0)
        ):
            off_scale[peaks.index[0]] = peaks.iloc[0]
            peaks = peaks.iloc[1:]
        return off_scale, peaks.iloc[0] if len(peaks) else 0

    def legend_columns(labels, available_width=640):
        """The most columns that fit across the chart, since Vega legends don't wrap on their own."""

        def row_width(columns):
            # about 6 pixels per character, plus each entry's symbol and padding
            return sum(
                30 + 6 * max(len(label) for label in labels[i::columns])
                for i in range(columns)
            )

        return max(
            (
                columns
                for columns in range(1, len(labels) + 1)
                if row_width(columns) <= available_width
            ),
            default=1,
        )

    def housing_timeline(lines, census_2010, series_order, end_year):
        # as text, so a missing percent reads n/a rather than null and a tiny loss doesn't read -0.00
        lines = lines.assign(
            pct_change_label=lines["pct_change"].map(
                lambda value: "n/a" if pd.isna(value) else f"{round(value, 1) + 0:+.1f}"
            ),
            units_per_acre_added_label=lines["units_per_acre_added"].map(
                lambda value: f"{round(value, 2) + 0:+.2f}"
            ),
        )
        year_ticks = [alt.DateTime(year=year) for year in range(2010, end_year + 1, 5)]
        # one hover shared by every panel, so a date lines up across all three
        hover = alt.selection_point(
            fields=["date"],
            on="pointerover",
            nearest=True,
            clear="pointerout",
            empty=False,
        )

        def panel(column, title, axis_format, is_bottom=False, zero=True, y_top=None):
            x = alt.X(
                "date:T",
                title=None,
                scale=alt.Scale(
                    domain=[alt.DateTime(year=2010), alt.DateTime(year=end_year)]
                ),
                axis=alt.Axis(
                    values=year_ticks, format="%Y", labels=is_bottom, ticks=is_bottom
                ),
            )
            # a fixed width for the y labels, so the three plots line up
            y_scale = (
                alt.Scale(zero=zero)
                if y_top is None
                else alt.Scale(zero=zero, domainMax=y_top, nice=False)
            )
            y = alt.Y(
                f"{column}:Q",
                title=None,
                scale=y_scale,
                axis=alt.Axis(format=axis_format, minExtent=56, maxExtent=56),
            )
            marks = alt.Chart(lines).encode(
                x=x,
                y=y,
                color=alt.Color(
                    "series:N",
                    title=None,
                    sort=series_order,
                    scale=alt.Scale(domain=series_order),
                    legend=alt.Legend(
                        orient="bottom",
                        columns=legend_columns(series_order),
                        labelLimit=320,
                    ),
                ),
                strokeDash=alt.StrokeDash(
                    "kind:N",
                    title=None,
                    scale=alt.Scale(
                        domain=[built_series, projected_series], range=[[1, 0], [6, 4]]
                    ),
                    legend=None,
                ),
            )
            hover_rule = (
                alt.Chart(lines)
                .transform_filter(hover)
                .mark_rule(color="#8c8c88")
                .encode(x=alt.X("date:T", title=None))
            )
            points = (
                marks.mark_point(filled=True, size=28, opacity=1, clip=True)
                .encode(
                    tooltip=[
                        alt.Tooltip("series:N", title="Place"),
                        alt.Tooltip("kind:N", title="Line"),
                        alt.Tooltip("as_of:N", title="As of"),
                        alt.Tooltip("units:Q", title="Total units", format=",.0f"),
                        alt.Tooltip(
                            "units_added:Q", title="Net units added", format="+,.0f"
                        ),
                        alt.Tooltip("pct_change_label:N", title="Change (%)"),
                        alt.Tooltip(
                            "units_per_acre_added_label:N", title="Units per acre added"
                        ),
                    ],
                )
                .add_params(hover)
            )
            return alt.layer(hover_rule, marks.mark_line(clip=True), points).properties(
                width=600,
                height=120,
                title=alt.TitleParams(title, anchor="start", fontSize=12),
            )

        census_dots = (
            alt.Chart(census_2010)
            .mark_point(filled=False, size=60)
            .encode(
                x="date:T",
                y="units:Q",
                color=alt.Color(
                    "series:N", sort=series_order, scale=alt.Scale(domain=series_order)
                ),
                tooltip=[
                    alt.Tooltip("series:N", title="Place"),
                    alt.Tooltip("units:Q", title="2010 census", format=",.0f"),
                ],
            )
        )
        off_scale, highest_on_scale = off_scale_series(lines)
        change_panel = panel(
            "pct_change",
            "Change since April 2010 (%)",
            "+.0f",
            y_top=1.1 * highest_on_scale if off_scale else None,
        )
        # labeled at the top of the panel, since their lines leave it
        for i, (series, peak) in enumerate(off_scale.items()):
            change_panel += (
                alt.Chart(
                    pd.DataFrame(
                        {
                            "series": [series],
                            "label": [f"↑ {series}: {peak:+,.0f}%, off the chart"],
                        }
                    )
                )
                .mark_text(align="left", fontSize=11, fontWeight="bold")
                .encode(
                    x=alt.value(6),
                    y=alt.value(10 + 14 * i),
                    text="label:N",
                    color=alt.Color(
                        "series:N",
                        sort=series_order,
                        scale=alt.Scale(domain=series_order),
                    ),
                )
            )
        return alt.vconcat(
            panel("units", "Total housing units", ",.0f", zero=False) + census_dots,
            panel("units_added", "Net units added since April 2010", "+,.0f"),
            change_panel,
            panel(
                "units_per_acre_added",
                "Net units added per acre of land since April 2010",
                "+.1f",
                is_bottom=True,
            ),
            spacing=10,
        )

    mo.vstack(
        [
            mo.ui.tabs(
                {
                    "District": housing_timeline(
                        district_housing,
                        pd.DataFrame(
                            {
                                "date": ["2010-04-01"],
                                "units": [focus["housing_units_2010_census"]],
                                "series": [focus["cdta_name"]],
                            }
                        ),
                        [focus["cdta_name"]],
                        2035,
                    ),
                    "NTAs": housing_timeline(
                        nta_housing,
                        pd.DataFrame(
                            {
                                "date": "2010-04-01",
                                "units": timeline_ntas["housing_units_2010_census"],
                                "series": timeline_ntas["nta_name"],
                            }
                        ),
                        timeline_ntas["nta_name"].tolist(),
                        2035,
                    ),
                }
            ),
            source_note(
                "Sources: DCP Housing Database (26Q2); 2010 and 2020 Census, via DCP Population FactFinder; DCP Known Projects Database (October 2026)"
            ),
        ]
    )
    return


@app.cell
def _(mo):
    mo.vstack(
        [
            mo.md("""
    ## Infrastructure

    ### Transportation

    #### TODO Transit services

    W drive, CAPS Basemaps, ...

    #### TODO Transit Access

    W drive, CAPS Basemaps, ...

    #### Transit Zones
    """),
            mo.accordion(
                {
                    "Notes": mo.md("""
    - Zones set parking requirements for new buildings.
    - None in Manhattan Core, Long Island City, and the Inner Transit Zone. Lower in the Outer Transit Zone.
    - 25-241 areas use the rules for beyond the Greater Transit Zone, except for affordable and senior housing.
    - Source: [DCP's Greater Transit Zone page](https://www.nyc.gov/site/planning/data-maps/open-data/dwn-greater-transit-zones.page).
    """),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(con):
    # whole zones, simplified to about 10 m (a pixel or so at district zoom), so the map can pan anywhere.
    # Built once rather than for each district.
    transit_zone_geojson = con.sql("""
        SELECT transit_zone, map_geojson(geom, 0.0001) AS geojson
        FROM dim_transit_zone
    """).df()
    return (transit_zone_geojson,)


@app.cell(hide_code=True)
def _(
    con,
    district_code,
    focus_outline,
    folium,
    json,
    mo,
    selected_district,
    source_note,
    transit_zone_geojson,
):
    # fewest parking requirements first, following the Zoning Resolution
    transit_zone_order = [
        "Manhattan Core and Long Island City Parking Areas",
        "Inner Transit Zone",
        "Outer Transit Zone",
        "Special 25-241 Parking Provisions",
        "Beyond the Greater Transit Zone",
    ]
    transit_zone_colors = dict(
        zip(transit_zone_order, ["#3f007d", "#6a51a3", "#9e9ac8", "#cbc9e2", "#f2f0f7"])
    )
    transit_zone_shares = dict(
        con.execute(
            """
            SELECT transit_zone, share_of_district
            FROM dim_community_district_transit_zone
            WHERE borocd = ?
            """,
            [selected_district],
        ).fetchall()
    )

    transit_zone_map = folium.Map(tiles="Esri.WorldGrayCanvas", zoom_snap=0.25)
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": json.loads(row.geojson),
                    "properties": {"transit_zone": row.transit_zone},
                }
                for row in transit_zone_geojson.itertuples()
            ],
        },
        name="Transit zones",
        style_function=lambda feature: {
            "fillColor": transit_zone_colors[feature["properties"]["transit_zone"]],
            "fillOpacity": 0.4,
            "color": "white",
            "weight": 1,
        },
        tooltip=folium.GeoJsonTooltip(fields=["transit_zone"], labels=False),
    ).add_to(transit_zone_map)
    district_layer = folium.GeoJson(
        json.loads(focus_outline),
        name="District boundary",
        interactive=False,
        style_function=lambda _feature: {
            "fill": False,
            "color": "#222222",
            "weight": 3,
        },
    ).add_to(transit_zone_map)
    transit_zone_rows = "".join(
        f"""<tr>
          <td><span style="display:inline-block;width:14px;height:10px;background:{color};opacity:0.8;
            outline:1px solid #d8d8d4"></span></td>
          <td style="padding:0 12px 0 4px">{zone}</td>
          <td style="text-align:right;font-weight:700">{transit_zone_shares.get(zone, 0):.0%}</td>
        </tr>"""
        for zone, color in transit_zone_colors.items()
    )
    transit_zone_map.get_root().html.add_child(
        folium.Element(f"""
    <div style="position:fixed;bottom:10px;left:10px;z-index:1000;pointer-events:none;background:rgba(255,255,255,0.92);
      padding:6px 10px;border-radius:4px;font:11px sans-serif;color:#222222;box-shadow:0 1px 4px rgba(0,0,0,0.3)">
      <table style="border-collapse:collapse">
        <tr style="color:#55554f"><td></td><td style="padding:0 12px 4px 4px;font-weight:600;color:#222222">Transit zone, {district_code}</td>
          <td style="padding-bottom:4px">Of district</td></tr>
        {transit_zone_rows}
      </table>
    </div>
    """)
    )
    transit_zone_map.fit_bounds(district_layer.get_bounds())
    mo.vstack(
        [
            mo.iframe(transit_zone_map.get_root().render(), height="500px"),
            source_note("Source: DCP Greater Transit Zones (December 2025)"),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.vstack(
        [
            mo.md("""
    #### Streets
    """),
            mo.accordion(
                {
                    "Notes": mo.md("""
    - Priority corridors: streets in each borough where the most pedestrians were killed or severely injured (KSI) per mile.
    - Priority intersections: intersections in each borough with the highest number of pedestrian KSI.
    - Street Improvement Projects: DOT's operational (non-capital) safety projects, such as signals, markings, and concrete, 2009 to 2026.
    - Legend counts include streets within 50 feet of the district, so streets along its edge count.
    - A priority street has a project when one is within 50 feet, since projects on wide boulevards are often drawn on the other side.
    """),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(con):
    # simplified to about 10 m like the transit zones, with bounding boxes so each map takes only what's near its
    # district. Built once rather than for each district.
    priority_corridor_geojson = con.sql("""
        SELECT ST_XMin(geom) AS xmin, ST_XMax(geom) AS xmax, ST_YMin(geom) AS ymin, ST_YMax(geom) AS ymax,
            map_geojson(geom, 0.0001) AS geojson
        FROM dim_vision_zero_priority_corridor
    """).df()
    priority_intersection_points = con.sql("""
        SELECT priority_intersection_id, ST_X(geom) AS lon, ST_Y(geom) AS lat FROM dim_vision_zero_priority_intersection
    """).df()
    street_project_geojson = con.sql("""
        SELECT street_improvement_project_id, sip_year,
            ST_XMin(geom) AS xmin, ST_XMax(geom) AS xmax, ST_YMin(geom) AS ymin, ST_YMax(geom) AS ymax,
            map_geojson(geom, 0.0001) AS geojson
        FROM dim_street_improvement_project
        WHERE project_type = 'Corridor'
    """).df()
    # one row per point, since folium's GeoJson markers fail on multipoints
    street_project_points = con.sql("""
        SELECT street_improvement_project_id, sip_year, ST_X(point) AS lon, ST_Y(point) AS lat
        FROM (
            SELECT street_improvement_project_id, sip_year, unnest(ST_Dump(geom)).geom AS point
            FROM dim_street_improvement_project
            WHERE project_type = 'Intersection'
        )
    """).df()
    return (
        priority_corridor_geojson,
        priority_intersection_points,
        street_project_geojson,
        street_project_points,
    )


@app.cell(hide_code=True)
def _(
    con,
    district_code,
    focus_outline,
    folium,
    json,
    mo,
    priority_corridor_geojson,
    priority_intersection_points,
    selected_district,
    source_note,
    street_project_geojson,
    street_project_points,
):
    # Vision Zero View's colors: red for priority streets, green for safety projects
    street_colors = {
        "Priority corridor": "#a51c1c",
        "Priority intersection": "#a51c1c",
        "Project, 2021-2026": "#3d8b2f",
        "Project, 2009-2020": "#9ccc84",
    }
    street_counts = con.execute(
        """
        SELECT priority_corridor_miles, priority_intersections, street_improvement_projects_2009_2026,
            street_improvement_projects_2021_2026, priority_corridor_miles_with_project,
            priority_intersections_with_project
        FROM dim_community_district
        WHERE borocd = ?
        """,
        [selected_district],
    ).fetchall()[0]

    # the district's box with half its size again on each side, about what the map shows
    street_box = con.execute(
        """
        SELECT ST_XMin(geom), ST_XMax(geom), ST_YMin(geom), ST_YMax(geom)
        FROM dim_community_district
        WHERE borocd = ?
        """,
        [selected_district],
    ).fetchall()[0]
    _pad = max(street_box[1] - street_box[0], street_box[3] - street_box[2]) / 2

    def near_district(df, x="xmin", x2="xmax", y="ymin", y2="ymax"):
        return df[
            (df[x2] >= street_box[0] - _pad)
            & (df[x] <= street_box[1] + _pad)
            & (df[y2] >= street_box[2] - _pad)
            & (df[y] <= street_box[3] + _pad)
        ]

    def street_project_color(sip_year):
        return street_colors[
            "Project, 2021-2026" if sip_year >= 2021 else "Project, 2009-2020"
        ]

    street_map = folium.Map(tiles="Esri.WorldGrayCanvas", zoom_snap=0.25)
    # priority streets wide underneath, so the projects on them show on top
    folium.GeoJson(
        {
            "type": "GeometryCollection",
            "geometries": [
                json.loads(geojson)
                for geojson in near_district(priority_corridor_geojson)["geojson"]
            ],
        },
        name="Priority corridors",
        interactive=False,
        style_function=lambda _feature: {
            "color": street_colors["Priority corridor"],
            "weight": 6,
            "opacity": 0.8,
        },
    ).add_to(street_map)
    _intersection_layer = folium.FeatureGroup(name="Priority intersections").add_to(
        street_map
    )
    for _row in near_district(
        priority_intersection_points, "lon", "lon", "lat", "lat"
    ).itertuples():
        folium.CircleMarker(
            [_row.lat, _row.lon],
            radius=7,
            color=street_colors["Priority intersection"],
            weight=0,
            fill=True,
            fill_opacity=0.85,
            tooltip=_row.priority_intersection_id.title(),
        ).add_to(_intersection_layer)
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": json.loads(project.geojson),
                    "properties": {
                        "project": project.street_improvement_project_id,
                        "color": street_project_color(project.sip_year),
                    },
                }
                for project in near_district(street_project_geojson).itertuples()
            ],
        },
        name="Street Improvement Projects",
        style_function=lambda feature: {
            "color": feature["properties"]["color"],
            "weight": 2.5,
            "opacity": 0.9,
        },
        tooltip=folium.GeoJsonTooltip(fields=["project"], labels=False),
    ).add_to(street_map)
    _project_point_layer = folium.FeatureGroup(
        name="Street Improvement Project intersections"
    ).add_to(street_map)
    for _point in near_district(
        street_project_points, "lon", "lon", "lat", "lat"
    ).itertuples():
        folium.CircleMarker(
            [_point.lat, _point.lon],
            radius=3.5,
            color=street_project_color(_point.sip_year),
            weight=0,
            fill=True,
            fill_opacity=0.9,
            tooltip=_point.street_improvement_project_id,
        ).add_to(_project_point_layer)
    _district_layer = folium.GeoJson(
        json.loads(focus_outline),
        name="District boundary",
        interactive=False,
        style_function=lambda _feature: {
            "fill": False,
            "color": "#222222",
            "weight": 3,
        },
    ).add_to(street_map)

    def street_symbol(label):
        color = street_colors[label]
        if label == "Priority corridor":
            return f'<span style="display:inline-block;width:16px;height:5px;background:{color};opacity:0.8"></span>'
        if label == "Priority intersection":
            return (
                f'<span style="display:inline-block;width:11px;height:11px;border-radius:50%;background:{color};'
                'opacity:0.85"></span>'
            )
        return f'<span style="display:inline-block;width:16px;height:3px;background:{color}"></span>'

    # in district, and with a project
    street_legend_counts = {
        "Priority corridor": (
            f"{street_counts[0]:.1f} mi",
            f"{street_counts[4]:.1f} mi",
        ),
        "Priority intersection": (f"{street_counts[1]:,}", f"{street_counts[5]:,}"),
        "Project, 2021-2026": (f"{street_counts[3]:,}", ""),
        "Project, 2009-2020": (f"{street_counts[2] - street_counts[3]:,}", ""),
    }
    street_rows = "".join(
        f"""<tr>
          <td style="text-align:center">{street_symbol(label)}</td>
          <td style="padding:0 12px 0 4px">{label}</td>
          <td style="text-align:right;font-weight:700">{count}</td>
          <td style="text-align:right;font-weight:700;padding-left:12px">{with_project}</td>
        </tr>"""
        for label, (count, with_project) in street_legend_counts.items()
    )
    street_map.get_root().html.add_child(
        folium.Element(f"""
    <div style="position:fixed;bottom:10px;left:10px;z-index:1000;pointer-events:none;background:rgba(255,255,255,0.92);
      padding:6px 10px;border-radius:4px;font:11px sans-serif;color:#222222;box-shadow:0 1px 4px rgba(0,0,0,0.3)">
      <table style="border-collapse:collapse">
        <tr style="color:#55554f"><td></td><td style="padding:0 12px 4px 4px;font-weight:600;color:#222222">Street safety, {district_code}</td>
          <td style="padding-bottom:4px">In district</td><td style="padding:0 0 4px 12px">With a project</td></tr>
        {street_rows}
      </table>
    </div>
    """)
    )
    street_map.fit_bounds(_district_layer.get_bounds())
    mo.vstack(
        [
            mo.iframe(street_map.get_root().render(), height="500px"),
            source_note(
                "Source: DOT Vision Zero View, via NYC Open Data (November 2026)"
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.vstack(
        [
            mo.md("""
    ### Wastewater

    #### Wastewater Systems

    """),
            mo.accordion(
                {
                    "Notes": mo.md("""
    - DEP's 14 Wastewater Resource Recovery Facilities (WRRFs).
    - Capacity and functions: DEP's [Map of the New York City Wastewater System](https://www.nyc.gov/assets/dep/downloads/pdf/water/wastewater/nyc-wastewater-system-map.pdf) (2025).
    - Flow: the 12-month running average in DEP's [monthly performance reports](https://data.cityofnewyork.us/d/hgue-hj96).
    """),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(alt, con, mo, source_note):
    # the map table's function columns, in its order and with its names
    wastewater_function_labels = {
        "has_preliminary_treatment": "Preliminary Treatment",
        "has_secondary_treatment": "Secondary Treatment",
        "has_biological_nutrient_removal": "Biological Nutrient Removal (BNR)",
        "has_chlorination": "Chlorination",
        "has_dechlorination": "Dechlorination",
        "has_sludge_dewatering": "Sludge Dewatering",
        "has_sludge_pumping": "Sludge Pumping",
        "has_docks": "Docks",
        "has_gas_to_grid_food_waste": "Gas to Grid / Food Waste",
        "has_cogen_engines": "Cogen Engines",
        "has_title_v_facility": "Title V Facility",
        "has_cso_facilities": "CSO Facilities",
    }
    wastewater_facilities = con.sql(
        "SELECT * FROM dim_wastewater_facility ORDER BY capacity_mgd DESC"
    ).df()
    wastewater_facilities["share_label"] = wastewater_facilities[
        "latest_flow_12_month_average_share_of_capacity"
    ].map("{:.0%}".format)
    _flow_month = wastewater_facilities["latest_report_month"].max().strftime("%B %Y")

    def _facility_y(show_names):
        return alt.Y(
            "wastewater_facility_name:N",
            title=None,
            sort=wastewater_facilities["wastewater_facility_name"].tolist(),
            axis=alt.Axis(labels=show_names, ticks=False, domain=False),
        )

    _tooltip = [
        alt.Tooltip("wastewater_facility_name:N", title="WRRF"),
        alt.Tooltip("borough:N", title="Borough"),
        alt.Tooltip("capacity_mgd:Q", title="Capacity (MGD)"),
        alt.Tooltip("wet_weather_flow_mgd:Q", title="Wet weather flow (MGD)"),
        alt.Tooltip(
            "latest_flow_12_month_average_mgd:Q",
            title=f"12-month average flow, {_flow_month} (MGD)",
        ),
        alt.Tooltip("population_served:Q", title="Population served", format=","),
        alt.Tooltip("drainage_area_acres:Q", title="Drainage area (acres)", format=","),
    ]
    _base = alt.Chart(wastewater_facilities).encode(
        y=_facility_y(True), tooltip=_tooltip
    )
    _flow_chart = alt.layer(
        _base.mark_bar(color="#e3e3df", height=16).encode(
            x=alt.X("capacity_mgd:Q", title="Million gallons per day (MGD)")
        ),
        _base.mark_bar(color="#3182bd", height=8).encode(
            x="latest_flow_12_month_average_mgd:Q"
        ),
        _base.mark_text(align="left", dx=4, fontSize=11, color="#55554f").encode(
            x="capacity_mgd:Q", text="share_label:N"
        ),
    ).properties(
        width=320,
        title=alt.Title(
            f"Capacity and 12-month average flow, {_flow_month}",
            subtitle="Gray: capacity · Blue: flow",
        ),
    )

    _functions = wastewater_facilities.fillna({"emergency_only_functions": ""}).melt(
        id_vars=["wastewater_facility_name", "emergency_only_functions"],
        value_vars=list(wastewater_function_labels),
        var_name="column",
        value_name="has_function",
    )
    _functions = _functions[_functions["has_function"]].assign(
        function=lambda df: df["column"].map(wastewater_function_labels)
    )
    _functions["emergency_only"] = [
        row.function in row.emergency_only_functions.split(", ")
        for row in _functions.itertuples()
    ]
    _functions["emergency_only_label"] = "Yes"
    _function_base = alt.Chart(_functions).encode(
        x=alt.X(
            "function:N",
            title=None,
            sort=list(wastewater_function_labels.values()),
            scale=alt.Scale(domain=list(wastewater_function_labels.values())),
            axis=alt.Axis(
                orient="top",
                labelAngle=-45,
                ticks=False,
                domain=False,
                labelLimit=200,
            ),
        ),
        y=_facility_y(False),
    )
    _function_tooltip = [
        alt.Tooltip("wastewater_facility_name:N", title="WRRF"),
        alt.Tooltip("function:N", title="Function"),
    ]
    # filled and hollow dots as separate layers, so only the hollow ones say emergency only on hover
    _function_chart = alt.layer(
        _function_base.transform_filter("!datum.emergency_only")
        .mark_circle(size=70, color="#3182bd", opacity=1)
        .encode(tooltip=_function_tooltip),
        _function_base.transform_filter("datum.emergency_only")
        .mark_circle(
            size=70, stroke="#3182bd", strokeWidth=1.5, fill="white", opacity=1
        )
        .encode(
            tooltip=[
                *_function_tooltip,
                alt.Tooltip("emergency_only_label:N", title="Emergency only"),
            ]
        ),
    ).properties(width=260, title="Functions")
    # one chart rather than two side by side, so the rows line up even though the axes are on different sides
    mo.vstack(
        [
            alt.hconcat(_flow_chart, _function_chart, spacing=8).configure_view(
                stroke=None
            ),
            source_note(
                "Sources: DEP Map of the New York City Wastewater System (2025); DEP Wastewater Treatment Plant Performance Data, NYC Open Data (through July 2025)"
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.vstack(
        [
            mo.md("""
    #### MS4 Drainage Areas

    Land that drains to the separate storm sewer system (MS4), and where it discharges.
    """),
            mo.accordion(
                {
                    "Notes": mo.md("""
    - Each area drains to an MS4 outfall, to a WRRF, or by overland flow straight to the water.
    - Areas with combined or sanitary sewers, most of the city, aren't on the map.
    - Source: DEP's [MS4 map](https://data.cityofnewyork.us/d/j57c-rqtq), as of August 2020.
    """),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(con):
    # whole layers, simplified to about 10 m like the transit zones, so the map can pan anywhere
    ms4_drainage_geojson = con.sql("""
        SELECT drains_to, drains_to_type, waterbody,
            map_geojson(geom, 0.0001) AS geojson
        FROM dim_ms4_drainage_area
    """).df()
    ms4_outfalls = con.sql("""
        SELECT outfall_id, outfall_type, waterbody, ST_X(geom) AS lon, ST_Y(geom) AS lat
        FROM dim_ms4_outfall
    """).df()
    return ms4_drainage_geojson, ms4_outfalls


@app.cell(hide_code=True)
def _(
    con,
    district_code,
    focus_outline,
    folium,
    json,
    mo,
    ms4_drainage_geojson,
    ms4_outfalls,
    selected_district,
    source_note,
):
    ms4_drainage_colors = {
        "MS4 outfall": "#3182bd",
        "Overland flow": "#9ecae1",
        "Wastewater facility": "#31a354",
    }
    ms4_drainage_labels = {"Wastewater facility": "WRRF"}
    ms4_outfall_colors = {
        "MS4 OUTFALL": "#08306b",
        "CSO WITH MS4 CONNECTION": "#d95f0e",
    }
    ms4_outfall_labels = {
        "MS4 OUTFALL": "MS4 outfall",
        "CSO WITH MS4 CONNECTION": "Combined sewer outfall with MS4 connection",
    }
    ms4_drainage_shares = dict(
        con.execute(
            """
            SELECT drains_to_type, share_of_district
            FROM dim_community_district_ms4_drainage
            WHERE borocd = ?
            """,
            [selected_district],
        ).fetchall()
    )

    ms4_map = folium.Map(tiles="Esri.WorldGrayCanvas", zoom_snap=0.25)
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": json.loads(row.geojson),
                    "properties": {
                        "drains_to": row.drains_to,
                        "drains_to_type": row.drains_to_type,
                        "waterbody": row.waterbody,
                    },
                }
                for row in ms4_drainage_geojson.itertuples()
            ],
        },
        name="Drainage areas",
        style_function=lambda feature: {
            "fillColor": ms4_drainage_colors[feature["properties"]["drains_to_type"]],
            "fillOpacity": 0.5,
            "color": "white",
            "weight": 0.5,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=["drains_to", "waterbody"], aliases=["Drains to", "Waterbody"]
        ),
    ).add_to(ms4_map)
    for row in ms4_outfalls.itertuples():
        folium.CircleMarker(
            [row.lat, row.lon],
            radius=3,
            color="white",
            weight=0.5,
            fill_color=ms4_outfall_colors[row.outfall_type],
            fill_opacity=1,
            tooltip=f"{row.outfall_id}: {ms4_outfall_labels[row.outfall_type]}, {row.waterbody}",
        ).add_to(ms4_map)
    ms4_district_layer = folium.GeoJson(
        json.loads(focus_outline),
        name="District boundary",
        interactive=False,
        style_function=lambda _feature: {
            "fill": False,
            "color": "#222222",
            "weight": 3,
        },
    ).add_to(ms4_map)
    ms4_drainage_rows = "".join(
        f"""<tr>
          <td><span style="display:inline-block;width:14px;height:10px;background:{color};opacity:0.8;
            outline:1px solid #d8d8d4"></span></td>
          <td style="padding:0 12px 0 4px">{ms4_drainage_labels.get(drainage_type, drainage_type)}</td>
          <td style="text-align:right;font-weight:700">{ms4_drainage_shares.get(drainage_type, 0):.0%}</td>
        </tr>"""
        for drainage_type, color in ms4_drainage_colors.items()
    )
    ms4_outfall_rows = "".join(
        f"""<tr>
          <td style="text-align:center"><span style="display:inline-block;width:7px;height:7px;border-radius:50%;
            background:{ms4_outfall_colors[outfall_type]}"></span></td>
          <td colspan="2" style="padding:0 0 0 4px">{label}</td>
        </tr>"""
        for outfall_type, label in ms4_outfall_labels.items()
    )
    ms4_map.get_root().html.add_child(
        folium.Element(f"""
    <div style="position:fixed;bottom:10px;left:10px;z-index:1000;pointer-events:none;background:rgba(255,255,255,0.92);
      padding:6px 10px;border-radius:4px;font:11px sans-serif;color:#222222;box-shadow:0 1px 4px rgba(0,0,0,0.3)">
      <table style="border-collapse:collapse">
        <tr style="color:#55554f"><td></td><td style="padding:0 12px 4px 4px;font-weight:600;color:#222222">Drains to, {district_code}</td>
          <td style="padding-bottom:4px">Of district</td></tr>
        {ms4_drainage_rows}
        <tr>
          <td><span style="display:inline-block;width:14px;height:10px;background:white;outline:1px solid #d8d8d4"></span></td>
          <td style="padding:0 12px 0 4px">Not in MS4 map</td>
          <td style="text-align:right;font-weight:700">{max(0, 1 - sum(ms4_drainage_shares.values())):.0%}</td>
        </tr>
        <tr><td colspan="3" style="padding-top:6px;font-weight:600">Outfalls</td></tr>
        {ms4_outfall_rows}
      </table>
    </div>
    """)
    )
    ms4_map.fit_bounds(ms4_district_layer.get_bounds())
    mo.vstack(
        [
            mo.iframe(ms4_map.get_root().render(), height="500px"),
            source_note("Source: DEP MS4 map, NYC Open Data (August 2020)"),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.vstack(
        [
            mo.md("""
    ### Flooding

    The 2080s 100-year coastal floodplain, and flooding from a moderate rainstorm with 2050 sea levels.
    """),
            mo.accordion(
                {
                    "Notes": mo.md("""
    - **100-year floodplain:** land with a 1% chance of flooding in any year, with 58 inches of sea level rise. FEMA's Preliminary Work Map data with the New York City Panel on Climate Change's 90th percentile projection.
    - **Rainstorm:** 2.13 inches per hour. Nuisance is 4 inches to 1 foot of ponding, deep and contiguous 1 foot or more.
    - **High tides 2050:** tidal flooding, not rain. DEP took it from DCP's Flood Hazard Mapper (2050s high estimate, 30 inches of sea level rise).
    - Sources: [Sea Level Rise Maps (2080s 100-year Floodplain)](https://data.cityofnewyork.us/d/ek8y-fsqz) and DEP's [NYC Stormwater Flood Maps](https://data.cityofnewyork.us/d/9i7c-xyvv), NYC Open Data.
    """),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(
    con,
    district_code,
    focus_outline,
    folium,
    json,
    mo,
    selected_district,
    source_note,
):
    floodplain_color = "#6a51a3"
    stormwater_colors = {
        "Nuisance Flooding": "#6baed6",
        "Deep and Contiguous Flooding": "#08306b",
        "Future High Tides 2050": "#41ab5d",
    }
    stormwater_labels = {
        "Nuisance Flooding": "Nuisance (4 in. to 1 ft.)",
        "Deep and Contiguous Flooding": "Deep and contiguous (1 ft.+)",
        "Future High Tides 2050": "High tides, 2050",
    }
    # shapes in the map's frame, since the citywide layers are too heavy to draw, and acres in the district itself.
    # Simplified to about 1 m, which keeps small ponds visible.
    floodplain = (
        con.execute(
            """
        SELECT floodplain_acres, floodplain_share_of_district, map_geojson(frame_geom, 0.00001) AS geojson,
            map_geojson(frame_edge_geom, 0.00001) AS edge_geojson
        FROM dim_community_district_floodplain_2080
        WHERE borocd = ?
        """,
            [selected_district],
        )
        .df()
        .iloc[0]
    )
    stormwater_flooding = con.execute(
        """
        SELECT frame.flooding_category,
            coalesce(district.overlap_acres, 0) AS overlap_acres,
            coalesce(district.share_of_district, 0) AS share_of_district,
            map_geojson(frame.geom, 0.00001) AS geojson
        FROM dim_community_district_stormwater_flooding_frame AS frame
        LEFT JOIN dim_community_district_stormwater_flooding AS district
            USING (borocd, flooding_category)
        WHERE frame.borocd = ?
        """,
        [selected_district],
    ).df()

    flooding_map = folium.Map(tiles="Esri.WorldGrayCanvas", zoom_snap=0.25)
    # the floodplain underneath as a light fill, since it can cover most of a district and a strong one would hide the
    # rainstorm flooding on top. Its outer edge is drawn as a separate line, so the edges of its dry holes aren't
    # outlined like flooded areas. A frame with no floodplain has nothing to draw.
    if floodplain["geojson"] is not None:
        folium.GeoJson(
            json.loads(floodplain["geojson"]),
            name="2080s 100-year floodplain",
            style_function=lambda _feature: {
                "fillColor": floodplain_color,
                "fillOpacity": 0.12,
                "weight": 0,
            },
            tooltip="2080s 100-year floodplain",
        ).add_to(flooding_map)
        folium.GeoJson(
            json.loads(floodplain["edge_geojson"]),
            name="2080s 100-year floodplain edge",
            interactive=False,
            style_function=lambda _feature: {"color": floodplain_color, "weight": 1.5},
        ).add_to(flooding_map)
    for _row in stormwater_flooding.itertuples():
        folium.GeoJson(
            json.loads(_row.geojson),
            name=stormwater_labels[_row.flooding_category],
            style_function=lambda _feature, color=stormwater_colors[_row.flooding_category]: {
                "fillColor": color,
                "fillOpacity": 0.7,
                "color": color,
                "weight": 0.5,
            },
            tooltip=stormwater_labels[_row.flooding_category],
        ).add_to(flooding_map)
    flooding_district_layer = folium.GeoJson(
        json.loads(focus_outline),
        name="District boundary",
        interactive=False,
        style_function=lambda _feature: {
            "fill": False,
            "color": "#222222",
            "weight": 3,
        },
    ).add_to(flooding_map)

    def _legend_row(swatch, label, acres, share):
        return f"""<tr>
          <td>{swatch}</td>
          <td style="padding:0 12px 0 4px">{label}</td>
          <td style="text-align:right;font-weight:700;padding-right:10px">{acres:,.0f}</td>
          <td style="text-align:right;font-weight:700">{share:.1%}</td>
        </tr>"""

    def _legend_group(title):
        return f"""<tr><td colspan="4" style="padding-top:6px;font-weight:600">{title}</td></tr>"""

    _stormwater_by_category = stormwater_flooding.set_index("flooding_category")

    def _stormwater_row(category):
        return _legend_row(
            f"""<span style="display:inline-block;width:14px;height:10px;background:{stormwater_colors[category]};
              opacity:0.8;outline:1px solid #d8d8d4"></span>""",
            stormwater_labels[category],
            _stormwater_by_category["overlap_acres"].get(category, 0),
            _stormwater_by_category["share_of_district"].get(category, 0),
        )

    # high tides comes with the stormwater map but is tidal, so it's listed with the floodplain
    _rows = (
        _legend_group("Coastal")
        + _legend_row(
            f"""<span style="display:inline-block;width:12px;height:8px;background:{floodplain_color}1f;
              outline:1.5px solid {floodplain_color}"></span>""",
            "100-year floodplain, 2080s",
            floodplain["floodplain_acres"],
            floodplain["floodplain_share_of_district"],
        )
        + _stormwater_row("Future High Tides 2050")
        + _legend_group("Rainstorm, 2050")
        + _stormwater_row("Nuisance Flooding")
        + _stormwater_row("Deep and Contiguous Flooding")
    )
    flooding_map.get_root().html.add_child(
        folium.Element(f"""
    <div style="position:fixed;bottom:10px;left:10px;z-index:1000;pointer-events:none;background:rgba(255,255,255,0.92);
      padding:6px 10px;border-radius:4px;font:11px sans-serif;color:#222222;box-shadow:0 1px 4px rgba(0,0,0,0.3)">
      <table style="border-collapse:collapse">
        <tr style="color:#55554f"><td></td><td style="font-weight:600;color:#222222">Flooding, {district_code}</td>
          <td style="padding:0 10px 0 0;text-align:right">Acres</td><td>Of district</td></tr>
        {_rows}
      </table>
    </div>
    """)
    )
    flooding_map.fit_bounds(flooding_district_layer.get_bounds())
    mo.vstack(
        [
            mo.iframe(flooding_map.get_root().render(), height="500px"),
            source_note(
                "Sources: Sea Level Rise Maps (2080s 100-year Floodplain) and DEP NYC Stormwater Flood Map "
                "(moderate flood with 2050 sea level rise), NYC Open Data"
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ### Open Space

    #### TODO Access to Vital Parks

    (https://www.nycgovparks.org/about/vital-parks/explorer) (This map reflects the percentage of people in each Community Board who live within a 10-minute walk to a chosen element of a vital parks system.)

    ...
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.vstack(
        [
            mo.md("""
    ### Schools

    #### Public School Utilization

    """),
            mo.accordion(
                {
                    "Data notes": mo.md("""
    - No Action projections from the AHFT schools workbook, taken to be 10 years out.
    - Only the subdistricts related to AHFT districts have projections. The rest are gray.
    - Labels mark the subdistricts in the district.
    """)
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(con):
    # whole subdistricts, simplified to about 10 m like the transit zones. Built once rather than for each district.
    school_subdistrict_geojson = con.sql("""
        SELECT
            school_subdistrict_id,
            subdistrict_name,
            ps_utilization_2036,
            is_utilization_2036,
            map_geojson(geom, 0.0001) AS geojson
        FROM dim_school_subdistrict
    """).df()
    return (school_subdistrict_geojson,)


@app.cell(hide_code=True)
def _(
    con,
    district_code,
    focus_outline,
    folium,
    json,
    mo,
    school_subdistrict_geojson,
    selected_district,
    source_note,
):
    # the workbook's own bands
    utilization_bands = {
        "Under 90%": "#1a9850",
        "90% to 100%": "#fee08b",
        "Over 100%": "#d73027",
    }

    def utilization_band(utilization):
        if utilization != utilization:
            return None
        if utilization < 0.9:
            return "Under 90%"
        if utilization <= 1:
            return "90% to 100%"
        return "Over 100%"

    # labeled where the subdistrict is more than a sliver of the district
    school_subdistrict_labels = con.execute(
        """
        SELECT
            overlap.school_subdistrict_id,
            ST_X(ST_PointOnSurface(subdistrict.geom)) AS lon,
            ST_Y(ST_PointOnSurface(subdistrict.geom)) AS lat
        FROM dim_community_district_school_subdistrict AS overlap
        INNER JOIN dim_school_subdistrict AS subdistrict
            ON overlap.school_subdistrict_id = subdistrict.school_subdistrict_id
        WHERE overlap.borocd = ? AND overlap.share_of_district > 0.02
        """,
        [selected_district],
    ).df()

    def school_utilization_map(column, title):
        utilization = dict(
            zip(
                school_subdistrict_geojson["school_subdistrict_id"],
                school_subdistrict_geojson[column],
            )
        )
        school_map = folium.Map(tiles="Esri.WorldGrayCanvas", zoom_snap=0.25)
        folium.GeoJson(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": json.loads(row.geojson),
                        "properties": {
                            "band": utilization_band(getattr(row, column)),
                            "label": f"{row.school_subdistrict_id} {row.subdistrict_name}: "
                            + (
                                "no projection"
                                if getattr(row, column) != getattr(row, column)
                                else f"{getattr(row, column):.0%}"
                            ),
                        },
                    }
                    for row in school_subdistrict_geojson.itertuples()
                ],
            },
            name="School subdistricts",
            style_function=lambda feature: {
                "fillColor": utilization_bands.get(
                    feature["properties"]["band"], "#8c8c88"
                ),
                "fillOpacity": 0.15 if feature["properties"]["band"] is None else 0.55,
                "color": "white",
                "weight": 1.5,
            },
            tooltip=folium.GeoJsonTooltip(fields=["label"], labels=False),
        ).add_to(school_map)
        district_layer = folium.GeoJson(
            json.loads(focus_outline),
            name="District boundary",
            interactive=False,
            style_function=lambda _feature: {
                "fill": False,
                "color": "#222222",
                "weight": 3,
            },
        ).add_to(school_map)
        for row in school_subdistrict_labels.itertuples():
            value = utilization[row.school_subdistrict_id]
            folium.Marker(
                [row.lat, row.lon],
                icon=folium.DivIcon(
                    icon_size=(0, 0),
                    html=f"""<div style="transform:translate(-50%,-50%);text-align:center;white-space:nowrap;
                      font:11px sans-serif;color:#222222;text-shadow:0 0 3px #fff,0 0 3px #fff,0 0 3px #fff">
                      {row.school_subdistrict_id}<br><b>{"n/a" if value != value else f"{value:.0%}"}</b></div>""",
                ),
            ).add_to(school_map)
        band_rows = "".join(
            f"""<tr>
              <td><span style="display:inline-block;width:14px;height:10px;background:{color};opacity:0.8;
                outline:1px solid #d8d8d4"></span></td>
              <td style="padding:0 4px">{band}</td>
            </tr>"""
            for band, color in [
                *utilization_bands.items(),
                ("No projection", "#d8d8d4"),
            ]
        )
        school_map.get_root().html.add_child(
            folium.Element(f"""
    <div style="position:fixed;bottom:10px;left:10px;z-index:1000;pointer-events:none;background:rgba(255,255,255,0.92);
      padding:6px 10px;border-radius:4px;font:11px sans-serif;color:#222222;box-shadow:0 1px 4px rgba(0,0,0,0.3)">
      <table style="border-collapse:collapse">
        <tr><td></td><td style="padding:0 4px 4px 4px;font-weight:600">{title}, {district_code}</td></tr>
        {band_rows}
      </table>
    </div>
    """)
        )
        school_map.fit_bounds(district_layer.get_bounds())
        return mo.iframe(school_map.get_root().render(), height="500px")

    mo.vstack(
        [
            mo.hstack(
                [
                    school_utilization_map(
                        "ps_utilization_2036", "Primary school utilization, 2036"
                    ),
                    school_utilization_map(
                        "is_utilization_2036", "Intermediate school utilization, 2036"
                    ),
                ],
                widths="equal",
                gap=0.5,
            ),
            source_note(
                "Sources: AHFT schools workbook; SCA school subdistricts (September 2025)"
            ),
        ]
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
