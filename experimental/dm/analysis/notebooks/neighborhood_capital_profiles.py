import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
    import re

    import altair as alt
    import branca.colormap
    import duckdb
    import folium
    import folium.plugins
    import marimo as mo
    import pandas as pd

    return alt, branca, duckdb, folium, json, mo, pd, re


@app.cell(hide_code=True)
def _(folium, mo, re):
    def source_note(text):
        """A small source line under a chart or map, which stays visible in a printout. The same size as the map
        legends' text once they're scaled down. Plain HTML rather than markdown, whose own text size would win. Takes
        text and [label](url) links only."""
        html = re.sub(
            r"\[([^\]]+)\]\(([^)]+)\)",
            r'<a href="\2" target="_blank" rel="noopener noreferrer">\1</a>',
            text,
        )
        return mo.Html(
            f'<div style="font-size:9.35px;line-height:1.3;color:#55554f">{html}</div>'
        )

    def legend_box(rows):
        """A map legend's box, fixed to the map's lower left and letting clicks through to the map. Scaled down as a
        whole, so the layout inside stays the same."""
        return f"""
    <div style="position:fixed;bottom:10px;left:10px;z-index:1000;pointer-events:none;background:rgba(255,255,255,0.92);
      padding:6px 10px;border-radius:4px;font:11px sans-serif;color:#222222;box-shadow:0 1px 4px rgba(0,0,0,0.3);
      transform:scale(0.85);transform-origin:bottom left">
    {rows}
    </div>
    """

    def add_legend(fmap, rows):
        fmap.get_root().html.add_child(folium.Element(legend_box(rows)))

    return add_legend, legend_box, source_note


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
        "dim_bluebelt_bmp",
        "dim_bluebelt_parcel",
        "dim_sewer_area",
        "dim_community_district_sewer_area",
        "dim_wastewater_sewershed",
        "dim_community_district_wastewater_sewershed",
        "dim_community_district_ms4_drainage",
        "dim_wastewater_facility",
        "dim_community_district_stormwater_flooding",
        "dim_community_district_stormwater_flooding_frame",
        "dim_community_district_floodplain_2080",
        "dim_borough",
        "dim_tcol_family_type",
        "dim_school_subdistrict",
        "dim_school",
        "dim_school_capacity_project",
        "dim_community_district_school_subdistrict",
        "dim_vision_zero_priority_corridor",
        "dim_vision_zero_priority_intersection",
        "dim_street_improvement_project",
        "dim_transit_access_block",
        "dim_bus_stop",
        "dim_park",
        "dim_walk_to_a_park_area",
        "dim_park_capital_project",
        "dim_park_capital_project_location",
        "dim_subway_station",
        "dim_subway_service_line",
        "dim_rail_station",
        "dim_ibx_station",
        "dim_ibx_track",
        "dim_ferry_landing",
        "dim_community_district_mta_capital_project",
        "dim_mta_capital_project",
        "dim_mta_capital_project_acep",
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
    # 10 cm), which cuts the size by about a third with no visible change. Assigned, so the cell shows no output.
    _macro = con.execute("""
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
            cdta_median_household_income_acs, citywide_median_household_income_acs, fhgs_target_new_homes_2030
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
    # NTAs touching the district's NTAs, corners included, or across less than a quarter mile of water, since they're
    # nearby to someone living there
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
            nta.nta_type_name = 'Residential' AS in_residential_nta,
            tract.population_2020_census,
            tract.housing_units_2020_census,
            tract.housing_units_2025_end,
            tract.population_in_households_2020_census,
            tract.occupied_housing_units_2020_census,
            tract.land_acres,
            COLUMNS('^(population_age_|households_|median_household_income_|people_below_poverty_|people_poverty_status_|families_|population_acs|people_in_families_)'),
            tract.people_per_acre,
            tract.housing_units_2020_census / tract.land_acres AS housing_units_per_acre_2020,
            tract.housing_units_2025_end / tract.land_acres AS housing_units_per_acre_2026,
            tract.average_household_size_2020_census,
            map_geojson(tract.geom, 0.00005) AS geojson
        FROM dim_census_tract AS tract
        INNER JOIN dim_nta AS nta ON tract.nta2020 = nta.nta2020
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


@app.cell(hide_code=True)
def _(con, folium, json, selected_district):
    # the district's boundary, for every map but the residents map's own outlines. In its own cell, so changing the
    # residents map doesn't redraw the others. Same tolerance as the tracts, so the two line up where they share a boundary.
    focus_outline = con.execute(
        """
        SELECT map_geojson(geom, 0.00005)
        FROM dim_community_district
        WHERE borocd = ?
        """,
        [selected_district],
    ).fetchall()[0][0]

    # share of a district an overlapping area has to cover to count as one of the district's, such as a school
    # subdistrict or sewershed, so slivers along shared edges don't
    district_overlap_min_share = 0.02

    def add_district_outline(fmap, opacity=1):
        """The district's boundary as a black line, added to a map and returned so the map can fit to it."""
        return folium.GeoJson(
            json.loads(focus_outline),
            name="District boundary",
            interactive=False,
            style_function=lambda _feature: {
                "fill": False,
                "color": "#222222",
                "weight": 3,
                "opacity": opacity,
            },
        ).add_to(fmap)

    return add_district_outline, district_overlap_min_share, focus_outline


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
    nta_shapes_map = nta_base.transform_filter(
        "!datum.properties.on_timeline"
    ).mark_geoshape(fill="#e4e4e0", stroke="white", strokeWidth=0.75).encode(
        tooltip=nta_tooltip
    ) + nta_base.transform_filter("datum.properties.on_timeline").mark_geoshape(
        stroke="white", strokeWidth=0.75
    ).encode(
        color=alt.Color(
            "properties.nta_name:N",
            scale=alt.Scale(domain=timeline_nta_names),
            legend=None,
        ),
        tooltip=nta_tooltip,
    )
    nta_map = (
        nta_shapes_map
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
            mo.vstack([district_picker]).style({"width": "360px"}),
            mo.hstack(
                [
                    mo.as_html(city_map).style({"flex": "0 0 400px"}),
                    mo.as_html(nta_map).style({"flex": "1 1 0", "min-width": "0"}),
                ],
                justify="start",
                align="start",
                gap=1,
            ),
        ]
    )
    return (nta_shapes_map,)


@app.cell(hide_code=True)
def _(
    citywide_tracts,
    con,
    mo,
    overall_value,
    pd,
    selected_district,
    source_note,
):
    overview = (
        con.execute(
            """
        SELECT
            district.population_2010_census,
            district.population_2020_census,
            district.housing_units_2010_census,
            district.housing_units_2020_census,
            district.average_household_size_2020_census,
            district.people_per_acre,
            district.projected_net_units_2025_2029,
            district.fhgs_target_new_homes_2030,
            district.fhgs_target_affordable_homes_2030,
            district.fhgs_target_deeply_affordable_homes_2030,
            district.fhgs_target_homes_for_homeless_households_2030,
            units_2015.housing_units_from_2020_census AS housing_units_2015_end,
            units_2025.housing_units_from_2020_census AS housing_units_2025_end
        FROM dim_community_district AS district
        INNER JOIN mart_community_district_date AS units_2015
            ON district.borocd = units_2015.borocd AND units_2015.as_of_date = DATE '2015-12-31'
        INNER JOIN mart_community_district_date AS units_2025
            ON district.borocd = units_2025.borocd AND units_2025.as_of_date = DATE '2025-12-31'
        WHERE district.borocd = ?
        """,
            [selected_district],
        )
        .df()
        .iloc[0]
    )

    def _count(value, sign=False):
        return (
            "n/a" if pd.isna(value) else f"{value:+,.0f}" if sign else f"{value:,.0f}"
        )

    def _stat_row(title, stats):
        return mo.vstack(
            [
                mo.md(f"**{title}**"),
                mo.hstack(stats, justify="start", gap=1, widths="equal"),
            ],
            gap=0.25,
        )

    def _share_bar(share):
        """A small bar filled to a share, capped at full."""
        return mo.Html(
            f"""<div style="width:72px;height:8px;background:#e3e3df;border-radius:2px">
              <div style="width:{min(share, 1) * 100:.0f}%;height:100%;background:#3182bd;border-radius:2px"></div></div>"""
        )

    _projected_2030 = overview["projected_net_units_2025_2029"]
    _target_2030 = overview["fhgs_target_new_homes_2030"]
    _target_share = _projected_2030 / _target_2030
    mo.vstack(
        [
            mo.md("## Residents"),
            _stat_row(
                "Population and Housing, 2020",
                [
                    mo.stat(
                        _count(overview["population_2020_census"]),
                        label="Population",
                        caption=f"{overview['population_2020_census'] / overview['population_2010_census'] - 1:+.1%} since 2010",
                        bordered=True,
                    ),
                    mo.stat(
                        _count(overview["housing_units_2020_census"]),
                        label="Housing units",
                        caption=f"{overview['housing_units_2020_census'] / overview['housing_units_2010_census'] - 1:+.1%} since 2010",
                        bordered=True,
                    ),
                    mo.stat(
                        f"{overview['average_household_size_2020_census']:.2f}",
                        label="People per household",
                        caption=f"NYC: {overall_value(citywide_tracts, 'average_household_size_2020_census'):.2f}",
                        bordered=True,
                    ),
                    mo.stat(
                        f"{overview['people_per_acre']:.1f}",
                        label="People per acre",
                        caption=f"NYC: {overall_value(citywide_tracts, 'people_per_acre'):.1f}",
                        bordered=True,
                    ),
                ],
            ),
            _stat_row(
                "Housing growth",
                [
                    mo.stat(
                        _count(
                            overview["housing_units_2025_end"]
                            - overview["housing_units_2015_end"],
                            sign=True,
                        ),
                        label="Built, 2016-2025",
                        caption="net units completed",
                        bordered=True,
                    ),
                    mo.stat(
                        _count(overview["housing_units_2025_end"]),
                        label="Housing units, 2026",
                        caption=f"{overview['housing_units_2025_end'] / overview['housing_units_2020_census'] - 1:+.1%} since 2020",
                        bordered=True,
                    ),
                    mo.stat(
                        _count(_projected_2030, sign=True),
                        label="Projected, by 2030",
                        caption="n/a"
                        if pd.isna(_target_share)
                        else f"{_target_share:.0%} of the FHGS target",
                        slot=None
                        if pd.isna(_target_share)
                        else _share_bar(_target_share),
                        bordered=True,
                    ),
                    mo.stat(
                        _count(_target_2030),
                        label="FHGS target, 2026-2030",
                        caption=f"{_count(overview['fhgs_target_affordable_homes_2030'])} affordable · "
                        f"{_count(overview['fhgs_target_deeply_affordable_homes_2030'])} deeply affordable · "
                        f"{_count(overview['fhgs_target_homes_for_homeless_households_2030'])} for homeless households",
                        bordered=True,
                    ),
                ],
            ),
            source_note(
                "Sources: 2010 and 2020 Census, via DCP Population FactFinder; DCP Housing Database (26Q2); DCP Known "
                "Projects Database (October 2026); [Fair Housing Growth Strategy](https://wherewelive.cityofnewyork.us/wp-content/uploads/2026/10/Fair-Housing-Growth-Strategy-Final-Report.pdf) (October 2026)"
            ),
        ],
        gap=0.75,
    )
    return


@app.cell
def _(con, mo, selected_district):
    tcol_borough = con.execute(
        "SELECT boro_name, tcol_pct_not_meeting FROM dim_borough WHERE boro_code = ?",
        [selected_district // 100],
    ).fetchall()[0]
    tcol_lowest, tcol_highest = con.execute(
        "SELECT min(median_cost), max(median_cost) FROM dim_tcol_family_type"
    ).fetchall()[0]
    mo.vstack(
        [
            mo.accordion(
                {
                    "Map colors": mo.md("""
    - **Citywide:** a tract's rank among all NYC tracts. Colors compare across districts.
    - **District:** the district's lowest tract to its highest. Colors don't compare across districts.
    - **Nearby:** adds the NTAs touching the district's, or across less than a quarter mile of water, with a halo around the district. Always citywide colors.
    - **Bivariate:** each measure split into thirds of NYC tracts, or of the district's with District colors. The first measure runs up, the second across. Darker means higher on both.
    - **Hatched:** non-residential NTAs, hidden unless switched on.
    - **Gray:** no value (no residents, households, or families, or no estimate).
    - **Lines:** black for NTAs, gray for the district.
    """),
                    "Data notes": mo.md(f"""
    - Age and density: 2020 census. Income and poverty: 2020-2024 ACS.
    - Housing units, 2026: the end of 2025, from the 2020 census plus Housing Database (26Q2) completions since.
    - Poverty rate uses the federal poverty line, the same everywhere in the country.
    - Families under the TCOL threshold: families whose ACS income is under $159,197, NYC's True Cost of Living
      threshold for families with children and adults under 65 (2022 dollars). It isn't the TCOL rate:
        - TCOL gives each family its own threshold, from ${tcol_lowest:,} to ${tcol_highest:,} by family type. This uses one for all.
        - The threshold counts taxes as a cost, so comparing it with before-tax ACS income is fair.
        - TCOL also counts benefits and tax credits that ACS income leaves out, such as SNAP and public housing, so
          some families counted here meet their threshold once those are added.
        - The TCOL rate counts people. This counts families, leaving out people living alone or with non-relatives.
          Map it against Residents in families (Bivariate) to see how many of a tract's residents it speaks for.
        - ACS income is in 2024 dollars, so this share runs a little low.
        - The report stops at boroughs: {tcol_borough[1]:.1f}% of {tcol_borough[0]} residents fall short of their threshold.
    - Tracts and NTAs are the ones in the district's CDTA, which can differ a little from the district.
    - Non-residential NTAs (parks, cemeteries, airports, and special areas like Fort Hamilton) are left out of the map and charts unless switched on. NYC figures include them.
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
        "Housing units, 2020": "housing_units_2020_census",
        # the end of 2025, which data users call 2026
        "Housing units, 2026": "housing_units_2025_end",
        "Housing units per acre, 2020": "housing_units_per_acre_2020",
        "Housing units per acre, 2026": "housing_units_per_acre_2026",
        "Median household income": "median_household_income_acs",
        "Poverty rate (% of people)": "people_below_poverty_acs",
        "Families under $159,197 TCOL threshold (%)": "families_income_under_tcol_acs",
        "Residents in families (%)": "people_in_families_acs",
        **{
            f"Age {label} (% of residents)": column
            for column, label in age_groups.items()
        },
        **{
            f"Household income {label} (% of households)": column
            for column, label in income_groups.items()
        },
    }
    # tract counts, which a district's total doesn't fit on a scale of
    count_measures = {
        "population_2020_census",
        "housing_units_2020_census",
        "housing_units_2025_end",
    }
    # per acre of land, like the growth charts
    housing_density_units = {
        "housing_units_per_acre_2020": "housing_units_2020_census",
        "housing_units_per_acre_2026": "housing_units_2025_end",
    }
    # each percent measure's base
    rate_denominators = {
        **{column: "population_2020_census" for column in age_groups},
        **{column: "households_acs" for column in income_groups},
        "people_below_poverty_acs": "people_poverty_status_determined_acs",
        "families_income_under_tcol_acs": "families_acs",
        "people_in_families_acs": "population_acs",
    }
    map_measure = mo.ui.dropdown(
        options=map_measures, value="People per acre, 2020", label="Map"
    )
    map_kind = mo.ui.tabs({"Single": "", "Bivariate": ""})
    map_measure_2 = mo.ui.dropdown(
        options=map_measures, value="Poverty rate (% of people)", label="and"
    )
    map_extent = mo.ui.tabs({"District": "", "Nearby": ""})
    map_scale_basis = mo.ui.tabs({"Citywide": "", "District": ""})
    nonresidential_switch = mo.ui.switch(value=False, label="Non-residential NTAs")
    map_opacity = mo.ui.slider(
        start=0, stop=100, step=5, value=65, label="Opacity", show_value=True
    )
    return (
        count_measures,
        housing_density_units,
        map_extent,
        map_kind,
        map_measure,
        map_measure_2,
        map_measures,
        map_opacity,
        map_scale_basis,
        nonresidential_switch,
        rate_denominators,
    )


@app.cell(hide_code=True)
def _(
    map_extent,
    map_kind,
    map_measure,
    map_measure_2,
    map_opacity,
    map_scale_basis,
    mo,
    nonresidential_switch,
):
    # shown here rather than where they're made, since Nearby is always citywide and hides the color scale. What to map
    # on the first row, how to show it on the second.
    map_choice_row = [
        map_kind,
        map_measure,
        *([map_measure_2] if map_kind.value == "Bivariate" else []),
    ]
    map_view_row = [mo.md("Show"), map_extent]
    if map_extent.value == "District":
        map_view_row += [
            mo.md("Color scale").style({"margin-left": "1.5rem"}),
            map_scale_basis,
        ]
    map_view_row.append(nonresidential_switch.style({"margin-left": "1.5rem"}))
    map_view_row.append(map_opacity.style({"margin-left": "1.5rem"}))
    mo.vstack(
        [
            mo.hstack(map_choice_row, justify="start", align="center", gap=0.5),
            mo.hstack(map_view_row, justify="start", align="center", gap=0.5),
        ],
        gap=0.5,
    )
    return


@app.cell
def _(
    con,
    count_measures,
    housing_density_units,
    map_measures,
    rate_denominators,
):
    def map_values_for(df, column):
        if column in rate_denominators:
            base = df[rate_denominators[column]]
            return 100 * df[column] / base.where(base > 0)
        population = df["population_2020_census"]
        return df[column].where(population > 0)

    def measure_format(column):
        if column in rate_denominators:
            return "{:.1f}%"
        if column == "median_household_income_acs":
            return "${:,.0f}"
        if column == "average_household_size_2020_census":
            return "{:,.2f}"
        if column in count_measures:
            return "{:,.0f}"
        return "{:,.1f}"

    def overall_value(df, column):
        """A place's own figure from its tract totals, so every person counts once however its tracts are drawn."""
        if column in rate_denominators:
            return 100 * df[column].sum() / df[rate_denominators[column]].sum()
        if column == "people_per_acre":
            return df["population_2020_census"].sum() / df["land_acres"].sum()
        if column in housing_density_units:
            return df[housing_density_units[column]].sum() / df["land_acres"].sum()
        if column == "average_household_size_2020_census":
            return (
                df["population_in_households_2020_census"].sum()
                / df["occupied_housing_units_2020_census"].sum()
            )
        raise ValueError(f"{column} has no overall value")

    citywide_tracts = con.sql("""
        SELECT
            * EXCLUDE (geom),
            housing_units_2020_census / land_acres AS housing_units_per_acre_2020,
            housing_units_2025_end / land_acres AS housing_units_per_acre_2026
        FROM dim_census_tract
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
        measure_format,
        overall_value,
    )


@app.cell(hide_code=True)
def _(legend_box, measure_format):
    # Joshua Stevens's green-blue palette (stevens.greenblue in R's pals package), by [third up][third across]: green
    # up, blue across
    bivariate_palette = [
        ["#e8e8e8", "#b5c0da", "#6c83b5"],
        ["#b8d6be", "#90b2b3", "#567994"],
        ["#73ae80", "#5a9178", "#2a5a5b"],
    ]

    def bivariate_edges(values):
        """The lowest value, the two splitting tracts into thirds, and the highest."""
        return [
            float(values.min()),
            float(values.quantile(1 / 3)),
            float(values.quantile(2 / 3)),
            float(values.max()),
        ]

    def bivariate_third(values, edges):
        """0, 1, or 2 for each value's third, None where there's no value."""
        return [
            None
            if value != value
            else 0
            if value <= edges[1]
            else 1
            if value <= edges[2]
            else 2
            for value in values
        ]

    def bivariate_label_format(column):
        """Whole numbers, except people per household, whose thirds are only tenths of a person apart."""
        if column == "average_household_size_2020_census":
            return "{:.1f}"
        return measure_format(column).replace(".1f", ".0f").replace(".2f", ".0f")

    def bivariate_legend(
        title_up,
        title_across,
        edges_up,
        edges_across,
        format_up,
        format_across,
        basis,
    ):
        """A 3x3 key with each measure's lowest, highest, and in-between values on its axis."""
        cells = "".join(
            f'<div style="background:{bivariate_palette[2 - row][column]};opacity:0.85"></div>'
            for row in range(3)
            for column in range(3)
        )
        cut_style = "position:absolute;white-space:nowrap;color:#55554f;font-size:10px"
        return legend_box(
            f"""
      <div style="font-weight:600">{title_up} ↑</div>
      <div style="color:#55554f;margin-bottom:10px">Thirds of {basis} tracts</div>
      <div style="position:relative;margin-left:64px;width:144px;height:72px;display:grid;
        grid-template-columns:repeat(3,48px);grid-template-rows:repeat(3,24px)">
        {cells}
        {
                "".join(
                    f'<span style="{cut_style};right:100%;margin-right:4px;top:{72 - 24 * step}px;transform:translateY(-50%)">'
                    f"{format_up.format(edge)}</span>"
                    for step, edge in enumerate(edges_up)
                )
            }
        {
                "".join(
                    f'<span style="{cut_style};top:100%;margin-top:2px;left:{48 * step}px;transform:translateX(-50%)">'
                    f"{format_across.format(edge)}</span>"
                    for step, edge in enumerate(edges_across)
                )
            }
      </div>
      <div style="margin-top:16px;font-weight:600">{title_across} →</div>
    """
        )

    return (
        bivariate_edges,
        bivariate_label_format,
        bivariate_legend,
        bivariate_palette,
        bivariate_third,
    )


@app.cell(hide_code=True)
def _(all_ntas, map_area_tracts, nonresidential_switch, tracts):
    # non-residential NTAs (parks, cemeteries, airports, and the like) only when switched on, with the map's district
    # colors and ticks and the charts following the tracts shown. Citywide figures keep every NYC tract.
    residents_tracts = (
        tracts if nonresidential_switch.value else tracts[tracts["in_residential_nta"]]
    )
    residents_area_tracts = (
        map_area_tracts
        if nonresidential_switch.value
        else map_area_tracts[map_area_tracts["in_residential_nta"]]
    )
    residents_ntas = (
        all_ntas
        if nonresidential_switch.value
        else all_ntas[all_ntas["nta_type_name"] == "Residential"]
    )
    return residents_area_tracts, residents_ntas, residents_tracts


@app.cell
def _(
    age_groups,
    bivariate_edges,
    bivariate_label_format,
    bivariate_legend,
    bivariate_palette,
    bivariate_third,
    branca,
    citywide_tracts,
    con,
    count_measures,
    district_code,
    focus,
    focus_outline,
    folium,
    income_groups,
    json,
    legend_box,
    map_area_tracts,
    map_citywide_values,
    map_extent,
    map_kind,
    map_measure,
    map_measure_2,
    map_opacity,
    map_ranges,
    map_scale_basis,
    map_values_for,
    measure_format,
    mo,
    nearby_nta_codes,
    nonresidential_switch,
    overall_value,
    pd,
    residents_area_tracts,
    residents_tracts,
    selected_district,
    source_note,
    tracts,
):
    map_column = map_measure.value
    map_column_2 = map_measure_2.value
    is_bivariate = map_kind.value == "Bivariate"
    map_values = map_values_for(residents_tracts, map_column)
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
    shown_tracts = residents_area_tracts if is_nearby else residents_tracts
    # non-residential tracts the switch hides, hatched so they don't read as tracts low on everything
    hidden_tracts = (
        (map_area_tracts if is_nearby else tracts).query("not in_residential_nta")
        if not nonresidential_switch.value
        else map_area_tracts.iloc[0:0]
    )
    shown_values = map_values_for(shown_tracts, map_column)
    map_format = measure_format(map_column)
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
    elif map_column in count_measures:
        # a place's total doesn't fit on a scale of tract populations
        tick_meaning = None
        district_label, district_value = "Average tract", map_values.mean()
        nyc_label, nyc_value = "Average tract", map_citywide.mean()
    else:
        tick_meaning = "Whole area"
        district_label, district_value = "", overall_value(residents_tracts, map_column)
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
    map_legend = legend_box(
        f"""
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
    """
    )

    if is_bivariate:
        # thirds of NYC tracts, or of the district's with District colors, so the key has only one choice in it
        _bases = {
            column: (
                map_values_for(residents_tracts, column)
                if is_district_scale
                else map_citywide_values[column]
            ).dropna()
            for column in (map_column, map_column_2)
        }
        bivariate_split = {
            column: bivariate_edges(values) for column, values in _bases.items()
        }
        shown_values_2 = map_values_for(shown_tracts, map_column_2)
        # the first measure up, the second across
        shown_colors = [
            None
            if third_1 is None or third_2 is None
            else bivariate_palette[third_1][third_2]
            for third_1, third_2 in zip(
                bivariate_third(shown_values, bivariate_split[map_column]),
                bivariate_third(shown_values_2, bivariate_split[map_column_2]),
            )
        ]
        map_legend = bivariate_legend(
            map_measure.selected_key,
            map_measure_2.selected_key,
            bivariate_split[map_column],
            bivariate_split[map_column_2],
            bivariate_label_format(map_column),
            bivariate_label_format(map_column_2),
            district_code if is_district_scale else "NYC",
        )
    else:
        shown_values_2 = pd.Series(float("nan"), index=shown_values.index)
        shown_colors = [None] * len(shown_values)

    def tract_style(feature):
        if is_bivariate:
            color = feature["properties"]["bivariate_color"]
            return {
                "fillColor": color or "#8c8c88",
                "fillOpacity": 0.3 if color is None else map_opacity.value / 100,
                # the palette's low corner is about the basemap's gray, so tracts get a visible edge
                "color": "#a8a8a3",
                "weight": 0.75,
            }
        map_value = feature["properties"]["color_value"]
        return {
            "fillColor": "#8c8c88" if map_value is None else map_scale(map_value),
            "fillOpacity": 0.3 if map_value is None else map_opacity.value / 100,
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
                    "bivariate_color": bivariate_color,
                    "map_label_2": "no value"
                    if map_value_2 != map_value_2
                    else measure_format(map_column_2).format(map_value_2),
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
            for row, map_value, map_rank, map_value_2, bivariate_color in zip(
                shown_tracts.itertuples(),
                shown_values,
                citywide_rank(shown_values),
                shown_values_2,
                shown_colors,
            )
        ],
    }
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
        WHERE (nta.cdta2020 = district.cdta2020 OR list_contains(?, nta.nta2020))
            AND (? OR nta.nta_type_name = 'Residential')
        """,
        [
            selected_district,
            nearby_nta_codes if is_nearby else [],
            nonresidential_switch.value,
        ],
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
                *(["map_label_2"] if is_bivariate else []),
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
                *([map_measure_2.selected_key] if is_bivariate else []),
                "Population, 2020",
                "Housing units, 2020",
                "People per acre, 2020",
                "People per household, 2020",
                "Median household income",
            ],
        ),
    )
    # quarter zoom steps, so fitting the tracts doesn't stop a whole step short
    focus_map = folium.Map(tiles=None, zoom_snap=0.25, attribution_control=False)
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
    if len(hidden_tracts):
        hidden_hatch = folium.plugins.StripePattern(
            angle=-45,
            color="#8c8c88",
            weight=1.5,
            space_weight=4,
            opacity=map_opacity.value / 100,
        ).add_to(focus_map)
        folium.GeoJson(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": json.loads(_row.geojson),
                        "properties": {
                            "label": f"{_row.nta_name}: non-residential, hidden"
                        },
                    }
                    for _row in hidden_tracts.itertuples()
                ],
            },
            name="Non-residential (hidden)",
            style_function=lambda _feature: {
                "fillPattern": hidden_hatch,
                "fillOpacity": 1,
                "color": "#a8a8a3",
                "weight": 0.75,
            },
            tooltip=folium.GeoJsonTooltip(fields=["label"], labels=False),
        ).add_to(focus_map)
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

    shown_columns = [map_column, *([map_column_2] if is_bivariate else [])]
    is_acs_measure = any(
        column.endswith("_acs") or column in income_groups for column in shown_columns
    )
    is_census_measure = not all(
        column.endswith("_acs") or column in income_groups for column in shown_columns
    )
    is_tcol_measure = "families_income_under_tcol_acs" in shown_columns
    # folium's own output is sized by aspect ratio and cuts off the legend, so fix the height
    mo.vstack(
        [
            mo.iframe(focus_map.get_root().render(), height="600px"),
            source_note(
                "Sources: "
                + " and ".join(
                    (["2020 Census"] if is_census_measure else [])
                    + (["2020-2024 ACS"] if is_acs_measure else [])
                )
                + ", via DCP Population FactFinder; "
                + (
                    "DCP Housing Database (26Q2); "
                    if "housing_units_2025_end" in shown_columns
                    else ""
                )
                + (
                    "[NYC True Cost of Living report](https://www.nyc.gov/assets/equity/downloads/pdf/2026-NYC-TCOL-Report.pdf) (2026); "
                    if is_tcol_measure
                    else ""
                )
                + "DCP census tract and NTA boundaries (26c)"
            ),
        ]
    )
    return


@app.cell
def _(alt, city_totals, pd, residents_ntas, residents_tracts):
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
                    **residents_tracts[columns].sum().to_dict(),
                },
                *[
                    {
                        "place": row["nta_name"],
                        "section": "nta",
                        "note": f"No {people} · {row['nta_type_name']}",
                        **row[columns].to_dict(),
                    }
                    for _, row in residents_ntas.iterrows()
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
def _(mo):
    mo.vstack(
        [
            mo.accordion(
                {
                    "Notes": mo.md("""
    - **Solid lines:** the 2020 census count, plus units built after it and minus units built before it (Housing Database).
    - **Hollow dots:** 2010 census counts. A dot off its line means the 2010 count doesn't match construction.
    - **Net units added:** new units minus units removed.
    - **Dashed lines:** KPDB's projected units added to the end-of-2024 count. KPDB groups projects by status, not date, so 2029 and 2034 are rough labels.
    - **Dotted line:** the Fair Housing Growth Strategy's 2026-2030 target for new homes, added to the end-of-2025 count. District tab only.
    - An NTA's dashed line is often mostly one project.
    - The NTAs tab shows a non-residential NTA only if it has projected units.
    """),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    growth_view = mo.ui.tabs({"District": "", "NTAs": ""})
    fhgs_target_switch = mo.ui.switch(
        value=True, label="Fair Housing Growth Strategy 2030 target"
    )
    return fhgs_target_switch, growth_view


@app.cell
def _(
    alt,
    con,
    fhgs_target_switch,
    focus,
    growth_view,
    mo,
    nta_shapes_map,
    pd,
    selected_district,
    source_note,
    timeline_ntas,
):
    import warnings

    # a hover selection shared by the three panels is the intended use, not a mistake
    warnings.filterwarnings(
        "ignore", message="Automatically deduplicated selection parameter"
    )
    built_series = "2020 census ± units built"
    growth_panel_height = 120
    growth_panel_spacing = 10
    projected_series = "KPDB projection"
    target_series = "FHGS 2030 target"
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
            land_acres=land_acres,
            units_per_acre=lines["units"] / land_acres,
            units_added=lines["units"] - start_units,
            pct_change=100 * (lines["units"] / start_units - 1),
            # not a percent change, which would match pct_change because land area is fixed
            units_per_acre_added=(lines["units"] - start_units) / land_acres,
        )

    # the target counts new homes in 2026-2030, so it starts from the built series at the end of 2025
    district_target = pd.DataFrame(
        {
            "date": ["2025-12-31", "2030-12-31"],
            "units": [
                district_built["units"].iloc[-1],
                district_built["units"].iloc[-1] + focus["fhgs_target_new_homes_2030"],
            ],
            "series": focus["cdta_name"],
            "kind": target_series,
        }
    )
    assert district_built["date"].iloc[-1] == "2025-12-31", district_built["date"].iloc[
        -1
    ]
    district_housing = with_growth_measures(
        pd.concat(
            [district_built, district_projection]
            + ([district_target] if fhgs_target_switch.value else [])
        ),
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

    def housing_timeline(lines, census_2010, series_order, end_year, width=600):
        # density for the census dots, from each place's land area on its lines
        census_2010 = census_2010.assign(
            units_per_acre=census_2010["units"]
            / census_2010["series"].map(lines.groupby("series")["land_acres"].first())
        )
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
                        columns=legend_columns(series_order, width),
                        labelLimit=320,
                    ),
                ),
                strokeDash=alt.StrokeDash(
                    "kind:N",
                    title=None,
                    scale=alt.Scale(
                        domain=[built_series, projected_series, target_series],
                        range=[[1, 0], [6, 4], [2, 3]],
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
                            "units_per_acre:Q", title="Units per acre", format=",.1f"
                        ),
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
                width=width,
                height=growth_panel_height,
                title=alt.TitleParams(title, anchor="start", fontSize=12),
            )

        census_dots = (
            alt.Chart(census_2010)
            .mark_point(filled=False, size=60)
            .encode(
                x="date:T",
                y="units_per_acre:Q",
                color=alt.Color(
                    "series:N", sort=series_order, scale=alt.Scale(domain=series_order)
                ),
                tooltip=[
                    alt.Tooltip("series:N", title="Place"),
                    alt.Tooltip("units:Q", title="2010 census units", format=",.0f"),
                    alt.Tooltip(
                        "units_per_acre:Q", title="Units per acre", format=",.1f"
                    ),
                ],
            )
        )
        off_scale, highest_on_scale = off_scale_series(lines)
        change_panel = panel(
            "pct_change",
            "Percent change in housing units since April 2010",
            "+.0f",
            is_bottom=True,
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
        # density first, so places of different sizes start out comparable. Then growth, as a count, the same count
        # per acre, and as a share of what was there
        return alt.vconcat(
            panel("units_per_acre", "Housing units per acre of land", ",.1f")
            + census_dots,
            panel("units_added", "Net units added since April 2010", "+,.0f"),
            panel(
                "units_per_acre_added",
                "Net units added per acre of land since April 2010",
                "+.1f",
            ),
            change_panel,
            spacing=growth_panel_spacing,
        )

    # the target switch only on District, since FHGS has no NTA targets
    growth_controls = [growth_view] + (
        [fhgs_target_switch.style({"margin-left": "1.5rem"})]
        if growth_view.value == "District"
        else []
    )
    growth_charts = {
        "District": lambda: housing_timeline(
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
        "NTAs": lambda: housing_timeline(
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
            # narrower, so the NTA map fits beside it
            width=440,
        ),
    }
    mo.vstack(
        [
            mo.hstack(growth_controls, justify="start", align="center", gap=0.5),
            # the NTA map from the top again, to match the colored lines to places
            mo.hstack(
                [
                    growth_charts[growth_view.value](),
                    # just the shapes, since the legend names each color and the labels don't fit beside the charts
                    # as wide as the space beside the charts and as tall as the four plots, so it fills what's free and
                    # centers there
                    mo.as_html(
                        nta_shapes_map.properties(
                            width="container",
                            height=4 * growth_panel_height + 3 * growth_panel_spacing,
                        )
                    ).style(
                        {
                            "flex": "1 1 0",
                            "min-width": "260px",
                            "display": "flex",
                            "flex-direction": "column",
                            "justify-content": "center",
                        }
                    ),
                ],
                justify="start",
                align="stretch",
                gap=1,
                wrap=True,
            )
            if growth_view.value == "NTAs"
            else growth_charts[growth_view.value](),
            source_note(
                "Sources: DCP Housing Database (26Q2); 2010 and 2020 Census, via DCP Population FactFinder; DCP Known Projects Database (October 2026); [Fair Housing Growth Strategy](https://wherewelive.cityofnewyork.us/wp-content/uploads/2026/10/Fair-Housing-Growth-Strategy-Final-Report.pdf) (October 2026)"
            ),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## Infrastructure

    ### Transportation
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.vstack(
        [
            mo.md("#### Transit"),
            mo.accordion(
                {
                    "Notes": mo.md("""
    - Lines: each subway service's route, in the MTA's colors. Services of different colors sharing track are drawn
      side by side.
    - Stations: subway and Staten Island Railway stations, labeled with the routes that stop there on weekdays.
    - Accessible: the MTA's ADA status. Partially accessible stations are accessible in one direction only.
    - Counts are stations inside the district. A station on two lines, such as 145 St, counts once.
    - Commuter rail: Long Island Rail Road and Metro-North stations.
    - Ferry landings: NYC Ferry landings and the Staten Island Ferry terminals. Landings are on piers past the
      district boundary, so each counts for the nearest district.
    - Bus stops (switch): MTA stops where riders can board, from the schedule in effect. Counts are stops inside the
      district.
    - Planned transit (switch): the proposed Interborough Express (IBX) stations and alignment, from the design
      layers the MTA's engineering consultant publishes. Subject to change.
    - MTA capital projects (switch): locations of projects in the 2020-2024 and 2025-2029 capital plans, from the
      MTA's Capital Dashboard, which has no stage or dates. Each project has one point per district: its location
      there nearest the center of its locations there. Counts and the table are projects with a location in the
      district.
      Shapes follow the agency's stations: circles for subway and Staten Island Railway, diamonds for LIRR and
      Metro-North, squares for the other agencies.
    - Stage and estimated completion come from the MTA's Capital Project Details, matched on the project's ACEP code
      through the MTA's budget data. Most projects don't match, and those that do are often packages, such as a set of
      ADA stations, so the stage and date are the package's.
    - Transit access (switch): acres reachable by transit within 60 minutes of each 2020 census block, in the file's
      20 percentile bands, the categories DCP's public maps use. From a CEQR file dated November 2026, without decay.
      The district median weights each block by its area. The file leaves out land such as cemeteries and parks,
      so the median covers only the district's blocks in it.
    """),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(con, selected_district):
    # the file's 20 percentile categories, lowest first
    transit_access_categories = [
        category
        for (category,) in con.execute("""
            SELECT access_category
            FROM dim_transit_access_block
            GROUP BY access_category, access_category_floor
            ORDER BY access_category_floor
        """).fetchall()
    ]
    # blocks in the district's box with its size again on each side, past what the map shows
    transit_access_blocks = con.execute(
        """
        WITH frame AS (
            SELECT
                ST_XMin(geom) AS x0,
                ST_XMax(geom) AS x1,
                ST_YMin(geom) AS y0,
                ST_YMax(geom) AS y1,
                greatest(ST_XMax(geom) - ST_XMin(geom), ST_YMax(geom) - ST_YMin(geom)) AS pad
            FROM dim_community_district
            WHERE borocd = ?
        )

        SELECT
            block.census_block_2020,
            block.access_category,
            block.acres_reachable_percentile,
            block.acres_reachable_in_60_minutes,
            map_geojson(block.geom, 0.00005) AS geojson
        FROM dim_transit_access_block AS block
        CROSS JOIN frame
        WHERE ST_XMax(block.geom) >= frame.x0 - frame.pad
            AND ST_XMin(block.geom) <= frame.x1 + frame.pad
            AND ST_YMax(block.geom) >= frame.y0 - frame.pad
            AND ST_YMin(block.geom) <= frame.y1 + frame.pad
        """,
        [selected_district],
    ).df()
    return transit_access_blocks, transit_access_categories


@app.cell(hide_code=True)
def _(mo):
    transit_access_switch = mo.ui.switch(value=False, label="Transit access")
    planned_transit_switch = mo.ui.switch(value=False, label="Planned transit")
    bus_stop_switch = mo.ui.switch(value=False, label="Bus stops")
    mta_capital_switch = mo.ui.switch(value=False, label="MTA capital projects")
    return (
        bus_stop_switch,
        mta_capital_switch,
        planned_transit_switch,
        transit_access_switch,
    )


@app.cell(hide_code=True)
def _(
    add_district_outline,
    add_legend,
    bus_stop_switch,
    con,
    district_code,
    folium,
    json,
    mo,
    mta_capital_switch,
    pd,
    planned_transit_switch,
    selected_district,
    source_note,
    transit_access_blocks,
    transit_access_categories,
    transit_access_switch,
):
    # route_color from the MTA's subway GTFS feed (routes.txt), by service in the service lines dataset
    subway_service_colors = {
        **dict.fromkeys(["1", "2", "3"], "#D82233"),
        **dict.fromkeys(["4", "5", "5 Peak", "6"], "#009952"),
        "7": "#9A38A1",
        **dict.fromkeys(["A", "C", "E"], "#0062CF"),
        **dict.fromkeys(["B", "D", "F", "M"], "#EB6800"),
        "G": "#799534",
        **dict.fromkeys(["J", "Z"], "#8E5C33"),
        **dict.fromkeys(["L", "SF", "SR", "ST"], "#7C858C"),
        **dict.fromkeys(["N", "Q", "R", "W"], "#F6BC26"),
        "SIR": "#08179C",
    }
    # services of different colors on shared track are drawn as separate lines a few feet apart, so at district zoom
    # they cover each other. Each color is shifted sideways by this many steps, picked so every pair of colors that
    # share track gets a different shift.
    service_offsets = {
        "#0062CF": -1,
        "#009952": -1,
        "#D82233": 1,
        "#F6BC26": 1,
        "#799534": 1,
        "#8E5C33": 1,
    }
    service_line_weight = 3.5
    # a full line width, so colors never cover each other, even where the MTA draws shared track a pixel apart
    service_line_step = service_line_weight

    def oriented(coords):
        """A line's [lat, lon] points, pointed toward a bearing of 60 degrees.

        A sideways shift follows the drawing direction, and the MTA's shapes run either way, so lines on the same
        track could shift onto the same side. Pointing every line the same general way keeps them apart.
        """
        # longitude degrees are about 0.76 as long as latitude degrees in NYC
        heading = (coords[-1][0] - coords[0][0]) * 0.76 * 0.866 + (
            coords[-1][1] - coords[0][1]
        ) * 0.5
        return [[lat, lon] for lon, lat in (coords if heading >= 0 else coords[::-1])]

    station_styles = {
        "Full": {"fill_color": "#222222", "color": "white"},
        "Partial": {"fill_color": "#8c8c88", "color": "white"},
        "None": {"fill_color": "white", "color": "#222222"},
    }
    station_labels = {
        "Full": "Fully accessible",
        "Partial": "Partially accessible",
        "None": "Not accessible",
    }
    # one hue, fainter to stronger, so the transit layers on top stay readable
    access_hue = "#2c7fb8"
    access_opacity = {
        category: 0.03 + 0.5 * rank / (len(transit_access_categories) - 1)
        for rank, category in enumerate(transit_access_categories)
    }
    transit_counts = con.execute(
        """
        SELECT subway_stations, subway_stations_fully_accessible, subway_stations_partially_accessible, rail_stations,
            proposed_ibx_stations, ferry_landings, median_acres_reachable_by_transit, transit_access_covered_share,
            median_acres_reachable_by_transit_percentile, bus_stops
        FROM dim_community_district
        WHERE borocd = ?
        """,
        [selected_district],
    ).fetchall()[0]
    # the district's box with its size again on each side, past what the map shows, for the layers too heavy to draw
    # citywide. Lines are drawn citywide.
    transit_frame = """
        (SELECT
            ST_MakeEnvelope(
                ST_XMin(geom) - pad, ST_YMin(geom) - pad, ST_XMax(geom) + pad, ST_YMax(geom) + pad
            ) AS geom
        FROM (
            SELECT geom, greatest(ST_XMax(geom) - ST_XMin(geom), ST_YMax(geom) - ST_YMin(geom)) AS pad
            FROM dim_community_district
            WHERE borocd = $district
        ))
    """
    service_lines = con.sql("""
        SELECT service, service_name, map_geojson(geom, 0.00002) AS geojson
        FROM dim_subway_service_line
        ORDER BY service
    """).df()
    subway_stations = con.execute(
        f"""
        SELECT
            station.stop_name,
            station.daytime_routes,
            station.ada_accessibility,
            ST_X(station.geom) AS lon,
            ST_Y(station.geom) AS lat
        FROM dim_subway_station AS station, {transit_frame} AS frame
        WHERE ST_Intersects(station.geom, frame.geom)
        """,
        {"district": selected_district},
    ).df()
    bus_stops = con.execute(
        f"""
        SELECT ST_X(stop.geom) AS lon, ST_Y(stop.geom) AS lat
        FROM dim_bus_stop AS stop, {transit_frame} AS frame
        WHERE ST_Intersects(stop.geom, frame.geom)
        """,
        {"district": selected_district},
    ).df()
    rail_stations = con.execute(
        f"""
        SELECT station.station_name, station.railroad, station.branch, ST_X(station.geom) AS lon, ST_Y(station.geom) AS lat
        FROM dim_rail_station AS station, {transit_frame} AS frame
        WHERE ST_Intersects(station.geom, frame.geom)
        """,
        {"district": selected_district},
    ).df()
    ferry_landings = con.execute(
        f"""
        SELECT landing.landing_name, landing.ferry_system, ST_X(landing.geom) AS lon, ST_Y(landing.geom) AS lat
        FROM dim_ferry_landing AS landing, {transit_frame} AS frame
        WHERE ST_Intersects(landing.geom, frame.geom)
        """,
        {"district": selected_district},
    ).df()

    # the current capital plans, listed past to future, in light blues, lighter for the earlier plan
    mta_capital_plans = {
        "Capital Plan 2020 - 2024": "#c6dbef",
        "Capital Plan 2025 - 2029": "#6baed6",
    }
    # each project's point in each district in the frame, so a project in several districts shows once in each
    mta_capital_locations = con.execute(
        f"""
        SELECT
            project.capital_plan,
            project.agency_name,
            project.project_description,
            ST_X(project.geom) AS lon,
            ST_Y(project.geom) AS lat
        FROM dim_community_district_mta_capital_project AS project, {transit_frame} AS frame
        WHERE ST_Intersects(project.geom, frame.geom) AND list_contains($plans, project.capital_plan)
        ORDER BY project.plan_series
        """,
        {"district": selected_district, "plans": list(mta_capital_plans)},
    ).df()
    # the district's projects, with the stage and estimated completion of any detailed project its ACEP code is in
    mta_district_projects = con.execute(
        """
        SELECT
            located.acep,
            located.plan_series,
            located.capital_plan,
            located.agency_name,
            located.category,
            located.project_description,
            string_agg(DISTINCT project.title, '; ') AS detailed_project,
            string_agg(DISTINCT project.stage, '; ') AS stage,
            max(project.estimated_completion_date) AS estimated_completion
        FROM dim_community_district_mta_capital_project AS located
        LEFT JOIN dim_mta_capital_project_acep AS acep ON located.acep = acep.acep
        LEFT JOIN dim_mta_capital_project AS project ON acep.project_id = project.project_id
        WHERE located.borocd = $district AND list_contains($plans, located.capital_plan)
        GROUP BY ALL
        """,
        {"district": selected_district, "plans": list(mta_capital_plans)},
    ).df()

    # the alignment's two tracks run side by side, so one is drawn
    ibx_tracks = con.sql("""
        SELECT map_geojson(geom, 0.00002) AS geojson
        FROM dim_ibx_track
        WHERE ibx_track_id = 'IBX-001'
    """).df()
    ibx_stations = con.sql("""
        SELECT station_name, ST_X(geom) AS lon, ST_Y(geom) AS lat
        FROM dim_ibx_station
    """).df()

    transit_map = folium.Map(
        tiles="Esri.WorldGrayCanvas", zoom_snap=0.25, attribution_control=False
    )
    if transit_access_switch.value:
        folium.GeoJson(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": json.loads(_row.geojson),
                        "properties": {
                            "access_category": _row.access_category,
                            "label": f"{_row.access_category}, "
                            f"{_row.acres_reachable_in_60_minutes:,.0f} acres in 60 minutes",
                        },
                    }
                    for _row in transit_access_blocks.itertuples()
                ],
            },
            name="Transit access",
            style_function=lambda feature: {
                "fillColor": access_hue,
                "fillOpacity": access_opacity[feature["properties"]["access_category"]],
                "stroke": False,
            },
            tooltip=folium.GeoJsonTooltip(fields=["label"], labels=False),
        ).add_to(transit_map)
    # small dots under everything else, to show where buses serve, without route details
    if bus_stop_switch.value:
        folium.GeoJson(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [_row.lon, _row.lat],
                        },
                        "properties": {},
                    }
                    for _row in bus_stops.itertuples()
                ],
            },
            name="Bus stops",
            interactive=False,
            marker=folium.CircleMarker(
                radius=1.5, color="#55554f", weight=0, fill=True, fill_opacity=0.8
            ),
        ).add_to(transit_map)
    _district_layer = add_district_outline(transit_map)
    _line_layer = folium.FeatureGroup(name="Subway lines").add_to(transit_map)
    for _row in service_lines.itertuples():
        _geometry = json.loads(_row.geojson)
        _parts = (
            [_geometry["coordinates"]]
            if _geometry["type"] == "LineString"
            else _geometry["coordinates"]
        )
        _color = subway_service_colors[_row.service]
        for _part in _parts:
            folium.plugins.PolyLineOffset(
                oriented(_part),
                offset=service_offsets.get(_color, 0) * service_line_step,
                color=_color,
                weight=service_line_weight,
                opacity=0.95,
                tooltip=f"{_row.service}: {_row.service_name}",
            ).add_to(_line_layer)
    if planned_transit_switch.value:
        # proposed, so dashed and gray rather than in a service color
        for _row in ibx_tracks.itertuples():
            folium.GeoJson(
                json.loads(_row.geojson),
                name="Proposed IBX",
                style_function=lambda _feature: {
                    "color": "#55554f",
                    "weight": 3,
                    "dashArray": "8 6",
                },
                tooltip="Proposed Interborough Express (IBX)",
            ).add_to(transit_map)
        _ibx_station_layer = folium.FeatureGroup(name="Proposed IBX stations").add_to(
            transit_map
        )
        for _row in ibx_stations.itertuples():
            folium.CircleMarker(
                [_row.lat, _row.lon],
                radius=5,
                color="#55554f",
                weight=1.5,
                dash_array="3 2",
                fill=True,
                fill_color="white",
                fill_opacity=1,
                tooltip=f"Proposed IBX: {_row.station_name}",
            ).add_to(_ibx_station_layer)
            folium.Marker(
                [_row.lat, _row.lon],
                icon=folium.DivIcon(
                    icon_size=(0, 0),
                    html="""<div style="transform:translate(8px,-50%);white-space:nowrap;font:600 10px sans-serif;
                      color:#55554f;text-shadow:0 0 3px #fff,0 0 3px #fff,0 0 3px #fff">IBX</div>""",
                ),
            ).add_to(_ibx_station_layer)
    # project shapes follow the stations they're often at: circles for subway agencies, diamonds for commuter rail,
    # squares for the rest (bridges and tunnels, buses, construction). Drawn under the stations and larger, so a
    # project at a station looks like a ring around it.
    mta_capital_shapes = {
        "New York City Transit": "circle",
        "Staten Island Railway": "circle",
        "Long Island Rail Road": "diamond",
        "Metro-North Railroad": "diamond",
    }
    if mta_capital_switch.value:
        _capital_layer = folium.FeatureGroup(name="MTA capital projects").add_to(
            transit_map
        )
        for _row in mta_capital_locations.itertuples():
            _style = {
                "color": "#636363",
                "weight": 0.75,
                "fill": True,
                "fill_color": mta_capital_plans[_row.capital_plan],
                "fill_opacity": 1,
                "tooltip": f"{_row.capital_plan.removeprefix('Capital Plan ')}: {_row.project_description}",
            }
            _shape = mta_capital_shapes.get(_row.agency_name, "square")
            if _shape == "circle":
                folium.CircleMarker([_row.lat, _row.lon], radius=8, **_style).add_to(
                    _capital_layer
                )
            else:
                folium.RegularPolygonMarker(
                    [_row.lat, _row.lon],
                    number_of_sides=4,
                    rotation=0 if _shape == "diamond" else 45,
                    radius=10 if _shape == "diamond" else 7,
                    **_style,
                ).add_to(_capital_layer)
    _station_layer = folium.FeatureGroup(name="Subway stations").add_to(transit_map)
    for _row in subway_stations.itertuples():
        folium.CircleMarker(
            [_row.lat, _row.lon],
            radius=5,
            weight=1.5,
            fill=True,
            fill_opacity=1,
            tooltip=f"{_row.stop_name}: {_row.daytime_routes}, {station_labels[_row.ada_accessibility].lower()}",
            **station_styles[_row.ada_accessibility],
        ).add_to(_station_layer)
        folium.Marker(
            [_row.lat, _row.lon],
            icon=folium.DivIcon(
                icon_size=(0, 0),
                html=f"""<div style="transform:translate(8px,-50%);white-space:nowrap;font:600 10px sans-serif;
                  color:#222222;text-shadow:0 0 3px #fff,0 0 3px #fff,0 0 3px #fff">{_row.daytime_routes}</div>""",
            ),
        ).add_to(_station_layer)
    _rail_layer = folium.FeatureGroup(name="Commuter rail stations").add_to(transit_map)
    for _row in rail_stations.itertuples():
        folium.RegularPolygonMarker(
            [_row.lat, _row.lon],
            number_of_sides=4,
            radius=6,
            color="white",
            weight=1.5,
            fill_color="#55554f",
            fill_opacity=1,
            tooltip=f"{_row.station_name} ({_row.railroad}, {_row.branch})",
        ).add_to(_rail_layer)
    _ferry_layer = folium.FeatureGroup(name="Ferry landings").add_to(transit_map)
    for _row in ferry_landings.itertuples():
        folium.RegularPolygonMarker(
            [_row.lat, _row.lon],
            number_of_sides=3,
            rotation=30,
            radius=5,
            color="white",
            weight=1,
            fill_color="#55554f",
            fill_opacity=1,
            tooltip=f"{_row.landing_name} ({_row.ferry_system})",
        ).add_to(_ferry_layer)
        folium.Marker(
            [_row.lat, _row.lon],
            icon=folium.DivIcon(
                icon_size=(0, 0),
                html=f"""<div style="transform:translate(7px,-50%);white-space:nowrap;font:500 9px sans-serif;
                  color:#55554f;text-shadow:0 0 3px #fff,0 0 3px #fff,0 0 3px #fff">{_row.landing_name}</div>""",
            ),
        ).add_to(_ferry_layer)

    def _swatch_row(swatch, label, count=""):
        return f"""<tr>
          <td style="text-align:center">{swatch}</td>
          <td style="padding:0 12px 0 4px">{label}</td>
          <td style="text-align:right;font-weight:700">{count}</td>
        </tr>"""

    def _group_row(title, column_title=""):
        return f"""<tr><td colspan="2" style="padding-top:6px;font-weight:600">{title}</td>
          <td style="padding-top:6px;color:#55554f;text-align:right">{column_title}</td></tr>"""

    _station_counts = {
        "Full": transit_counts[1],
        "Partial": transit_counts[2],
        "None": transit_counts[0] - transit_counts[1] - transit_counts[2],
    }
    _legend = _group_row(f"Transit stations, {district_code}", "In district") + "".join(
        _swatch_row(
            f'<span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:{style["fill_color"]};'
            f'outline:1.5px solid {style["color"] if access == "None" else "#222222"}"></span>',
            f"Subway, {station_labels[access].lower()}",
            f"{_station_counts[access]:,}",
        )
        for access, style in station_styles.items()
    )
    _legend += _swatch_row(
        '<span style="display:inline-block;width:8px;height:8px;background:#55554f;'
        'transform:rotate(45deg)"></span>',
        "Commuter rail (LIRR, Metro-North)",
        f"{transit_counts[3]:,}",
    )
    _legend += _swatch_row(
        '<span style="display:inline-block;width:0;height:0;border-left:4px solid transparent;'
        'border-right:4px solid transparent;border-bottom:7px solid #55554f"></span>',
        "Ferry landing (NYC Ferry, Staten Island Ferry)",
        f"{transit_counts[5]:,}",
    )
    if bus_stop_switch.value:
        _legend += _swatch_row(
            '<span style="display:inline-block;width:4px;height:4px;border-radius:50%;background:#55554f"></span>',
            "Bus stop",
            f"{transit_counts[9]:,}",
        )
    if planned_transit_switch.value:
        _legend += _swatch_row(
            '<span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:white;'
            'outline:1.5px dashed #55554f"></span>',
            "Proposed IBX station",
            f"{transit_counts[4]:,}",
        )
    if mta_capital_switch.value:
        _legend += _group_row("MTA capital projects", "In district") + "".join(
            _swatch_row(
                f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{color};'
                'outline:0.75px solid #636363"></span>',
                plan.removeprefix("Capital Plan "),
                f"{(mta_district_projects['capital_plan'] == plan).sum():,}",
            )
            for plan, color in mta_capital_plans.items()
        )
        _legend += _swatch_row(
            "",
            '<span style="color:#55554f">Circle subway, diamond commuter rail, square other</span>',
        )
    if transit_access_switch.value:
        _legend += _group_row("Transit access", "Percentile") + _swatch_row(
            '<span style="display:inline-flex">'
            + "".join(
                f'<span style="display:inline-block;width:2px;height:8px;background:{access_hue};'
                f'opacity:{min(1, access_opacity[category] * 1.6):.2f}"></span>'
                for category in transit_access_categories
            )
            + "</span>",
            "Acres reachable in 60 minutes",
            "0 - 100",
        )
        # the district's median, weighted by area, over the part of it the file's blocks cover
        if transit_counts[6] is not None:
            _legend += _swatch_row(
                "",
                f"Median, {transit_counts[6]:,.0f} acres ({transit_counts[7]:.0%} covered)",
                f"{transit_counts[8]:.0f}",
            )
    add_legend(
        transit_map,
        f"""
      <table style="border-collapse:collapse">{_legend}</table>
    """,
    )
    transit_map.fit_bounds(_district_layer.get_bounds())
    mo.vstack(
        [
            mo.hstack(
                [
                    transit_access_switch,
                    bus_stop_switch,
                    planned_transit_switch,
                    mta_capital_switch,
                ],
                justify="start",
                gap=1.5,
            ),
            mo.iframe(transit_map.get_root().render(), height="600px"),
            source_note(
                "Sources: MTA [Subway Stations](https://data.ny.gov/d/39hk-dx4f), [Subway Service Lines](https://data.ny.gov/d/s692-irgq), and [Rail Stations](https://data.ny.gov/d/wxmd-5cpm), "
                "via data.ny.gov (November 2026); [NYC Ferry GTFS](https://nycferry.connexionz.net/rtt/public/utility/gtfs.aspx) (November 2026); DOT ferry terminals (August 2024)"
                + (
                    "; access to land area, no decay (CEQR file, November 2026)"
                    if transit_access_switch.value
                    else ""
                )
                + (
                    "; MTA [Bus Stops](https://data.ny.gov/d/2ucp-7wg5) via data.ny.gov (October 2026)"
                    if bus_stop_switch.value
                    else ""
                )
                + (
                    "; proposed IBX [design layers](https://www.arcgis.com/home/item.html?id=f605c0be149648b0a07a7872f312d73b), HDR for the MTA (October 2026)"
                    if planned_transit_switch.value
                    else ""
                )
                + (
                    "; MTA [Capital Dashboard Project Locations](https://data.ny.gov/d/wcsa-vkhf) via data.ny.gov (May 2026)"
                    if mta_capital_switch.value
                    else ""
                )
            ),
            mo.md(f"##### MTA capital projects in {district_code}"),
            mo.ui.table(
                mta_district_projects.sort_values(
                    [
                        "plan_series",
                        "estimated_completion",
                        "agency_name",
                        "project_description",
                    ]
                )
                .reset_index(drop=True)[
                    [
                        "capital_plan",
                        "agency_name",
                        "category",
                        "project_description",
                        "stage",
                        "estimated_completion",
                        "detailed_project",
                    ]
                ]
                .assign(
                    capital_plan=lambda df: df["capital_plan"].str.removeprefix(
                        "Capital Plan "
                    )
                )
                .rename(
                    columns={
                        "capital_plan": "Plan",
                        "agency_name": "Agency",
                        "category": "Category",
                        "project_description": "Project",
                        "stage": "Stage",
                        "estimated_completion": "Estimated completion",
                        "detailed_project": "Detailed as",
                    }
                ),
                selection=None,
                show_column_summaries=False,
                show_data_types=False,
                # formatted for display only, so the column sorts as dates
                format_mapping={
                    "Estimated completion": lambda value: (
                        "" if pd.isna(value) else f"{value:%b %Y}"
                    ),
                },
            ),
            source_note(
                "Sources: MTA [Capital Dashboard Project Locations](https://data.ny.gov/d/wcsa-vkhf) (May 2026), "
                "[Capital Project Details](https://data.ny.gov/d/9hy6-8j6t) (October 2026), and "
                "[Capital Project Budgets](https://data.ny.gov/d/f6fd-xfps) (October 2026), via data.ny.gov"
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(con, mo, pd, selected_district, source_note):
    _access = (
        con.execute(
            """
            SELECT
                district.transit_access_residents AS residents,
                district.transit_access_residents_at_or_above_median_block AS above_median,
                district.transit_access_residents_at_or_below_10th_percentile AS below_10th,
                district.transit_access_residents_not_in_file AS not_in_file,
                district.median_acres_reachable_by_transit AS median_acres,
                district.median_acres_reachable_by_transit_percentile AS median_percentile,
                district.transit_access_covered_share AS covered_share,
                city.residents AS city_residents,
                city.above_median AS city_above_median,
                city.below_10th AS city_below_10th,
                city.not_in_file AS city_not_in_file
            FROM dim_community_district AS district
            CROSS JOIN (
                SELECT
                    sum(transit_access_residents) AS residents,
                    sum(transit_access_residents_at_or_above_median_block) AS above_median,
                    sum(transit_access_residents_at_or_below_10th_percentile) AS below_10th,
                    sum(transit_access_residents_not_in_file) AS not_in_file
                FROM dim_community_district
            ) AS city
            WHERE district.borocd = ?
            """,
            [selected_district],
        )
        .df()
        .iloc[0]
    )

    def _resident_share(count, city_count, label):
        share = count / _access["residents"] if _access["residents"] else float("nan")
        return mo.stat(
            "n/a" if share != share else f"{share:.0%}",
            label=label,
            caption=f"{count:,.0f} residents · NYC: {city_count / _access['city_residents']:.0%}",
            bordered=True,
        )

    mo.vstack(
        [
            mo.vstack(
                [
                    mo.md("**Transit access, residents in 2020**"),
                    mo.hstack(
                        [
                            _resident_share(
                                _access["above_median"],
                                _access["city_above_median"],
                                "At or above the median block",
                            ),
                            _resident_share(
                                _access["below_10th"],
                                _access["city_below_10th"],
                                "At or below the 10th percentile",
                            ),
                            _resident_share(
                                _access["not_in_file"],
                                _access["city_not_in_file"],
                                "Not in the access file",
                            ),
                            mo.stat(
                                "n/a"
                                if pd.isna(_access["median_acres"])
                                else f"{_access['median_acres']:,.0f}",
                                label="Median acres reachable",
                                caption="n/a"
                                if pd.isna(_access["median_acres"])
                                else f"{_access['median_percentile']:.0f}th percentile block · "
                                f"{_access['covered_share']:.0%} of the district's area covered",
                                bordered=True,
                            ),
                        ],
                        justify="start",
                        gap=1,
                        widths="equal",
                    ),
                ],
                gap=0.25,
            ),
            source_note(
                "Percentiles are among the access file's blocks, so the median block is the 50th. Residents are 2020 "
                "Census block counts, via DCP Population FactFinder, in the district holding most of each block's area. "
                "The median weights the district's blocks by area."
            ),
        ],
        gap=0.5,
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.vstack(
        [
            mo.md("#### Transit Zones"),
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
    add_district_outline,
    add_legend,
    con,
    district_code,
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

    transit_zone_map = folium.Map(
        tiles="Esri.WorldGrayCanvas", zoom_snap=0.25, attribution_control=False
    )
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
    district_layer = add_district_outline(transit_zone_map)
    transit_zone_rows = "".join(
        f"""<tr>
          <td><span style="display:inline-block;width:14px;height:10px;background:{color};opacity:0.8;
            outline:1px solid #d8d8d4"></span></td>
          <td style="padding:0 12px 0 4px">{zone}</td>
          <td style="text-align:right;font-weight:700">{transit_zone_shares.get(zone, 0):.0%}</td>
        </tr>"""
        for zone, color in transit_zone_colors.items()
    )
    add_legend(
        transit_zone_map,
        f"""
      <table style="border-collapse:collapse">
        <tr style="color:#55554f"><td></td><td style="padding:0 12px 4px 4px;font-weight:600;color:#222222">Transit zone, {district_code}</td>
          <td style="padding-bottom:4px">Of district</td></tr>
        {transit_zone_rows}
      </table>
    """,
    )
    transit_zone_map.fit_bounds(district_layer.get_bounds())
    mo.vstack(
        [
            mo.iframe(transit_zone_map.get_root().render(), height="500px"),
            source_note(
                "Source: [DCP Greater Transit Zones](https://www.nyc.gov/site/planning/data-maps/open-data/dwn-greater-transit-zones.page) (December 2025)"
            ),
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
    add_district_outline,
    add_legend,
    con,
    district_code,
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

    street_map = folium.Map(
        tiles="Esri.WorldGrayCanvas", zoom_snap=0.25, attribution_control=False
    )
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
    _district_layer = add_district_outline(street_map)

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
    add_legend(
        street_map,
        f"""
      <table style="border-collapse:collapse">
        <tr style="color:#55554f"><td></td><td style="padding:0 12px 4px 4px;font-weight:600;color:#222222">Street safety, {district_code}</td>
          <td style="padding-bottom:4px">In district</td><td style="padding:0 0 4px 12px">With a project</td></tr>
        {street_rows}
      </table>
    """,
    )
    street_map.fit_bounds(_district_layer.get_bounds())
    mo.vstack(
        [
            mo.iframe(street_map.get_root().render(), height="500px"),
            source_note(
                "Source: DOT [Vision Zero View](https://vzv.nyc) priority [corridors](https://data.cityofnewyork.us/d/36nr-7fbp) and [intersections](https://data.cityofnewyork.us/d/tmt9-43em), and Street Improvement Project [corridors](https://data.cityofnewyork.us/d/if4c-w48d) and [intersections](https://data.cityofnewyork.us/d/shr7-eqdc), NYC Open Data (November 2026)"
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(con, mo, selected_district):
    _borough = con.execute(
        "SELECT boro_name FROM dim_community_district WHERE borocd = ?",
        [selected_district],
    ).fetchone()[0]
    # DOT's page has a section per borough, anchored by the name in lowercase with hyphens
    mo.md(
        f"DOT [current projects in {_borough}](https://www.nyc.gov/html/dot/html/about/current-projects.shtml#"
        f"{_borough.lower().replace(' ', '-')}). Not mapped, since the Street and Highway Capital Reconstruction "
        "Projects datasets on NYC Open Data only have completed projects."
    )
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
    - Labels mark the subdistricts in the district, with primary (PS) and intermediate (IS) utilization in its band's
      color.
    - Schools: DOE's LCGMS list as FacDB 26v1 geocodes it. Charter schools FacDB takes from NYSED instead aren't shown.
    - Capacity projects: SCA's, from the AHFT schools workbook's data (June 2026). Four additions to existing Brooklyn schools have no location, so they're only in the table.
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
    schools = con.sql("""
        SELECT school_name, school_type, school_subgroup, ST_X(geom) AS lon, ST_Y(geom) AS lat
        FROM dim_school
    """).df()
    school_capacity_projects = con.sql("""
        SELECT
            project_name,
            org_level,
            seats,
            start_date,
            capital_plan,
            school_subdistrict_id,
            ST_X(geom) AS lon,
            ST_Y(geom) AS lat
        FROM dim_school_capacity_project
    """).df()
    return school_capacity_projects, school_subdistrict_geojson, schools


@app.cell(hide_code=True)
def _(
    add_district_outline,
    add_legend,
    con,
    district_code,
    district_overlap_min_share,
    folium,
    json,
    mo,
    school_capacity_projects,
    school_subdistrict_geojson,
    schools,
    selected_district,
    source_note,
):
    # the blue and orange from the NYC logo, picked by eye. One dark outline for all, which keeps the lighter two
    # visible on every band and every point the same size. White is left for capacity projects.
    school_fills = {
        "PUBLIC K-12 SCHOOLS": ("Public school", "#252525"),
        "CHARTER K-12 SCHOOLS": ("Charter school", "#6f93cf"),
        "PUBLIC AND PRIVATE SPECIAL EDUCATION SCHOOLS": (
            "Special education school",
            "#f4a83a",
        ),
    }

    # capacity project markers sized by seats, area for area, up to the largest project in the file, with a floor a
    # couple of pixels past a school point so every project shows
    project_max_seats = school_capacity_projects["seats"].max()
    # the district's subdistricts, which the map labels and the legend counts in
    district_subdistricts = """
        SELECT school_subdistrict_id
        FROM dim_community_district_school_subdistrict
        WHERE borocd = $district AND share_of_district > $min_share
    """
    # schools in those subdistricts, for the legend
    school_counts = dict(
        con.execute(
            f"""
            SELECT school.school_subgroup, count(*)
            FROM dim_school AS school
            INNER JOIN dim_school_subdistrict AS subdistrict
                ON ST_Intersects(subdistrict.geom, school.geom)
            WHERE subdistrict.school_subdistrict_id IN ({district_subdistricts})
            GROUP BY school.school_subgroup
            """,
            {
                "district": selected_district,
                "min_share": district_overlap_min_share,
            },
        ).fetchall()
    )
    # capacity projects in those subdistricts, by the file's subdistrict, so the four without a location count too, as
    # in the table
    district_project_count = con.execute(
        f"""
        SELECT count(*)
        FROM dim_school_capacity_project
        WHERE school_subdistrict_id IN ({district_subdistricts})
        """,
        {"district": selected_district, "min_share": district_overlap_min_share},
    ).fetchall()[0][0]

    def project_radius(seats):
        return max(5, 9 * (seats / project_max_seats) ** 0.5)

    def utilization_band(utilization):
        if utilization != utilization:
            return None
        if utilization < 0.9:
            return "Under 90%"
        if utilization <= 1:
            return "90% to 100%"
        return "Over 100%"

    # each label where it covers the fewest school points and capacity projects at the starting view, from a grid of
    # spots inside its subdistrict, staying near the subdistrict's usual interior point when spots tie. The map frames
    # the subdistricts in 500 pixels of height and at least about 600 of width, narrower in compact width, so this
    # plans for 600, which only makes the label box cover more ground on wider maps. A label is about 64 by 44 pixels.
    school_subdistrict_labels = con.execute(
        f"""
        WITH subdistrict AS (
            SELECT
                school_subdistrict_id,
                ST_Transform(geom, 'EPSG:4326', 'EPSG:2263', true) AS geom
            FROM dim_school_subdistrict
            WHERE school_subdistrict_id IN ({district_subdistricts})
        ),

        frame AS (
            SELECT greatest(
                (max(ST_XMax(geom)) - min(ST_XMin(geom))) / 600,
                (max(ST_YMax(geom)) - min(ST_YMin(geom))) / 500
            ) AS feet_per_pixel
            FROM subdistrict
        ),

        label_box AS (
            SELECT 32 * feet_per_pixel AS half_width, 22 * feet_per_pixel AS half_height FROM frame
        ),

        candidate AS (
            SELECT
                subdistrict.school_subdistrict_id,
                ST_PointOnSurface(subdistrict.geom) AS home,
                ST_Point(
                    ST_XMin(subdistrict.geom) + (ST_XMax(subdistrict.geom) - ST_XMin(subdistrict.geom)) * x.step / 12,
                    ST_YMin(subdistrict.geom) + (ST_YMax(subdistrict.geom) - ST_YMin(subdistrict.geom)) * y.step / 12
                ) AS spot
            FROM subdistrict, range(1, 12) AS x(step), range(1, 12) AS y(step), label_box
            -- the whole label box inside the subdistrict
            WHERE ST_Contains(
                subdistrict.geom,
                ST_MakeEnvelope(
                    ST_XMin(subdistrict.geom) + (ST_XMax(subdistrict.geom) - ST_XMin(subdistrict.geom)) * x.step / 12
                    - label_box.half_width,
                    ST_YMin(subdistrict.geom) + (ST_YMax(subdistrict.geom) - ST_YMin(subdistrict.geom)) * y.step / 12
                    - label_box.half_height,
                    ST_XMin(subdistrict.geom) + (ST_XMax(subdistrict.geom) - ST_XMin(subdistrict.geom)) * x.step / 12
                    + label_box.half_width,
                    ST_YMin(subdistrict.geom) + (ST_YMax(subdistrict.geom) - ST_YMin(subdistrict.geom)) * y.step / 12
                    + label_box.half_height
                )
            )
            UNION ALL
            SELECT school_subdistrict_id, ST_PointOnSurface(geom), ST_PointOnSurface(geom) FROM subdistrict
        ),

        marker AS (
            SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:2263', true) AS geom FROM dim_school
            UNION ALL
            SELECT ST_Transform(geom, 'EPSG:4326', 'EPSG:2263', true)
            FROM dim_school_capacity_project
            WHERE geom IS NOT NULL
        ),

        scored AS (
            SELECT
                candidate.school_subdistrict_id,
                candidate.spot,
                count(marker.geom) AS covered,
                ST_Distance(candidate.spot, candidate.home) AS drift
            FROM candidate
            CROSS JOIN label_box
            LEFT JOIN marker
                ON abs(ST_X(marker.geom) - ST_X(candidate.spot)) <= label_box.half_width
                AND abs(ST_Y(marker.geom) - ST_Y(candidate.spot)) <= label_box.half_height
            GROUP BY ALL
        ),

        chosen AS (
            SELECT
                school_subdistrict_id,
                arg_min(spot, covered * 1e12 + drift) AS spot
            FROM scored
            GROUP BY school_subdistrict_id
        )

        SELECT
            school_subdistrict_id,
            ST_X(ST_Transform(spot, 'EPSG:2263', 'EPSG:4326', true)) AS lon,
            ST_Y(ST_Transform(spot, 'EPSG:2263', 'EPSG:4326', true)) AS lat
        FROM chosen
        """,
        {"district": selected_district, "min_share": district_overlap_min_share},
    ).df()

    # darker versions of the band colors for label text, which the light ones are too faint for
    utilization_text_colors = {
        "Under 90%": "#137a3f",
        "90% to 100%": "#a67c00",
        "Over 100%": "#b2182b",
    }

    def utilization_label(level, utilization):
        """A label piece like PS 65%, in its band's text color."""
        color = utilization_text_colors.get(utilization_band(utilization), "#55554f")
        value = "n/a" if utilization != utilization else f"{utilization:.0%}"
        return f"""<span style="color:{color}">{level} <b>{value}</b></span>"""

    def utilization_text(utilization):
        return "no projection" if utilization != utilization else f"{utilization:.0%}"

    _relevant_subdistricts = set(school_subdistrict_labels["school_subdistrict_id"])
    school_map = folium.Map(
        tiles="Esri.WorldGrayCanvas", zoom_snap=0.25, attribution_control=False
    )
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": json.loads(_row.geojson),
                    "properties": {
                        "relevant": _row.school_subdistrict_id
                        in _relevant_subdistricts,
                        "label": f"{_row.school_subdistrict_id} {_row.subdistrict_name}: primary "
                        + utilization_text(_row.ps_utilization_2036)
                        + ", intermediate "
                        + utilization_text(_row.is_utilization_2036),
                    },
                }
                for _row in school_subdistrict_geojson.itertuples()
            ],
        },
        name="School subdistricts",
        # the district's own subdistricts filled light gray, apart from the gray district line, the rest only outlined
        style_function=lambda feature: {
            "fillColor": "#8c8c88",
            "fillOpacity": 0.2 if feature["properties"]["relevant"] else 0,
            "color": "#8c8c88" if feature["properties"]["relevant"] else "#b8b8b4",
            "weight": 1.5 if feature["properties"]["relevant"] else 1,
        },
        tooltip=folium.GeoJsonTooltip(fields=["label"], labels=False),
    ).add_to(school_map)
    # lighter than on the other maps, so the subdistrict outlines stand out
    _district_layer = add_district_outline(school_map, opacity=0.45)
    # white circles like the school points, under them, so a school at a project sits inside its ring
    for _row in school_capacity_projects.dropna(subset=["lon"]).itertuples():
        folium.CircleMarker(
            [_row.lat, _row.lon],
            radius=project_radius(_row.seats),
            color="#252525",
            weight=1,
            fill=True,
            fill_color="white",
            fill_opacity=1,
            tooltip=f"{_row.project_name}: {_row.seats:,} seats",
        ).add_to(school_map)
    # one layer rather than a marker each, which is much faster to draw
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [_row.lon, _row.lat]},
                    "properties": {
                        "subgroup": _row.school_subgroup,
                        "label": f"{_row.school_name}: {_row.school_type}",
                    },
                }
                for _row in schools.itertuples()
            ],
        },
        name="Schools",
        marker=folium.CircleMarker(radius=3, weight=1, fill=True, fill_opacity=1),
        style_function=lambda feature: {
            "color": "#252525",
            "fillColor": school_fills[feature["properties"]["subgroup"]][1],
        },
        tooltip=folium.GeoJsonTooltip(fields=["label"], labels=False),
    ).add_to(school_map)
    _utilization = school_subdistrict_geojson.set_index("school_subdistrict_id")
    for _row in school_subdistrict_labels.itertuples():
        _values = _utilization.loc[_row.school_subdistrict_id]
        folium.Marker(
            [_row.lat, _row.lon],
            icon=folium.DivIcon(
                icon_size=(0, 0),
                html=f"""<div style="transform:translate(-50%,-50%);text-align:center;white-space:nowrap;
                  font:11px sans-serif;color:#222222;text-shadow:0 0 3px #fff,0 0 3px #fff,0 0 3px #fff">
                  {_row.school_subdistrict_id}<br>{utilization_label("PS", _values.ps_utilization_2036)}<br>{utilization_label("IS", _values.is_utilization_2036)}</div>""",
            ),
        ).add_to(school_map)
    band_rows = "".join(
        f"""<tr>
          <td colspan="3" style="padding:0;color:{color};font-weight:700">{band}</td>
        </tr>"""
        for band, color in utilization_text_colors.items()
    )
    school_rows = "".join(
        f"""<tr>
          <td style="text-align:center"><span style="display:inline-block;width:7px;height:7px;border-radius:50%;
            background:{fill};outline:1px solid #252525"></span></td>
          <td style="padding:0 4px">{label}</td>
          <td style="text-align:right;font-weight:700">{school_counts.get(subgroup, 0)}</td>
        </tr>"""
        for subgroup, (label, fill) in school_fills.items()
    )
    add_legend(
        school_map,
        f"""
      <table style="border-collapse:collapse">
        <tr><td colspan="3" style="padding:0 0 4px 0;font-weight:600">School utilization, 2036, {district_code}</td></tr>
        {band_rows}
        <tr><td colspan="2" style="padding-top:6px;font-weight:600">Schools</td>
          <td style="padding:6px 0 0 8px;color:#55554f">In subdistricts</td></tr>
        {school_rows}
        <tr>
          <td style="text-align:center"><span style="display:inline-block;width:11px;height:11px;border-radius:50%;
            background:white;outline:1px solid #252525"></span></td>
          <td style="padding:0 4px">Capacity project, sized by seats</td>
          <td style="text-align:right;font-weight:700">{district_project_count}</td>
        </tr>
      </table>
    """,
    )
    # framed on the district's subdistricts, which reach past it
    _subdistrict_bounds = con.execute(
        f"""
        SELECT min(ST_YMin(geom)), min(ST_XMin(geom)), max(ST_YMax(geom)), max(ST_XMax(geom))
        FROM dim_school_subdistrict
        WHERE school_subdistrict_id IN ({district_subdistricts})
        """,
        {"district": selected_district, "min_share": district_overlap_min_share},
    ).fetchall()[0]
    school_map.fit_bounds(
        _district_layer.get_bounds()
        if _subdistrict_bounds[0] is None
        else [_subdistrict_bounds[:2], _subdistrict_bounds[2:]]
    )

    mo.vstack(
        [
            mo.iframe(school_map.get_root().render(), height="500px"),
            source_note(
                "Sources: AHFT schools workbook; SCA school subdistricts (September 2025); DOE LCGMS via FacDB 26v1"
            ),
            mo.md(f"##### Capacity projects in {district_code}'s subdistricts"),
            mo.ui.table(
                school_capacity_projects[
                    school_capacity_projects["school_subdistrict_id"].isin(
                        school_subdistrict_labels["school_subdistrict_id"]
                    )
                ]
                .sort_values("start_date")
                .assign(start_date=lambda df: df["start_date"].dt.strftime("%B %Y"))[
                    [
                        "school_subdistrict_id",
                        "project_name",
                        "org_level",
                        "seats",
                        "start_date",
                        "capital_plan",
                    ]
                ]
                .reset_index(drop=True)
                .rename(
                    columns={
                        "school_subdistrict_id": "Subdistrict",
                        "project_name": "Capacity project",
                        "org_level": "Level",
                        "seats": "Seats",
                        "start_date": "Start date",
                        "capital_plan": "Capital plan",
                    }
                ),
                selection=None,
                show_column_summaries=False,
                show_data_types=False,
            ),
            source_note("Source: AHFT schools workbook"),
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
    add_district_outline,
    add_legend,
    con,
    district_code,
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
        SELECT floodplain_acres, floodplain_share_of_district, ST_AsGeoJSON(map_frame_geom) AS geojson,
            ST_AsGeoJSON(map_frame_edge_geom) AS edge_geojson
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
            ST_AsGeoJSON(frame.map_geom) AS geojson
        FROM dim_community_district_stormwater_flooding_frame AS frame
        LEFT JOIN dim_community_district_stormwater_flooding AS district
            USING (borocd, flooding_category)
        WHERE frame.borocd = ?
        """,
        [selected_district],
    ).df()

    flooding_map = folium.Map(
        tiles="Esri.WorldGrayCanvas", zoom_snap=0.25, attribution_control=False
    )
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
    flooding_district_layer = add_district_outline(flooding_map)

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
    add_legend(
        flooding_map,
        f"""
      <table style="border-collapse:collapse">
        <tr style="color:#55554f"><td></td><td style="font-weight:600;color:#222222">Flooding, {district_code}</td>
          <td style="padding:0 10px 0 0;text-align:right">Acres</td><td>Of district</td></tr>
        {_rows}
      </table>
    """,
    )
    flooding_map.fit_bounds(flooding_district_layer.get_bounds())
    mo.vstack(
        [
            mo.iframe(flooding_map.get_root().render(), height="500px"),
            source_note(
                "Sources: [Sea Level Rise Maps (2080s 100-year Floodplain)](https://data.cityofnewyork.us/d/ek8y-fsqz) and DEP [NYC Stormwater Flood Map](https://data.cityofnewyork.us/d/9i7c-xyvv) "
                "(moderate flood with 2050 sea level rise), NYC Open Data"
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

    Which treatment plant treats the district's sewage.

    """),
            mo.accordion(
                {
                    "Notes": mo.md("""
    - DEP's 14 wastewater treatment plants (Wastewater Resource Recovery Facilities).
    - Sewersheds: DEP's drainage areas for each plant, from a DCP copy of unknown date. It puts Rikers Island in Bowery Bay, as DEP's wastewater system map does.
    - Capacity and functions: DEP's [Map of the New York City Wastewater System](https://www.nyc.gov/assets/dep/downloads/pdf/water/wastewater/nyc-wastewater-system-map.pdf) (2025).
    - Flow: the 12-month running average in DEP's [monthly performance reports](https://data.cityofnewyork.us/d/hgue-hj96).
    """),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(con, mo):
    # nested areas within 1/5, 2/5, ... of the way from each plant to the farthest point of its sewershed, by straight
    # line. Drawn faintly on top of each other, they shade the sewershed darker toward its plant. Sewage follows the
    # interceptor sewers, not a straight line, so this only points to the plant.
    sewershed_band_count = 12
    # each band's opacity, so that all of them stacked at the plant come to 30%
    sewershed_band_opacity = 1 - (1 - 0.3) ** (1 / sewershed_band_count)
    sewershed_shading = mo.ui.switch(
        label="Shade sewersheds toward their plants", value=False
    )
    sewershed_bands = con.sql(f"""
        WITH shed AS (
            SELECT
                sewershed.wastewater_facility_id,
                ST_Transform(sewershed.geom, 'EPSG:4326', 'EPSG:2263', true) AS geom,
                ST_Transform(facility.geom, 'EPSG:4326', 'EPSG:2263', true) AS plant
            FROM dim_wastewater_sewershed AS sewershed
            INNER JOIN dim_wastewater_facility AS facility
                ON sewershed.wastewater_facility_id = facility.wastewater_facility_id
        ),

        reach AS (
            SELECT
                *,
                list_max([ST_Distance(plant, vertex.geom) FOR vertex IN ST_Dump(ST_Points(geom))]) AS farthest_ft
            FROM shed
        )

        SELECT
            band,
            map_geojson(
                ST_Transform(
                    ST_Intersection(geom, ST_Buffer(plant, farthest_ft * band / {sewershed_band_count}, 32)),
                    'EPSG:2263', 'EPSG:4326', true
                ),
                0.0001
            ) AS geojson
        FROM reach, range(1, {sewershed_band_count} + 1) AS bands(band)
    """).df()
    return sewershed_band_opacity, sewershed_bands, sewershed_shading


@app.cell(hide_code=True)
def _(
    add_district_outline,
    add_legend,
    con,
    district_code,
    district_overlap_min_share,
    folium,
    json,
    mo,
    selected_district,
    sewershed_band_opacity,
    sewershed_bands,
    sewershed_shading,
    source_note,
):
    # whole sewersheds, simplified to about 10 m like the transit zones, labeled with their plant
    sewersheds = con.sql("""
        SELECT
            sewershed.wastewater_facility_id,
            facility.wastewater_facility_name AS plant_name,
            map_geojson(sewershed.geom, 0.0001) AS geojson,
            ST_X(ST_PointOnSurface(sewershed.geom)) AS label_lon,
            ST_Y(ST_PointOnSurface(sewershed.geom)) AS label_lat
        FROM dim_wastewater_sewershed AS sewershed
        INNER JOIN dim_wastewater_facility AS facility
            ON sewershed.wastewater_facility_id = facility.wastewater_facility_id
    """).df()
    # the plants' own properties on DEP's MS4 map, drawn as areas like the MS4 map does
    wastewater_plants = con.sql("""
        SELECT facility.wastewater_facility_name, map_geojson(area.geom, 0.00001) AS geojson
        FROM dim_ms4_drainage_area AS area
        INNER JOIN dim_wastewater_facility AS facility
            ON area.wastewater_facility_id = facility.wastewater_facility_id
        WHERE area.drains_to_type = 'Wastewater facility'
    """).df()
    # a point for every plant, since only 10 of the 14 have a site on the MS4 map
    wastewater_plant_points = con.sql("""
        SELECT wastewater_facility_name, ST_X(geom) AS lon, ST_Y(geom) AS lat
        FROM dim_wastewater_facility
    """).df()
    sewershed_shares = dict(
        con.execute(
            """
            SELECT wastewater_facility_id, share_of_district
            FROM dim_community_district_wastewater_sewershed
            WHERE borocd = ?
            ORDER BY share_of_district DESC
            """,
            [selected_district],
        ).fetchall()
    )
    # the same green as the plant sites on the MS4 map for the plants, the sewershed borders, and the shading, and a
    # lighter one to fill the sewersheds when they aren't shaded
    sewershed_color = "#31a354"
    sewershed_fill = "#a1d99b"
    sewershed_plant_names = dict(
        zip(sewersheds["wastewater_facility_id"], sewersheds["plant_name"])
    )

    sewershed_map = folium.Map(
        tiles="Esri.WorldGrayCanvas", zoom_snap=0.25, attribution_control=False
    )
    if sewershed_shading.value:
        folium.GeoJson(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": json.loads(_row.geojson),
                        "properties": {},
                    }
                    for _row in sewershed_bands.itertuples()
                ],
            },
            name="Sewershed shading toward plants",
            interactive=False,
            style_function=lambda _feature: {
                "fillColor": sewershed_color,
                "fillOpacity": sewershed_band_opacity,
                "stroke": False,
            },
        ).add_to(sewershed_map)
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": json.loads(_row.geojson),
                    "properties": {
                        "plant": _row.plant_name,
                    },
                }
                for _row in sewersheds.itertuples()
            ],
        },
        name="Sewersheds",
        style_function=lambda _feature: {
            "fillColor": sewershed_fill,
            "fillOpacity": 0 if sewershed_shading.value else 0.25,
            "color": sewershed_color,
            "weight": 1.5,
        },
        tooltip=folium.GeoJsonTooltip(fields=["plant"], labels=False),
    ).add_to(sewershed_map)
    _district_layer = add_district_outline(sewershed_map)
    for _row in sewersheds.itertuples():
        folium.Marker(
            [_row.label_lat, _row.label_lon],
            icon=folium.DivIcon(
                icon_size=(0, 0),
                html=f"""<div style="transform:translate(-50%,-50%);white-space:nowrap;font:600 11px sans-serif;
                  color:#55554f;text-shadow:0 0 3px #fff,0 0 3px #fff,0 0 3px #fff">{_row.plant_name}</div>""",
            ),
        ).add_to(sewershed_map)
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": json.loads(_row.geojson),
                    "properties": {"plant": f"{_row.wastewater_facility_name} WWTP"},
                }
                for _row in wastewater_plants.itertuples()
            ],
        },
        name="Wastewater treatment plants",
        style_function=lambda _feature: {
            "fillColor": sewershed_color,
            "fillOpacity": 0.5,
            "color": sewershed_color,
            "weight": 0.5,
        },
        tooltip=folium.GeoJsonTooltip(fields=["plant"], labels=False),
    ).add_to(sewershed_map)
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [_row.lon, _row.lat]},
                    "properties": {"plant": f"{_row.wastewater_facility_name} WWTP"},
                }
                for _row in wastewater_plant_points.itertuples()
            ],
        },
        name="Wastewater treatment plant locations",
        marker=folium.CircleMarker(radius=7, weight=2, fill=True, fill_opacity=1),
        style_function=lambda _feature: {
            "color": "white",
            "fillColor": sewershed_color,
        },
        tooltip=folium.GeoJsonTooltip(fields=["plant"], labels=False),
    ).add_to(sewershed_map)
    _sewershed_swatch = (
        f"""<span style="display:inline-block;width:14px;height:10px;outline:1px solid {sewershed_color};
            background:linear-gradient(to right, {sewershed_color}12, {sewershed_color}4d)"></span>"""
        if sewershed_shading.value
        else f"""<span style="display:inline-block;width:14px;height:10px;background:{sewershed_fill};opacity:0.6;
            outline:1px solid {sewershed_color}"></span>"""
    )
    _sewershed_rows = "".join(
        f"""<tr>
          <td></td>
          <td style="padding:0 12px 0 4px">{sewershed_plant_names[name]}</td>
          <td style="text-align:right;font-weight:700">{share:.0%}</td>
        </tr>"""
        for name, share in sewershed_shares.items()
    )
    add_legend(
        sewershed_map,
        f"""
      <table style="border-collapse:collapse">
        <tr style="color:#55554f"><td></td><td style="padding:0 12px 4px 4px;font-weight:600;color:#222222">Sewage treated at, {district_code}</td>
          <td style="padding-bottom:4px">Of district</td></tr>
        {_sewershed_rows}
        <tr><td colspan="3" style="padding-top:6px"></td></tr>
        <tr>
          <td>{_sewershed_swatch}</td>
          <td colspan="2" style="padding:0 0 0 4px">Sewershed{", darker toward its plant" if sewershed_shading.value else ""}</td>
        </tr>
        <tr>
          <td style="text-align:center"><span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:{sewershed_color};
            box-shadow:0 0 0 2px white,0 0 0 3px #d8d8d4"></span></td>
          <td colspan="2" style="padding:0 0 0 4px">Wastewater treatment plant</td>
        </tr>

      </table>
    """,
    )
    # framed on the district's sewersheds, as the school map is on its subdistricts
    _sewershed_bounds = con.execute(
        """
        SELECT min(ST_YMin(sewershed.geom)), min(ST_XMin(sewershed.geom)), max(ST_YMax(sewershed.geom)), max(ST_XMax(sewershed.geom))
        FROM dim_wastewater_sewershed AS sewershed
        INNER JOIN dim_community_district_wastewater_sewershed AS overlap
            ON sewershed.wastewater_facility_id = overlap.wastewater_facility_id
        WHERE overlap.borocd = ? AND overlap.share_of_district > ?
        """,
        [selected_district, district_overlap_min_share],
    ).fetchall()[0]
    sewershed_map.fit_bounds(
        _district_layer.get_bounds()
        if _sewershed_bounds[0] is None
        else [_sewershed_bounds[:2], _sewershed_bounds[2:]]
    )
    mo.vstack(
        [
            sewershed_shading,
            mo.iframe(sewershed_map.get_root().render(), height="500px"),
            source_note(
                "Sources: DEP WRRF drainage areas (DCP copy, date unknown); DEP [MS4 map](https://data.cityofnewyork.us/d/j57c-rqtq), NYC Open Data (August 2020); plant locations from the MS4 map, or FacDB 26v1 where it has no site"
            ),
        ]
    )
    return sewershed_plant_names, sewershed_shares


@app.cell(hide_code=True)
def _(
    alt,
    con,
    json,
    mo,
    sewershed_plant_names,
    sewershed_shares,
    source_note,
):
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
        "SELECT * EXCLUDE (geom) FROM dim_wastewater_facility ORDER BY capacity_mgd DESC"
    ).df()
    wastewater_facilities["share_label"] = wastewater_facilities[
        "latest_flow_12_month_average_share_of_capacity"
    ].map("{:.0%}".format)
    _flow_month = wastewater_facilities["latest_report_month"].max().strftime("%B %Y")

    # the plants treating some of the district's sewage, per the sewershed map above, in bold
    _district_plants = [
        sewershed_plant_names[name]
        for name in sewershed_shares
        if name in sewershed_plant_names
    ]

    def _facility_y(show_names):
        return alt.Y(
            "wastewater_facility_name:N",
            title=None,
            sort=wastewater_facilities["wastewater_facility_name"].tolist(),
            axis=alt.Axis(
                labels=show_names,
                ticks=False,
                domain=False,
                labelFontWeight=alt.expr(
                    f"indexof({json.dumps(_district_plants)}, datum.value) >= 0 ? 'bold' : 'normal'"
                ),
            ),
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
        _base.mark_text(
            align="left",
            dx=4,
            fontSize=11,
            color="#55554f",
            fontWeight=alt.expr(
                f"indexof({json.dumps(_district_plants)}, datum.wastewater_facility_name) >= 0 ? 'bold' : 'normal'"
            ),
        ).encode(x="capacity_mgd:Q", text="share_label:N"),
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
                "Sources: DEP [Map of the New York City Wastewater System](https://www.nyc.gov/assets/dep/downloads/pdf/water/wastewater/nyc-wastewater-system-map.pdf) (2025); DEP [Wastewater Treatment Plant Performance Data](https://data.cityofnewyork.us/d/hgue-hj96), NYC Open Data (through July 2025)"
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.vstack(
        [
            mo.md("""
    #### Sewer Types

    Where the district has separate sewers, and land that drains straight to the water.
    """),
            mo.accordion(
                {
                    "Notes": mo.md("""
    - Separate: sewage goes to a treatment plant, stormwater straight to the water.
    - Direct drainage: land that drains straight to the water.
    - The rest isn't labeled. DEP's file has no combined class, and DEP's 2025 wastewater system map shows separate
      sewers in some places the file leaves out, such as JFK Airport.
    - DEP's file is revised 2001 to 2008, from a DCP copy.
    """),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(
    add_district_outline,
    add_legend,
    con,
    district_code,
    folium,
    json,
    mo,
    selected_district,
    source_note,
):
    sewer_area_colors = {
        "Separate": "#3182bd",
        "Direct drainage": "#9ecae1",
    }
    sewer_areas = con.sql("""
        SELECT sewer_area_type, map_geojson(geom, 0.0001) AS geojson
        FROM dim_sewer_area
    """).df()
    sewer_area_shares = dict(
        con.execute(
            """
            SELECT sewer_area_type, share_of_district
            FROM dim_community_district_sewer_area
            WHERE borocd = ?
            """,
            [selected_district],
        ).fetchall()
    )

    sewer_area_map = folium.Map(
        tiles="Esri.WorldGrayCanvas", zoom_snap=0.25, attribution_control=False
    )
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": json.loads(_row.geojson),
                    "properties": {"sewer_area_type": _row.sewer_area_type},
                }
                for _row in sewer_areas.itertuples()
            ],
        },
        name="Sewer types",
        style_function=lambda feature: {
            "fillColor": sewer_area_colors[feature["properties"]["sewer_area_type"]],
            "fillOpacity": 0.5,
            "color": "white",
            "weight": 0.5,
        },
        tooltip=folium.GeoJsonTooltip(fields=["sewer_area_type"], labels=False),
    ).add_to(sewer_area_map)
    _district_layer = add_district_outline(sewer_area_map)
    _sewer_area_rows = "".join(
        f"""<tr>
          <td><span style="display:inline-block;width:14px;height:10px;background:{color};opacity:0.8;
            outline:1px solid #d8d8d4"></span></td>
          <td style="padding:0 12px 0 4px">{area_type}</td>
          <td style="text-align:right;font-weight:700">{sewer_area_shares.get(area_type, 0):.0%}</td>
        </tr>"""
        for area_type, color in sewer_area_colors.items()
    )
    add_legend(
        sewer_area_map,
        f"""
      <table style="border-collapse:collapse">
        <tr style="color:#55554f"><td></td><td style="padding:0 12px 4px 4px;font-weight:600;color:#222222">Sewer type, {district_code}</td>
          <td style="padding-bottom:4px">Of district</td></tr>
        {_sewer_area_rows}
        <tr>
          <td><span style="display:inline-block;width:14px;height:10px;background:white;outline:1px solid #d8d8d4"></span></td>
          <td style="padding:0 12px 0 4px">Not separate or direct drainage</td>
          <td style="text-align:right;font-weight:700">{max(0, 1 - sum(sewer_area_shares.values())):.0%}</td>
        </tr>
      </table>
    """,
    )
    sewer_area_map.fit_bounds(_district_layer.get_bounds())
    mo.vstack(
        [
            mo.iframe(sewer_area_map.get_root().render(), height="500px"),
            source_note(
                "Source: DEP separate sewer and direct drainage areas (DCP copy, revised 2001 to 2008)"
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
    - Each area drains to an MS4 outfall, or by overland flow straight to the water.
    - Areas with combined or sanitary sewers, most of the city, aren't on the map.
    - Bluebelt BMPs: DEP's best management practices (ponds, wetlands, and the like), complete or in construction as of
      March 2024, as points.
    - Bluebelt land: DEP-owned Bluebelt parcels on Staten Island, from a DCP copy of DEP's file from about 2014. A
      parcel's tooltip lists the BMPs inside it.
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
    bluebelt_bmps = con.sql("""
        SELECT bmp_id, bmp_type, year_built, status, ST_X(geom) AS lon, ST_Y(geom) AS lat
        FROM dim_bluebelt_bmp
    """).df()
    bluebelt_parcels = con.sql("""
        SELECT watershed, block, lot, bmps, map_geojson(geom, 0.00001) AS geojson
        FROM dim_bluebelt_parcel
    """).df()
    return bluebelt_bmps, bluebelt_parcels, ms4_drainage_geojson, ms4_outfalls


@app.cell(hide_code=True)
def _(
    add_district_outline,
    add_legend,
    bluebelt_bmps,
    bluebelt_parcels,
    con,
    district_code,
    folium,
    json,
    mo,
    ms4_drainage_geojson,
    ms4_outfalls,
    pd,
    selected_district,
    source_note,
):
    ms4_drainage_colors = {
        "MS4 outfall": "#3182bd",
        "Overland flow": "#9ecae1",
        "Wastewater facility": "#31a354",
    }
    # the areas draining to a wastewater facility are the plants' own properties, so they're listed as plants rather
    # than as where the district drains
    ms4_drainage_legend = ["MS4 outfall", "Overland flow"]
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

    ms4_map = folium.Map(
        tiles="Esri.WorldGrayCanvas", zoom_snap=0.25, attribution_control=False
    )
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
    # one layer rather than a marker each, which is much faster to draw
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [row.lon, row.lat]},
                    "properties": {
                        "outfall_type": row.outfall_type,
                        "label": f"{row.outfall_id}: {ms4_outfall_labels[row.outfall_type]}, {row.waterbody}",
                    },
                }
                for row in ms4_outfalls.itertuples()
            ],
        },
        name="Outfalls",
        marker=folium.CircleMarker(radius=3, weight=0.5, fill=True, fill_opacity=1),
        style_function=lambda feature: {
            "color": "white",
            "fillColor": ms4_outfall_colors[feature["properties"]["outfall_type"]],
        },
        tooltip=folium.GeoJsonTooltip(fields=["label"], labels=False),
    ).add_to(ms4_map)
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": json.loads(row.geojson),
                    "properties": {
                        "label": f"{row.watershed} Bluebelt, "
                        + (
                            f"block {row.block} lot {row.lot}"
                            if row.block
                            else "roadbed"
                        )
                        + ("" if row.bmps is None else f": {row.bmps}")
                    },
                }
                for row in bluebelt_parcels.itertuples()
            ],
        },
        name="Bluebelt land, about 2014",
        # a yellow green, for preserved land, apart from the blue drainage areas and the plants' darker green
        style_function=lambda feature: {
            "fillColor": "#addd8e",
            "fillOpacity": 0.7,
            "color": "#78c679",
            "weight": 1,
        },
        tooltip=folium.GeoJsonTooltip(fields=["label"], labels=False),
    ).add_to(ms4_map)
    _bluebelt_layer = folium.FeatureGroup(name="Bluebelt BMPs, 2024").add_to(ms4_map)
    for row in bluebelt_bmps.itertuples():
        folium.RegularPolygonMarker(
            [row.lat, row.lon],
            number_of_sides=4,
            rotation=45,
            radius=5,
            color="white",
            weight=1,
            fill_color="#01665e",
            fill_opacity=1,
            tooltip=f"{row.bmp_type} ({row.bmp_id}), "
            + (
                row.status.lower()
                if pd.isna(row.year_built)
                else f"built {row.year_built:.0f}"
            ),
        ).add_to(_bluebelt_layer)
    ms4_district_layer = add_district_outline(ms4_map)
    ms4_drainage_rows = "".join(
        f"""<tr>
          <td><span style="display:inline-block;width:14px;height:10px;background:{color};opacity:0.8;
            outline:1px solid #d8d8d4"></span></td>
          <td style="padding:0 12px 0 4px">{drainage_type}</td>
          <td style="text-align:right;font-weight:700">{ms4_drainage_shares.get(drainage_type, 0):.0%}</td>
        </tr>"""
        for drainage_type, color in ms4_drainage_colors.items()
        if drainage_type in ms4_drainage_legend
    )
    ms4_outfall_rows = "".join(
        f"""<tr>
          <td style="text-align:center"><span style="display:inline-block;width:7px;height:7px;border-radius:50%;
            background:{ms4_outfall_colors[outfall_type]}"></span></td>
          <td colspan="2" style="padding:0 0 0 4px">{label}</td>
        </tr>"""
        for outfall_type, label in ms4_outfall_labels.items()
    )
    add_legend(
        ms4_map,
        f"""
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
        <tr><td colspan="3" style="padding-top:6px;font-weight:600">Other</td></tr>
        <tr>
          <td><span style="display:inline-block;width:14px;height:10px;background:{ms4_drainage_colors["Wastewater facility"]};
            opacity:0.8;outline:1px solid #d8d8d4"></span></td>
          <td colspan="2" style="padding:0 0 0 4px">Wastewater treatment plant</td>
        </tr>
        <tr>
          <td style="text-align:center"><span style="display:inline-block;width:8px;height:8px;background:#01665e"></span></td>
          <td colspan="2" style="padding:0 0 0 4px">Bluebelt BMP, 2024</td>
        </tr>
        <tr>
          <td><span style="display:inline-block;width:14px;height:10px;background:#addd8e;opacity:0.85;
            outline:1px solid #78c679"></span></td>
          <td colspan="2" style="padding:0 0 0 4px">Bluebelt land, about 2014</td>
        </tr>
      </table>
    """,
    )
    ms4_map.fit_bounds(ms4_district_layer.get_bounds())
    mo.vstack(
        [
            mo.iframe(ms4_map.get_root().render(), height="500px"),
            source_note(
                "Sources: DEP [MS4 map](https://data.cityofnewyork.us/d/j57c-rqtq), NYC Open Data (August 2020); DEP [Bluebelt BMPs](https://services.arcgis.com/at3rDjch5X7i9Bag/arcgis/rest/services/Bluebelt_BMPs/FeatureServer/0) (March 2024); DEP Bluebelt properties (DCP copy, about 2014)"
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.vstack(
        [
            mo.md("""
    ### Open Space

    #### Walk to a Park

    How many of the district's residents live within walking distance of a park.
    """),
            mo.accordion(
                {
                    "Notes": mo.md("""
    - Walking distance is NYC Parks' Walk to a Park measure: a quarter mile for small sites like playgrounds and sitting
      areas, a half mile for larger parks. Parks' CEQR version counts a tax lot as served if half or more of it is in reach.
    - Residents are 2020 Census block counts. A block counts as served, whole, if a point inside it is in Parks' served
      area, so a block whose point falls on a street or an unserved lot counts as unserved.
    - Active Recreation Areas and New or Renovated Parks are from NYC Parks' Vital Parks Explorer: the share of the
      district's residents within a 10-minute walk of each.
    - The map shows NYC Parks properties and Parks' served area.
    - Capital projects: NYC Parks' Capital Project Tracker, by phase, at each of a project's locations. Counts are projects
      with a location in the district. Completed projects show only with the switch on.
    - In the table, phase finish and construction finish are the actual month if there is one, then Parks' adjusted
      month, then its projected month.
    """),
                }
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(con, selected_district):
    # parks in the district's box with its size again on each side, past what the map shows
    open_space_parks = con.execute(
        """
        WITH frame AS (
            SELECT
                ST_XMin(geom) AS x0,
                ST_XMax(geom) AS x1,
                ST_YMin(geom) AS y0,
                ST_YMax(geom) AS y1,
                greatest(ST_XMax(geom) - ST_XMin(geom), ST_YMax(geom) - ST_YMin(geom)) AS pad
            FROM dim_community_district
            WHERE borocd = ?
        )

        SELECT park.park_name, park.park_type, park.acres, map_geojson(park.geom, 0.00002) AS geojson
        FROM dim_park AS park
        CROSS JOIN frame
        WHERE ST_XMax(park.geom) >= frame.x0 - frame.pad
            AND ST_XMin(park.geom) <= frame.x1 + frame.pad
            AND ST_YMax(park.geom) >= frame.y0 - frame.pad
            AND ST_YMin(park.geom) <= frame.y1 + frame.pad
        """,
        [selected_district],
    ).df()
    open_space_access = (
        con.execute(
            """
            SELECT
                district.walk_to_a_park_residents AS residents,
                district.walk_to_a_park_residents_served AS served,
                district.active_recreation_areas_pct,
                district.new_or_renovated_parks_pct,
                district.new_or_renovated_parks_future_pct,
                district.citywide_active_recreation_areas_pct,
                district.citywide_new_or_renovated_parks_pct,
                district.citywide_new_or_renovated_parks_future_pct,
                city.residents AS city_residents,
                city.served AS city_served
            FROM dim_community_district AS district
            CROSS JOIN (
                SELECT
                    sum(walk_to_a_park_residents) AS residents,
                    sum(walk_to_a_park_residents_served) AS served
                FROM dim_community_district
            ) AS city
            WHERE district.borocd = ?
            """,
            [selected_district],
        )
        .df()
        .iloc[0]
    )
    # Parks' served area in the same box as the parks
    walk_to_a_park_area = con.execute(
        """
        WITH frame AS (
            SELECT
                ST_XMin(geom) AS x0,
                ST_XMax(geom) AS x1,
                ST_YMin(geom) AS y0,
                ST_YMax(geom) AS y1,
                greatest(ST_XMax(geom) - ST_XMin(geom), ST_YMax(geom) - ST_YMin(geom)) AS pad
            FROM dim_community_district
            WHERE borocd = ?
        )

        SELECT map_geojson(ST_Union_Agg(area.geom), 0.00002) AS geojson
        FROM dim_walk_to_a_park_area AS area
        CROSS JOIN frame
        WHERE ST_XMax(area.geom) >= frame.x0 - frame.pad
            AND ST_XMin(area.geom) <= frame.x1 + frame.pad
            AND ST_YMax(area.geom) >= frame.y0 - frame.pad
            AND ST_YMin(area.geom) <= frame.y1 + frame.pad
        """,
        [selected_district],
    ).fetchall()[0][0]
    # Parks capital projects with a location in the same box, one row per location, and the projects with a location in
    # the district, for the legend counts
    park_projects = con.execute(
        """
        WITH frame AS (
            SELECT
                ST_XMin(geom) AS x0,
                ST_XMax(geom) AS x1,
                ST_YMin(geom) AS y0,
                ST_YMax(geom) AS y1,
                greatest(ST_XMax(geom) - ST_XMin(geom), ST_YMax(geom) - ST_YMin(geom)) AS pad
            FROM dim_community_district
            WHERE borocd = ?
        )

        SELECT
            project.tracker_id,
            project.title,
            project.current_phase,
            project.total_funding,
            ST_X(location.geom) AS lon,
            ST_Y(location.geom) AS lat
        FROM dim_park_capital_project_location AS location
        INNER JOIN dim_park_capital_project AS project ON location.tracker_id = project.tracker_id
        CROSS JOIN frame
        WHERE ST_X(location.geom) BETWEEN frame.x0 - frame.pad AND frame.x1 + frame.pad
            AND ST_Y(location.geom) BETWEEN frame.y0 - frame.pad AND frame.y1 + frame.pad
        """,
        [selected_district],
    ).df()
    park_project_counts = dict(
        con.execute(
            """
            SELECT project.current_phase, count(DISTINCT project.tracker_id)
            FROM dim_park_capital_project_location AS location
            INNER JOIN dim_park_capital_project AS project ON location.tracker_id = project.tracker_id
            INNER JOIN dim_community_district AS district ON ST_Intersects(district.geom, location.geom)
            WHERE district.borocd = ?
            GROUP BY project.current_phase
            """,
            [selected_district],
        ).fetchall()
    )
    park_district_projects_raw = con.execute(
        """
        SELECT DISTINCT
            project.tracker_id,
            project.title,
            project.current_phase,
            project.total_funding,
            array_to_string(project.funding_sources, ', ') AS funding_sources,
            -- the current phase's percent complete and finish, the actual date if there is one, then the adjusted, then
            -- the projected
            CASE project.current_phase
                WHEN 'design' THEN project.design_percent_complete
                WHEN 'design-procurement' THEN project.procurement_percent_complete
                WHEN 'procurement' THEN project.procurement_percent_complete
                WHEN 'construction' THEN project.construction_percent_complete
            END AS phase_percent_complete,
            CASE project.current_phase
                WHEN 'design' THEN coalesce(
                    project.design_actual_completion, project.design_adjusted_completion, project.design_projected_completion
                )
                WHEN 'design-procurement' THEN coalesce(
                    project.procurement_actual_completion,
                    project.procurement_adjusted_completion,
                    project.procurement_projected_completion
                )
                WHEN 'procurement' THEN coalesce(
                    project.procurement_actual_completion,
                    project.procurement_adjusted_completion,
                    project.procurement_projected_completion
                )
                WHEN 'construction' THEN coalesce(
                    project.construction_actual_completion,
                    project.construction_adjusted_completion,
                    project.construction_projected_completion
                )
            END AS phase_finish,
            coalesce(
                project.construction_actual_completion,
                project.construction_adjusted_completion,
                project.construction_projected_completion
            ) AS construction_finish
        FROM dim_park_capital_project_location AS location
        INNER JOIN dim_park_capital_project AS project ON location.tracker_id = project.tracker_id
        INNER JOIN dim_community_district AS district ON ST_Intersects(district.geom, location.geom)
        WHERE district.borocd = ?
        """,
        [selected_district],
    ).df()
    return (
        open_space_access,
        open_space_parks,
        park_district_projects_raw,
        park_project_counts,
        park_projects,
        walk_to_a_park_area,
    )


@app.cell(hide_code=True)
def _(mo):
    park_projects_completed_switch = mo.ui.switch(
        value=False, label="Completed projects"
    )
    return (park_projects_completed_switch,)


@app.cell(hide_code=True)
def _(
    add_district_outline,
    add_legend,
    district_code,
    folium,
    json,
    mo,
    open_space_access,
    open_space_parks,
    park_district_projects_raw,
    park_project_counts,
    park_projects,
    park_projects_completed_switch,
    pd,
    source_note,
    walk_to_a_park_area,
):
    park_color = "#41ab5d"
    # capital projects by phase: oranges darkening as they near completion, then a light green once done. The feed's
    # design-procurement projects go with procurement.
    # Listed past to future, which sets the order of the legend and the table.
    project_phases = {
        "Completed": (["completed"], "#a1d99b"),
        "Construction": (["construction"], "#e6550d"),
        "Procurement": (["design-procurement", "procurement"], "#fd8d3c"),
        "Design": (["design"], "#fdc895"),
        "Proposed": (["proposed"], "#ffffff"),
    }
    project_phase_of = {
        feed_phase: phase
        for phase, (feed_phases, _color) in project_phases.items()
        for feed_phase in feed_phases
    }
    served_color = "#c7e9c0"
    park_map = folium.Map(
        tiles="Esri.WorldGrayCanvas", zoom_snap=0.25, attribution_control=False
    )
    # Parks' served area under the parks, so unserved blocks show as plain map
    if walk_to_a_park_area is not None:
        folium.GeoJson(
            json.loads(walk_to_a_park_area),
            name="Within walking distance of a park",
            interactive=False,
            style_function=lambda _feature: {
                "fillColor": served_color,
                "fillOpacity": 0.55,
                "stroke": False,
            },
        ).add_to(park_map)
    folium.GeoJson(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": json.loads(_row.geojson),
                    "properties": {
                        "label": f"{_row.park_name} ({_row.park_type}, {_row.acres:,.1f} acres)"
                    },
                }
                for _row in open_space_parks.itertuples()
            ],
        },
        name="Parks",
        style_function=lambda _feature: {
            "fillColor": park_color,
            "fillOpacity": 0.6,
            "color": park_color,
            "weight": 0.75,
        },
        tooltip=folium.GeoJsonTooltip(fields=["label"], labels=False),
    ).add_to(park_map)
    _district_layer = add_district_outline(park_map)
    # completed projects drawn first, so the active ones sit on top. A gray outline keeps the light fills visible on
    # the park greens.
    for _row in park_projects.sort_values(
        "current_phase", key=lambda phases: phases != "completed"
    ).itertuples():
        _phase = project_phase_of[_row.current_phase]
        if _phase == "Completed" and not park_projects_completed_switch.value:
            continue
        folium.CircleMarker(
            [_row.lat, _row.lon],
            radius=3.75,
            color="#636363",
            weight=0.75,
            fill=True,
            fill_color=project_phases[_phase][1],
            fill_opacity=1,
            tooltip=f"{_phase}: {_row.title}",
        ).add_to(park_map)
    _project_rows = "".join(
        f"""<tr>
          <td style="text-align:center"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;
            background:{color};outline:0.75px solid #636363"></span></td>
          <td style="padding:0 4px">{phase}</td>
          <td style="text-align:right;font-weight:700">{sum(park_project_counts.get(feed_phase, 0) for feed_phase in feed_phases)}</td>
        </tr>"""
        for phase, (feed_phases, color) in project_phases.items()
        if phase != "Completed" or park_projects_completed_switch.value
    )
    add_legend(
        park_map,
        f"""
      <table style="border-collapse:collapse">
        <tr><td colspan="3" style="padding:0 0 4px 0;font-weight:600">Parks, {district_code}</td></tr>
        <tr>
          <td><span style="display:inline-block;width:14px;height:10px;background:{park_color};opacity:0.7;
            outline:1px solid {park_color}"></span></td>
          <td style="padding:0 4px">NYC Parks property</td>
        </tr>
        <tr>
          <td><span style="display:inline-block;width:14px;height:10px;background:{served_color};opacity:0.8"></span></td>
          <td style="padding:0 4px">Within walking distance of a park</td>
        </tr>
        <tr><td colspan="2" style="padding-top:6px;font-weight:600">Parks capital projects</td>
          <td style="padding:6px 0 0 8px;color:#55554f">In district</td></tr>
        {_project_rows}
      </table>
    """,
    )
    park_map.fit_bounds(_district_layer.get_bounds())
    _share = (
        open_space_access["served"] / open_space_access["residents"]
        if open_space_access["residents"]
        else float("nan")
    )

    def _pct(value):
        return "n/a" if pd.isna(value) else f"{value:.0f}%"

    # the district's projects, once each, in the legend's phases, and by finish dates within each phase
    park_district_projects = park_district_projects_raw.assign(
        phase=lambda df: df["current_phase"].map(project_phase_of),
        phase_order=lambda df: df["phase"].map(list(project_phases).index),
    )
    mo.vstack(
        [
            mo.vstack(
                [
                    mo.md(f"**Walk to a park, {district_code}**"),
                    mo.hstack(
                        [
                            mo.stat(
                                "n/a" if _share != _share else f"{_share:.0%}",
                                label="Within walking distance of a park",
                                caption="n/a"
                                if _share != _share
                                else f"{open_space_access['served']:,.0f} residents · NYC: "
                                f"{open_space_access['city_served'] / open_space_access['city_residents']:.0%}",
                                bordered=True,
                            ),
                            mo.stat(
                                "n/a"
                                if _share != _share
                                else f"{open_space_access['residents'] - open_space_access['served']:,.0f}",
                                label="Residents farther than walking distance",
                                caption="n/a"
                                if _share != _share
                                else f"of {open_space_access['residents']:,.0f} in 2020",
                                bordered=True,
                            ),
                        ],
                        justify="start",
                        gap=1,
                        widths="equal",
                    ),
                    mo.hstack(
                        [
                            mo.stat(
                                _pct(open_space_access["active_recreation_areas_pct"]),
                                label="Active Recreation Areas",
                                caption=f"NYC: {_pct(open_space_access['citywide_active_recreation_areas_pct'])}",
                                bordered=True,
                            ),
                            mo.stat(
                                f"{_pct(open_space_access['new_or_renovated_parks_pct'])} → "
                                f"{_pct(open_space_access['new_or_renovated_parks_future_pct'])}",
                                label="New or Renovated Parks, now → future",
                                caption=f"NYC: {_pct(open_space_access['citywide_new_or_renovated_parks_pct'])} → "
                                f"{_pct(open_space_access['citywide_new_or_renovated_parks_future_pct'])}",
                                bordered=True,
                            ),
                        ],
                        justify="start",
                        gap=1,
                        widths="equal",
                    ),
                ],
                gap=0.25,
            ),
            source_note(
                "Sources: NYC Parks Walk to a Park service area; 2020 Census block counts, via DCP Population FactFinder; "
                "NYC Parks [Vital Parks Explorer](https://www.nycgovparks.org/about/vital-parks/explorer) (November 2026)"
            ),
            park_projects_completed_switch,
            mo.iframe(park_map.get_root().render(), height="500px"),
            source_note(
                "Sources: NYC Parks [Parks Properties](https://data.cityofnewyork.us/d/enfh-gkve) (October 2026) and "
                "[Walk to a Park Service Area for CEQR](https://data.cityofnewyork.us/d/k66c-pzws) (October 2026), NYC Open Data; "
                "NYC Parks [Capital Project Tracker](https://www.nycgovparks.org/planning-and-building/capital-project-tracker) (June 2026)"
            ),
            mo.md(f"##### Parks capital projects in {district_code}"),
            mo.ui.table(
                park_district_projects[
                    park_projects_completed_switch.value
                    | (park_district_projects["phase"] != "Completed")
                ]
                .sort_values(
                    ["phase_order", "construction_finish", "phase_finish", "title"]
                )
                .reset_index(drop=True)[
                    [
                        "phase",
                        "title",
                        "tracker_id",
                        "phase_percent_complete",
                        "phase_finish",
                        "construction_finish",
                        "total_funding",
                        "funding_sources",
                    ]
                ]
                .rename(
                    columns={
                        "phase": "Phase",
                        "title": "Project",
                        "phase_percent_complete": "Phase complete",
                        "phase_finish": "Phase finish",
                        "construction_finish": "Construction finish",
                        "total_funding": "Funding",
                        "funding_sources": "Funding sources",
                        "tracker_id": "Project Tracker",
                    }
                ),
                selection=None,
                show_column_summaries=False,
                show_data_types=False,
                # formatted for display only, so the columns sort as numbers and dates
                format_mapping={
                    "Phase complete": lambda value: (
                        "" if pd.isna(value) else f"{value:.0f}%"
                    ),
                    "Phase finish": lambda value: (
                        "" if pd.isna(value) else f"{value:%b %Y}"
                    ),
                    "Construction finish": lambda value: (
                        "" if pd.isna(value) else f"{value:%b %Y}"
                    ),
                    "Project Tracker": lambda tracker_id: mo.Html(
                        f'<a href="https://www.nycgovparks.org/planning-and-building/capital-project-tracker/project/{tracker_id}"'
                        f' target="_blank" style="color:#1a5fb4;text-decoration:underline">{tracker_id} ↗</a>'
                    ),
                },
                column_widths={"Project Tracker": 90},
            ),
            source_note(
                "Source: NYC Parks [Capital Project Tracker](https://www.nycgovparks.org/planning-and-building/capital-project-tracker) (June 2026)"
            ),
        ],
        gap=0.5,
    )
    return


if __name__ == "__main__":
    app.run()
