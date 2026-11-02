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


@app.cell
def _(duckdb, mo):
    db_path = mo.notebook_dir().parent / "data" / "analysis_growth.duckdb"
    core_tables = ["dim_community_district", "dim_nta", "dim_census_tract"]
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
    return (con,)


@app.cell
def _(mo):
    mo.md("""
    # Demographics exploration

    Community districts are too coarse to say who lives somewhere and what it's like to live
    there, so this notebook zooms into one district's census tracts. It's built by the dbt project in
    this folder, and the column docs are in `models/core/_core_models.yml`.

    Tracts don't nest in community districts. They do nest in CDTAs, which match or approximate each
    district, so a district's tracts here are the tracts in its CDTA. The gray outline on the map is
    the district's actual boundary, and the table shows how much of each tract's land is inside it.

    - **AHFT**: the Affordable Housing Fair Share tracker. The 12 AHFT districts have the lowest rate
      of affordable housing development.
    - **CDTA**: community district tabulation area, built from census tracts.
    - **NTA**: neighborhood tabulation area, built from census tracts. Tracts nest in NTAs, and NTAs
      nest in CDTAs.
    """)
    return


@app.cell
def _(con, mo):
    districts = con.sql("""
        SELECT borocd, cdta_name, cdta_boundary_match, is_ahft_district, ahft_rank, population_2020_census,
            cdta_median_household_income_acs, citywide_median_household_income_acs
        FROM dim_community_district
        WHERE NOT is_joint_interest_area
        ORDER BY is_ahft_district DESC, CASE WHEN is_ahft_district THEN ahft_rank END, borocd
    """).df()
    district_options = {
        f"{row.cdta_name} (AHFT rank {row.ahft_rank})"
        if row.is_ahft_district
        else row.cdta_name: row.borocd
        for row in districts.itertuples()
    }
    district_picker = mo.ui.dropdown(
        options=district_options,
        value=next(iter(district_options)),
        label="Community district",
        searchable=True,
    )
    district_picker
    return district_picker, districts


@app.cell
def _(con, district_picker, districts):
    focus = districts[districts["borocd"] == district_picker.value].iloc[0]
    tracts = con.execute(
        """
        WITH district AS (
            SELECT cdta2020, geom FROM dim_community_district WHERE borocd = ?
        )

        SELECT
            tract.boroct2020,
            tract.tract_label,
            tract.nta_name,
            tract.population_2020_census,
            tract.housing_units_2020_census,
            tract.group_quarters_population_2020_census,
            COLUMNS('^(population_age_|households_|median_household_income_)'),
            tract.people_per_acre,
            100 * (tract.population_2020_census - tract.population_2010_census)
                / nullif(tract.population_2010_census, 0) AS population_change_2010_2020_pct,
            100 * ST_Area_Spheroid(ST_Intersection(tract.geom, district.geom))
                / ST_Area_Spheroid(tract.geom) AS pct_of_land_in_district,
            ST_AsGeoJSON(ST_SimplifyPreserveTopology(tract.geom, 0.00005)) AS geojson
        FROM dim_census_tract AS tract
        INNER JOIN district ON tract.cdta2020 = district.cdta2020
        ORDER BY tract.nta_name, tract.boroct2020
        """,
        [district_picker.value],
    ).df()
    ntas = con.execute(
        """
        SELECT nta.* EXCLUDE (geom)
        FROM dim_nta AS nta
        INNER JOIN dim_community_district AS district ON nta.cdta2020 = district.cdta2020
        WHERE district.borocd = ?
        ORDER BY nta.nta_name
        """,
        [district_picker.value],
    ).df()
    # Parks and cemeteries would otherwise read as very low-density neighborhoods.
    few_residents = 100
    lived_in = tracts[tracts["population_2020_census"] >= few_residents]

    return few_residents, focus, lived_in, ntas, tracts


@app.cell
def _(con):
    # citywide reference values for the charts, summed from every tract
    city_totals = con.sql("""
        SELECT
            sum(COLUMNS('^population_age_')) ,
            sum(population_2020_census) AS population_2020_census,
            sum(COLUMNS('^households_income_')),
            sum(households_acs) AS households_acs
        FROM dim_census_tract
    """).df().iloc[0]
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
def _(alt, con, district_picker, json):
    # simplified so the map stays light, same tolerance as the growth notebook
    city_shapes = con.sql("""
        SELECT
            borocd,
            coalesce(is_ahft_district, false) AS is_ahft_district,
            ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, 0.0002)) AS geojson
        FROM dim_community_district
    """).fetchall()
    city_features = [
        {
            "type": "Feature",
            "geometry": json.loads(geojson),
            "properties": {"is_ahft_district": is_ahft_district, "is_focus": borocd == district_picker.value},
        }
        for borocd, is_ahft_district, geojson in city_shapes
    ]
    city_base = alt.Chart(alt.Data(values=city_features)).project(type="mercator")
    city_map = (
        city_base.mark_geoshape(fill="#e4e4e0", stroke="white", strokeWidth=0.5)
        + city_base.transform_filter("datum.properties.is_ahft_district").mark_geoshape(
            fill=None, stroke="#555555", strokeWidth=0.75, strokeJoin="round"
        )
        + city_base.transform_filter("datum.properties.is_focus").mark_geoshape(
            fill="#256abf", stroke="#222222", strokeWidth=1.5, strokeJoin="round"
        )
    ).properties(width=150, height=190)

    return (city_map,)


@app.cell
def _(city_map, focus, mo, tracts):
    tract_population = tracts["population_2020_census"].sum()
    overview_text = mo.vstack([
        mo.md(f"""**{focus["cdta_name"]}** has **{len(tracts)}** tracts in **{tracts["nta_name"].nunique()}** NTAs."""),
        mo.md(f"""
    The CDTA is an
    **{focus["cdta_boundary_match"].lower()}** of the district: its tracts held
    **{tract_population:,.0f}** people in 2020 and the district held
    **{focus["population_2020_census"]:,.0f}**
    ({100 * (tract_population / focus["population_2020_census"] - 1):+.1f}%).
    """),
    ])
    mo.vstack([
        mo.md("## District overview"),
        mo.hstack([city_map, overview_text], justify="space-between", align="start", gap=2),
    ])
    return


@app.cell
def _(age_groups, income_groups, mo):
    map_measures = {
        "People per acre, 2020": "people_per_acre",
        "Median household income": "median_household_income_acs",
        **{f"Age {label} (% of residents)": column for column, label in age_groups.items()},
        **{f"Household income {label} (% of households)": column for column, label in income_groups.items()},
    }
    map_measure = mo.ui.dropdown(options=map_measures, value="People per acre, 2020", label="Map")
    map_scale_basis = mo.ui.radio(
        options={"Citywide": "city", "This district": "district"},
        value="Citywide",
        label="Color scale",
        inline=True,
    )
    mo.hstack([map_measure, map_scale_basis], justify="start", gap=2)
    return map_measure, map_measures, map_scale_basis


@app.cell
def _(age_groups, con, few_residents, income_groups, map_measures):
    def map_values_for(df, column):
        population = df["population_2020_census"]
        if column in age_groups:
            return 100 * df[column] / population.where(population >= few_residents)
        if column in income_groups:
            households = df["households_acs"]
            return 100 * df[column] / households.where(households >= few_residents)
        return df[column].where(population >= few_residents)


    citywide_tracts = con.sql("""
        SELECT * EXCLUDE (geom) FROM dim_census_tract
    """).df()
    # Citywide colors follow rank, not value: a few tracts (such as college housing and nursing homes) sit far
    # above the rest and would wash out every other tract on a value scale.
    map_citywide_values = {
        column: map_values_for(citywide_tracts, column).dropna().sort_values() for column in map_measures.values()
    }
    map_ranges = {
        column: {"min": values.min(), "median": values.median(), "max": values.max()}
        for column, values in map_citywide_values.items()
    }

    return citywide_tracts, map_citywide_values, map_ranges, map_values_for


@app.cell
def _(
    age_groups,
    branca,
    con,
    district_picker,
    few_residents,
    folium,
    income_groups,
    json,
    map_citywide_values,
    map_measure,
    map_ranges,
    map_scale_basis,
    map_values_for,
    mo,
    pd,
    sampling_share,
    tracts,
):
    map_column = map_measure.value
    map_values = map_values_for(tracts, map_column)
    is_district_scale = map_scale_basis.value == "district"
    map_citywide = map_citywide_values[map_column]
    # share of NYC tracts at or below each tract's value. searchsorted puts missing values last, so put them back.
    map_ranks = pd.Series(
        100 * map_citywide.searchsorted(map_values, side="right") / len(map_citywide), index=map_values.index
    ).where(map_values.notna())
    if map_column in age_groups or map_column in income_groups:
        map_format = "{:.1f}%"
    elif map_column == "median_household_income_acs":
        map_format = "${:,.0f}"
    else:
        map_format = "{:,.1f}"
    # orange for age, blue for everything else, matching the charts
    map_colors = ["#fbdcc9", "#6b2f0e"] if map_column in age_groups else ["#cde2fb", "#0d366b"]
    map_scale = branca.colormap.LinearColormap(
        map_colors,
        vmin=float(map_values.min()) if is_district_scale else 0,
        vmax=float(map_values.max()) if is_district_scale else 100,
    )

    # one axis for both settings, spaced by rank among NYC tracts, so the district setting shows where its
    # tracts sit in the city. Only the part the colors span is colored, so one color range shows at a time.
    legend_from = float(map_ranks.min()) if is_district_scale else 0
    legend_to = float(map_ranks.max()) if is_district_scale else 100
    # district colors follow value but this axis follows rank, so sample the gradient to match the map
    legend_gradient = (
        ", ".join(
            map_scale(map_citywide.quantile((legend_from + step * (legend_to - legend_from) / 10) / 100)) for step in range(11)
        )
        if is_district_scale
        else ", ".join(map_colors)
    )
    district_labels = (
        f"""
        <div style="position:relative;height:15px">
          <span style="position:absolute;right:{100 - legend_from:.2f}%;padding-right:2px">{map_format.format(map_scale.vmin)}</span>
          <span style="position:absolute;left:{legend_to:.2f}%;padding-left:2px">{map_format.format(map_scale.vmax)}</span>
        </div>"""
        if is_district_scale
        else ""
    )
    legend_key = (
        '<div style="color:#55554f;margin-top:2px">Colored: this district\'s tracts. Gray: the rest of the NYC range.</div>'
        if is_district_scale
        else ""
    )
    map_legend = f"""
    <div style="position:fixed;bottom:24px;left:10px;z-index:1000;pointer-events:none;background:rgba(255,255,255,0.92);
      padding:6px 10px;border-radius:4px;font:11px sans-serif;color:#222222;box-shadow:0 1px 4px rgba(0,0,0,0.3)">
      <div style="font-weight:600;margin-bottom:4px">{map_measure.selected_key}</div>
      <div style="width:280px;margin:0 48px">{district_labels}
        <div style="position:relative;height:12px;background:#d8d8d4">
          <div style="position:absolute;left:{legend_from:.2f}%;width:{legend_to - legend_from:.2f}%;height:100%;
            background:linear-gradient(to right, {legend_gradient})"></div>
          <div style="position:absolute;left:50%;top:-2px;bottom:-16px;width:2px;margin-left:-1px;background:#222222"></div>
        </div>
        <div style="position:relative;height:30px;color:#55554f">
          <span style="position:absolute;left:0;transform:translateX(-50%);white-space:nowrap">{map_format.format(map_ranges[map_column]["min"])}</span>
          <span style="position:absolute;left:100%;transform:translateX(-50%);white-space:nowrap">{map_format.format(map_ranges[map_column]["max"])}</span>
          <span style="position:absolute;left:50%;top:16px;transform:translateX(-50%);white-space:nowrap;color:#222222">NYC median {map_format.format(map_ranges[map_column]["median"])}</span>
        </div>
      </div>{legend_key}
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
                    "color_value": None if map_value != map_value else float(map_value if is_district_scale else map_rank),
                    "map_label": "no value"
                    if map_value != map_value
                    else map_format.format(map_value) + ("" if is_district_scale else f" (at or above {map_rank:.0f}% of NYC tracts)"),
                    "population_label": f"{row.population_2020_census:,.0f}",
                    "housing_units_label": f"{row.housing_units_2020_census:,.0f}",
                    "people_per_acre_label": f"{row.people_per_acre:,.1f}",
                    "income_label": "no estimate"
                    if row.median_household_income_acs != row.median_household_income_acs
                    else f"${row.median_household_income_acs:,.0f} ± ${row.median_household_income_acs_moe:,.0f}",
                },
            }
            for row, map_value, map_rank in zip(tracts.itertuples(), map_values, map_ranks)
        ],
    }
    # same tolerance as the tracts, so the two line up where they share a boundary
    focus_outline = con.execute(
        """
        SELECT ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, 0.00005))
        FROM dim_community_district
        WHERE borocd = ?
        """,
        [district_picker.value],
    ).fetchall()[0][0]
    nta_outlines = con.execute(
        """
        SELECT ST_AsGeoJSON(ST_SimplifyPreserveTopology(nta.geom, 0.00005))
        FROM dim_nta AS nta
        INNER JOIN dim_community_district AS district ON nta.cdta2020 = district.cdta2020
        WHERE district.borocd = ?
        """,
        [district_picker.value],
    ).fetchall()

    tract_layer = folium.GeoJson(
        tract_geojson,
        name="Tracts",
        style_function=tract_style,
        tooltip=folium.GeoJsonTooltip(
            fields=["tract", "nta", "map_label", "population_label", "housing_units_label", "people_per_acre_label", "income_label"],
            aliases=["Tract", "NTA", map_measure.selected_key, "Population, 2020", "Housing units, 2020", "People per acre, 2020", "Median household income"],
        ),
    )
    focus_map = folium.Map(tiles=None)
    # basemaps that don't need an API key, picked from the layer control. Added here rather than
    # through Map's tiles, which would cap every basemap at the first one's max zoom
    for basemap_name, provider in {
        "light gray": "Esri.WorldGrayCanvas",
        "OpenStreetMap": "OpenStreetMap.Mapnik",
        "satellite": "Esri.WorldImagery",
    }.items():
        folium.TileLayer(provider, name=basemap_name, show=basemap_name == "light gray").add_to(focus_map)
    tract_layer.add_to(focus_map)
    folium.GeoJson(
        {"type": "GeometryCollection", "geometries": [json.loads(geojson) for (geojson,) in nta_outlines]},
        name="NTA boundaries",
        interactive=False,
        style_function=lambda _feature: {"fill": False, "color": "#222222", "weight": 1.5},
    ).add_to(focus_map)
    folium.GeoJson(
        json.loads(focus_outline),
        name="District boundary",
        interactive=False,
        style_function=lambda _feature: {"fill": False, "color": "#8c8c88", "weight": 2},
    ).add_to(focus_map)
    focus_map.get_root().html.add_child(folium.Element(map_legend))
    folium.LayerControl().add_to(focus_map)
    focus_map.fit_bounds(tract_layer.get_bounds())

    map_scale_note = (
        "The colors show each tract's rank among NYC tracts and are the same for every district. Equal color steps are equal steps in rank, not in value, so tracts with close values can look different."
        if not is_district_scale
        else "The colors run from this district's lowest tract to its highest, so they show differences inside the district and can't be compared across districts. The legend shows that range against the citywide range."
    )
    map_note = (
        " A single tract's share of households in an income group is noisy, so the bars below are steadier."
        if map_column in income_groups
        else " Tract medians are noisy, so see the margins of error in the income chart below."
        if map_column == "median_household_income_acs"
        else ""
    )
    if map_column == "median_household_income_acs":
        map_note += (
            f" Sampling error accounts for about {min(sampling_share, 100):.0f}% of the variance between these"
            " tract medians, so the colors can show noise as well as real differences."
        )
    mo.vstack([
        mo.md(f"{map_scale_note} Gray tracts have too few people for a value (under {few_residents} residents or households, or no estimate). Black lines are NTAs and the gray line is the district.{map_note}"),
        # folium's own output is sized by aspect ratio and cuts off the legend, so fix the height
        mo.iframe(focus_map.get_root().render(), height="600px"),
    ])

    return


@app.cell
def _(mo):
    mo.md("""
    ## Age

    Share of residents in each age group, from the 2020 census. Tracts are summed to NTAs, with the whole
    district and city for reference. These are full counts, so they have no margin of error, but the Census
    Bureau adds some noise to protect privacy.
    """)
    return


@app.cell
def _(age_groups, alt, city_totals, few_residents, ntas, pd, tracts):
    age_group_columns = list(age_groups)
    nta_ages = ntas.set_index("nta_name")[[*age_group_columns, "population_2020_census"]]
    shown_age_ntas = nta_ages[nta_ages["population_2020_census"] >= few_residents].sort_index()
    age_rows = [
        {"place": "New York City", **city_totals[[*age_group_columns, "population_2020_census"]].to_dict()},
        {"place": "District (CDTA)", **tracts[[*age_group_columns, "population_2020_census"]].sum().to_dict()},
        *[{"place": name, **row.to_dict()} for name, row in shown_age_ntas.iterrows()],
    ]
    age_mix = pd.DataFrame(age_rows).melt(
        id_vars=["place", "population_2020_census"], value_vars=age_group_columns, var_name="column", value_name="residents"
    )
    age_mix["group"] = age_mix["column"].map(age_groups)
    age_mix["share"] = 100 * age_mix["residents"] / age_mix["population_2020_census"]
    age_mix["group_order"] = age_mix["column"].map({column: i for i, column in enumerate(age_group_columns)})
    age_mix["section"] = age_mix["place"].map({"New York City": "city", "District (CDTA)": "district"}).fillna("nta")
    alt.Chart(age_mix).mark_bar(stroke="white", strokeWidth=2).encode(
        x=alt.X("share:Q", stack="zero", title="Share of residents (%)", scale=alt.Scale(domain=[0, 100])),
        y=alt.Y("place:N", title=None, sort=[row["place"] for row in age_rows], axis=alt.Axis(
            labelLimit=320,
            labelFontWeight=alt.expr("indexof(['New York City', 'District (CDTA)'], datum.value) >= 0 ? 'normal' : 'bold'"),
        )),
        color=alt.Color(
            "group:N",
            title="Age",
            sort=list(age_groups.values()),
            scale=alt.Scale(domain=list(age_groups.values()), range=["#f5a06c", "#e8793f", "#c95f24", "#9e4818", "#6b2f0e"]),
            legend=alt.Legend(orient="bottom", direction="horizontal"),
        ),
        order=alt.Order("group_order:Q"),
        tooltip=[
            alt.Tooltip("place:N", title="Place"),
            alt.Tooltip("group:N", title="Age"),
            alt.Tooltip("share:Q", title="Share of residents (%)", format=".1f"),
            alt.Tooltip("residents:Q", title="Residents", format=",.0f"),
        ],
    ).properties(width=560, height=alt.Step(26)).facet(
        # separate panels so the gaps show the city, the district, and the NTAs are different kinds of rows
        row=alt.Row("section:N", sort=["city", "district", "nta"], title=None, header=alt.Header(labels=False)),
        spacing=8,
        title="Residents by age",
    ).resolve_scale(y="independent")

    return


@app.cell
def _(few_residents, mo):
    mo.md(f"""
    NTA bars hide differences between tracts, so this shows each tract with at least {few_residents} residents as a
    tick. A wide spread means the district's overall share hides real differences.
    """)
    return


@app.cell
def _(age_groups, alt, city_totals, lived_in, pd, tracts):
    age_shares = lived_in.melt(
        id_vars=["tract_label", "nta_name", "population_2020_census"],
        value_vars=list(age_groups),
        var_name="column",
        value_name="residents",
    )
    age_shares["age_group"] = age_shares["column"].map(age_groups)
    age_shares["share"] = 100 * age_shares["residents"] / age_shares["population_2020_census"]
    age_references = pd.DataFrame(
        [
            {
                "level": level,
                "age_group": label,
                "share": 100 * totals[column] / totals["population_2020_census"],
            }
            for level, totals in {
                "District (CDTA)": tracts[[*age_groups, "population_2020_census"]].sum(),
                "New York City": city_totals,
            }.items()
            for column, label in age_groups.items()
        ]
    )
    age_y = alt.Y("age_group:N", title=None, sort=list(age_groups.values()))
    (
        alt.Chart(age_shares).mark_tick(color="#2a78d6", opacity=0.55, thickness=2, size=26).encode(
            x=alt.X("share:Q", title="Share of residents (%)"),
            y=age_y,
            tooltip=[
                alt.Tooltip("tract_label:N", title="Tract"),
                alt.Tooltip("nta_name:N", title="NTA"),
                alt.Tooltip("age_group:N", title="Age"),
                alt.Tooltip("share:Q", title="Share of residents (%)", format=".1f"),
                alt.Tooltip("residents:Q", title="Residents", format=",.0f"),
            ],
        )
        + alt.Chart(age_references).mark_point(filled=True, size=110, opacity=1, stroke="white", strokeWidth=1).encode(
            x="share:Q",
            y=age_y,
            color=alt.Color(
                "level:N",
                title=None,
                scale=alt.Scale(domain=["District (CDTA)", "New York City"], range=["#222222", "#eb6834"]),
                legend=alt.Legend(orient="bottom", direction="horizontal"),
            ),
            shape=alt.Shape("level:N", title=None, scale=alt.Scale(domain=["District (CDTA)", "New York City"], range=["circle", "diamond"])),
            tooltip=[
                alt.Tooltip("level:N", title="Level"),
                alt.Tooltip("age_group:N", title="Age"),
                alt.Tooltip("share:Q", title="Share of residents (%)", format=".1f"),
            ],
        )
    ).properties(width=640, height=alt.Step(44), title="Age groups by tract")

    return


@app.cell
def _(age_groups, alt, citywide_tracts, con, few_residents, pd):
    import numpy as np


    def age_pattern_shares(places):
        lived_in_places = places[places["population_2020_census"] >= few_residents]
        return (100 * lived_in_places[list(age_groups)].div(lived_in_places["population_2020_census"], axis=0)).rename(
            columns=age_groups
        )


    def age_patterns(shares, weights=None, equal_age_weight=False):
        """Principal components of age shares: each one's share of the variance, and its loadings by age group."""
        row_weights = (pd.Series(1.0, index=shares.index) if weights is None else weights.loc[shares.index]).pipe(lambda w: w / w.sum())
        centered = (shares - shares.mul(row_weights, axis=0).sum()).mul(np.sqrt(row_weights), axis=0)
        if equal_age_weight:
            centered = centered / centered.std()
        _, singular_values, components = np.linalg.svd(centered.values, full_matrices=False)
        # a component's sign is arbitrary, so make the age group that matters most positive
        signs = np.sign(components[np.arange(len(components)), np.abs(components).argmax(axis=1)])
        return 100 * singular_values**2 / (singular_values**2).sum(), pd.DataFrame(
            (components.T * signs).T, columns=shares.columns
        )


    def age_pattern_summary(variance, loadings, i):
        return f"{variance[i]:.0f}%, {loadings.loc[i].idxmax()} vs {loadings.loc[i].idxmin()}"


    age_pattern_tract_shares = age_pattern_shares(citywide_tracts)
    age_pattern_variance, age_pattern_loadings = age_patterns(age_pattern_tract_shares)
    age_pattern_scores = (age_pattern_tract_shares - age_pattern_tract_shares.mean()).values @ age_pattern_loadings.values.T
    age_axis_match = (
        abs(np.corrcoef(age_pattern_tract_shares["25-44"], age_pattern_scores[:, 0])[0, 1]),
        abs(np.corrcoef(age_pattern_tract_shares["Under 18"] - age_pattern_tract_shares["65+"], age_pattern_scores[:, 1])[0, 1]),
    )

    mostly_households = citywide_tracts[
        citywide_tracts["group_quarters_population_2020_census"] < 0.25 * citywide_tracts["population_2020_census"]
    ]
    age_pattern_checks = {
        "Tracts (used in the charts)": age_patterns(age_pattern_tract_shares),
        "Tracts, weighted by population": age_patterns(age_pattern_tract_shares, weights=citywide_tracts["population_2020_census"]),
        "Tracts under 25% group quarters": age_patterns(age_pattern_shares(mostly_households)),
        "NTAs": age_patterns(age_pattern_shares(con.sql("SELECT * EXCLUDE (geom) FROM dim_nta").df())),
        "Tracts, every age group weighted equally": age_patterns(age_pattern_tract_shares, equal_age_weight=True),
    }

    # the last pattern is always 0 because the shares add up to 100%
    age_pattern_variance_chart = alt.Chart(
        pd.DataFrame({"pattern": [f"Pattern {i + 1}" for i in range(4)], "share": age_pattern_variance[:4]})
    ).mark_bar(color="#c95f24", cornerRadiusEnd=4).encode(
        x=alt.X("share:Q", title="Share of the differences between tracts (%)", scale=alt.Scale(domain=[0, 100])),
        y=alt.Y("pattern:N", title=None),
        tooltip=[alt.Tooltip("pattern:N", title="Pattern"), alt.Tooltip("share:Q", title="Share (%)", format=".1f")],
    ).properties(width=560, height=alt.Step(26), title="How much of the differences between tracts each pattern accounts for")

    age_pattern_loading_rows = pd.DataFrame(
        [
            {"pattern": f"Pattern {i + 1} ({age_pattern_variance[i]:.0f}%)", "age_group": age_group, "loading": age_pattern_loadings.loc[i, age_group]}
            for i in range(3)
            for age_group in age_groups.values()
        ]
    )
    age_pattern_loading_chart = alt.Chart(age_pattern_loading_rows).mark_bar(cornerRadiusEnd=4).encode(
        x=alt.X("loading:Q", title="Loading", scale=alt.Scale(domain=[-1, 1])),
        y=alt.Y("age_group:N", title=None, sort=list(age_groups.values())),
        color=alt.condition("datum.loading > 0", alt.value("#c95f24"), alt.value("#1c5cab")),
        tooltip=[
            alt.Tooltip("pattern:N", title="Pattern"),
            alt.Tooltip("age_group:N", title="Age"),
            alt.Tooltip("loading:Q", title="Loading", format=".2f"),
        ],
    ).properties(width=190, height=alt.Step(24)).facet(
        column=alt.Column("pattern:N", title=None), title="Which age groups each pattern contrasts"
    )
    return (
        age_axis_match,
        age_pattern_checks,
        age_pattern_loading_chart,
        age_pattern_summary,
        age_pattern_tract_shares,
        age_pattern_variance_chart,
    )


@app.cell
def _(
    age_axis_match,
    age_pattern_checks,
    age_pattern_loading_chart,
    age_pattern_summary,
    age_pattern_tract_shares,
    age_pattern_variance_chart,
    alt,
    citywide_tracts,
    few_residents,
    focus,
    mo,
    tracts,
):
    age_landscape = citywide_tracts[citywide_tracts["population_2020_census"] >= few_residents].assign(
        share_25_44=lambda df: 100 * df["population_age_25_44_2020_census"] / df["population_2020_census"],
        kids_minus_older=lambda df: 100
        * (df["population_age_under_18_2020_census"] - df["population_age_65_plus_2020_census"])
        / df["population_2020_census"],
        tract_group=lambda df: df["boroct2020"].isin(tracts["boroct2020"]).map({True: focus["cdta_name"], False: "Other NYC tracts"}),
    )[["tract_label", "nta_name", "share_25_44", "kids_minus_older", "tract_group"]]
    age_landscape_points = alt.Chart(age_landscape).encode(
        x=alt.X("share_25_44:Q", title="Share of residents age 25-44 (%)"),
        y=alt.Y("kids_minus_older:Q", title="Under 18 share minus 65+ share (points)"),
        color=alt.Color(
            "tract_group:N",
            title=None,
            scale=alt.Scale(domain=[focus["cdta_name"], "Other NYC tracts"], range=["#c95f24", "#c9c9c5"]),
            legend=alt.Legend(orient="bottom", direction="horizontal"),
        ),
        tooltip=[
            alt.Tooltip("tract_label:N", title="Tract"),
            alt.Tooltip("nta_name:N", title="NTA"),
            alt.Tooltip("share_25_44:Q", title="Age 25-44 (%)", format=".1f"),
            alt.Tooltip("kids_minus_older:Q", title="Under 18 minus 65+ (points)", format="+.1f"),
        ],
    )
    age_landscape_chart = (
        alt.Chart(alt.Data(values=[{"y": 0}])).mark_rule(color="#8c8c88", strokeWidth=1).encode(y="y:Q")
        + age_landscape_points.transform_filter("datum.tract_group == 'Other NYC tracts'").mark_circle(size=22, opacity=0.6)
        + age_landscape_points.transform_filter("datum.tract_group != 'Other NYC tracts'").mark_circle(
            size=60, opacity=1, stroke="white", strokeWidth=0.75
        )
    ).properties(width=640, height=420, title="Age mix of NYC tracts")
    age_pattern_check_rows = "\n".join(
        f"| {name} | {age_pattern_summary(variance, loadings, 0)} | {age_pattern_summary(variance, loadings, 1)} |"
        for name, (variance, loadings) in age_pattern_checks.items()
    )
    mo.accordion({
        "Exploration": mo.vstack([
            mo.md("""
    **How do NYC tracts differ in age mix?** Principal component analysis (PCA) answers this from each tract's five age
    shares, in percentage points. It finds the pattern of age groups rising and falling together that accounts for the most
    difference between tracts, then the next most, and so on. There are only four patterns because the shares add up to 100%.
    """),
            age_pattern_variance_chart,
            age_pattern_loading_chart,
            mo.md(f"""
    Longer bars mean an age group matters more to that pattern, and bars on opposite sides move in opposite directions.
    Which side is which doesn't mean anything. Pattern 1 is mostly 25-44 against everyone else, and pattern 2 is mostly
    under 18 against 65+.

    This changes in one case. Shares of 25-44 vary the most between tracts (standard deviation
    {age_pattern_tract_shares["25-44"].std():.1f} points), which is part of why pattern 1 centers on them. When every age
    group is weighted equally, the top pattern becomes older adults against younger adults, as in the last row.

    | Tested on | Pattern 1 | Pattern 2 |
    |---|---|---|
    {age_pattern_check_rows}
    """),
            mo.md(f"""
    **Those patterns as a chart.** The axes below are plain versions of patterns 1 and 2, and they track them closely
    (correlations of {age_axis_match[0]:.2f} and {age_axis_match[1]:.2f}). Each dot is a tract with at least
    {few_residents} residents. Above the line, a tract has a larger share of kids than of people 65 and over.
    """),
            age_landscape_chart,
        ])
    })
    return


@app.cell
def _(mo):
    mo.md("""
    ## Income

    Share of households in each income group, from the 2020-2024 ACS. A bracket count for a single tract is too
    noisy to read, so tracts are summed to NTAs, with the whole district and city for reference.
    """)
    return


@app.cell
def _(alt, city_totals, income_groups, ntas, pd, tracts):
    income_group_columns = list(income_groups)
    nta_incomes = ntas.set_index("nta_name")[[*income_group_columns, "households_acs"]]
    few_households = 100
    shown_ntas = nta_incomes[nta_incomes["households_acs"] >= few_households].sort_index()
    income_rows = [
        {"place": "New York City", **city_totals[[*income_group_columns, "households_acs"]].to_dict()},
        {"place": "District (CDTA)", **tracts[[*income_group_columns, "households_acs"]].sum().to_dict()},
        *[{"place": name, **row.to_dict()} for name, row in shown_ntas.iterrows()],
    ]
    income_mix = pd.DataFrame(income_rows).melt(
        id_vars=["place", "households_acs"], value_vars=income_group_columns, var_name="column", value_name="households"
    )
    income_mix["group"] = income_mix["column"].map(income_groups)
    income_mix["share"] = 100 * income_mix["households"] / income_mix["households_acs"]
    income_mix["group_order"] = income_mix["column"].map({column: i for i, column in enumerate(income_group_columns)})
    income_mix["section"] = income_mix["place"].map({"New York City": "city", "District (CDTA)": "district"}).fillna("nta")
    alt.Chart(income_mix).mark_bar(stroke="white", strokeWidth=2).encode(
        x=alt.X("share:Q", stack="zero", title="Share of households (%)", scale=alt.Scale(domain=[0, 100])),
        y=alt.Y("place:N", title=None, sort=[row["place"] for row in income_rows], axis=alt.Axis(
            labelLimit=320,
            labelFontWeight=alt.expr("indexof(['New York City', 'District (CDTA)'], datum.value) >= 0 ? 'normal' : 'bold'"),
        )),
        color=alt.Color(
            "group:N",
            title="Household income",
            sort=list(income_groups.values()),
            scale=alt.Scale(domain=list(income_groups.values()), range=["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#0d366b"]),
            legend=alt.Legend(orient="bottom", direction="horizontal"),
        ),
        order=alt.Order("group_order:Q"),
        tooltip=[
            alt.Tooltip("place:N", title="Place"),
            alt.Tooltip("group:N", title="Household income"),
            alt.Tooltip("share:Q", title="Share of households (%)", format=".1f"),
            alt.Tooltip("households:Q", title="Households", format=",.0f"),
        ],
    ).properties(width=560, height=alt.Step(26)).facet(
        # separate panels so the gaps show the city, the district, and the NTAs are different kinds of rows
        row=alt.Row("section:N", sort=["city", "district", "nta"], title=None, header=alt.Header(labels=False)),
        spacing=8,
        title="Households by income",
    ).resolve_scale(y="independent")

    return


@app.cell
def _(mo, tracts):
    with_income = tracts.dropna(subset=["median_household_income_acs"])
    # observed variance between tracts is real variance plus sampling variance, so the mean squared
    # standard error over the observed variance estimates sampling's share. 1.645 converts a 90% margin.
    sampling_share = 100 * (
        (with_income["median_household_income_acs_moe"] / 1.645).pow(2).mean()
        / with_income["median_household_income_acs"].var()
    )
    top_coded = int((with_income["median_household_income_acs"] >= 250001).sum())
    mo.md(f"""
    Median household income by tract, from the 2020-2024 ACS. Each dot is a tract and its line is the
    90% margin of error, so tracts whose lines overlap may not really differ. Sampling error accounts
    for about **{min(sampling_share, 100):.0f}%** of the variance between these tract medians.
    {len(tracts) - len(with_income)} of {len(tracts)} tracts have no estimate, and {top_coded} are at
    the $250,001 top code, so their true medians could be higher.
    """)

    return sampling_share, with_income


@app.cell
def _(alt, focus, with_income):
    income_chart_width = 700
    income_sorted = with_income.sort_values("median_household_income_acs").assign(
        tract_order=lambda df: range(len(df)),
        income_low=lambda df: (df["median_household_income_acs"] - df["median_household_income_acs_moe"]).clip(lower=0),
        income_high=lambda df: df["median_household_income_acs"] + df["median_household_income_acs_moe"],
    )[["tract_label", "nta_name", "tract_order", "median_household_income_acs", "median_household_income_acs_moe", "income_low", "income_high"]]
    income_x = alt.X("tract_order:O", axis=None, title=None)
    income_tooltip = [
        alt.Tooltip("tract_label:N", title="Tract"),
        alt.Tooltip("nta_name:N", title="NTA"),
        alt.Tooltip("median_household_income_acs:Q", title="Median household income", format="$,.0f"),
        alt.Tooltip("median_household_income_acs_moe:Q", title="Margin of error", format="$,.0f"),
    ]
    income_reference = alt.Chart(
        alt.Data(values=[
            {"label": f"District (CDTA): ${focus['cdta_median_household_income_acs']:,.0f}", "income": float(focus["cdta_median_household_income_acs"]), "x": 4, "align": "left"},
            {"label": f"New York City: ${focus['citywide_median_household_income_acs']:,.0f}", "income": float(focus["citywide_median_household_income_acs"]), "x": income_chart_width - 4, "align": "right"},
        ])
    )
    (
        alt.Chart(income_sorted).mark_rule(color="#86b6ef", strokeWidth=2).encode(
            x=income_x,
            y=alt.Y("income_low:Q", title="Median household income", axis=alt.Axis(format="$,.0f")),
            y2="income_high:Q",
            tooltip=income_tooltip,
        )
        + alt.Chart(income_sorted).mark_circle(color="#1c5cab", size=45, opacity=1).encode(
            x=income_x, y="median_household_income_acs:Q", tooltip=income_tooltip
        )
        + income_reference.mark_rule(color="#222222", strokeWidth=1, strokeDash=[4, 3]).encode(y="income:Q")
        + income_reference.transform_filter("datum.align == 'left'").mark_text(align="left", dy=-7, color="#222222").encode(
            x=alt.value(4), y="income:Q", text="label:N"
        )
        + income_reference.transform_filter("datum.align == 'right'").mark_text(align="right", dy=-7, color="#222222").encode(
            x=alt.value(income_chart_width - 4), y="income:Q", text="label:N"
        )
    ).properties(width=income_chart_width, height=340, title="Tracts from lowest to highest median household income")

    return


@app.cell
def _(mo, tracts):
    tract_table = tracts[
        [
            "boroct2020",
            "tract_label",
            "nta_name",
            "population_2020_census",
            "housing_units_2020_census",
            "group_quarters_population_2020_census",
            "people_per_acre",
            "population_change_2010_2020_pct",
            "median_household_income_acs",
            "pct_of_land_in_district",
        ]
    ].rename(
        columns={
            "boroct2020": "BoroCT2020",
            "tract_label": "Tract",
            "nta_name": "NTA",
            "population_2020_census": "Population, 2020",
            "housing_units_2020_census": "Housing units, 2020",
            "group_quarters_population_2020_census": "Group quarters population, 2020",
            "people_per_acre": "People per acre, 2020",
            "median_household_income_acs": "Median household income",
            "population_change_2010_2020_pct": "Population change 2010-2020 (%)",
            "pct_of_land_in_district": "Land in district (%)",
        }
    )
    tract_table_view = mo.ui.table(
        tract_table,
        selection=None,
        show_data_types=False,
        pagination=True,
        page_size=15,
        format_mapping={
            "Population, 2020": "{:,.0f}",
            "Housing units, 2020": "{:,.0f}",
            "Group quarters population, 2020": "{:,.0f}",
            "People per acre, 2020": "{:,.1f}",
            "Median household income": "${:,.0f}",
            "Population change 2010-2020 (%)": "{:+.1f}",
            "Land in district (%)": "{:.0f}",
        },
    )
    mo.accordion({"Tract table": tract_table_view})

    return


if __name__ == "__main__":
    app.run()
