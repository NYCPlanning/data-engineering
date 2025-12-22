import marimo

__generated_with = "0.18.3"
app = marimo.App(width="full", sql_output="pandas")


@app.cell(hide_code=True)
def _():
    import os
    import sqlalchemy
    import marimo as mo
    import pandas as pd
    import geopandas as gpd
    import folium
    from shapely import wkb

    _schema = "ar_pluto_new_fields_1893"
    _password = os.environ.get("BUILD_ENGINE_PASSWORD")
    _username = os.environ.get("BUILD_ENGINE_USER")
    _host = os.environ.get("BUILD_ENGINE_HOST")
    _database = "db-pluto"
    DATABASE_URL = f"postgresql://{_username}:{_password}@{_host}:25060/{_database}"

    engine = sqlalchemy.create_engine(DATABASE_URL)
    with engine.connect() as connection:
        connection.execute(sqlalchemy.text(f"SET search_path TO {_schema}, public;"))
        connection.commit()
    engine
    return engine, folium, gpd, mo, wkb


@app.cell
def _(map_output):
    map_output
    return


@app.cell
def _(dcp_mih, engine, mihperorder, mo, pluto):
    split_bbls = mo.sql(
        f"""
        -- BBLs split between multiple MIH areas, sorted by closest to 50% (most evenly split)

        with dupes as (
        select m.*, p.geom as bbl_geom, p.address, d.wkb_geometry as mih_geom,
               abs(m.perbblgeom - 50.0) as distance_from_50, 
               max(abs(m.perbblgeom - 50.0)) over (	
                   partition by m.bbl
               ) as max_diff
        from mihperorder m
        join pluto p on m.bbl = p.bbl
        join dcp_mih d on m.mih_area_key = concat(d.project_name, ' - ', d.mih_option)
        where m.bbl in (select bbl from mihperorder where row_number = 2)
        order by bbl
        )
        select distinct on (mih_area_key, bbl, max_diff) * from dupes order by max_diff
        """,
        output=False,
        engine=engine
    )
    return (split_bbls,)


@app.cell
def _(mo, split_bbls):
    # Create interactive table for BBL selection (already sorted by closest to 50% in SQL)
    bbl_table = mo.ui.table(
        split_bbls[
            [
                "bbl",
                "address",
                "mih_area_key",
                "perbblgeom",
                "segbblgeom",
                "maxpermihgeom",
            ]
        ],
        selection="single",
        label="Select a BBL to view on map (sorted by most evenly split):",
    )
    bbl_table
    return (bbl_table,)


@app.cell
def _(bbl_table, folium, gpd, mo, split_bbls, wkb):
    if len(bbl_table.value) > 0:
        # Get the selected row data
        selected_row = bbl_table.value
        selected_bbl = selected_row["bbl"].item()
        bbl_data = split_bbls[split_bbls["bbl"] == selected_bbl].copy()

        # Create map
        if not bbl_data.empty:
            # Get BBL geometry and center map on it - convert from WKB
            bbl_geom = wkb.loads(bbl_data["bbl_geom"].iloc[0])
            bbl_gdf = gpd.GeoDataFrame(
                [{"geometry": bbl_geom}], geometry="geometry", crs="EPSG:4326"
            )
            centroid = bbl_gdf.geometry.iloc[0].centroid

            m = folium.Map(
                location=[centroid.y, centroid.x],
                zoom_start=16,
                tiles="CartoDB positron",
            )

            # Add BBL geometry (blue)
            bbl_popup_text = f"""
            <b>BBL Lot</b><br>
            BBL: {selected_bbl}<br>
            Total Records: {len(bbl_data)}
            """

            folium.GeoJson(
                bbl_gdf.geometry.iloc[0],
                style_function=lambda x: {
                    "fillColor": "#3388ff",
                    "color": "#000000",
                    "weight": 3,
                    "fillOpacity": 0.4,
                    "dashArray": "5, 5",  # Hatching pattern
                },
                popup=folium.Popup(bbl_popup_text, max_width=300),
            ).add_to(m)

            # Add MIH geometries - all same orange color
            for idx, (_, row) in enumerate(bbl_data.iterrows()):
                # Check if mih_geom is not None/null and not empty
                if row["mih_geom"] is not None:
                    try:
                        # Convert from WKB
                        mih_geom = wkb.loads(row["mih_geom"])

                        # Create detailed popup with percentages
                        mih_popup_text = f"""
                        <b>MIH Area</b><br>
                        <b>Area:</b> {row["mih_area_key"]}<br>
                        <b>BBL Coverage:</b> {row["perbblgeom"]:.1f}%<br>
                        <b>Segment Coverage:</b> {row["segbblgeom"]:.1f}%<br>
                        <b>Max MIH Overlap:</b> {row["maxpermihgeom"]:.1f}%
                        """

                        # Handle both Polygon and MultiPolygon geometries
                        if mih_geom.geom_type == "MultiPolygon":
                            # For MultiPolygon, add each polygon separately
                            for i, polygon in enumerate(mih_geom.geoms):
                                folium.GeoJson(
                                    polygon,
                                    style_function=lambda x: {
                                        "fillColor": "#ff7800",  # Orange for all MIH areas
                                        "color": "#000000",
                                        "weight": 2,
                                        "fillOpacity": 0.7,
                                    },
                                    popup=folium.Popup(
                                        f"{mih_popup_text}<br><b>Part:</b> {i + 1} of {len(mih_geom.geoms)}",
                                        max_width=300,
                                    ),
                                ).add_to(m)

                                # Add label for each polygon part
                                centroid = polygon.centroid
                                folium.Marker(
                                    location=[centroid.y, centroid.x],
                                    icon=folium.DivIcon(
                                        html=f'<div style="font-size: 10px; font-weight: bold; color: white; background-color: rgba(0,0,0,0.7); padding: 2px 4px; border-radius: 3px; white-space: nowrap;">{row["mih_area_key"]} ({i + 1})</div>',
                                        icon_size=(1, 1),
                                        icon_anchor=(0, 0),
                                    ),
                                ).add_to(m)
                        else:
                            # For regular Polygon, handle as before
                            folium.GeoJson(
                                mih_geom,
                                style_function=lambda x: {
                                    "fillColor": "#ff7800",  # Orange for all MIH areas
                                    "color": "#000000",
                                    "weight": 2,
                                    "fillOpacity": 0.7,
                                },
                                popup=folium.Popup(mih_popup_text, max_width=300),
                            ).add_to(m)

                            # Add MIH area label centered on geometry
                            centroid = mih_geom.centroid
                            folium.Marker(
                                location=[centroid.y, centroid.x],
                                icon=folium.DivIcon(
                                    html=f'<div style="font-size: 10px; font-weight: bold; color: white; background-color: rgba(0,0,0,0.7); padding: 2px 4px; border-radius: 3px; white-space: nowrap;">{row["mih_area_key"]}</div>',
                                    icon_size=(1, 1),
                                    icon_anchor=(0, 0),
                                ),
                            ).add_to(m)
                    except Exception as e:
                        print(
                            f"Error processing MIH geometry for area {row['mih_area_key']}: {e}"
                        )

            # Show summary info and map
            valid_mih_count = bbl_data[bbl_data["mih_geom"].notna()].shape[0]
            summary = mo.md(f"""
            ## BBL: {selected_bbl}
            **Total MIH Records:** {len(bbl_data)}
            **Valid MIH Geometries:** {valid_mih_count}

            **MIH Areas:**
            {", ".join(bbl_data["mih_area_key"].astype(str).tolist())}

            - **Blue area**: BBL geometry
            - **Orange areas**: MIH geometries (hover for details)
            """)

            # Create containers for side-by-side layout
            summary_container = mo.Html(f"""
            <div style="width: 25%; padding: 10px;">
                {summary._repr_html_()}
            </div>
            """)

            map_container = mo.Html(f"""
            <div style="width: 75%; height: 400px;">
                {m._repr_html_()}
            </div>
            """)

            # Last expression - side-by-side layout
            map_output = mo.hstack([summary_container, map_container])
        else:
            map_output = mo.md("No data found for selected BBL")
    else:
        map_output = mo.md("Select a row from the table above to view on map")
    return (map_output,)


if __name__ == "__main__":
    app.run()
