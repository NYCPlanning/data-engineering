import folium
import geopandas as gpd
from streamlit_folium import st_folium

FILL = "#d96b27"
LINE = "black"


def output_map(df):
    df_clean = df.dropna(subset=["geometry"])
    geometries = gpd.GeoSeries.from_wkt(df_clean["geometry"])
    gdf = gpd.GeoDataFrame(df_clean, geometry=geometries, crs="EPSG:4326")
    base_map_nyc = folium.Map(
        location=[40.70, -73.94],
        zoom_start=10,
        tiles="CartoDB positron",
        control_scale=True,
    )

    def plot_point(p):
        lat, lon = p.centroid.y, p.centroid.x
        return folium.CircleMarker(
            location=[lat, lon],
            radius=3,
            fill=True,
            fill_color=FILL,
            fill_opacity=0.5,
            color=LINE,
            opacity=0.3,
            weight=1,
        )

    for _, row in gdf.iterrows():
        if row["geometry"].geom_type == "Point":
            objs = [plot_point(row["geometry"])]
        elif row["geometry"].geom_type == "MultiPoint":
            points = gpd.GeoSeries(row["geometry"].geoms)
            objs = [plot_point(point) for point in points]
        else:
            geo = gpd.GeoSeries(row["geometry"])
            obj = geo.to_json()
            obj = folium.GeoJson(
                data=obj,
                style_function=lambda _: {
                    "fillColor": FILL,
                    "fillOpacity": 0.5,
                    "color": LINE,
                    "opacity": 0.3,
                    "weight": 1,
                },
            )
            objs = [obj]

        for obj in objs:
            folium.Popup(row["fms_id"]).add_to(obj)
            obj.add_to(base_map_nyc)

    return st_folium(base_map_nyc, width=900, height=500)
