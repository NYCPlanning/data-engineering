import geopandas as gpd
import leafmap.foliumap as lmf

DEFAULT_MAP_SIZE = {"width": 900, "height": 700}
DEFAULT_BASEMAP = "Stadia.AlidadeSmoothDark"
NYC_CENTER_COORDS = [40.726446, -73.983307]
NYC_CENTER_ZOOM = 13


def generate_folium_map(gdf: gpd.GeoDataFrame) -> lmf.Map:
    basic_map = lmf.Map(
        center=NYC_CENTER_COORDS,
        zoom=NYC_CENTER_ZOOM,
    )
    basic_map.add_basemap(DEFAULT_BASEMAP)

    gdf_points = gdf[gdf.geom_type == "Point"]
    gdf_points["latitude"] = gdf_points.centroid.y
    gdf_points["longitude"] = gdf_points.centroid.x
    gdf_polygons = gdf[gdf.geom_type == "Polygon"]
    gdf_multipolygons = gdf[gdf.geom_type == "MultiPolygon"]

    if len(gdf_points) > 0:
        basic_map.add_points_from_xy(
            gdf_points, crs=str(gdf_points.crs), layer_name="Points"
        )
    if len(gdf_polygons) > 0:
        basic_map.add_gdf(gdf_polygons, layer_name="Polygons")
    if len(gdf_multipolygons) > 0:
        basic_map.add_gdf(
            gdf_multipolygons,
            layer_name="MultiPolygons",
        )

    return basic_map
