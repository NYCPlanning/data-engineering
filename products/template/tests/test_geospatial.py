# from . import TEST_DATA_DIR
# import geopandas as gpd

# from python.geospatial import (
#     NYC_PROJECTION,
#     WKT_PROJECTION,
#     convert_to_geodata,
#     reproject_geometry,
# )
# from python.utils import load_data_file


# def test_convert_to_geodata():
#     data = load_data_file(filepath=f"{TEST_DATA_DIR}/census_counties_nyc.csv")
#     data = convert_to_geodata(data)
#     assert isinstance(data, gpd.GeoDataFrame)
#     assert data.crs == WKT_PROJECTION


# def test_reporject_geometry():
#     data = load_data_file(filepath=f"{TEST_DATA_DIR}/census_counties_nyc.csv")
#     data = convert_to_geodata(data)
#     data = reproject_geometry(data, WKT_PROJECTION, NYC_PROJECTION)
#     assert data.crs == NYC_PROJECTION
