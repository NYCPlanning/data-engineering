import pyogrio

# disable the limit on the size of features in GeoJSON files
pyogrio.set_gdal_config_options({"OGR_GEOJSON_MAX_OBJ_SIZE": 0})
