import pyogrio
from pydantic import BaseModel

BaseModel.model_config["coerce_numbers_to_str"] = True
# disable the limit on the size of features in GeoJSON files
pyogrio.set_gdal_config_options({"OGR_GEOJSON_MAX_OBJ_SIZE": 0})
