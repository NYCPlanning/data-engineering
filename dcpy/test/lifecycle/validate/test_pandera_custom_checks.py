import geopandas as gpd
import pandera as pa
import pytest

from dcpy.lifecycle.validate import pandera_utils
from dcpy.models.dataset import Column


def test_is_geom_point_valid_points():
    gdf = gpd.GeoDataFrame(
        {
            "geometry": gpd.GeoSeries.from_wkt(
                [
                    "POINT (1 1)",
                    "POINT (2 3)",
                ]
            )
        }
    )
    pandera_utils.run_data_checks(
        df=gdf,
        columns=[Column(id="geometry", checks=["is_geom_point"])],
    )


def test_is_geom_point_invalid_geoms():
    gdf = gpd.GeoDataFrame(
        {
            "geometry": gpd.GeoSeries.from_wkt(
                [
                    "POINT (1 1)",
                    "LINESTRING (0 0, 1 1)",
                    "POLYGON ((0 0, 1 1, 1 0, 0 0))",
                ]
            )
        }
    )
    with pytest.raises(pa.errors.SchemaError):
        pandera_utils.run_data_checks(
            df=gdf,
            columns=[Column(id="geometry", checks=["is_geom_point"])],
        )
