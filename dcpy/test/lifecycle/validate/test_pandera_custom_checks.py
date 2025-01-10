import pytest
import pandera as pa
from shapely.geometry import Point, LineString, Polygon
import pandas as pd

from dcpy.models.dataset import Column
from dcpy.lifecycle.validate import pandera_utils


def test_is_geom_point_valid_points():
    df = pd.DataFrame(
        {
            "geometry": [
                Point(1, 1),
                Point(2, 3),
            ]
        }
    )
    pandera_utils.run_data_checks(
        df=df,
        columns=[Column(id="geometry", checks=["is_geom_point"])],
    )


def test_is_geom_point_invalid_geoms():
    df = pd.DataFrame(
        {
            "geometry": [
                Point(1, 1),
                LineString([(0, 0), (1, 1)]),
                Polygon([(0, 0), (1, 1), (1, 0)]),
            ]
        }
    )
    with pytest.raises(pa.errors.SchemaError):
        pandera_utils.run_data_checks(
            df=df,
            columns=[Column(id="geometry", checks=["is_geom_point"])],
        )
