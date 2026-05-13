import re
from enum import Enum
from typing import TypeAlias

from pydantic import BaseModel, field_validator


class EpsgCode(str, Enum):
    WGS84 = "EPSG:4326"
    StatePlane = "EPSG:2263"


class StandardGeometryFormat(str, Enum):
    wkt = "wkt"
    wkb = "wkb"


class PointXYStr(BaseModel, extra="forbid"):
    """
    Class with non-wkt representation of point geometry.
    'point_xy_str' is a pattern which tells the class how to parse this representation.
    This string must contain both an x and y character once and only once each.

    For example, wkt would be `point_xy_str = "Point(x y)"`
    Or a column with value "-74.0, 40.7" would be "x y"

    The wkt function of this class assumes that any string to be parsed has x and y
    each represented by a decimal (digits, followed by period, followed by digits)
    For now, this does not need to be more flexible, but this could be another optional input
    """

    point_xy_str: str

    def wkt(self, s: str) -> str:
        def capture(x: str) -> str:
            return rf"(?P<{x}>-?\d+\.\d+)"

        regex_str = (
            re.escape(self.point_xy_str)
            .replace("x", capture("x"))
            .replace("y", capture("y"))
        )
        re_match = re.match(rf"^{regex_str}$", s)
        if re_match:
            return f"Point({re_match['x']} {re_match['y']})"
        else:
            raise ValueError(
                f"Geom field '{s}' does not match format '{self.point_xy_str}'"
            )

    @field_validator("point_xy_str")
    @classmethod
    def must_contain_one_each_x_y(cls, s: str) -> str:
        if not re.match(r"^(?=[^x]*x[^x]*$)(?=[^y]*y[^y]*$)", s):
            raise ValueError(
                "'point_xy_str' must contain exactly one 'x' and one 'y' character"
            )
        return s


GeometryFormat: TypeAlias = StandardGeometryFormat | PointXYStr
