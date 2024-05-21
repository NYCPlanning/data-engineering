from enum import Enum
import geopandas as gpd
import pandas as pd
from pathlib import Path
import pprint
from shapely import wkb, wkt
from typing import Callable, Any
import pandera as pa
from pydantic import BaseModel
from pandera import extensions

from dcpy.models.validate import Field, Fields
from dcpy.utils.logging import logger


@extensions.register_check_method(check_type="element_wise")
def is_valid_wkb(g):
    try:
        wkb.loads(g)
        return True
    except Exception:
        return False


@extensions.register_check_method(check_type="element_wise")
def is_valid_wkt(g):
    try:
        wkt.loads(g)
        return True
    except Exception:
        return False


@extensions.register_check_method()
def is_float_or_double(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


@extensions.register_check_method()
def is_int(s):
    if s[0] in ("-", "+"):
        return s[1:].isdigit()
    return s.isdigit()


@extensions.register_check_method(check_type="element_wise")
def is_geom_point(s):
    try:
        return s.geom_type == "Point"
    except ValueError:
        return False


@extensions.register_check_method(check_type="element_wise")
def is_geom_poly(s):
    try:
        return s.geom_type in {"Polygon", "MultiPolygon"}
    except ValueError:
        return False


@extensions.register_check_method()
def not_null(s):
    return s is not None or (not s.isna()) or (not s.isnan())


def get_check(check_name: str, args: dict | None = None) -> pa.Check:
    args = args or {}
    if hasattr(pa.Check, check_name):
        return getattr(pa.Check, check_name)(args)


def generate_schema(fields: list[Field]):
    schema = {}
    for field in fields:
        checks = (
            [get_check(check, field.checks[check]) for check in field.checks]
            if field.checks
            else None
        )
        schema[field.name] = pa.Column(
            dtype=None, checks=checks, required=field.required
        )
    return pa.DataFrameSchema(schema)


def validate_schema(df: pd.DataFrame, fields: list[Field]):
    schema = generate_schema(fields)
    schema.validate(df)
