"""Geocode DEP CATS permit data using Geosupport.

Ported from products/ceqr/ceqr_app/recipes/dep_cats_permits/build.py and its
_helper/geo.py, to pull dep_cats_permits directly from Socrata via ingest instead of
via the CEQR app's own pipeline. Address matching falls back through Geosupport
functions in order: house+street via `1B` (then `1B`-tpad), intersections via `2`,
"stretches" (e.g. "MAIN ST BETWEEN 1ST AVE AND 2ND AVE") via `3`.
"""

import re
import unicodedata

import pandas as pd
from geosupport import Geosupport, GeosupportError
from pyproj import Transformer

# NAD83 / New York Long Island (ft) -> WGS84, used only for intersection matches,
# which Geosupport returns as a state-plane x/y pair rather than lat/lon.
_TRANSFORMER_2263_TO_4326 = Transformer.from_crs(
    "EPSG:2263", "EPSG:4326", always_xy=True
)


class _GeosupportWrapper:
    """Geosupport wrapper - lazy initialization to avoid loading until needed."""

    def __init__(self) -> None:
        self.g = None

    @property
    def geosupport(self):
        if not self.g:
            self.g = Geosupport()
        return self.g


_gw = _GeosupportWrapper()


def _safe_str(x) -> str:
    """Coerce a possibly-missing cell value to a string, treating any NA-like value
    (None, float nan, pd.NA) as empty.

    `Series.astype(str)` isn't reliable for this on its own: with pandas' newer
    Arrow-backed string dtype (default under `future.infer_string`), it can leave
    missing values as an actual float `nan` rather than stringifying them, which
    then breaks any code (like clean_house/clean_street below) expecting a string.
    """
    return "" if pd.isna(x) else str(x)


def _to_ascii(s: str) -> str:
    """Strip to plain ASCII (e.g. non-breaking space -> space, accented letters ->
    base letter).

    Geosupport's fixed-width text buffer expects one byte per character; any
    non-ASCII character UTF-8-encodes to 2+ bytes, which overflows the buffer slot
    and crashes with `ValueError: memoryview assignment: lvalue and rvalue have
    different structures` deep inside the geosupport package. Real example caught in
    production: a street name containing a non-breaking space (U+00A0) instead of a
    regular one.
    """
    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", errors="ignore")
        .decode("ascii")
    )


def clean_boro_name(b: str | None) -> str | None:
    """Clean one-word Staten Island and NULL out invalid borough names."""
    if b == "STATENISLAND":
        b = "STATEN ISLAND"
    if b not in ["BRONX", "MANHATTAN", "BROOKLYN", "QUEENS", "STATEN ISLAND"]:
        b = None
    return b.title() if b is not None else None


def clean_house(s) -> str:
    """Transform a house number to a geosupport-readable format."""
    s = " " if pd.isna(s) else _to_ascii(str(s))
    return (
        re.sub(r"\([^)]*\)", "", s)
        .replace(" - ", "-")
        .strip()
        .split("(", maxsplit=1)[0]
        .split("/", maxsplit=1)[0]
    )


def clean_street(s) -> str:
    """Transform a street name to a geosupport-readable format."""
    s = "" if pd.isna(s) else _to_ascii(str(s))
    s = "JFK INTERNATIONAL AIRPORT" if "JFK" in s else s
    return (
        re.sub(r"\([^)]*\)", "", s)
        .replace("'", "")
        .replace("VARIOUS", "")
        .replace("LOCATIONS", "")
        .split("(", maxsplit=1)[0]
        .split("/", maxsplit=1)[0]
    )


def find_stretch(address: str) -> tuple[str, str, str]:
    """Find addresses that indicate a stretch and split into components."""
    if "BETWEEN" in address.upper():
        street_1 = address.upper().split("BETWEEN")[0].strip()
        bounding_streets = address.upper().split("BETWEEN")[1].strip()
        parts = re.split("&| AND | and", bounding_streets)
        return street_1, parts[0].strip(), parts[1].strip()
    return "", "", ""


def find_intersection(address: str) -> tuple[str, str]:
    """Find addresses that indicate an intersection and split into two streets."""
    upper = address.upper()
    if (
        ("&" in upper)
        or (" AND " in upper)
        or (" CROSS " in upper)
        or (" CRS " in upper)
    ):
        parts = re.split("&| AND | and | CROSS | CRS ", address)
        return parts[0].strip(), parts[1].strip()
    return "", ""


def geo_parser(geo: dict) -> dict:
    """Extract the fields used downstream from a Geosupport return dict."""
    return dict(
        geo_housenum=geo.get("House Number - Display Format", ""),
        geo_streetname=geo.get("First Street Name Normalized", ""),
        geo_address=None,
        geo_bbl=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
            "BOROUGH BLOCK LOT (BBL)", ""
        ),
        geo_bin=geo.get(
            "Building Identification Number (BIN) of Input Address or NAP", ""
        ),
        geo_latitude=geo.get("Latitude", ""),
        geo_longitude=geo.get("Longitude", ""),
        geo_x_coord=geo.get("SPATIAL COORDINATES", {}).get("X Coordinate", ""),
        geo_y_coord=geo.get("SPATIAL COORDINATES", {}).get("Y Coordinate", ""),
        geo_grc=geo.get("Geosupport Return Code (GRC)", ""),
    )


def geocode(inputs: dict) -> dict:
    """Run cleaned/parsed address information through Geosupport.

    First attempt is 1B, then 1B-tpad. Stretches use function 3, intersections use
    function 2.
    """
    g = _gw.geosupport
    hnum, sname, borough, street_name_1, street_name_2, street_name_3 = (
        str("" if k not in inputs or inputs[k] is None else inputs[k])
        for k in (
            "hnum",
            "sname",
            "borough",
            "streetname_1",
            "streetname_2",
            "streetname_3",
        )
    )

    if street_name_1 != "":
        if street_name_3 != "":
            try:
                geo = g["3"](
                    street_name_1=street_name_1,
                    street_name_2=street_name_2,
                    street_name_3=street_name_3,
                    borough_code=borough,
                )
            except GeosupportError as e:
                geo = e.result
            parsed = geo_parser(geo)
            parsed["geo_function"] = "Stretch"
            return parsed
        else:
            try:
                geo = g["2"](
                    street_name_1=street_name_1,
                    street_name_2=street_name_2,
                    borough_code=borough,
                )
            except GeosupportError as e:
                geo = e.result
            parsed = geo_parser(geo)
            parsed["geo_function"] = "Intersection"
            return parsed
    else:
        try:
            geo = g["1B"](street_name=sname, house_number=hnum, borough=borough)
            parsed = geo_parser(geo)
            parsed["geo_function"] = "1B"
            return parsed
        except GeosupportError:
            try:
                geo = g["1B"](
                    street_name=sname, house_number=hnum, borough=borough, mode="tpad"
                )
                parsed = geo_parser(geo)
                parsed["geo_function"] = "1B-tpad"
                return parsed
            except GeosupportError as e:
                parsed = geo_parser(e.result)
                parsed["geo_function"] = "1B"
                return parsed


def process(df: pd.DataFrame) -> pd.DataFrame:
    """Clean, filter, geocode, and normalize geometry for dep_cats_permits.

    Mirrors products/ceqr/ceqr_app/recipes/dep_cats_permits/build.py + create.sql,
    minus the CEQR app dependency:
    1. Clean/parse addresses (borough, house number, street name, stretch/intersection)
    2. Apply the same permit-status filter rules as the CEQR build's create.sql
    3. Geocode each row with the 1B -> 1B-tpad -> 2 -> 3 fallback
    4. Normalize geometry to longitude/latitude in EPSG:4326 (intersections come back
       as an x/y pair in EPSG:2263 and need reprojecting; everything else is already
       lon/lat), dropping rows Geosupport couldn't match at all
    """
    df = df.rename(
        columns={
            "house": "housenum",
            "street": "streetname",
            "issuedate": "issue_date",
            "expirationdate": "expiration_date",
        }
    )

    df["borough"] = df["borough"].apply(clean_boro_name)
    df.loc[
        df["streetname"].apply(lambda x: "JFK" in _safe_str(x)) & df["borough"].isna(),
        "borough",
    ] = "Queens"

    df["hnum"] = df["housenum"].apply(clean_house)
    df["sname"] = df["streetname"].apply(clean_street)
    df["address"] = df["hnum"] + " " + df["sname"]

    stretches = df["address"].apply(find_stretch)
    df["streetname_1"] = stretches.apply(lambda t: t[0])
    df["streetname_2"] = stretches.apply(lambda t: t[1])
    df["streetname_3"] = stretches.apply(lambda t: t[2])

    intersections = df["address"].apply(find_intersection)
    has_intersection = intersections.apply(lambda t: t[0] != "")
    df.loc[has_intersection, "streetname_1"] = intersections[has_intersection].apply(
        lambda t: t[0]
    )
    df.loc[has_intersection, "streetname_2"] = intersections[has_intersection].apply(
        lambda t: t[1]
    )

    df["status"] = df["status"].apply(lambda x: _safe_str(x).strip())
    applicationid = df["applicationid"].apply(_safe_str)

    # Permit-status filter rules, ported from create.sql's WHERE clause
    keep = (
        (df["status"] != "CANCELLED")
        & (applicationid.str[0] != "G")
        & (
            (applicationid.str[0] != "C")
            | (
                ~df["requesttype"].isin(
                    [
                        "REGISTRATION",
                        "REGISTRATION INSPECTION",
                        "BOILER REGISTRATION II",
                    ]
                )
            )
        )
        & (
            (applicationid.str[:2] != "CA")
            | (df["requesttype"] != "WORK PERMIT")
            | (df["status"] != "EXPIRED")
        )
    )
    df = df[keep].reset_index(drop=True)

    geo_columns = [
        "geo_housenum",
        "geo_streetname",
        "geo_address",
        "geo_bbl",
        "geo_bin",
        "geo_latitude",
        "geo_longitude",
        "geo_x_coord",
        "geo_y_coord",
        "geo_grc",
        "geo_function",
    ]
    if df.empty:
        geocoded = pd.DataFrame(columns=geo_columns)
    else:
        geocoded = df.apply(
            lambda row: geocode(row.to_dict()), axis=1, result_type="expand"
        )
    df = pd.concat([df, geocoded], axis=1)

    df = df[df["geo_grc"] != "71"].reset_index(drop=True)

    is_intersection = df["geo_function"] == "Intersection"
    x = pd.to_numeric(df.loc[is_intersection, "geo_x_coord"], errors="coerce")
    y = pd.to_numeric(df.loc[is_intersection, "geo_y_coord"], errors="coerce")
    lon, lat = _TRANSFORMER_2263_TO_4326.transform(x.to_numpy(), y.to_numpy())

    df["longitude"] = pd.to_numeric(df["geo_longitude"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["geo_latitude"], errors="coerce")
    df.loc[is_intersection, "longitude"] = lon
    df.loc[is_intersection, "latitude"] = lat

    df["geo_bbl"] = df["geo_bbl"].apply(
        lambda v: None if v in ("0000000000", "") else v
    )

    df = df[df["longitude"].notna() & df["latitude"].notna()].reset_index(drop=True)

    return df
