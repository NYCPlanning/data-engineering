"""Export the DEP CATS permits archived by ingest for the edm-publishing staging folder.

Ingest pulls, filters, and geocodes the permits (dcpy.lifecycle.scripts.dep_cats_permits_geocode),
so this only selects the published columns, casts them the way the old Postgres export did, and
writes the csv, shapefile, and README.
"""

import datetime as dt
import os
import shutil
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
import shapely
from jinja2 import Template

from dcpy.utils import s3

NAME = "dep_cats_permits"
OUTPUT = Path("output")

COLUMNS = [
    "requestid",
    "applicationid",
    "requesttype",
    "ownername",
    "expiration_date",
    "make",
    "model",
    "burnermake",
    "burnermodel",
    "primaryfuel",
    "secondaryfuel",
    "quantity",
    "issue_date",
    "status",
    "premisename",
    "housenum",
    "streetname",
    "address",
    "streetname_1",
    "streetname_2",
    "borough",
    "geo_housenum",
    "geo_streetname",
    "geo_address",
    "geo_bbl",
    "geo_bin",
    "geo_latitude",
    "geo_longitude",
    "geo_x_coord",
    "geo_y_coord",
    "geo_function",
]
DATES = ["expiration_date", "issue_date"]
FLOATS = ["geo_latitude", "geo_longitude", "geo_x_coord", "geo_y_coord"]


def _read_archive(version: str) -> gpd.GeoDataFrame:
    local = s3.download_file(
        os.environ.get("RECIPES_BUCKET", "edm-recipes"),
        f"datasets/{NAME}/{version}/{NAME}.parquet",
        OUTPUT / f"{NAME}.parquet",
    )
    gdf = gpd.read_parquet(local)
    local.unlink()
    return gdf


def _cast(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf[COLUMNS + ["geom"]].copy()
    text = [c for c in COLUMNS if c not in DATES + FLOATS + ["quantity", "geo_bbl"]]
    gdf[text] = gdf[text].replace("", None)
    for c in DATES:
        gdf[c] = pd.to_datetime(gdf[c], format="mixed").dt.date
    for c in FLOATS:
        gdf[c] = pd.to_numeric(gdf[c].replace("", None))
    gdf["quantity"] = pd.to_numeric(gdf["quantity"]).astype("Int64").astype("string")
    gdf["geo_bbl"] = pd.to_numeric(gdf["geo_bbl"].replace("", None)).astype("Int64")
    return gdf


def _write(gdf: gpd.GeoDataFrame) -> None:
    csv = pd.DataFrame(gdf[COLUMNS])
    for c in FLOATS:
        # Postgres prints whole doubles without a trailing ".0"
        csv[c] = csv[c].map(lambda v: None if pd.isna(v) else str(v).removesuffix(".0"))
    csv["geom"] = shapely.to_wkb(
        shapely.set_srid(gdf.geometry.values, 4326), hex=True, include_srid=True
    )
    csv.to_csv(OUTPUT / f"{NAME}.csv", index=False)

    shp_dir = OUTPUT / NAME
    shp_dir.mkdir()
    gdf.to_file(shp_dir / f"{NAME}.shp")
    shutil.make_archive(str(OUTPUT / NAME), "zip", shp_dir)
    shutil.rmtree(shp_dir)


def _readme() -> None:
    date_last_update = dt.datetime.today().strftime("%B %d, %Y")
    metadata = requests.get(
        "https://data.cityofnewyork.us/api/views/f4rp-2kvy.json"
    ).json()
    date_underlying_data = dt.datetime.fromtimestamp(
        metadata["rowsUpdatedAt"]
    ).strftime("%B %d, %Y")
    with open("README.md") as readme:
        rendered = Template(readme.read()).render(
            date_last_update=date_last_update,
            date_underlying_data=date_underlying_data,
        )
    (OUTPUT / "README.md").write_text(rendered)


if __name__ == "__main__":
    OUTPUT.mkdir(exist_ok=True)
    _write(_cast(_read_archive(os.environ["ARCHIVE_VERSION"])))
    _readme()
