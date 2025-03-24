import sys

sys.path.insert(0, "..")
import pandas as pd
import numpy as np
from _helper.geo import (
    clean_street,
    clean_house,
    clean_boro_name,
    find_stretch,
    find_intersection,
    geocode,
)
from multiprocessing import Pool, cpu_count
import datetime as dt
from jinja2 import Template
import requests


def _import() -> pd.DataFrame:
    """
    Download and format DEP CATS permit data from open data API

    Gets raw data from API and saves to output/raw.csv
    Checks raw data to ensure necessary columns are included
    Gets boroughs from zipcodes, and cleans and parses addresses

    Returns:
    df (DataFrame): Contains fields requestid, applicationid,
        requesttype, housenum, hnum, streetname, sname, borough,
        bin, block, lot, ownername, expiration_date, make,
        model, burnermake, burnermodel, primaryfuel, secondaryfuel,
        quantity, issue_date, status, premisename, streetname_1,
        streetname_2
    """
    url = "https://data.cityofnewyork.us/api/views/f4rp-2kvy/rows.csv"
    cols = [
        "requestid",
        "applicationid",
        "requesttype",
        "house",
        "street",
        "borough",
        "bin",
        "block",
        "lot",
        "ownername",
        "expirationdate",
        "make",
        "model",
        "burnermake",
        "burnermodel",
        "primaryfuel",
        "secondaryfuel",
        "quantity",
        "issuedate",
        "status",
        "premisename",
    ]

    df = pd.read_csv(url, dtype=str, engine="c")
    df.columns = [i.lower() for i in df.columns]
    for col in cols:
        assert col in df.columns

    df.to_csv("output/raw.csv", index=False)

    df.rename(
        columns={
            "house": "housenum",
            "street": "streetname",
            "issuedate": "issue_date",
            "expirationdate": "expiration_date",
        },
        inplace=True,
    )

    # Clean borough
    df["borough"] = df.borough.apply(clean_boro_name)
    df["borough"] = np.where(
        (df.streetname.str.contains("JFK")) & (df.borough is None), "Queens", df.borough
    )

    # Clean house number & street names
    df["hnum"] = df.housenum.astype(str).apply(clean_house)
    df["sname"] = df.streetname.astype(str).apply(clean_street)

    # Concatenate house & street to form full address
    df["address"] = df.hnum + " " + df.sname

    # Parse stretches
    df[["streetname_1", "streetname_2", "streetname_3"]] = df.apply(
        lambda row: pd.Series(find_stretch(row["address"])), axis=1
    )

    # Parse intersections
    df[["streetname_1", "streetname_2"]] = df.apply(
        lambda row: pd.Series(find_intersection(row["address"])), axis=1
    )

    df["status"] = df["status"].apply(lambda x: x.strip())

    return df


def _geocode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Geocode cleaned DEP CATS permit data using helper/geocode()

    Parameters:
    df (DataFrame): Contains data  with
                    hnum and sname parsed
                    from address
    Returns:
    df (DataFrame): Contains input fields along
                    with geosupport fields
    """
    # geocoding
    records = df.to_dict("records")
    del df

    # Multiprocess
    with Pool(processes=cpu_count()) as pool:
        it = pool.map(geocode, records, 10000)

    df = pd.DataFrame(it)
    df = df[df["geo_grc"] != "71"]
    df["geo_address"] = None
    df["geo_longitude"] = pd.to_numeric(df["geo_longitude"], errors="coerce")
    df["geo_latitude"] = pd.to_numeric(df["geo_latitude"], errors="coerce")
    df["geo_bbl"] = df.geo_bbl.apply(
        lambda x: None if (x == "0000000000") | (x == "") else x
    )
    return df


def _output(df: pd.DataFrame):
    """
    Output geocoded data to stdout for transfer to postgres

    Parameters:
    df (DataFrame): Contains input fields along
                    with geosupport fields
    """
    cols = [
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
    df[cols].to_csv("output/dep_cats_permits.csv", index=False)


def _readme():
    date_last_update = dt.datetime.today().strftime("%B %d, %Y")
    metadata = requests.get(
        "https://data.cityofnewyork.us/api/views/f4rp-2kvy.json"
    ).json()
    date_underlying_data = dt.datetime.fromtimestamp(
        metadata["rowsUpdatedAt"]
    ).strftime("%B %d, %Y")

    with open("README.md", "r") as readme:
        template = Template(readme.read())
        rendered = template.render(
            date_last_update=date_last_update, date_underlying_data=date_underlying_data
        )
    with open("output/README.md", "w") as _readme:
        _readme.write(rendered)


if __name__ == "__main__":
    df = _import()
    df = _geocode(df)
    _readme()
    _output(df)
