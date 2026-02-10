import sys

sys.path.insert(0, "..")
from multiprocessing import Pool, cpu_count

import pandas as pd
from _helper.geo import (
    GEOSUPPORT_RETURN_CODE_REJECTION,
    clean_address,
    find_intersection,
    find_stretch,
    geocode,
    get_hnum,
    get_sname,
)

URL_NYSDEC_TITLE_V_PERMITS = "https://data.ny.gov/resource/4n3a-en4b.csv"

CORR = pd.read_csv("../_data/air_corr.csv", dtype=str, engine="c")
CORR_DICT = CORR.loc[CORR.datasource == "nysdec_title_v_facility_permits", :].to_dict(
    "records"
)


def _import() -> pd.DataFrame:
    """
    Download and format nysdec title v data from open data API
    Gets raw data from API and saves to output/raw.csv
    Checks raw data to ensure necessary columns are included
    Gets boroughs from zipcodes, and cleans and parses addresses
    Returns:
    df (DataFrame): Contains fields facility_name,
        permit_id, url_to_permit_text, facility_location,
        facility_city, facility_state, zipcode, issue_date,
        expiration_date, location, address, borough,
        hnum, sname, streetname_1, streetname_2
    """
    cols = [
        "facility_name",
        "permit_id",
        "url_to_permit_text",
        "facility_location",
        "facility_city",
        "facility_state",
        "facility_zip",
        "issue_date",
        "expiration_date",
        "georeference",
    ]
    df = pd.read_csv(URL_NYSDEC_TITLE_V_PERMITS, dtype=str, engine="c", index_col=False)
    df.to_csv("output/raw.csv", index=False)

    df.columns = [i.lower().replace(" ", "_") for i in df.columns]
    for col in cols:
        assert col in df.columns, f"Missing {col} in input data"
    df = df.rename(columns={"facility_zip": "zipcode", "georeference": "location"})

    # Get borough and limit to NYC via city
    city_borough = pd.read_csv("../_data/city_boro.csv", dtype=str, engine="c")
    df = pd.merge(
        df,
        city_borough,
        how="left",
        left_on="facility_city",
        right_on="city",
    )
    df = df.rename(columns={"boro": "borough"}, errors="raise")

    # Apply corrections to addresses
    for record in CORR_DICT:
        if record["location"] != record["correction"].upper():
            df.loc[
                (df["facility_location"] == record["location"])
                & (df["permit_id"] == record["id"]),
                "facility_location",
            ] = record["correction"].upper()

    # Extract first location
    df["address"] = df["facility_location"].astype(str).apply(clean_address)

    # Parse stretches
    df[["streetname_1", "streetname_2", "streetname_3"]] = df.apply(
        lambda row: pd.Series(find_stretch(row["address"])), axis=1
    )

    # Parse intersections
    df[["streetname_1", "streetname_2"]] = df.apply(
        lambda row: pd.Series(find_intersection(row["address"])), axis=1
    )

    # Parse house numbers
    df["hnum"] = (
        df["address"]
        .astype(str)
        .apply(get_hnum)
        .apply(lambda x: x.split("/", maxsplit=1)[0] if x is not None else x)
    )

    # Parse street names
    df["sname"] = df["address"].astype(str).apply(get_sname)
    return df


def _geocode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Geocode cleaned nysdec title v data using helper/air_geocode()

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
    df = df[df["geo_grc"] != GEOSUPPORT_RETURN_CODE_REJECTION]
    df["geo_address"] = None
    df["geo_longitude"] = pd.to_numeric(df["geo_longitude"], errors="coerce")
    df["geo_latitude"] = pd.to_numeric(df["geo_latitude"], errors="coerce")
    df["geo_bbl"] = df.geo_bbl.apply(
        lambda x: None if (x == "0000000000") | (x == "") else x
    )
    return df


def correct_coords(df):
    for record in CORR_DICT:
        if record["location"] == record["correction"].upper():
            df.loc[
                (df["facility_location"] == record["location"])
                & (df["permit_id"] == record["id"]),
                "geo_latitude",
            ] = float(record["latitude"])
            df.loc[
                (df["facility_location"] == record["location"])
                & (df["permit_id"] == record["id"]),
                "geo_longitude",
            ] = float(record["longitude"])
            df.loc[
                (df["facility_location"] == record["location"])
                & (df["permit_id"] == record["id"]),
                "geo_function",
            ] = "Manual Correction"
    return df


def _output(df):
    """
    Output geocoded data to stdout for transfer to postgres

    Parameters:
    df (DataFrame): Contains input fields along
                    with geosupport fields
    """
    cols = [
        "facility_name",
        "permit_id",
        "url_to_permit_text",
        "facility_location",
        "address",
        "housenum",
        "streetname",
        "streetname_1",
        "streetname_2",
        "facility_city",
        "facility_state",
        "borough",
        "zipcode",
        "issue_date",
        "expiration_date",
        "location",
        "geo_grc",
        "geo_message",
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
    df = df.rename(columns={"hnum": "housenum", "sname": "streetname"})
    df[cols].to_csv("output/raw.csv", index=False)
    df[cols].to_csv(sys.stdout, index=False)


if __name__ == "__main__":
    df = _import()
    df = _geocode(df)
    df = correct_coords(df)
    _output(df)
