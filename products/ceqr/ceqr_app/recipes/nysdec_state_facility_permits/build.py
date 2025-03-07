import sys
import pandas as pd
from multiprocessing import Pool, cpu_count

# fmt: off
sys.path.insert(0, "..")
from _helper.geo import get_hnum, get_sname, clean_address, find_intersection, find_stretch, geocode, GEOSUPPORT_RETURN_CODE_REJECTION
from _helper.utils import psql_insert_copy
from _helper import EDM_DATA_SQL_ENGINE, DATE, execute_sql_query
# fmt: on

URL_NYSDEC_STATE_FACILITY_PERMITS = "https://data.ny.gov/resource/2wgt-bc53.csv"

CORR = pd.read_csv("../_data/air_corr.csv", dtype=str, engine="c")
CORR_DICT = CORR.loc[CORR.datasource == "nysdec_state_facility_permits", :].to_dict(
    "records"
)

NAME = "nysdec_state_facility_permits"


def _import() -> pd.DataFrame:
    """
    Download and format nysdec state facility permit data from open data API
    Gets raw data from API and saves to output/raw.csv
    Checks raw data to ensure necessary columns are included
    Gets boroughs from zipcodes, and cleans and parses addresses
    Returns:
    df (DataFrame): Contains fields facility_name,
        permit_id, url_to_permit_text, facility_location,
        facility_city, facility_state, zipcode,
        issue_date, expiration_date, location,
        address, borough, hnum, sname,
        streetname_1, streetname_2
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
        "expire_date",
        "georeference",
    ]
    df = pd.read_csv(
        URL_NYSDEC_STATE_FACILITY_PERMITS, dtype=str, engine="c", index_col=False
    )
    df.to_csv("output/raw.csv", index=False)

    # Check input columns and replace column names
    df.columns = [i.lower().replace(" ", "_") for i in df.columns]
    for col in cols:
        assert col in df.columns, f"Missing {col} in input data"

    df = df.rename(
        columns={
            "expire_date": "expiration_date",
            "facility_zip": "zipcode",
            "georeference": "location",
        }
    )

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
    df.to_csv("output/pre-geocoding.csv")
    return df


def _geocode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Geocode cleaned nysdec state facility permit data using helper/air_geocode()

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
    df[cols].to_sql(
        NAME,
        con=EDM_DATA_SQL_ENGINE,
        if_exists="replace",
        index=False,
        method=psql_insert_copy,
    )


if __name__ == "__main__":
    print(f"Using {NAME=} and {DATE=}")
    df = _import()
    df = _geocode(df)
    df = correct_coords(df)
    _output(df)
    execute_sql_query(
        f"""
            DROP TABLE IF EXISTS {NAME}."{DATE}" CASCADE;
            SELECT 
                *,
                (CASE WHEN geo_function = 'Intersection'
                    THEN ST_TRANSFORM(ST_SetSRID(ST_MakePoint(
                        geo_x_coord::double precision,
                        geo_y_coord::double precision),2263),4326)
                    ELSE ST_SetSRID(ST_MakePoint(geo_longitude,geo_latitude),4326)
                END)::geometry(Point,4326) as geom
            INTO {NAME}."{DATE}"
            FROM nysdec_state_facility_permits;

            DROP TABLE IF EXISTS {NAME}."geo_rejects";
            SELECT *
            INTO {NAME}."geo_rejects"
            FROM {NAME}."{DATE}"
            WHERE geom IS NULL;

            DELETE FROM {NAME}."{DATE}" WHERE geom IS NULL;

            DROP VIEW IF EXISTS {NAME}.latest;
            CREATE VIEW {NAME}.latest AS (
                SELECT '{DATE}' as v, * 
                FROM {NAME}."{DATE}"
            );
            DROP TABLE IF EXISTS {NAME}; 
        """
    )
    print("Done with build.py")
