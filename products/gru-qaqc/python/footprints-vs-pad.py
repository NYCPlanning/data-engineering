from .geo_utils import g, GeosupportError, geo_parser_bn
from .utils import psql_insert_copy
from . import engine, Pool, cpu_count
import pandas as pd


def geocode(inputs):
    """
    Uses the geosupport BN function to geocode
    based on input BIN numbers. Parses the results
    to get information needed to identify whether
    the return is PAD or TPAD

    Parameters
    ----------
    inputs: dict
        Containing 'bin'

    Returns
    -------
    geo: dict
        Contains input fields as well as return codes,
        reason codes, and messages

    """
    bin = inputs.get("bin", "")
    try:
        geo = g["BN"](bin=bin, mode="tpad")
    except GeosupportError as e:
        geo = e.result

    geo = geo_parser_bn(geo)
    geo.update(inputs)
    return geo


if __name__ == "__main__":
    # Get BINs from building footprints
    print("Loading building footprint data...")
    import_sql = f"""SELECT bin, doitt_id FROM doitt_buildingfootprints;"""
    df = pd.read_sql(import_sql, engine)
    if df.empty:
        raise Exception("No records retrieved from doitt_buildingfootprints, update source data")

    records = df.to_dict("records")

    # Geocode using BN function
    print("Geocoding footprints using BN function...")

    with Pool(processes=cpu_count()) as pool:
        it = pool.map(geocode, records, 10000)
    df = pd.DataFrame(it)

    # Extract results that are in TPAD or that have invalid BINs
    print("Filter results to find footprints in TPAD...")
    invalid_bin_codes = ["22", "23"]
    df_qaqc = df[
        (df["geo_return_code"].isin(invalid_bin_codes))
        | (
            (df["geo_tpad_conflict_flag"] != "1")
            & (df["geo_tpad_conflict_flag"] != "")
            & (df["geo_return_code"] == "01")
        )
    ]

    df_qaqc.loc[df["geo_return_code"] == "22", "qaqc_error"] = "Million BIN"
    df_qaqc.loc[df["geo_return_code"] == "23", "qaqc_error"] = "Temp BIN - DOB Only"
    df_qaqc.loc[df["geo_reason_code"] == "*", "qaqc_error"] = "In TPAD"
    df_qaqc = df_qaqc.replace(",", "", regex=True)

    # Export QAQC results
    df_qaqc.to_sql(
        "rejects_footprintbin_padbin",
        con=engine,
        if_exists="replace",
        index=False,
        method=psql_insert_copy,
    )
