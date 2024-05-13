from .geo_utils import g, GeosupportError, get_sname, geo_parser_1n
from .utils import psql_insert_copy
from . import engine, Pool, cpu_count
import pandas as pd
import re


def geocode(inputs):
    """
    Uses the geosupport 1N function to geocode

    Parameters
    ----------
    inputs: dict
        Containing 'street_name' and 'borough'

    Returns
    -------
    geo: dict
        Contains input fields as well as return codes,
        reason codes, and messages
    """
    borough = inputs.get("borough", "")
    street_name = re.sub(
        r"[,\%\$\#\@\!\_\.\?\`\’\"\(\)]", "", inputs.get("street_name", "")
    )

    try:
        geo = g["1N"](borough=borough, street_name=street_name)
    except GeosupportError as e:
        geo = e.result

    geo = geo_parser_1n(geo)
    geo.update(inputs)
    return geo


if __name__ == "__main__":
    print("Get borough and street name")
    import_sql = f"""
        SELECT distinct borough, street_nm as street_name
        FROM dcp_dcmstreetcenterline
        WHERE borough is not null 
        AND street_nm is not null ;"""
    df = pd.read_sql(import_sql, engine)
    records = df.to_dict("records")

    # Geocode using 1N function
    print("Geocoding footprints using 1N function...")

    with Pool(processes=cpu_count()) as pool:
        it = pool.map(geocode, records, 10000)
    df = pd.DataFrame(it)

    print("Filter failed results...")
    df_qaqc = df.loc[df.geo_return_code != "00", :]

    # Export QAQC results
    df_qaqc.to_sql(
        "rejects_sn_dcm_snd",
        con=engine,
        if_exists="replace",
        index=False,
        method=psql_insert_copy,
    )
