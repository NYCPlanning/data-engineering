from .geo_utils import g, GeosupportError, geo_parser_1
from .utils import psql_insert_copy
from . import engine, Pool, cpu_count
import pandas as pd


def geocode(input):
    """
    Uses the geosupport 1B function to geocode
    based on input addresses. Parses the results
    to get information needed to identify whether
    the return is PAD or TPAD

    Parameters
    ----------
    inputs: dict
        Containing 'address_numbr', 'address_st', 'boro'

    Returns
    -------
    geo: dict
        Contains input fields as well as return codes,
        reason codes, and messages

    """
    hnum = input.get("address_numbr", "")
    sname = input.get("address_st", "")
    borough = input.get("boro", "")

    try:
        geo = g["1B"](
            street_name=sname, house_number=hnum, borough=borough, mode="tpad"
        )
    except GeosupportError as e:
        geo = e.result

    geo = geo_parser_1(geo)
    geo.update(input)
    return geo


if __name__ == "__main__":
    # Get complete housing jobs
    print("Loading complete housing jobs data for New Buildings and Alterations...")
    df = pd.read_sql(
        """SELECT job_type, job_number, job_desc,
                        job_number||date_lastupdt AS uid, 
                        address_numbr, address_st, boro, 
                        classa_init, classa_prop,
                        date_complete, date_permittd, date_filed
                        FROM dcp_developments WHERE date_complete IS NOT NULL AND job_type <> 'Demolition' """,
        engine,
    )
    records = df.to_dict("records")

    # Geocode using 1B function
    print("Geocoding records with a CO using 1B function...")
    with Pool(processes=cpu_count()) as pool:
        it = pool.map(geocode, records, 10000)
    df = pd.DataFrame(it)
    print("Number of geocoded records before filtering: ", df.shape)

    # Extract results that are in TPAD
    print("Filter results to find CO buildings in TPAD...")

    df_qaqc = df[
        (df["geo_tpad_conflict_flag"] != "1")
        & (df["geo_tpad_conflict_flag"] != "")
        & ((df["geo_return_code_2"] == "01") | (df["geo_return_code_2"] == "00"))
    ]

    df_qaqc = df_qaqc.replace(",", "", regex=True)
    print("Number of geocoded records after filtering: ", df_qaqc.shape[0])

    df_deduped = df_qaqc.groupby(["geo_borough", "address_numbr", "address_st"]).apply(
        lambda group: group.loc[
            group[group["job_number"] == group["job_number"].max()].index[0]
        ]
    )
    print("Number of records per address: ", df_deduped.shape[0])

    # Export QAQC results
    df_deduped.to_sql(
        "tbins_certf_occp",
        con=engine,
        if_exists="replace",
        index=False,
        method=psql_insert_copy,
    )
