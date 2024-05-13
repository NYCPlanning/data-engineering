from .geo_utils import g, GeosupportError, geo_parser_1a, convert_to_sname, get_borocode
from .utils import psql_insert_copy
from . import engine, Pool, cpu_count
import pandas as pd


def geocode(inputs):
    """
    Normalizes street names then applies geosupport
    function 1A. Then parses return for information
    needed to identify failures and warnings.

    Parameters
    ----------
    inputs: dict
        Containing 'housenum', 'b7sc'

    Returns
    -------
    geo: dict
        Contains input fields as well as return codes,
        reason codes, and messages

    """
    hnum = inputs.get("housenum", "")
    b7sc = inputs.get("b7sc", "")
    sname = convert_to_sname(b7sc)
    borough = get_borocode(b7sc)

    hnum = str("" if hnum is None else hnum)
    sname = str("" if sname is None else sname)
    borough = str("" if borough is None else borough)
    try:
        geo = g["1A"](house_number=hnum, street_name=sname, borough=borough)
    except GeosupportError as e:
        geo = e.result

    geo = geo_parser_1a(geo)
    geo.update(inputs)
    return geo


if __name__ == "__main__":
    # Get DCP address points from recipes
    import_sql = f"""
        SELECT DISTINCT addresspoi,
            CONCAT(house_nu_1,' ',house_numb) as housenum,
            CASE
                WHEN special_co = 'V' THEN b7sc_vanit
            ELSE  b7sc_actua
            END as b7sc
        FROM dcp_addresspoints
        WHERE house_nu_1 IS NOT NULL OR house_numb IS NOT NULL
        AND hyphen_typ <> 'U'
        UNION
        SELECT DISTINCT addresspoi,
            CONCAT(house_nu_2,' ',house_nu_3) as housenum,
            CASE
                WHEN special_co = 'V' THEN b7sc_vanit
            ELSE  b7sc_actua
            END as b7sc
        FROM dcp_addresspoints
        WHERE house_nu_2 IS NOT NULL OR house_nu_3 IS NOT NULL
        AND hyphen_typ <> 'U';
    """

    df = pd.read_sql(import_sql, engine)
    records = df.to_dict("records")

    # Geocode using AP function
    print("Geocoding addresses using 1A function...")
    with Pool(processes=cpu_count()) as pool:
        it = pool.map(geocode, records, 10000)
    df = pd.DataFrame(it)

    # Extract results that have no match or a pseudo-address match
    print("Filter results to addresses not in PAD...")
    df["geo_borough_code"] = df["geo_b10sc"].apply(lambda x: get_borocode(x))
    df_qaqc = df[
        (df["geo_return_code"] != "00") & (df["geo_return_code"] != "01")
        | (df["geo_return_code"] == "01") & (df["geo_reason_code"].isin(["8", "9"]))
    ]
    df_qaqc = df_qaqc.replace(",", "", regex=True)

    # Export QAQC results
    df_qaqc.to_sql(
        "rejects_pad_addrpts",
        con=engine,
        if_exists="replace",
        index=False,
        method=psql_insert_copy,
    )
