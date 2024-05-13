from .geo_utils import g, GeosupportError, geo_parser_1b, convert_to_sname, get_borocode
from .utils import psql_insert_copy
from . import engine, Pool, cpu_count
import pandas as pd


def geocode(inputs):
    hnum = inputs.get("housenum", "")
    b7sc = inputs.get("b7sc", "")
    sname = convert_to_sname(b7sc)
    borough = get_borocode(b7sc)

    hnum = str("" if hnum is None else hnum)
    sname = str("" if sname is None else sname)
    borough = str("" if borough is None else borough)
    try:
        geo = g["1E"](
            house_number=hnum,
            street_name=sname,
            borough=borough,
            mode="extended",
            roadbed_request_switch="R",
        )
    except GeosupportError as e:
        geo = e.result

    geo = geo_parser_1b(geo)
    geo.update(inputs)
    return geo


if __name__ == "__main__":
    # spaital join between dcp_addresspoints and dcp_atomicpolygons
    # Filter Out Unit Address Points
    import_sql = f"""
                    WITH hasatomicidjoin AS (
                        SELECT a.*, b.atomicid 
                        FROM dcp_addresspoints a, 
                            dcp_atomicpolygons b
                        WHERE ST_Within(a.wkb_geometry,b.wkb_geometry)),
                    geocodesubset as(
                    SELECT DISTINCT addresspoi AS addresspoid,
                        CONCAT(house_nu_1,' ',house_numb) as housenum,
                        CASE 
                            WHEN special_co = 'V' THEN b7sc_vanit
                            ElSE  b7sc_actua
                        END as b7sc,
                        atomicid
                    FROM hasatomicidjoin
                    WHERE hyphen_typ <> 'U' AND (house_nu_1 IS NOT NULL OR house_numb IS NOT NULL)
                    UNION
                    SELECT DISTINCT addresspoi AS addresspoid,
                        CONCAT(house_nu_2,' ',house_nu_3) as housenum,
                        CASE 
                            WHEN special_co = 'V' THEN b7sc_vanit
                            ElSE  b7sc_actua
                        END as b7sc, 
                        atomicid
                    FROM hasatomicidjoin
                    WHERE hyphen_typ <> 'U' AND (house_nu_2 IS NOT NULL OR house_nu_3 IS NOT NULL))

                    SELECT * FROM geocodesubset;
    """

    # Read in from build engine
    df = pd.read_sql(import_sql, engine)
    records = df.to_dict("records")

    print("Geocoding addresses using 1E function...")
    # Multiprocess
    with Pool(processes=cpu_count()) as pool:
        it = pool.map(geocode, records, 10000)

    print("geocoding finished, dumping to postgres ...")

    df = pd.DataFrame(it)
    df["geo_borough_code"] = df["geo_b10sc"].apply(lambda x: get_borocode(x))
    df["geo_atomicid"] = (
        df["geo_borough_code"] + df["geo_censtract"] + df["geo_atomicpolygon"]
    )

    df_qaqc = df[(df.atomicid != df.geo_atomicid) & (df.geo_atomicpolygon != "")]
    geo_rejects = df[df.geo_atomicpolygon == ""]

    # Export QAQC results
    df_qaqc.to_sql(
        "geocode_diffs_address_spatial",
        con=engine,
        if_exists="replace",
        index=False,
        method=psql_insert_copy,
    )

    geo_rejects.to_sql(
        "rejects_address_spatial",
        con=engine,
        if_exists="replace",
        index=False,
        method=psql_insert_copy,
    )
