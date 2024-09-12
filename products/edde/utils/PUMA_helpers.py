import geopandas as gp
from shapely.geometry import Point
import pandas as pd
from numpy import nan
from utils.geocode import from_eviction_address

acs_years = ["0812", "1519", "1721"]

geocode_functions = {"from_eviction_address": from_eviction_address}

borough_code_mapper = {
    "037": "BX",
    "038": "MN",
    "039": "SI",
    "040": "BK",
    "041": "QN",
}

borough_name_mapper = {
    "Bronx": "BX",
    "Brooklyn": "BK",
    "Manhattan": "MN",
    "Queens": "QN",
    "Staten Island": "SI",
}

census_races = ["anh", "bnh", "hsp", "onh", "wnh"]

dcp_pop_races = ["anh", "bnh", "hsp", "wnh"]
NYC_PUMAS_url = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/NYC_Public_Use_Microdata_Areas_PUMAs_2010/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=pgeojson"


def year_range(acs_year: str) -> str:
    return f"20{acs_year[:2]}-20{acs_year[2:]}"


def sheet_name(acs_year: str) -> str:
    return f"{acs_year[:2]}-{acs_year[2:]}"


def puma_to_borough(record):
    borough_code = record.puma[:3]

    borough = borough_code_mapper.get(borough_code, None)
    return borough


def clean_PUMAs(puma) -> pd.DataFrame:
    """Re-uses code from remove_state_code_from_PUMA col in access to subway, call this instead
    Possible refactor: apply to dataframe and ensure that re-named column is label \"puma\"
    """
    puma = str(puma)
    puma = puma.split(".")[0]
    if puma == "nan" or puma == nan:
        return nan
    elif puma[:2] == "36":
        puma = puma[2:]
    elif puma[0] != "0":
        puma = "0" + puma
    return puma


def NYC_PUMA_geographies() -> gp.GeoDataFrame:
    gdf = gp.GeoDataFrame.from_file("./resources/puma_boundaries.json")
    gdf.rename(columns={"PUMA": "puma"}, inplace=True)
    gdf["puma"] = gdf["puma"].apply(clean_PUMAs)
    return gdf


PUMAs = NYC_PUMA_geographies()


def assign_PUMA_col(df: pd.DataFrame, lat_col, long_col, geocode_process=None):
    df.rename(columns={lat_col: "latitude", long_col: "longitude"}, inplace=True)
    df["puma"] = df.apply(assign_PUMA, axis=1, args=(geocode_process,))
    print(f"got {df.shape[0]} addresses to assign PUMAs to ")
    print(f"assigned PUMAs to {df['puma'].notnull().sum()}")
    return df


def assign_PUMA(record: gp.GeoDataFrame, geocode_process):
    if pd.notnull(record.latitude) and pd.notnull(record.longitude):
        return PUMA_from_coord(record)
    if geocode_process:
        return geocode_functions[geocode_process](record)


def PUMA_from_coord(record):
    """Don't think I need to make a geodata frame here, shapely object would do"""
    record_loc = Point(record.longitude, record.latitude)
    matched_PUMA = PUMAs[PUMAs.geometry.contains(record_loc)]
    if matched_PUMA.empty:
        return None
    return matched_PUMA.puma.values[0]


def get_all_NYC_PUMAs():
    """Adopted from code in PUMS_query_manager"""
    geo_ids = [
        range(4001, 4019),  # Brooklyn
        range(3701, 3711),  # Bronx
        range(4101, 4115),  # Queens
        range(3901, 3904),  # Staten Island
        range(3801, 3811),  # Manhattan
    ]
    rv = []
    for borough in geo_ids:
        rv.extend(["0" + str(PUMA) for PUMA in borough])
    return rv


def get_all_boroughs():
    return ["BK", "BX", "MN", "QN", "SI"]


def filter_for_recognized_pumas(df):
    """Written for income restricted indicator but can be used for many other
    indicators that have rows by puma but include some non-PUMA rows. Sometimes
    we set nrows in read csv/excel but this approach is more flexible"""
    return df[df["puma"].isin(get_all_NYC_PUMAs())]
