import geopandas as gp
from shapely.geometry import Point
import pandas as pd
from numpy import nan
from ingest.ingestion_helpers import load_data

# Why is this a PUMA helper?
# TODO: move
acs_years = ["0812", "1923"]

borough_code_mapper = {
    "042": "BX",
    "041": "MN",
    "045": "SI",
    "043": "BK",
    "044": "QN",
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


def year_range(acs_year: str) -> str:
    return f"20{acs_year[:2]}-20{acs_year[2:]}"


def sheet_name(acs_year: str) -> str:
    return f"{acs_year[:2]}-{acs_year[2:]}"


def puma_to_borough(record):
    borough_code = record.puma[:3]

    borough = borough_code_mapper.get(borough_code, None)
    return borough


def clean_PUMAs(puma: str | int):
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


def _get_nyc_puma_geographies_2010() -> gp.GeoDataFrame:
    gdf = gp.GeoDataFrame.from_file("./resources/puma_boundaries.json")
    gdf.rename(columns={"PUMA": "puma"}, inplace=True)
    gdf["puma"] = gdf["puma"].apply(clean_PUMAs)
    return gdf


PUMAS_2010 = _get_nyc_puma_geographies_2010()


def assign_2010_puma_col(df: pd.DataFrame, lat_col, long_col):
    df.rename(columns={lat_col: "latitude", long_col: "longitude"}, inplace=True)
    df["puma"] = df.apply(_assign_2010_puma, axis=1)
    print(f"got {df.shape[0]} addresses to assign PUMAs to ")
    print(f"assigned PUMAs to {df['puma'].notnull().sum()}")
    return df


def _assign_2010_puma(record: gp.GeoDataFrame):
    if pd.notnull(record.latitude) and pd.notnull(record.longitude):
        return _2010_puma_from_coord(record)


def _2010_puma_from_coord(record):
    """Don't think I need to make a geodata frame here, shapely object would do"""
    record_loc = Point(record.longitude, record.latitude)
    matched_PUMA = PUMAS_2010[PUMAS_2010.geometry.contains(record_loc)]
    if matched_PUMA.empty:
        return None
    return matched_PUMA.puma.values[0]


def get_all_NYC_PUMAs():
    """Adopted from code in PUMS_query_manager"""
    puma_boundaries = load_data("dcp_pumas2020")
    return [f"0{p}" for p in puma_boundaries["puma"]]


def get_all_boroughs():
    return ["BK", "BX", "MN", "QN", "SI"]


def filter_for_recognized_pumas(df):
    """Written for income restricted indicator but can be used for many other
    indicators that have rows by puma but include some non-PUMA rows. Sometimes
    we set nrows in read csv/excel but this approach is more flexible"""
    return df[df["puma"].isin(get_all_NYC_PUMAs())]
