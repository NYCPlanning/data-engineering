from functools import cache
from shapely.geometry import Point
from numpy import nan
from ingest.ingestion_helpers import load_data

# Why is this a PUMA helper?
# TODO: move
acs_years = ["0812", "1923"]
acs_years_end_to_full = {"12": "0812", "23": "1923"}

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


@cache
def _get_2020_pumas():
    return load_data("dcp_pumas2020", is_geospatial=True)


def puma_from_coord(longitude, latitude) -> str | None:
    assert latitude and longitude
    pumas = _get_2020_pumas()
    matched = pumas[pumas.geom.contains(Point(longitude, latitude))]
    return None if matched.empty else matched.puma.values[0]


def get_all_NYC_PUMAs(prefix_zeros=True):
    # prefix_zeros is an unfortuante hack for now
    puma_boundaries = load_data("dcp_pumas2020")
    return [f"{'0' if prefix_zeros else ''}{p}" for p in puma_boundaries["puma"]]


def get_all_boroughs():
    return ["BK", "BX", "MN", "QN", "SI"]


def filter_for_recognized_pumas(df):
    """Written for income restricted indicator but can be used for many other
    indicators that have rows by puma but include some non-PUMA rows. Sometimes
    we set nrows in read csv/excel but this approach is more flexible"""
    return df[df["puma"].isin(get_all_NYC_PUMAs())]
