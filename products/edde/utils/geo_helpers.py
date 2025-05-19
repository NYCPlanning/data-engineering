from dcpy.utils.logging import logger
from functools import cache
from shapely.geometry import Point
from numpy import nan

from ingest.ingestion_helpers import load_data
from ingest import ingestion_helpers


# Why is this a PUMA helper?
# TODO: move
acs_years = ["0812", "1923"]
acs_years_end_to_full = {"12": "0812", "23": "1923"}

borough_num_mapper = {"1": "MN", "2": "BX", "3": "BK", "4": "QN", "5": "SI"}

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
def get_2020_pumas():
    pumas = load_data("dcp_pumas2020", is_geospatial=True).to_crs("EPSG:2263")  # type: ignore
    pumas["puma"] = pumas["puma"].apply(lambda p: f"0{p}")
    pumas["borough"] = pumas.apply(puma_to_borough, axis=1)
    return pumas


def puma_from_point(point) -> str | None:
    pumas = get_2020_pumas()
    matched = pumas[pumas.geom.contains(point)]
    return None if matched.empty else matched.puma.values[0]


def puma_from_coord(longitude, latitude) -> str | None:
    assert latitude and longitude
    pumas = get_2020_pumas()
    matched = pumas[pumas.geom.contains(Point(longitude, latitude))]
    return None if matched.empty else matched.puma.values[0]


def get_all_NYC_PUMAs() -> list[str]:
    return list(get_2020_pumas()["puma"])


def get_all_boroughs():
    return ["BK", "BX", "MN", "QN", "SI"]


def filter_for_recognized_pumas(df):
    """Written for income restricted indicator but can be used for many other
    indicators that have rows by puma but include some non-PUMA rows. Sometimes
    we set nrows in read csv/excel but this approach is more flexible"""
    return df[df["puma"].isin(get_all_NYC_PUMAs())]


@cache
def _get_cd_puma_crosswalk() -> dict[str, str]:
    """Get a map of community district keys to approximate PUMAS
    e.g. BX3 -> 04263
    """
    logger.info("loading cd->puma crosswalk")
    cw = ingestion_helpers.load_data("dcp_population_cd_puma_crosswalk_2020")
    cw["puma_code"] = cw["puma_code"].apply(lambda c: f"0{c}")
    cw["dist_key"] = cw["borough_code"] + cw["community_district_num"]
    return cw.set_index("dist_key")["puma_code"].to_dict()


def community_district_to_puma(
    borough_abbrev, comm_dist_num: str | int, ignore_errors=False
):
    try:
        comm_dist_num = int(comm_dist_num)
    except Exception:
        return ""
    if not ignore_errors:
        assert len(borough_abbrev) == 2, (
            f"Abbreviated borough expected, e.g. BX. got {borough_abbrev}"
        )
        assert len(str(comm_dist_num)) <= 2, (
            f"Community District number should be two chars or less. got {comm_dist_num}"
        )
    return _get_cd_puma_crosswalk().get(f"{borough_abbrev}{comm_dist_num}")
