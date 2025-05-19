"""Functions for processing files with CD"""

from dcpy.utils.logging import logger
from functools import cache
import pandas as pd
import re

from utils.PUMA_helpers import borough_name_mapper, clean_PUMAs
from ingest import ingestion_helpers


def add_CD_code(df):
    df["borough"] = df["Borough"].replace(borough_name_mapper)
    df["borough_CD_code"] = df.Geography.str.extract(r"(\d+)")
    df["CD_code"] = df["borough"] + df["borough_CD_code"]


def get_CD_NTA_puma_crosswalk():
    puma_cross = pd.read_excel(
        "https://www1.nyc.gov/assets/planning/download/office/data-maps/nyc-population/census2010/nyc2010census_tabulation_equiv.xlsx",
        sheet_name="NTA in PUMA_",
        header=6,
        dtype=str,
    )
    puma_cross.columns = puma_cross.columns.str.replace(" \n", "")
    puma_cross.rename(
        columns={
            "Unnamed: 0": "borough",
            "Unnamed: 1": "county_code",
            "Unnamed: 2": "borough_code",
        },
        inplace=True,
    )
    puma_cross.rename(
        columns={
            "Community District(PUMAs approximate NYC Community  Districts and are not coterminous)": "CD",
            "PUMACode": "puma",
            "NTACode": "nta",
            "Name": "name",
        },
        inplace=True,
    )
    puma_cross["puma"] = puma_cross["puma"].apply(clean_PUMAs)

    return puma_cross


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


# TODO: rename, once older version is deleted
def community_district_to_pum_new(
    borough_abbrev, comm_dist_num: str | int, ignore_errors=False
):
    if not ignore_errors:
        assert len(borough_abbrev) == 2, "Abbreviated borough expected, e.g. BX"
        assert len(str(comm_dist_num)) <= 2, (
            f"Community District number should be two chars or less. got {comm_dist_num}"
        )
    return _get_cd_puma_crosswalk().get(f"{borough_abbrev}{comm_dist_num}")


def community_district_to_PUMA(df, CD_col, CD_abbr_type="alpha_borough"):
    """CD_abbr_type refers to how CD is referred to in source data.
    alpha_borough refers to two letter borough abbreviation plus the
    community district number(BX01, QN11)
    numeric_borough refers to borough code (found in crosswalk df) plus the community
    district number
    """
    assert CD_abbr_type in ["alpha_borough", "numeric_borough"]

    puma_cross = get_CD_NTA_puma_crosswalk()

    mapper = {}

    for _, row in puma_cross.iterrows():
        for cd_num in re.findall(r"\d+", row["CD"]):
            if CD_abbr_type == "alpha_borough":
                cd_code = row["CD"][:2] + cd_num
            else:
                cd_code = construct_three_digit_CD_code(row["borough_code"], cd_num)
            mapper[cd_code] = row.puma
    df["puma"] = df[CD_col].map(mapper)
    return df


def construct_three_digit_CD_code(borough_code: str, cd_num: str) -> str:
    if len(cd_num) == 1:
        cd_num = f"0{cd_num}"
    return f"{borough_code}{cd_num}"


def get_borough_num_mapper():
    return {"1": "MN", "2": "BX", "3": "BK", "4": "QN", "5": "SI"}
