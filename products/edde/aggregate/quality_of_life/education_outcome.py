import pandas as pd
from ingest.ingestion_helpers import read_from_excel
from internal_review.set_internal_review_file import set_internal_review_files

SOURCE_DATA_PATH_EDU_OUTCOME = "resources/quality_of_life/education_outcome/NTA_data_prepared_for_ArcMap_wCodebook.xlsx"
SOURCE_DATA_URL_PUMA_CROSS = "https://www1.nyc.gov/assets/planning/download/office/data-maps/nyc-population/census2010/nyc2010census_tabulation_equiv.xlsx"

# TODO resolve issue with new data's racial groups
# new data seems to have replaced all caolumns for Other
# with new racial groups
RACIAL_GROUPS = [
    "ALL",
    "ASN",
    "BLK",
    "HIS",
    "OTH",
    "WHT",
]


def calculate_edu_outcome(df: pd.DataFrame, geography: str):
    agg = df.groupby(geography).sum().reset_index()

    for group in RACIAL_GROUPS:
        agg[f"E38PRFP{group}"] = agg[f"E38PRFN{group}"] / agg[f"E38TEST{group}"]  # ELA
        agg[f"M38PRFP{group}"] = agg[f"M38PRFN{group}"] / agg[f"M38TEST{group}"]  # MATH
        agg[f"GRAD17P{group}"] = (
            agg[f"GRAD17N{group}"] / agg[f"GRAD17C{group}"]
        )  # graduation

    cols = (
        [geography]
        + [f"E38PRFP{group}" for group in RACIAL_GROUPS]
        + [f"M38PRFP{group}" for group in RACIAL_GROUPS]
        + [f"GRAD17P{group}" for group in RACIAL_GROUPS]
    )

    result = agg[cols].set_index(geography).apply(lambda x: x * 100).round(2)

    rename_fields(result, geography)

    return result


def rename_fields(df: pd.DataFrame, geography: str):
    race_rename = {
        "ALL": "",
        "ASN": "anh_",
        "BLK": "bnh_",
        "HIS": "hsp_",
        "OTH": "onh_",
        "WHT": "wnh_",
    }

    for group in RACIAL_GROUPS:
        df.rename(
            columns={
                f"E38PRFP{group}": f"edu_ela_{race_rename[group]}pct",
                f"M38PRFP{group}": f"edu_math_{race_rename[group]}pct",
                f"GRAD17P{group}": f"edu_graduation_{race_rename[group]}pct",
            },
            inplace=True,
        )

    return None


def get_education_outcome(
    geography: str, write_to_internal_review=False
) -> pd.DataFrame:
    puma_cross = pd.read_excel(
        SOURCE_DATA_URL_PUMA_CROSS,
        sheet_name="NTA in PUMA_",
        header=6,
        dtype=str,
    )
    # puma cross reformatting
    puma_cross.columns = puma_cross.columns.str.replace(" \n", "")
    puma_cross = puma_cross.loc[
        ~puma_cross.NTACode.isin(["BX99", "BK99", "MN99", "QN99"])
    ]
    puma_cross["PUMACode"] = puma_cross["PUMACode"].apply(lambda x: "0" + x)

    # Read in source and do some cleanning and merging with puma cross walk
    raw_edu_outcome = read_from_excel(
        SOURCE_DATA_PATH_EDU_OUTCOME,
        category="quality_of_life",
        sheet_name="5_StudentPerformance",
        header=1,
    )

    raw_edu_outcome.fillna(value=0, inplace=True)
    raw_edu_outcome_puma = raw_edu_outcome.merge(
        puma_cross[["NTACode", "PUMACode"]], how="left", on="NTACode"
    )
    raw_edu_outcome_puma.rename(columns={"PUMACode": "puma"}, inplace=True)
    raw_edu_outcome_puma["borough"] = raw_edu_outcome_puma.NTACode.str[:2]
    raw_edu_outcome_puma["citywide"] = "citywide"

    result = calculate_edu_outcome(raw_edu_outcome_puma, geography)

    if write_to_internal_review:
        set_internal_review_files(
            [(result, "education_outcome.csv", geography)], category="quality_of_life"
        )

    return result
