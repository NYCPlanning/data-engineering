import pandas as pd

from ingest.ingestion_helpers import read_from_excel
from utils.CD_helpers import community_district_to_PUMA, borough_name_mapper
from internal_review.set_internal_review_file import set_internal_review_files

SOURCE_DATA_FILE = "resources/quality_of_life/diabetes_self_report/diabetes_self_report_processed_2023.xlsx"
CATEGORY = "quality_of_life"
SOURCE_SHEET_NAME = "DCHP_Diabetes_SelfRepHealth"
SOURCE_INDICATOR_COLUMNS = {
    "diabetes": "A:C, J:M",
    "self_reported": "A:G",
}
COLUMN_MAPPINGS = {
    "cd_number": "ID",
    "percent_diabetes": "Diabetes",
    "percent_self_report": "Self_Rep_Health",
    "ci_lower": "lower_95CL",
    "ci_upper": "upper_95CL",
}


def health_diabetes(geography: str, write_to_internal_review=False):
    clean_df = load_clean_source_data(geography, "diabetes")
    clean_df["pct"] = clean_df[COLUMN_MAPPINGS["percent_diabetes"]]

    clean_df["lower_pct_moe"] = (
        clean_df[f'{COLUMN_MAPPINGS["percent_diabetes"]}_{COLUMN_MAPPINGS["ci_lower"]}']
        - clean_df["pct"]
    )
    clean_df["upper_pct_moe"] = (
        clean_df[f'{COLUMN_MAPPINGS["percent_diabetes"]}_{COLUMN_MAPPINGS["ci_upper"]}']
        - clean_df["pct"]
    )

    final = clean_df[["pct", "lower_pct_moe", "upper_pct_moe"]].round(2)
    final.columns = ["health_diabetes_" + x for x in final.columns]

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "health_diabetes.csv", geography)],
            category="quality_of_life",
        )
    return final


def health_self_reported(geography: str, write_to_internal_review=False):
    clean_df = load_clean_source_data(geography, "self_reported")
    clean_df["pct"] = clean_df[COLUMN_MAPPINGS["percent_self_report"]]

    clean_df["lower_pct_moe"] = (
        clean_df[
            f'{COLUMN_MAPPINGS["percent_self_report"]}_{COLUMN_MAPPINGS["ci_lower"]}'
        ]
        - clean_df["pct"]
    )
    clean_df["upper_pct_moe"] = (
        clean_df[
            f'{COLUMN_MAPPINGS["percent_self_report"]}_{COLUMN_MAPPINGS["ci_upper"]}'
        ]
        - clean_df["pct"]
    )

    final = clean_df[["pct", "lower_pct_moe", "upper_pct_moe"]].round(2)
    final.columns = ["health_selfreportedhealth_" + x for x in final.columns]

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "health_selfreportedhealth.csv", geography)],
            category="quality_of_life",
        )
    return final


def load_clean_source_data(geography: str, indicator: str):
    assert geography in ["citywide", "borough", "puma"]

    df = read_from_excel(
        file_path=SOURCE_DATA_FILE, category=CATEGORY, sheet_name=SOURCE_SHEET_NAME
    )

    if geography == "puma":
        boro = {"2": "BX", "3": "BK", "1": "MN", "4": "QN", "5": "SI"}

        df = df[df["Borough"].isin(list(borough_name_mapper.keys()))]

        df["CD Code"] = df[COLUMN_MAPPINGS["cd_number"]].astype(str).str[0].map(
            boro
        ) + df[COLUMN_MAPPINGS["cd_number"]].astype(str).str[-2:].astype(int).astype(
            str
        )
        df = community_district_to_PUMA(df, CD_col="CD Code")
        df.drop_duplicates(subset=["puma"], keep="first", inplace=True)
    elif geography == "borough":
        df = df[df["Borough"] == "NYC"]
        df["borough"] = df["Name"].str.strip().map(borough_name_mapper)
    else:
        df = df[df["Borough"] == "City"]
        df["citywide"] = "citywide"

    clean_df = df.set_index(geography)

    return clean_df
