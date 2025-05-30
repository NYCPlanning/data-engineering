from ingest.ingestion_helpers import read_from_excel
from utils.geo_helpers import (
    borough_name_mapper,
    community_district_to_puma,
)
from internal_review.set_internal_review_file import set_internal_review_files

SOURCE_DATA_FILE = "resources/quality_of_life/diabetes_self_report/diabetes_self_report_processed_2024.xlsx"
CATEGORY = "quality_of_life"
SOURCE_SHEET_NAME = "DCHP_Diabetes_SelfRepHealth"
COLUMN_MAPPINGS = {
    "cd_number": "ID",
    "percent_diabetes": "Diabetes",
    "percent_self_report": "Self_Rep_Health",
}

HEALTH_DIABETES_PREFIX = "health_diabetes"


def health_diabetes(geography: str, write_to_internal_review=False):
    # Output Columns
    pct_col = f"{HEALTH_DIABETES_PREFIX}_pct"
    lower_95_col = f"{HEALTH_DIABETES_PREFIX}_lower_pct_moe"
    upper_95_col = f"{HEALTH_DIABETES_PREFIX}_upper_pct_moe"
    ordered_numeric_cols = [pct_col, lower_95_col, upper_95_col]

    clean_df = load_clean_source_data(geography).rename(columns={"Diabetes": pct_col})
    clean_df[lower_95_col] = clean_df["Diabetes_lower_95CL"] - clean_df[pct_col]
    clean_df[upper_95_col] = clean_df["Diabetes_upper_95CL"] - clean_df[pct_col]
    final = clean_df[ordered_numeric_cols].round(2)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "health_diabetes.csv", geography)],
            category="quality_of_life",
        )
    return final


HEALTH_SELF_REPORTED_PREFIX = "health_selfreportedhealth"


def health_self_reported(geography: str, write_to_internal_review=False):
    # Output Columns
    pct_col = f"{HEALTH_SELF_REPORTED_PREFIX}_pct"
    lower_pct_col = f"{HEALTH_SELF_REPORTED_PREFIX}_lower_pct_moe"
    upper_pct_col = f"{HEALTH_SELF_REPORTED_PREFIX}_upper_pct_moe"
    ordered_numeric_cols = [pct_col, lower_pct_col, upper_pct_col]

    clean_df = load_clean_source_data(geography).rename(
        columns={"Self_Rep_Health": pct_col}
    )
    clean_df[lower_pct_col] = clean_df["Self_Rep_Health_lower_95CL"] - clean_df[pct_col]
    clean_df[upper_pct_col] = clean_df["Self_Rep_Health_upper_95CL"] - clean_df[pct_col]
    final = clean_df[ordered_numeric_cols].round(2)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "health_selfreportedhealth.csv", geography)],
            category="quality_of_life",
        )
    return final


def load_clean_source_data(geography: str):
    assert geography in ["citywide", "borough", "puma"]

    df = read_from_excel(
        file_path=SOURCE_DATA_FILE, category=CATEGORY, sheet_name=SOURCE_SHEET_NAME
    )
    df["borough"] = df["Borough"].map(borough_name_mapper)

    if geography == "puma":
        df = df[df["geo_type"] == "community_district"]

        df["cd_code"] = df["ID"].astype(str).str[-2:].astype(int)
        df["puma"] = df.apply(
            lambda r: community_district_to_puma(r.borough, r.cd_code), axis=1
        )
        df.drop_duplicates(subset=["puma"], keep="first", inplace=True)
    elif geography == "borough":
        df = df[df["geo_type"] == "borough"]
    else:
        df["citywide"] = "citywide"
        df = df[df["geo_type"] == "citywide"]

    return df.set_index(geography)
