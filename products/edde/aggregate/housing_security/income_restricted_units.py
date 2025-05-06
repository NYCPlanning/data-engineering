import pandas as pd
from internal_review.set_internal_review_file import set_internal_review_files
from utils.PUMA_helpers import (
    clean_PUMAs,
    filter_for_recognized_pumas,
    puma_to_borough,
    borough_name_mapper,
)

from aggregate.load_aggregated import initialize_dataframe_geo_index
from ingest.ingestion_helpers import load_data


def income_restricted_units(
    geography: str, write_to_internal_review=False
) -> pd.DataFrame:
    """Main accessor"""
    assert geography in ["puma", "borough", "citywide"]

    source_data = load_clean_income_restricted()
    empty_df = initialize_dataframe_geo_index(geography=geography)
    final = source_data.groupby(geography).sum()[["units_nycha_count"]]
    final = pd.concat([empty_df, final], axis=1)
    final.fillna(0, inplace=True)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "income_restricted_units.csv", geography)],
            "housing_security",
        )
    return final


def load_clean_income_restricted():
    source_data = pd.read_excel(
        "resources/housing_security/nycha_tenants/nycha_tenants_processed_2025.xlsx",
        sheet_name="PUMA",
    )
    source_data.rename(columns={"PUMA (2020)": "puma"}, inplace=True)
    source_data["puma"] = source_data["puma"].apply(clean_PUMAs)
    source_data = filter_for_recognized_pumas(source_data)
    source_data["borough"] = source_data.apply(axis=1, func=puma_to_borough)
    source_data["citywide"] = "citywide"

    source_data.rename(
        columns={"Total Unit Count": "units_nycha_count"},
        inplace=True,
    )
    return source_data


def income_restricted_units_hpd(
    geography: str, write_to_internal_review=False
) -> pd.DataFrame:
    """Main accessor"""
    assert geography in ["puma", "borough", "citywide"]

    source_data = load_clean_hpd_data()
    final = source_data.groupby(geography).sum()[["units_hpd_count"]]

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "income_restricted_units_hpd.csv", geography)],
            "housing_security",
        )
    return final


def load_clean_hpd_data():
    source_data = load_data(
        "hpd_hny_units_by_building",
        cols=get_columns(),
    )
    # casting to numeric for calculation
    for c in ["latitude_(internal)", "longitude_(internal)", "all_counted_units"]:
        source_data[c] = pd.to_numeric(source_data[c])

    source_data.rename(
        columns={
            "nta_-_neighborhood_tabulation_area": "nta",
            "all_counted_units": "units_hpd_count",
        },
        inplace=True,
    )
    source_data["borough"] = source_data["borough"].map(borough_name_mapper)
    source_data["citywide"] = "citywide"
    ntas_to_pumas: dict = (
        load_data("dcp_population_nta_puma_crosswalk_2020")
        .set_index("nta_code")
        .to_dict()["puma_code"]
    )
    source_data["puma"] = source_data["nta"].map(ntas_to_pumas)

    # TODO: we can potentially infer the remaining 1600 using the CB
    # source_data["community_board_num"] = source_data["community_board"].apply(
    #     lambda x: x.split("-")[1]
    # )

    return source_data


def get_columns() -> list:
    cols = [
        "project_id",
        "project_name",
        "project_start_date",
        "project_completion_date",
        "number",
        "street",
        "borough",
        "community_board",
        "nta_-_neighborhood_tabulation_area",
        "latitude_(internal)",
        "longitude_(internal)",
        "all_counted_units",
    ]
    return cols
