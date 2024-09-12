"""Aggregation for this indicator is unusual in that some records have borough
but no CD. Something to watch out for when testing"""

from aggregate.load_aggregated import initialize_dataframe_geo_index
from ingest.housing_security.DHS_shelter import load_DHS_shelter
from internal_review.set_internal_review_file import set_internal_review_files
from utils.CD_helpers import community_district_to_PUMA
from utils.PUMA_helpers import borough_name_mapper


def DHS_shelter(geography, write_to_internal_review=False):
    final = initialize_dataframe_geo_index(geography)
    for year in [2020, 2022]:
        single_year = DHS_shelter_single_year(geography, year)
        final = final.merge(single_year, left_index=True, right_index=True)
    if write_to_internal_review:
        set_internal_review_files(
            [(final, f"DHS_shelter_{year}.csv", geography)],
            "housing_security",
        )
    return final


def DHS_shelter_single_year(geography, year, write_to_internal_review=False):
    """Main accessor"""
    source_data = load_DHS_shelter(year)
    source_data["citywide"] = "citywide"
    source_data["borough"] = source_data["borough"].map(borough_name_mapper)
    source_data["CD_code"] = source_data["borough"] + source_data[
        "community_district"
    ].astype(str)

    source_data = community_district_to_PUMA(
        source_data, "CD_code", CD_abbr_type="alpha_borough"
    )
    source_data["individuals"] = source_data["individuals"].astype(float)
    single_year = source_data.groupby(geography).sum()[["individuals"]]
    single_year.rename(
        columns={"individuals": f"dhs_shelter_{year}_count"}, inplace=True
    )
    if write_to_internal_review:
        set_internal_review_files(
            [(single_year, f"DHS_shelter_single_year_{year}.csv", geography)],
            "housing_security",
        )
    return single_year
