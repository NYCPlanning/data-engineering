"""Aggregation for this indicator is unusual in that some records have borough
but no CD. Something to watch out for when testing"""

from aggregate.load_aggregated import initialize_dataframe_geo_index
from internal_review.set_internal_review_file import set_internal_review_files
from utils.geo_helpers import borough_name_mapper, community_district_to_puma
from ingest import ingestion_helpers

DATASET_NAME = "dhs_shelterd_indiv_by_comm_dist"


def _dhs_shelter_single_year(geography: str, year: str, write_to_internal_review=False):
    raw_source_data = ingestion_helpers.load_data(name=DATASET_NAME)
    # return raw_source_data

    # Previously the edde DHS ingest script would filter down in the api calls.
    # The new ingested version contains all reports, so we must filter here
    report_year_filter = f"06/30/{year}"

    source_data = raw_source_data[
        raw_source_data["report_date"] == report_year_filter
    ].copy()

    source_data["citywide"] = "citywide"
    source_data["borough"] = source_data["borough"].map(borough_name_mapper)

    source_data["puma"] = source_data.apply(
        lambda r: community_district_to_puma(
            r.borough, r.community_district, ignore_errors=True
        ),  # type: ignore
        axis=1,
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


def dhs_shelter(geography, write_to_internal_review=False):
    final = initialize_dataframe_geo_index(geography)

    years = ["2020", "2022"]
    for year in years:
        single_year = _dhs_shelter_single_year(geography, year)
        final = final.merge(single_year, left_index=True, right_index=True)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, f"DHS_shelter_{years[-1]}.csv", geography)],
            "housing_security",
        )
    return final
