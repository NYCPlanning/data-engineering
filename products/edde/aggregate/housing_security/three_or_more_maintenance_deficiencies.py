from ingest.load_data import load_HVS
from aggregate.race_assign import HVS_race_assign
from statistical.calculate_counts import calculate_counts
from ingest.clean_replicate_weights import rw_cols_clean
from internal_review.set_internal_review_file import set_internal_review_files
from aggregate.load_aggregated import initialize_dataframe_geo_index

implemeted_years = [2017]
implemented_geographies = ["borough", "puma", "citywide"]


def count_units_three_or_more_deficiencies(
    geography_level, year=2017, crosstab_by_race=False, requery=False
):
    """Main accessor"""
    raise NotImplementedError(
        "outdated, this indicator comes from rent_stable_three_maintenance.py"
    )
    if year not in implemeted_years:
        raise Exception(
            f"HVS ingestion for {year} survey not implemented yet. \
            Aggregation be run on surveys from following years {implemeted_years}"
        )
    if geography_level not in implemented_geographies:
        raise Exception(
            f"Aggregation on {geography_level} not allowed. Geographies to aggregate on: {implemented_geographies}"
        )
    if geography_level in ["puma", "citywide"]:
        return initialize_dataframe_geo_index(
            geography=geography_level, columns=["three_plus_maintence_deficencies"]
        )
    HVS = load_HVS(requery=requery, year=year, human_readable=True)
    HVS["three_plus_deficiencies"] = (
        ## commenting just to log - this functionality is no longer used but in case we ever return, this logic is wrong
        ## per https://www2.census.gov/programs-surveys/nychvs/technical-documentation/record-layouts/2017/occupied-units-17.pdf
        ## this should be >= 4
        HVS["Number of 2017 maintenance deficiencies"]
        >= 3
    ).replace({True: "3 or more", False: "Less than 3"})
    if crosstab_by_race:
        HVS["race"] = HVS["Race and Ethnicity of householder"].apply(HVS_race_assign)

    if crosstab_by_race:
        crosstab = "race"
    else:
        crosstab = None
    aggregated = calculate_counts(
        HVS,
        variable_col="three_plus_deficiencies",
        rw_cols=rw_cols_clean,
        weight_col="Household weight",
        geo_col=geography_level,
        crosstab=crosstab,
        keep_SE=False,
        add_MOE=True,
    )
    # aggregated = sort_columns(aggregated)
    aggregated.name = (
        f"HVS_{year}_{geography_level}_crosstab_by_race_{crosstab_by_race}"
    )
    set_internal_review_files([aggregated])
    return aggregated
