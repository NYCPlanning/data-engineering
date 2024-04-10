import pandas as pd
from aggregate.clean_aggregated import (
    rename_col_housing_security,
    order_PUMS_QOL_multiple_years,
)
from utils.dcp_population_excel_helpers import (
    race_suffix_mapper,
    count_suffix_mapper_global,
)
from utils.PUMA_helpers import acs_years
from internal_review.set_internal_review_file import set_internal_review_files
from aggregate.load_aggregated import load_clean_housing_security_pop_data
from aggregate.aggregation_helpers import get_geography_pop_data


def units_overcrowd(
    geography: str,
    start_year=acs_years[0],
    end_year=acs_years[-1],
    write_to_internal_review=False,
) -> pd.DataFrame:
    """denom units occupied not included because the denom would come from other housing tenure indicator"""
    name_mapper = {
        "OcR1p": "units_overcrowded",
        "OcRU1": "units_notovercrowded",
    }

    clean_data = load_clean_housing_security_pop_data(name_mapper, start_year, end_year)

    final = get_geography_pop_data(clean_data=clean_data, geography=geography)

    final = rename_col_housing_security(final, name_mapper, race_suffix_mapper, "count")

    col_order = order_PUMS_QOL_multiple_years(
        categories=["units_overcrowded", "units_notovercrowded"],
        measures=count_suffix_mapper_global.values(),
        years=[start_year, end_year],
    )

    final = final.reindex(columns=col_order)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "units_overcrowd.csv", geography)],
            "housing_security",
        )

    return final
