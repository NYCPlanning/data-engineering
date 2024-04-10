import pandas as pd
from aggregate.clean_aggregated import (
    rename_col_housing_security,
    order_PUMS_QOL_multiple_years,
)
from utils.dcp_population_excel_helpers import race_suffix_mapper
from utils.PUMA_helpers import acs_years
from internal_review.set_internal_review_file import set_internal_review_files
from aggregate.load_aggregated import load_clean_housing_security_pop_data
from aggregate.aggregation_helpers import get_geography_pop_data


def homevalue_median(geography: str, start_year=acs_years[0], end_year=acs_years[-1], write_to_internal_review=False) -> pd.DataFrame:

    name_mapper = {"MdVl": "homevalue_median"}

    clean_data = load_clean_housing_security_pop_data(name_mapper, start_year, end_year)

    final = get_geography_pop_data(
        clean_data=clean_data, geography=geography
    )

    final = rename_col_housing_security(
        final, name_mapper, race_suffix_mapper, "median"
    )

    final.dropna(axis=1, how="all", inplace=True)

    col_order = order_PUMS_QOL_multiple_years(
        categories=["homevalue_median"],
        measures=["median", "median_moe", "median_cv"],
        years=[start_year, end_year],
    )

    final = final.reindex(columns=col_order)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "homevalue_median.csv", geography)],
            "housing_security",
        )

    return final
