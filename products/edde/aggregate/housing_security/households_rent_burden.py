import pandas as pd
from aggregate.clean_aggregated import (
    rename_col_housing_security,
    order_PUMS_QOL_multiple_years,
)
from utils.dcp_population_excel_helpers import (
    race_suffix_mapper,
    count_suffix_mapper_global,
)
from utils.geo_helpers import acs_years
from internal_review.set_internal_review_file import set_internal_review_files
from aggregate.load_aggregated import load_clean_housing_security_pop_data
from aggregate.aggregation_helpers import get_geography_pop_data


def households_rent_burden(
    geography: str,
    start_year=acs_years[0],
    end_year=acs_years[-1],
    write_to_internal_review=False,
) -> pd.DataFrame:
    name_mapper = {
        "GRPI30": "households_rb",
        "GRPI50": "households_erb",
        "OHURt": "households_grapi",
    }

    clean_data = load_clean_housing_security_pop_data(name_mapper, start_year, end_year)

    final = get_geography_pop_data(clean_data=clean_data, geography=geography)

    final = rename_col_housing_security(final, name_mapper, race_suffix_mapper, "count")

    col_order = order_PUMS_QOL_multiple_years(
        categories=["households_rb", "households_erb", "households_grapi"],
        measures=count_suffix_mapper_global.values(),
        years=[start_year, end_year],
    )

    final = final.reindex(columns=col_order)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "households_rent_burden.csv", geography)],
            "housing_security",
        )

    return final
