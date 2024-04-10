import pandas as pd
import numpy as np
from aggregate.clean_aggregated import (
    rename_col_housing_security,
    order_PUMS_QOL_multiple_years,
)
from utils.dcp_population_excel_helpers import race_suffix_mapper, count_suffix_mapper_global
from utils.PUMA_helpers import acs_years
from internal_review.set_internal_review_file import set_internal_review_files
from aggregate.load_aggregated import load_clean_housing_security_pop_data
from aggregate.aggregation_helpers import get_geography_pop_data

def rent_median(geography: str, start_year=acs_years[0], end_year=acs_years[-1], write_to_internal_review=False) -> pd.DataFrame:

    name_mapper_md = {
        "MdGR": "rent_median", 
    }

    name_mapper_hh = {
        "HUPRt": "units_payingrent"
    }

    clean_data_md = load_clean_housing_security_pop_data(name_mapper_md, start_year, end_year)

    clean_data_hh = load_clean_housing_security_pop_data(name_mapper_hh, start_year, end_year)

    final_md = get_geography_pop_data(
        clean_data=clean_data_md, geography=geography
    )
    rename_col_housing_security(
        df=final_md, 
        name_mapper=name_mapper_md, 
        race_mapper=race_suffix_mapper,
        suffix_mode="median"
    )
    final_md = final_md.reindex(
        columns=order_PUMS_QOL_multiple_years(
            categories=["rent_median"],
            measures=["median", "median_moe", "median_cv"],
            years=[start_year, end_year],
        )
    )
    final_hh = get_geography_pop_data(
        clean_data=clean_data_hh, geography=geography
    )
    rename_col_housing_security(
        df=final_hh, 
        name_mapper=name_mapper_hh, 
        race_mapper=race_suffix_mapper,
        suffix_mode="count"
    )

    final_hh = final_hh.reindex(
        columns=order_PUMS_QOL_multiple_years(
            categories=["units_payingrent"],
            measures=count_suffix_mapper_global.values(),
            years=[start_year, end_year],
        )
    )

    final = pd.concat([final_md, final_hh], axis=1)

    final[f"units_payingrent_{end_year}_pct"] = 100
    final[f"units_payingrent_{end_year}_pct_moe"] = 1

    # TODO this currently drops most pct_moe columns. However, its super dangerous - simply removes columns in case of error
    final.dropna(how="all", axis=1, inplace=True)

    final[f"units_payingrent_{end_year}_pct_moe"] = np.nan
    

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "rent_median.csv", geography)],
            "housing_security",
        )

    return final
