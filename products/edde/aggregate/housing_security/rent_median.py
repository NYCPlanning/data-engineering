import pandas as pd
import numpy as np
from utils.geo_helpers import acs_years
from internal_review.set_internal_review_file import set_internal_review_files


from aggregate.load_aggregated import ACSAggregator

rent_median_agg = ACSAggregator(
    name="rent_median",
    dcp_base_variables=["rent_median"],
    dcp_base_variables_conf={"include_race": True, "include_year_in_out_col": True},
    internal_review_filename="rent_median.csv",
    internal_review_category="housing_security",
)

units_payingrent_agg = ACSAggregator(
    name="units_payingrent",
    dcp_base_variables=["units_payingrent"],
    dcp_base_variables_conf={"include_race": True, "include_year_in_out_col": True},
    internal_review_filename="units_payingrent.csv",
    internal_review_category="housing_security",
)


# TODO: This is producing some weird weird discrepancies from the original
# The post-processing breaks the pattern... we could just add a
# post-processor to ACSAggregator, but I'm in no hurry
def rent_median(
    geography: str,
    start_year=acs_years[0],
    end_year=acs_years[-1],
    write_to_internal_review=False,
) -> pd.DataFrame:
    final_md = rent_median_agg.run(
        geography,
        start_year,
        end_year,
        write_to_internal_review=False,
    )
    final_hh = units_payingrent_agg.run(
        geography,
        start_year,
        end_year,
        write_to_internal_review=False,
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
