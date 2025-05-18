from dcpy.utils.logging import logger
import pandas as pd

from aggregate.clean_aggregated import rename_columns_demo
from aggregate.aggregation_helpers import (
    demographic_indicators_denom,
    order_aggregated_columns,
    get_category,
)
from aggregate.load_aggregated import load_acs
from aggregate.PUMS.pums_2000_demographics import pums_2000_demographics
from internal_review.set_internal_review_file import set_internal_review_files
from utils.PUMA_helpers import dcp_pop_races, acs_years


def acs_pums_demographics(
    geography: str, year: str = acs_years[-1], write_to_internal_review=False
) -> pd.DataFrame:
    logger.info(f"Running acs_pums_demographics: {geography}, {year}")
    if year == "2000":
        return pums_2000_demographics(
            geography, write_to_internal_review=write_to_internal_review
        )
    assert geography in ["citywide", "borough", "puma"]
    assert year in acs_years

    categories = {
        "LEP": ["lep"],
        "foreign_born": ["fb"],
        "age_bucket": get_category("age_bucket"),
        "total_pop": ["pop_denom"],
        "age_p5pl": ["age_p5pl"],
        "race": dcp_pop_races,
    }

    acs = pd.DataFrame(load_acs(year).loc[geography].rename_axis(geography))
    renamed = rename_columns_demo(acs, year[2:])
    final = order_aggregated_columns(
        df=renamed,
        indicators_denom=demographic_indicators_denom,
        categories=categories,
        household=False,
        exclude_denom=True,
        demographics_category=True,
    )

    if write_to_internal_review:
        set_internal_review_files(
            [(final, f"ACS_PUMS_demographics_{year}.csv", geography)],
            "demographics",
        )
    return final
