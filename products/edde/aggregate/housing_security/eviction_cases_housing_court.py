import pandas as pd
from internal_review.set_internal_review_file import set_internal_review_files

from utils.CD_helpers import get_borough_num_mapper


def load_filings():
    filings = pd.read_excel(
        "resources/housing_security/eviction_filings.xlsx", skiprows=4, nrows=59
    )
    filings.rename(
        columns={"Eviction Fillings*": "eviction_filings_count"}, inplace=True
    )
    filings["citywide"] = "citywide"

    filings["Community District"] = filings["Community District"].astype(str)
    filings["borough"] = (
        filings["Community District"].str[0].map(get_borough_num_mapper())
    )
    assert False, "Fix below before running"
    # filings = community_district_to_PUMA(
    #     df=filings, CD_col="Community District", CD_abbr_type="numeric_borough"
    # )

    return filings


def eviction_cases(geography: str, write_to_internal_review=False):
    """Main Accessor"""
    filings = load_filings()
    final = filings.groupby(geography).sum()[["eviction_filings_count"]]
    if write_to_internal_review:
        set_internal_review_files(
            [(final, "eviction_cases.csv", geography)],
            "housing_security",
        )
    return final
