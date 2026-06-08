from resources import load
from utils.geo_helpers import borough_num_mapper


def load_filings():
    filings = load("eviction_filings")
    filings.rename(
        columns={"Eviction Fillings*": "eviction_filings_count"}, inplace=True
    )
    filings["citywide"] = "citywide"

    filings["Community District"] = filings["Community District"].astype(str)
    filings["borough"] = filings["Community District"].str[0].map(borough_num_mapper)
    assert False, "Fix below before running"
    # filings = community_district_to_PUMA(
    #     df=filings, CD_col="Community District", CD_abbr_type="numeric_borough"
    # )

    return filings


def eviction_cases(geography: str):
    """Main Accessor"""
    filings = load_filings()
    final = filings.groupby(geography).sum()[["eviction_filings_count"]]
    return final
