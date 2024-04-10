"""Similar to ingestion process, can be called by load_aggregated or the aggregation
class"""


def PUMS_cache_fn(
    EDDT_category,
    calculation_type,
    year,
    geography,
    by_household=False,
    limited_PUMA=False,
):
    fn = f"{EDDT_category}_{calculation_type}_{year}_{geography}"
    if by_household:
        fn += "_household"
    if limited_PUMA:
        fn += "_limitedPUMA"
    return f"{fn}.csv"
