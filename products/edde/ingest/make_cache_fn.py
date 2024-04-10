""""""


def make_PUMS_cache_fn(
    year: int, variable_types=None, limited_PUMA=False, include_rw=False
):
    fn = f'PUMS_{"_".join(variable_types)}'
    fn = f"{fn}_{year}"
    if limited_PUMA:
        fn += "_limitedPUMA"
    if not include_rw:
        fn += "_noRepWeights"
    return f"data/{fn}.pkl"


def make_HVS_cache_fn(year: 2017, human_readable=True, output_type=".pkl"):
    rv = f"data/HVS_data_{year}"
    if human_readable:
        rv = f"{rv}_human_readable"

    return f"{rv}{output_type}"
