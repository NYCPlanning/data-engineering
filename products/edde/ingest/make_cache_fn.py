""""""


def make_HVS_cache_fn(year: 2017, human_readable=True, output_type=".pkl"):
    rv = f"data/HVS_data_{year}"
    if human_readable:
        rv = f"{rv}_human_readable"

    return f"{rv}{output_type}"
