from dcpy.models import ds
from dcpy.connectors.edm import qa as edm_qa
from pathlib import Path


def compare(ds1, ds2, comparison) -> Path:
    comparison_path = comparison.run(ds1.download(), ds2.download())
    return comparison_path


def generate_series_comparisons(
    series: ds.DatasetSeries, comparison: ds.DatasetQAScript | ds.DatasetQAModule
):
    existing_comparisons: list[
        ds.DatasetComparisonOutput
    ] = []  # qa.query_comparisons(series)

    # ensure that comparisons exist for the adjacent pairs in the series
    for previous_ds, current_ds in zip(series, series[1:]):
        comparison_key = f"{previous_ds}_{current_ds}_{comparison.name}"
        if comparison_key not in existing_comparisons:
            output_path = compare(previous_ds, current_ds, comparison)
            edm_qa.upload(output_path)
