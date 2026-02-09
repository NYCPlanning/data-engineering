import pytest
from aggregate.PUMS.economic_indicators import industry_mapper, occupation_mapper
from tests.PUMS.local_loader import LocalLoader

median_loader = LocalLoader()
count_loader = LocalLoader()


def test_local_loader(all_data):
    """This code to take all_data arg from command line and get the corresponding data has to be put in test because of how pytest works.
    This test exists for the sake of passing all_data arg from command line to local loader, it DOESN'T test anything
    """
    median_loader.load_aggregated_medians(all_data, type="economics")
    count_loader.load_aggregated_counts(all_data, "economics")


def test_all_wages_positive():
    aggregated = median_loader.aggregated
    assert (
        aggregated[[c for c in aggregated.columns if "median" == c[-6:]]].min().min()
        > 0
    )


economic_crosstabs = [("occupation", occupation_mapper), ("industry", industry_mapper)]


@pytest.mark.parametrize("indicator, mapper", economic_crosstabs)
def test_no_workers_median_NaN(indicator, mapper):
    counts_aggregated = count_loader.aggregated
    medians_aggregated = median_loader.aggregated
    for k, i in mapper.items():
        for race in ["wnh", "bnh", "anh", "onh", "hsp"]:
            col_name = f"{indicator}-{i.lower()}-{race}-count"
            zero_workers = counts_aggregated.index[counts_aggregated[col_name] == 0]
            if not zero_workers.empty:
                assert (
                    medians_aggregated[f"{indicator}-{i.lower()}-wage-{race}-median"][
                        zero_workers
                    ]
                    .isna()
                    .all()
                )
