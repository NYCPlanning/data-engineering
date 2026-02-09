"""Use process like
aggregator.aggregated[[f'{r}-fraction' for r in aggregator.categories['race']]].sum(axis=1)
should all be close to one for each indicator. categories attribute is dictionary created for this purpose
Note that changes from fraction to pct will break these tests

"""

import itertools

import numpy as np
import pytest
from tests.PUMS.local_loader import LocalLoader

local_loader = LocalLoader()


@pytest.mark.test_aggregation
def test_local_loader(all_data):
    """This code to take all_data arg from command line and get the corresponding data has to be put in test because of how pytest works.
    This test exists for the sake of passing all_data arg from command line to local loader, it DOESN'T test anything
    """
    local_loader.load_count_aggregator(all_data, variance_measure="SE")


@pytest.mark.test_aggregation
def test_all_fractions_sum_to_one():
    aggregator = local_loader.count_aggregator
    for ind in aggregator.indicators_denom:
        assert np.isclose(
            aggregator.aggregated[[f"{r}-pct" for r in aggregator.categories[ind]]].sum(
                axis=1
            ),
            1,
        ).all()


@pytest.mark.test_aggregation
def test_total_pop_one_no_se():
    aggregator = local_loader.count_aggregator
    assert (aggregator.aggregated["total_pop-pct"] == 1).all()
    assert (aggregator.aggregated["total_pop-pct-se"] == 0).all()


@pytest.mark.test_aggregation
def test_crosstabs_sum_to_one():
    aggregator = local_loader.count_aggregator
    for ind, ct in itertools.product(aggregator.indicators_denom, aggregator.crosstabs):
        for ct_category in aggregator.categories[ct]:
            ct_columns = [
                f"{ind_cat}-{ct_category}-pct" for ind_cat in aggregator.categories[ind]
            ]
            print(aggregator.aggregated[ct_columns])
            print()
            assert np.isclose(
                aggregator.aggregated[ct_columns].sum(axis=1),
                1,
            ).all()
