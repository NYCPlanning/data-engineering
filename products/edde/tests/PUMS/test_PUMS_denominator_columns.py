"""Each fraction comes with a denominator column that has the denominator used in the
calculation. Some indicators such as limited english proficiency have a custom denominator
Note that changes from fraction to pct will break these tests
"""

import pytest
from tests.PUMS.local_loader import LocalLoader
from tests.util import age_buckets, races

demographic_count_loader = LocalLoader()


def test_local_loader(all_data):
    demographic_count_loader.load_aggregated_counts(
        all_data=all_data, type="demographics", add_MOE=True, keep_SE=False
    )


indicator_categories = ["lep", "fb", "total_pop"] + age_buckets


@pytest.mark.parametrize("indicator", indicator_categories)
def test_count_over_denom_is_fraction(indicator):
    """For each data point, count over denom should equal fraction"""
    aggregated = demographic_count_loader.aggregated
    assert (
        aggregated[f"{indicator}-count"] / aggregated[f"{indicator}-pct-denom"]
        == aggregated[f"{indicator}-pct"]
    ).all()


@pytest.mark.parametrize("indicator", ["fb"] + age_buckets)
def test_default_denom_total_pop(indicator):
    """For indicators with no custom denominator, the denominator should be total pop"""
    aggregated = demographic_count_loader.aggregated
    assert (aggregated[f"{indicator}-pct-denom"] == aggregated["total_pop-count"]).all()


def test_LEP_denom_less_than_total_pop():
    aggregated = demographic_count_loader.aggregated
    assert (aggregated["lep-pct-denom"] < aggregated["total_pop-count"]).all()


def test_LEP_denom_is_pop_over_age_four():
    aggregated = demographic_count_loader.aggregated
    PUMS = demographic_count_loader.by_person
    over_age_four = PUMS[PUMS["AGEP"] >= 5]
    over_age_four_by_PUMA = over_age_four.groupby("PUMA").sum()["PWGTP"]
    pop_by_puma = PUMS.groupby("PUMA").sum()["PWGTP"]

    pop_by_puma = pop_by_puma.sort_index()
    aggregated = aggregated.sort_index()

    assert (pop_by_puma == aggregated["total_pop-count"]).all()

    assert (
        (
            (over_age_four_by_PUMA / pop_by_puma) * aggregated["total_pop-count"]
        ).sort_index()
        == aggregated["lep-fraction-denom"].sort_index()
    ).all()


@pytest.mark.parametrize("indicator", ["fb", "total_pop"] + age_buckets)
def tests_race_crosstab_denom_is_total_people_in_race_group(indicator):
    """Crosstabs fractions are percentage of people in each race group that fall within
    category"""
    aggregated = demographic_count_loader.aggregated
    for race in races:
        assert (
            aggregated[f"{indicator}-{race}-pct-denom"]
            == aggregated[f"total_pop-{race}-count"]
        ).all()
