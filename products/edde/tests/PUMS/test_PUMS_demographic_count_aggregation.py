"""Doing aggregation is runtime-intensive so all tests use same aggregator object"""

import pytest
from tests.PUMS.local_loader import LocalLoader
from tests.util import age_bucket_counts, race_counts

local_loader = LocalLoader()


@pytest.mark.test_aggregation
@pytest.mark.test_new_crosstabs
def test_local_loader(all_data):
    """This code to take all_data arg from command line and get the corresponding data has to be put in test because of how pytest works.
    This test exists for the sake of passing all_data arg from command line to local loader, it DOESN'T test anything
    """
    local_loader.load_count_aggregator(all_data)


@pytest.mark.test_aggregation
@pytest.mark.test_new_crosstabs
def test_all_counts_sum_to_total_pop():
    aggregator = local_loader.count_aggregator
    for ind in aggregator.indicators_denom:
        assert (
            aggregator.aggregated[
                [f"{category}-count" for category in aggregator.categories[ind]]
            ].sum(axis=1)
            == aggregator.aggregated["total_pop-count"]
        ).all()


@pytest.mark.test_aggregation
def test_that_all_races_sum_to_total_within_indicator():
    """Parameterize this to look at nativity, age buckets as well"""
    lep_race_cols = [
        f"lep-{r}-count" for r in race_counts
    ]  # Generate this from function
    assert (
        local_loader.aggregated[lep_race_cols].sum(axis=1)
        == local_loader.aggregated["lep-count"]
    ).all()


@pytest.mark.test_aggregation
def test_that_all_races_sum_to_total():
    assert (
        local_loader.aggregated[race_counts].sum(axis=1)
        == local_loader.aggregated["total_pop-count"]
    ).all()


@pytest.mark.test_aggregation
def test_that_all_age_buckets_sum_to_total():
    assert (
        local_loader.aggregated[age_bucket_counts].sum(axis=1)
        == local_loader.aggregated["total_pop-count"]
    ).all()


@pytest.mark.test_aggregation
def test_total_counts_match():
    citywide_gb = local_loader.by_person.groupby("age_bucket_by_race").agg(
        {"PWGTP": "sum"}
    )
    for variable in citywide_gb.index:
        assert (
            citywide_gb.at[variable, "PWGTP"]
            == local_loader.aggregated[f"{variable}-count"].sum()
        )


@pytest.mark.test_aggregation
def test_age_bucket_assignment_correct():
    """95 is top coded value for age"""
    by_person_data = local_loader.by_person
    assert min(by_person_data[by_person_data["age_bucket"] == "PopU16"]["AGEP"]) == 0
    assert max(by_person_data[by_person_data["age_bucket"] == "PopU16"]["AGEP"]) == 15
    assert min(by_person_data[by_person_data["age_bucket"] == "P16t64"]["AGEP"]) == 16
    assert max(by_person_data[by_person_data["age_bucket"] == "P16t64"]["AGEP"]) == 64
    assert min(by_person_data[by_person_data["age_bucket"] == "P65pl"]["AGEP"]) == 65
    assert max(by_person_data[by_person_data["age_bucket"] == "P65pl"]["AGEP"]) == 95


@pytest.mark.test_aggregation
def test_that_fb_correctly_assigned():
    by_person_data = local_loader.by_person
    assert (
        by_person_data[by_person_data["NATIVITY"] == "Native"]["foreign_born"]
        .isna()
        .all()
    )
    assert (
        by_person_data[by_person_data["NATIVITY"] == "Foreign born"]["foreign_born"]
        == "fb"
    ).all()


TEST_CASES = [
    ("20150000144861", "fb_wnh"),
    ("2019HU13853441", "fb_hsp"),
    ("2019HU12654531", "fb_anh"),
    ("20160002802551", "fb_onh"),
    ("2019HU12564633", "fb_bnh"),
]


@pytest.mark.test_aggregation
@pytest.mark.parametrize("person_id, expected_val", TEST_CASES)
def test_that_fb_by_race_correctly_assigned(person_id, expected_val):
    assert local_loader.by_person.loc[person_id]["foreign_born_by_race"] == expected_val


@pytest.mark.test_aggregation
def test_that_native_born_no_fb_by_race():
    by_person_data = local_loader.by_person
    assert (
        by_person_data[by_person_data["NATIVITY"] == "Native"]["foreign_born_by_race"]
        .isna()
        .all()
    )


TEST_RECORDS = [
    ("20150001199221", "lep_hsp"),
    ("20150000267661", "lep_wnh"),
    ("20150002963751", "lep_bnh"),
    ("20150003090011", "lep_anh"),
    ("20160006078411", "lep_onh"),
]


@pytest.mark.test_aggregation
@pytest.mark.parametrize("record_id, expected_val", TEST_RECORDS)
def test_that_LEP_by_race_correctly_assigned(record_id, expected_val):
    assert local_loader.by_person.loc[record_id]["LEP_by_race"] == expected_val
