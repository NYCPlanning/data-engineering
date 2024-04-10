""""""

import pytest
from tests.PUMS.local_loader import LocalLoader

#years = [2012, 2019, 2021]
#local_loaders = [LocalLoader(year=year) for year in years]
local_loader_2021 = LocalLoader(year=2021)
local_loader_2019 = LocalLoader(year=2019)
local_loader_2012 = LocalLoader(year=2012)
local_loaders = [local_loader_2012, local_loader_2019, local_loader_2021]

EXPECTED_COLS_VALUES_CATEGORICAL = [
    (
        "SCHL",
        [
            "N/A (less than 3 years old)",
            "No schooling completed",
            "Kindergarten",
            "Grade 9",
            "Regular high school diploma",
        ],
    ),
    (
        "ESR",
        ["N/A (less than 16 years old)", "Unemployed", "Civilian employed, at work"],
    ),
]


@pytest.mark.test_download
def test_local_loader(all_data):
    """This code to take all_data arg from command line and get the corresponding data has to be put in test because of how pytest works.
    This test exists for the sake of passing all_data arg from command line to local loader, it DOESN'T test anything"""
    for loader in local_loaders:
        loader.load_by_person(
            all_data, include_rw=False, variable_set="economics"
        )


@pytest.mark.parametrize("local_loader", local_loaders)
@pytest.mark.test_download
@pytest.mark.parametrize("column, expected_values", EXPECTED_COLS_VALUES_CATEGORICAL)
def test_categorical_columns_have_expected_values(
    column, expected_values, local_loader
):

    assert column in local_loader.by_person.columns
    for ev in expected_values:
        assert ev in local_loader.by_person[column].values


EXPECTED_COLS_VALUES_CONTINOUS = [("HINCP", -60000, 99999999), ("WAGP", -1, 999999)]


@pytest.mark.parametrize("local_loader", local_loaders)
@pytest.mark.test_download
@pytest.mark.parametrize("column, min_val, max_val", EXPECTED_COLS_VALUES_CONTINOUS)
def test_continous_columns_have_expected_values(column, min_val, max_val, local_loader):
    """These tests aren't great"""
    assert column in local_loader.by_person.columns
    assert min(local_loader.by_person[column]) >= min_val
    assert max(local_loader.by_person[column]) <= max_val


EXPECTED_COLS_VALUES_RANGE_CATEGORICAL = [
    ("INDP", 170, "Agriculture, Forestry, Fishing and Hunting, and Mining"),
    ("OCCP", 4040, "Service Occupations"),
]


@pytest.mark.test_download
@pytest.mark.parametrize(
    "column, old_val, new_val", EXPECTED_COLS_VALUES_RANGE_CATEGORICAL
)
def test_categorical_range_variables_have_expected_values(column, old_val, new_val):
    """This test only works for 2019 as 2012 has differently named OCCP"""
    ids = local_loader_2019.by_person_raw[
        local_loader_2019.by_person_raw[column] == old_val
    ].index

    sum(local_loader_2019.by_person.loc[ids][column] == new_val) == len(ids)


@pytest.mark.test_download
@pytest.mark.parametrize(
    "column, old_val, new_val", EXPECTED_COLS_VALUES_RANGE_CATEGORICAL
)
def test_categorical_range_variables_have_expected_values_2021(column, old_val, new_val):
    """See how this goes for 2021"""
    ids = local_loader_2021.by_person_raw[
        local_loader_2021.by_person_raw[column] == old_val
    ].index

    sum(local_loader_2021.by_person.loc[ids][column] == new_val) == len(ids)
