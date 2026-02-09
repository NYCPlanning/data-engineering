"""Three general things to test: does query manager construct correct URL's,
does request module handle errenous status codes correctly, does PUMS data class
clean/collate data correctly"""

import pytest
from tests.PUMS.local_loader import LocalLoader

years = [2012, 2019, 2021]
local_loaders = [LocalLoader(year=year) for year in years]


@pytest.mark.test_download
def test_local_loader(all_data):
    """This code to take all_data arg from command line and get the corresponding data has to be put in test because of how pytest works.
    This test exists for the sake of passing all_data arg from command line to local loader, it DOESN'T test anything
    """
    for loader in local_loaders:
        loader.load_by_person(all_data, variable_set="demographics")


@pytest.mark.parametrize("local_loader", local_loaders)
@pytest.mark.test_download
def test_PUMS_download(all_data: bool, local_loader):
    if all_data:
        assert local_loader.by_person.shape[0] > 3 * (10**5)
    else:
        assert local_loader.by_person.shape[0] > 3 * (10**4)


@pytest.mark.parametrize("local_loader", local_loaders)
@pytest.mark.test_download
def test_PUMS_includes_replicate_weights(local_loader):
    """The full query doesn't work yet so first test limited PUMAs.
    Test that PUMS download gets correct columns"""
    assert "PWGTP" in local_loader.by_person.columns, (
        "Person weights column not present"
    )
    for i in range(1, 81):
        assert f"PWGTP{i}" in local_loader.by_person.columns, (
            f"Replicate weight {i} not present"
        )


@pytest.mark.parametrize("local_loader", local_loaders)
@pytest.mark.test_download
def test_PUMA_column_present(local_loader):
    assert "PUMA" in local_loader.by_person.columns.str.upper(), (
        "PUMA column not present"
    )


@pytest.mark.parametrize("local_loader", local_loaders)
@pytest.mark.test_download
def test_PUMS_data_unique(local_loader):
    assert local_loader.by_person.index.is_unique, "Duplicates in PUMS data"
