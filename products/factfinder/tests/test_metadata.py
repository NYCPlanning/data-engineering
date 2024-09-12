from factfinder.metadata import Metadata

meta = Metadata(year=2019, source="acs")


def test_metadata():
    assert isinstance(meta.metadata, list)


def test_profile_only_variables():
    print("\nprofile only variables: \n", meta.profile_only_variables)
    assert isinstance(meta.profile_only_variables, list)


def test_base_variables():
    print("\nbase variables: \n", meta.base_variables)
    assert isinstance(meta.base_variables, list)


def test_median_variables():
    print("\nmedian variables: \n", meta.median_variables)
    assert isinstance(meta.median_variables, list)


def test_special_variables():
    print("\nspecial variables: \n", meta.special_variables)
    assert isinstance(meta.special_variables, list)


def test_create_variable():
    v = meta.create_variable("pop_1")
    assert v.pff_variable == "pop_1"


def test_create_census_variables():
    v = meta.create_variable("pop_1")
    E_variables, M_variables = v.create_census_variables(v.census_variable)
    assert isinstance(E_variables, list)
    assert isinstance(M_variables, list)
    assert len(E_variables) == len(M_variables)
    assert len(E_variables) == len(v.census_variable)
