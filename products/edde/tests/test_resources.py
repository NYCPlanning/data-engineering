"""
Tests for the resources module.

This test validates that all registered resources can be loaded successfully.
"""

import pytest
from resources import RESOURCES, get_resource_info, list_resources, load


def test_list_resources():
    """Test that list_resources returns a non-empty sorted list."""
    resources = list_resources()
    assert len(resources) > 0
    assert resources == sorted(resources)


def test_get_resource_info():
    """Test getting metadata about a resource."""
    info = get_resource_info("2010_census_housing_units_by_2020_nta")
    assert "filepath" in info
    assert "type" in info
    assert info["type"] == "csv"


def test_get_resource_info_invalid():
    """Test that get_resource_info raises KeyError for invalid resource."""
    with pytest.raises(KeyError):
        get_resource_info("nonexistent_resource")


def test_load_invalid_resource():
    """Test that load raises KeyError for invalid resource."""
    with pytest.raises(KeyError) as exc_info:
        load("nonexistent_resource")
    assert "not found" in str(exc_info.value)


@pytest.mark.parametrize("resource_name", list(RESOURCES.keys()))
def test_load_all_resources(resource_name):
    """
    Test that each registered resource can be loaded successfully.

    This parametrized test runs once for each resource in RESOURCES,
    ensuring that:
    1. The file exists and is readable
    2. The loader function executes without error
    3. A pandas DataFrame is returned
    """
    df = load(resource_name)
    assert df is not None
    assert hasattr(df, "columns")  # Basic check that it's a DataFrame
    assert len(df) >= 0  # DataFrame can be loaded (even if empty)


def test_resources_have_required_fields():
    """Test that all resources have the required metadata fields."""
    required_fields = {"filepath", "type", "data_table", "loader"}
    for resource_name, resource_data in RESOURCES.items():
        assert required_fields.issubset(resource_data.keys()), (
            f"Resource '{resource_name}' is missing required fields"
        )
        assert callable(resource_data["loader"]), (
            f"Loader for '{resource_name}' is not callable"
        )


def test_resource_types_are_valid():
    """Test that all resources have valid type values."""
    valid_types = {"csv", "excel"}
    for resource_name, resource_data in RESOURCES.items():
        assert resource_data["type"] in valid_types, (
            f"Resource '{resource_name}' has invalid type: {resource_data['type']}"
        )


def test_resources_have_required_columns_field():
    """Test that all resources have the required_columns field."""
    for resource_name, resource_data in RESOURCES.items():
        assert "required_columns" in resource_data, (
            f"Resource '{resource_name}' is missing 'required_columns' field"
        )
        assert isinstance(resource_data["required_columns"], list), (
            f"Resource '{resource_name}' required_columns must be a list"
        )


@pytest.mark.parametrize("resource_name", list(RESOURCES.keys()))
def test_required_columns_exist(resource_name):
    """
    Test that required columns exist in each loaded resource.

    This test loads each resource and verifies that all columns
    listed in required_columns are present in the DataFrame.
    """
    resource_info = RESOURCES[resource_name]
    required_cols = resource_info.get("required_columns", [])

    if not required_cols:
        pytest.skip(f"No required columns specified for {resource_name}")

    # Try to load the resource
    try:
        df = load(resource_name)
    except FileNotFoundError:
        pytest.skip(f"File not found for {resource_name} (expected for missing files)")

    # Check that all required columns are present
    missing_columns = [col for col in required_cols if col not in df.columns]

    assert not missing_columns, (
        f"Resource '{resource_name}' is missing required columns: {missing_columns}. "
        f"Available columns: {list(df.columns)}"
    )


@pytest.mark.parametrize(
    "resource_name",
    [
        k
        for k in RESOURCES.keys()
        if k.startswith("acs_") or k.startswith("census_2000")
    ],
)
def test_acs_datasets_have_unique_columns(resource_name):
    """
    Test that ACS datasets don't have duplicate column names.

    Duplicate columns in ACS data can cause issues when processing
    economics and demographics data, as column name normalization may
    create unexpected duplicates (e.g., P16t64 vs P16t64y).

    This test ensures the source data is clean and forces source data
    fixes rather than code workarounds.
    """
    try:
        df = load(resource_name)
    except FileNotFoundError:
        pytest.skip(f"File not found for {resource_name}")

    # Check for duplicate column names
    duplicate_cols = df.columns[df.columns.duplicated(keep=False)]

    assert len(duplicate_cols) == 0, (
        f"Resource '{resource_name}' has duplicate columns: "
        f"{sorted(set(duplicate_cols))}. "
        f"This indicates a source data issue that should be fixed upstream. "
        f"Duplicate columns can cause errors when column names are normalized "
        f"in PUMS processing."
    )


@pytest.mark.parametrize(
    "resource_name", [k for k in RESOURCES.keys() if k.startswith("acs_")]
)
def test_acs_datasets_no_semantic_duplicates(resource_name):
    """
    Test that ACS datasets don't have columns that will become duplicates
    after normalization (e.g., P16t64 and P16t64y both normalize to p16t64).

    This catches issues where columns have different names in the source
    but represent the same data and will cause conflicts during processing.
    """
    try:
        df = load(resource_name)
    except FileNotFoundError:
        pytest.skip(f"File not found for {resource_name}")

    # Check for P16t64 vs P16t64y - these normalize to the same thing
    p16t64_cols = [col for col in df.columns if col.startswith("P16t64_")]
    p16t64y_cols = [col for col in df.columns if col.startswith("P16t64y_")]

    if p16t64_cols and p16t64y_cols:
        pytest.fail(
            f"Resource '{resource_name}' contains both P16t64 and P16t64y columns. "
            "Remove one upstream to avoid downstream conflicts or silent drops during processing."
        )
