import pytest

from dcpy.product_metadata.models.metadata import org as md


@pytest.mark.parametrize(
    ("filters", "expected_destination_paths", "description"),
    [
        (
            {
                "product_ids": {"test_dcpy"},
                "dataset_ids": {"test_overrides"},
                "destination_filter": {"types": {"open_data", "bytes"}},
            },
            [
                "test_dcpy.test_overrides.bytes",
                "test_dcpy.test_overrides.socrata",
            ],
            "Filtering for test_dcpy product with multiple types",
        ),
        (
            {"destination_filter": {"tags": {"test_tag"}}},
            ["test_dcpy.test_overrides.socrata"],
            "Filtering with just tags",
        ),
        (
            {"dataset_ids": {"test_overrides"}},
            [
                "test_dcpy.test_overrides.bytes",
                "test_dcpy.test_overrides.socrata",
            ],
            "Filtering with dataset_ids",
        ),
    ],
)
def test_query_destinations(
    org_md: md.OrgMetadata, filters, expected_destination_paths, description
):
    """Test querying destinations with various filters."""
    result = org_md.query_product_dataset_destinations(**filters)
    assert sorted(result) == sorted(expected_destination_paths), (
        f"Failure: {description}"
    )


def test_query_destinations_no_filters(test_dcpy_product):
    """Test that querying with no filters returns all destinations."""
    all_dataset_destination_paths = sorted(
        [dest["destination_path"] for dest in test_dcpy_product.all_destinations()]
    )

    assert (
        all_dataset_destination_paths == test_dcpy_product.query_dataset_destinations()
    ), "If no truthy filters are passed, all destinations should be returned"
