from pathlib import Path

import pytest

from dcpy.models import dataset
from dcpy.product_metadata.models.metadata import org as md
from dcpy.product_metadata.models.metadata import product as ds_md


@pytest.fixture
def test_metadata_repo_path(package_and_dist_test_resources):
    return package_and_dist_test_resources.REPO_PATH


@pytest.fixture
def org_md(package_and_dist_test_resources):
    return package_and_dist_test_resources.org_md


def test_missing_product(org_md: md.OrgMetadata):
    NONEXISTENT_PRODUCT = "asdfasdfsadf"

    with pytest.raises(Exception) as e:
        org_md.product(NONEXISTENT_PRODUCT)

    assert str(e.value).startswith(md.OrgMetadata.PRODUCT_NOT_LISTED_ERROR)
    assert NONEXISTENT_PRODUCT in str(e.value), (
        "The error message should mention the missing product"
    )


def test_missing_dataset(org_md: md.OrgMetadata):
    NONEXISTENT_DATASET = "asdfasdfsadf"

    with pytest.raises(Exception) as e:
        org_md.product("lion").dataset(NONEXISTENT_DATASET)

    assert str(e.value).startswith(md.ProductMetadata.DATASET_NOT_LISTED_ERROR)
    assert NONEXISTENT_DATASET in str(e.value), (
        "The error message should mention the missing product"
    )


def test_org_md_overrides(org_md: md.OrgMetadata, package_and_dist_test_resources):
    lion_md = org_md.product("lion")
    pseudo_lots_with_defaults = lion_md.dataset("pseudo_lots")

    # Display name is both a product and dataset field. However, the product.display_name
    # should not act as a default for the dataset
    assert (
        pseudo_lots_with_defaults.attributes.display_name
        != lion_md.metadata.attributes.display_name
    ), "Product-level attribute `display_name` should not be overridden."

    assert (
        pseudo_lots_with_defaults.attributes.publishing_frequency
        == package_and_dist_test_resources.PSEUDO_LOTS_PUB_FREQ
    ), "Pseudo lots pub-freq should not be affected by the product defaults."

    assert (
        pseudo_lots_with_defaults.attributes.publishing_purpose
        == lion_md.metadata.dataset_defaults.publishing_purpose
    ), "The missing field `publishing_purpose` should use the product-level default"

    assert (
        pseudo_lots_with_defaults.attributes.agency
        == package_and_dist_test_resources.AGENCY
    ), "The field `agency` should use the org-level default"


@pytest.mark.parametrize(
    ("filters", "expected_destination_paths", "description"),
    [
        (
            {
                "product_ids": {"lion"},
                "dataset_ids": {"school_districts"},
                "destination_filter": {"types": {"socrata", "socrata_2"}},
            },
            [
                "lion.school_districts.socrata",
                "lion.school_districts.socrata_2",
            ],
            "Filtering for a single product with multiple types",
        ),
        (
            {"destination_filter": {"tags": {"school_districts_tag"}}},
            ["lion.school_districts.socrata_2"],
            "Filtering with just tags",
        ),
        (
            {"dataset_ids": {"pseudo_lots", "school_districts"}},
            [
                "lion.pseudo_lots.garlic_sftp",
                "lion.pseudo_lots.socrata",
                "lion.school_districts.other",
                "lion.school_districts.socrata",
                "lion.school_districts.socrata_2",
            ],
            "Filtering with dataset_ids",
        ),
    ],
)
def test_query_destinations(
    org_md: md.OrgMetadata, filters, expected_destination_paths, description
):
    assert expected_destination_paths == org_md.query_product_dataset_destinations(
        **filters
    ), f"Failure: {description}"


def test_query_destinations_no_filters(org_md: md.OrgMetadata):
    lion = org_md.product("lion")

    all_dataset_destination_paths = sorted(
        [dest["destination_path"] for dest in lion.all_destinations()]
    )

    assert all_dataset_destination_paths == lion.query_dataset_destinations(), (
        "If no truthy filters are passed, all destinations should be returned"
    )


def test_product_metadata_validation_happy_path(org_md: md.OrgMetadata):
    assert not org_md.product("lion").validate_dataset_metadata(), (
        "LION dataset should have no validation issues"
    )


def test_product_validation_happy_path(org_md: md.OrgMetadata):
    validation = org_md.validate_metadata()
    assert validation == {}, "No errors should have been found"


def test_product_validation_with_error_product(
    org_md: md.OrgMetadata, package_and_dist_test_resources
):
    """Tests that validation produces the expected errors."""
    org_md.metadata.products.append(package_and_dist_test_resources.PRODUCT_WITH_ERRORS)

    validation_errors = org_md.validate_metadata()
    assert len(validation_errors.keys()) == 1, (
        "The correct number of products should have errors"
    )

    assert package_and_dist_test_resources.PRODUCT_WITH_ERRORS in validation_errors, (
        "The correct product should have the errors."
    )
    all_dataset_errors = validation_errors[
        package_and_dist_test_resources.PRODUCT_WITH_ERRORS
    ]
    assert len(all_dataset_errors.keys()) == 3, (
        "The product should report the correct num of errors"
    )

    # The following two datasets should have thrown an exception when being
    # instantiated
    instantiaton_error = all_dataset_errors["dataset_with_bad_yaml"][0]
    assert instantiaton_error.startswith(
        md.ERROR_PRODUCT_DATASET_METADATA_INSTANTIATION
    )

    nonexistent_dataset_error = all_dataset_errors["nonexistent_dataset"][0]
    assert nonexistent_dataset_error.startswith(
        md.ERROR_PRODUCT_DATASET_METADATA_INSTANTIATION
    )

    # This column has a missing reference
    reference_error = all_dataset_errors["dataset_with_reference_errors"][0]
    assert reference_error.startswith(ds_md.ERROR_MISSING_COLUMN)


def test_org_get_snippets(test_metadata_repo_path: Path):
    assert md.OrgMetadata.get_string_snippets(test_metadata_repo_path) == {
        "sample_text": "SAMPLE_TEXT"
    }


def test_org_get_column_defaults(test_metadata_repo_path: Path):
    assert md.OrgMetadata.get_column_defaults(test_metadata_repo_path) == {
        ("bbl", "bbl"): ds_md.DatasetColumn(
            id="bbl",
            name="BBL",
            data_type="bbl",
            description="sample bbl description",
            example="1016370141",
        )
    }


def test_product_snippets_applied(org_md: md.OrgMetadata):
    assert (
        org_md.product("colp").metadata.attributes.description
        == "Product description SAMPLE_TEXT"
    )


def test_dataset_snippets_applied(org_md: md.OrgMetadata):
    assert (
        org_md.product("colp").dataset("colp").attributes.description
        == "Dataset description SAMPLE_TEXT"
    )


def test_column_defaults_applied(org_md: md.OrgMetadata):
    colp_ds = org_md.product("colp").dataset("colp")
    assert ds_md.DatasetColumn(
        id="uid",
        name="uid",
        data_type="text",
        data_source="Department of City Planning",
        checks=dataset.Checks(is_primary_key=True),
    ) == colp_ds.get_column("uid"), "The uid column should have had defaults applied"

    assert ds_md.DatasetColumn(
        id="bbl",
        data_type="bbl",
        name="BBL",
        description="sample bbl description",
        example="1016370141",
    ) == colp_ds.get_column("bbl"), "The bbl column should have had defaults applied"


def test_current_version_overrides(org_md: md.OrgMetadata):
    """Test that current_version can be overridden at org, product, dataset, and destination levels."""
    lion_md = org_md.product("lion")

    # Test dataset with dataset-level override
    pseudo_lots = lion_md.dataset("pseudo_lots")
    assert pseudo_lots.attributes.current_version == "dataset-override-3.0", (
        "Dataset-level current_version should override product default"
    )

    # Test destination with destination-level override
    socrata_dest = pseudo_lots.get_destination("socrata")
    assert socrata_dest.current_version == "destination-override-4.0", (
        "Destination should have its own current_version override"
    )

    # Test destination without override (should use dataset version via method)
    garlic_dest = pseudo_lots.get_destination("garlic_sftp")
    assert garlic_dest.current_version == "", (
        "Destination without override should have empty string"
    )

    # Test dataset without dataset-level override (should use product default)
    school_districts = lion_md.dataset("school_districts")
    assert school_districts.attributes.current_version == "product-default-2.0", (
        "Dataset without override should use product-level default"
    )


def test_get_all_destination_current_versions(org_md: md.OrgMetadata):
    """Test that get_all_destination_current_versions returns correct sorted list."""
    # Temporarily filter out the error product since it's designed to fail
    original_products = org_md.metadata.products
    org_md.metadata.products = [
        p for p in original_products if p != "mock_product_with_errors"
    ]

    try:
        versions = org_md.get_all_destination_current_versions()

        # Should be sorted
        assert versions == sorted(versions), "Result should be sorted"

        # Check that specific entries exist with correct version resolution
        assert "lion.pseudo_lots.socrata|destination-override-4.0" in versions, (
            "Destination-level override should be in output"
        )
        assert "lion.pseudo_lots.garlic_sftp|dataset-override-3.0" in versions, (
            "Destination without override should use dataset version"
        )
        assert "lion.school_districts.socrata|product-default-2.0" in versions, (
            "Destination on dataset without override should use product default"
        )
        assert "lion.school_districts.socrata_2|product-default-2.0" in versions, (
            "Another destination should also use product default"
        )
        assert "lion.school_districts.other|product-default-2.0" in versions, (
            "Third destination should also use product default"
        )
    finally:
        # Restore original products list
        org_md.metadata.products = original_products
