from dcpy.product_metadata.models.metadata import org as md


def test_org_md_overrides(test_overrides_dataset, test_dcpy_product, org_md):
    """Test that dataset attributes override product and org defaults correctly."""
    # Display name is both a product and dataset field. However, the product.display_name
    # should not act as a default for the dataset
    assert (
        test_overrides_dataset.attributes.display_name
        != test_dcpy_product.metadata.attributes.display_name
    ), "Product-level attribute `display_name` should not be overridden."

    assert (
        test_overrides_dataset.attributes.publishing_frequency
        == "Test Dataset Frequency"
    ), "Dataset pub-freq should not be affected by the product defaults."

    assert (
        test_overrides_dataset.attributes.publishing_purpose
        == test_dcpy_product.metadata.dataset_defaults.publishing_purpose
    ), "The missing field `publishing_purpose` should use the product-level default"

    assert (
        test_overrides_dataset.attributes.agency == "Department of City Planning (DCP)"
    ), "The field `agency` should use the org-level default"


def test_current_version_overrides(test_dcpy_product, test_overrides_dataset):
    """Test that current_version can be overridden at org, product, dataset, and destination levels."""

    # Test dataset with dataset-level override
    assert (
        test_overrides_dataset.attributes.current_version == "dataset-override-3.0"
    ), "Dataset-level current_version should override product default"

    # Test destination with destination-level override
    socrata_dest = test_overrides_dataset.get_destination("socrata")
    assert socrata_dest.current_version == "destination-override-4.0", (
        "Destination should have its own current_version override"
    )

    # Test destination without override (should use dataset version via method)
    bytes_dest = test_overrides_dataset.get_destination("bytes")
    assert bytes_dest.current_version == "", (
        "Destination without override should have empty string"
    )


def test_get_all_destination_current_versions(org_md: md.OrgMetadata):
    """Test that get_all_destination_current_versions returns correct sorted list."""
    versions = org_md.get_all_destination_current_versions()

    # Should be sorted
    assert versions == sorted(versions), "Result should be sorted"

    # Check that specific entries exist with correct version resolution for test_dcpy
    assert "test_dcpy.test_overrides.socrata|destination-override-4.0" in versions, (
        "Destination-level override should be in output"
    )
    assert "test_dcpy.test_overrides.bytes|dataset-override-3.0" in versions, (
        "Destination without override should use dataset version"
    )
