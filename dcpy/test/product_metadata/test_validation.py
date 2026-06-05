from dcpy.product_metadata.models.metadata import org as md


def test_product_metadata_validation_happy_path(test_dcpy_product):
    """Test that test_dcpy product has no validation issues."""
    assert not test_dcpy_product.validate_dataset_metadata(), (
        "test_dcpy product should have no validation issues"
    )


def test_product_validation_happy_path(org_md: md.OrgMetadata):
    """Test that org metadata validation finds no errors."""
    validation = org_md.validate_metadata()
    assert validation == {}, "No errors should have been found"
