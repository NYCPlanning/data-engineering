import pytest

from dcpy.product_metadata.models.metadata import org as md


def test_missing_product(org_md: md.OrgMetadata):
    """Test that accessing a non-existent product raises an error."""
    NONEXISTENT_PRODUCT = "asdfasdfsadf"

    with pytest.raises(Exception) as e:
        org_md.product(NONEXISTENT_PRODUCT)

    assert str(e.value).startswith(md.OrgMetadata.PRODUCT_NOT_LISTED_ERROR)
    assert NONEXISTENT_PRODUCT in str(e.value), (
        "The error message should mention the missing product"
    )


def test_missing_dataset(test_dcpy_product):
    """Test that accessing a non-existent dataset raises an error."""
    NONEXISTENT_DATASET = "asdfasdfsadf"

    with pytest.raises(Exception) as e:
        test_dcpy_product.dataset(NONEXISTENT_DATASET)

    assert str(e.value).startswith(md.ProductMetadata.DATASET_NOT_LISTED_ERROR)
    assert NONEXISTENT_DATASET in str(e.value), (
        "The error message should mention the missing dataset"
    )
