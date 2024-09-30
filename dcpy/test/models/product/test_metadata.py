from pathlib import Path
import pytest

from dcpy.models.product import metadata as md


@pytest.fixture
def test_metadata_repo(resources_path: Path):
    return resources_path / "test_product_metadata_repo"


@pytest.fixture
def lion_md_path(test_metadata_repo: Path):
    return test_metadata_repo / "products" / "lion"


lion_product_level_pub_freq = "monthly"
pseudo_lots_pub_freq = "bimonthly"
template_vars = {
    "version": "24c",
    "lion_prod_level_pub_freq": lion_product_level_pub_freq,
    "pseudo_lots_pub_freq": pseudo_lots_pub_freq,
}


def test_dataset_md_overrides(lion_md_path: Path):
    product_folder = md.ProductFolder(
        root_path=lion_md_path, template_vars=template_vars
    )
    lion_md = product_folder.get_product_metadata()
    pseudo_lots_with_defaults = product_folder.get_product_dataset(
        "pseudo_lots", product_metadata=lion_md
    )

    # Display name is both a product and dataset field. However, the product.display_name
    # should not act as a default for the dataset
    assert (
        pseudo_lots_with_defaults.attributes.display_name
        != lion_md.attributes.display_name
    ), "Product-level attrbite `display_name` should not be overridden."

    assert (
        pseudo_lots_with_defaults.attributes.publishing_frequency
        == pseudo_lots_pub_freq
    ), "Pseudo lots pub-freq should not be effected by the defaults."

    assert (
        pseudo_lots_with_defaults.attributes.publishing_frequency
        == pseudo_lots_pub_freq
    ), "Pseudo lots pub-freq should not be effected by the defaults."

    assert (
        pseudo_lots_with_defaults.attributes.publishing_purpose
        == lion_md.attributes.publishing_purpose
    ), "The missing field `publishing_purpose` should use the product-level default"


def test_get_tagged_destinations(lion_md_path: Path):
    product_folder = md.ProductFolder(root_path=lion_md_path)

    TAG = "prod_tag"
    datasets = product_folder.get_tagged_destinations(TAG)

    assert 1 == len(datasets.keys())
    assert "school_districts" in datasets
    assert datasets["school_districts"].keys() == {"socrata"}


def test_product_metadata_validation(lion_md_path: Path):
    lion_product = md.ProductFolder(root_path=lion_md_path)
    assert lion_product.validate_dataset_metadata() == []


def test_product_validation_happy_path(test_metadata_repo: Path):
    repo = md.Repo.from_path(test_metadata_repo)
    validation = repo.validate_metadata()
    assert validation == [], "No errors should have been found"
