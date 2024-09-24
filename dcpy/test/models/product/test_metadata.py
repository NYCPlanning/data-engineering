from pathlib import Path
import pytest

from dcpy.models.product import metadata as md


@pytest.fixture
def lion_md_path(resources_path: Path):
    return resources_path / "mini_lion_prod_with_datasets"


def test_dataset_md_overrides(lion_md_path: Path):
    lion_product_level_pub_freq = "monthly"
    pseudo_lots_pub_freq = "bimonthly"
    template_vars = {
        "version": "24c",
        "lion_prod_level_pub_freq": lion_product_level_pub_freq,
        "pseudo_lots_pub_freq": pseudo_lots_pub_freq,
    }

    lion_md = md.Metadata.from_path(
        lion_md_path / "metadata.yml", template_vars=template_vars
    )
    pseudo_lots_md = ds_md.Metadata.from_path(
        lion_md_path / "pseudo_lots" / "metadata.yml", template_vars=template_vars
    )

    pseudo_lots_with_defaults = pseudo_lots_md.attributes.apply_defaults(
        lion_md.attributes.to_dataset_attributes()
    )

    # Display name is both a product and dataset field. However, the product.display_name
    # should not act as a default for the dataset
    assert (
        pseudo_lots_with_defaults.display_name != lion_md.attributes.display_name
    ), "Product-level attrbite `display_name` should not be overridden."

    assert (
        pseudo_lots_with_defaults.publishing_frequency == pseudo_lots_pub_freq
    ), "Pseudo lots pub-freq should not be effected by the defaults."

    assert (
        pseudo_lots_with_defaults.publishing_frequency == pseudo_lots_pub_freq
    ), "Pseudo lots pub-freq should not be effected by the defaults."

    assert (
        pseudo_lots_with_defaults.publishing_purpose
        == lion_md.attributes.publishing_purpose
    ), "The missing field `publishing_purpose` should use the product-level default"
