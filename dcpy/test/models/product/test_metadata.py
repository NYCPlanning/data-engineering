from pathlib import Path
import pytest

from dcpy.models.product import metadata as md
from dcpy.models.product.dataset import metadata_v2 as ds_md


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
PRODUCT_WITH_ERRORS = "mock_product_with_errors"


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
    assert not lion_product.validate_dataset_metadata()


def test_product_validation_happy_path(test_metadata_repo: Path):
    repo = md.OrgMetadata.from_path(test_metadata_repo)
    validation = repo.validate_metadata()
    assert validation == {}, "No errors should have been found"


def test_product_validation_with_error_product(test_metadata_repo: Path):
    """Tests that validation produces the expected errors."""
    repo = md.OrgMetadata.from_path(test_metadata_repo)
    repo.metadata.products.append(PRODUCT_WITH_ERRORS)

    validation_errors = repo.validate_metadata()
    assert (
        len(validation_errors.keys()) == 1
    ), "The correct number of products should have errors"

    assert (
        PRODUCT_WITH_ERRORS in validation_errors
    ), "The correct product should have the errors."
    all_dataset_errors = validation_errors[PRODUCT_WITH_ERRORS]
    assert (
        len(all_dataset_errors.keys()) == 3
    ), "The product should report the correct num of errors"

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


def test_query_product_dataset_tags(test_metadata_repo: Path):
    TAG = "prod_tag"
    repo = md.OrgMetadata.from_path(test_metadata_repo)
    assert [
        md.ProductDatasetDestinationKey(
            product="lion", dataset="school_districts", destination="socrata"
        )
    ] == repo.query_dataset_destinations(TAG)


@pytest.fixture
def test_metadata_repo_snippets(resources_path: Path):
    yield resources_path / "test_product_metadata_repo_with_snippets"


@pytest.fixture
def product_with_snippets(test_metadata_repo_snippets: Path):
    repo = md.OrgMetadata.from_path(
        test_metadata_repo_snippets, template_vars={"version": "VERSION"}
    )
    yield repo.product("test_product")


@pytest.fixture
def dataset_with_snippets(product_with_snippets: md.ProductFolder):
    product_md = product_with_snippets.get_product_metadata()
    yield product_with_snippets.get_product_dataset("test_dataset", product_md)


def test_org_get_snippets(test_metadata_repo_snippets: Path):
    assert md.OrgMetadata.get_string_snippets(test_metadata_repo_snippets) == {
        "sample_text": "SAMPLE_TEXT"
    }


def test_org_get_column_defaults(test_metadata_repo_snippets: Path):
    assert md.OrgMetadata.get_column_defaults(test_metadata_repo_snippets) == {
        ("bbl", "bbl"): ds_md.DatasetColumn(
            id="bbl",
            name="BBL",
            data_type="bbl",
            description="sample bbl description",
            example="1016370141",
        )
    }


def test_product_snippets_applied(product_with_snippets: md.ProductFolder):
    assert (
        product_with_snippets.get_product_metadata().attributes.description
        == "Product description SAMPLE_TEXT"
    )


def test_dataset_snippets_applied(dataset_with_snippets: ds_md.Metadata):
    assert (
        dataset_with_snippets.attributes.description
        == "Dataset description SAMPLE_TEXT"
    )


def test_column_defaults_applied(dataset_with_snippets: ds_md.Metadata):
    assert dataset_with_snippets.columns == [
        ds_md.DatasetColumn(
            id="uid",
            name="uid",
            data_type="text",
            data_source="Department of City Planning",
            checks=ds_md.Checks(is_primary_key=True),
        ),
        ds_md.DatasetColumn(
            id="bbl",
            data_type="bbl",
            name="BBL",
            description="sample bbl description",
            example="1016370141",
        ),
    ]
