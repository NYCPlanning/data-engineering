from pathlib import Path
import pytest

from dcpy.models.product import metadata as md
from dcpy.models.product.dataset import metadata as ds_md
from dcpy.models import dataset


@pytest.fixture
def test_metadata_repo(resources_path: Path):
    return resources_path / "test_product_metadata_repo"


@pytest.fixture
def lion_md_path(test_metadata_repo: Path):
    return test_metadata_repo / "products" / "lion"


lion_product_level_pub_freq = "lion_product_freq"
pseudo_lots_pub_freq = "pseudo_lots-freq"
agency = "DCP"
template_vars = {
    "version": "24c",
    "lion_prod_level_pub_freq": lion_product_level_pub_freq,
    "pseudo_lots_pub_freq": pseudo_lots_pub_freq,
    "agency": agency,
}
PRODUCT_WITH_ERRORS = "mock_product_with_errors"


def test_org_md_overrides(test_metadata_repo: Path):
    org_md = md.OrgMetadata.from_path(test_metadata_repo, template_vars=template_vars)
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
        == pseudo_lots_pub_freq
    ), "Pseudo lots pub-freq should not be affected by the product defaults."

    assert (
        pseudo_lots_with_defaults.attributes.publishing_purpose
        == lion_md.metadata.dataset_defaults.publishing_purpose
    ), "The missing field `publishing_purpose` should use the product-level default"

    assert pseudo_lots_with_defaults.attributes.agency == agency, (
        "The field `agency` should use the org-level default"
    )


def test_query_destinations_by_type(lion_md_path: Path):
    lion_product = md.ProductMetadata.from_path(root_path=lion_md_path)

    DEST_TYPE = "socrata"
    datasets = lion_product.query_destinations(destination_type=DEST_TYPE)

    assert 2 == len(datasets.keys())
    assert "school_districts" in datasets
    assert datasets["school_districts"].keys() == {"socrata", "socrata_2"}


def test_get_tagged_destinations(lion_md_path: Path):
    product_folder = md.ProductMetadata.from_path(root_path=lion_md_path)

    TAG = "school_districts_tag"
    datasets = product_folder.query_destinations(destination_tag=TAG)

    assert 1 == len(datasets.keys())
    assert "school_districts" in datasets
    assert datasets["school_districts"].keys() == {"socrata_2"}


def test_query_multiple_filters_destinations(lion_md_path: Path):
    product_folder = md.ProductMetadata.from_path(root_path=lion_md_path)

    TAG = "prod_tag"
    DEST_TYPE = "socrata"
    DATASET_NAMES = frozenset({"pseudo_lots", "school_districts"})
    datasets = product_folder.query_destinations(
        destination_tag=TAG, destination_type=DEST_TYPE, datasets=DATASET_NAMES
    )

    assert DATASET_NAMES == datasets.keys(), "The correct datasets should be returned"
    for ds in DATASET_NAMES:
        assert datasets[ds].keys() == {"socrata"}


def test_product_metadata_validation(lion_md_path: Path):
    lion_product = md.ProductMetadata.from_path(root_path=lion_md_path)
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
    assert len(validation_errors.keys()) == 1, (
        "The correct number of products should have errors"
    )

    assert PRODUCT_WITH_ERRORS in validation_errors, (
        "The correct product should have the errors."
    )
    all_dataset_errors = validation_errors[PRODUCT_WITH_ERRORS]
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
def dataset_with_snippets(product_with_snippets: md.ProductMetadata):
    yield product_with_snippets.dataset("test_dataset")


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


def test_product_snippets_applied(product_with_snippets: md.ProductMetadata):
    assert (
        product_with_snippets.metadata.attributes.description
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
            checks=dataset.Checks(is_primary_key=True),
        ),
        ds_md.DatasetColumn(
            id="bbl",
            data_type="bbl",
            name="BBL",
            description="sample bbl description",
            example="1016370141",
        ),
    ]
