from pathlib import Path
import yaml
import pytest

from dcpy.lifecycle.package import validate
import dcpy.models.product.dataset.metadata as md

COLP_PACKAGE_PATH = (
    Path(__file__).parent.resolve() / "resources" / "colp_single_feature_package"
)
METADATA_PATH = COLP_PACKAGE_PATH / "metadata.yml"

COLP_VERSION = "24b"
RAW_MD = yaml.safe_load(open(METADATA_PATH, "r"))


def _get_colp_md():
    return md.Metadata.from_path(METADATA_PATH, template_vars={"version": COLP_VERSION})


def test_colp_single_feature_package():
    raw_md_dataset_files = RAW_MD["package"]["dataset_files"]

    validation = validate.validate_package_from_path(
        COLP_PACKAGE_PATH, metadata_args={"version": COLP_VERSION}
    )
    assert len(raw_md_dataset_files) == len(validation.validations)
    assert not validation.get_dataset_errors()


def test_missing_attachments():
    overridden_md = _get_colp_md()

    fake_attachment_name = "I_dont_exist.pdf"
    overridden_md.package.attachments.append(fake_attachment_name)

    validation = validate.validate_package(COLP_PACKAGE_PATH, overridden_md)
    assert (
        len(validation.errors) == 1
    ), f"An error should have been found for the missing attachment. Found: {validation.errors}"

    assert fake_attachment_name in validation.errors[0].message


def test_destination_overrides():
    """
    Tests overrides at both the destination dataset_file level.
    Destination overrides should take priority over dataset_file overrides.
    """
    colp_md = _get_colp_md()
    dest = colp_md.get_destination("socrata_prod")
    assert type(dest) == md.SocrataDestination
    soc_md = dest.get_metadata(colp_md)

    soc_destination_raw = [
        d for d in RAW_MD["destinations"] if d["id"] == "socrata_prod"
    ][0]
    soc_dataset_file_raw = [
        f
        for f in RAW_MD["package"]["dataset_files"]
        if f["name"] == "primary_shapefile"
    ][0]

    assert (
        soc_md.name == soc_destination_raw["overrides"]["display_name"]
    ), """
    Display Name is overridden at both the destination and dataset_file level,
    but destination should take priority"""

    assert (
        soc_md.description == soc_dataset_file_raw["overrides"]["description"]
    ), """
    Description is overridden ONLY at the dataset_file level"""

    assert soc_md.tags == RAW_MD["tags"], "Tags should be unchanged"


def test_destination_filename_templating():
    VERSION = "24b"
    dest = _get_colp_md().get_destination("socrata_unparsed")
    assert type(dest) == md.SocrataDestination

    assert dest.overrides.destination_file_name == f"shapefile_blob_{VERSION}"
