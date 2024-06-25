from pathlib import Path
import yaml
import pytest

from dcpy.lifecycle.package import validate
import dcpy.models.product.dataset.metadata as md

COLP_PACKAGE_PATH = (
    Path(__file__).parent.resolve() / "resources" / "colp_single_feature_package"
)
METADATA_PATH = COLP_PACKAGE_PATH / "metadata.yml"

COLP_MD = md.Metadata.from_yaml(METADATA_PATH)
RAW_MD = yaml.safe_load(open(METADATA_PATH, "r"))


def test_colp_single_feature_package():
    validation = validate.validate_package(COLP_PACKAGE_PATH)
    assert len(validation.validations) == 2
    assert not validation.get_dataset_errors()


def test_missing_attachments():
    overridden_md = md.Metadata.from_yaml(COLP_PACKAGE_PATH / "metadata.yml")

    fake_attachment_name = "I_dont_exist.pdf"
    overridden_md.package.attachments.append(fake_attachment_name)

    validation = validate.validate_package(COLP_PACKAGE_PATH, overridden_md)
    assert (
        len(validation.errors) == 1
    ), "An error should have been found for the missing attachment"

    assert fake_attachment_name in validation.errors[0].message


def test_destination_overrides():
    """
    Tests overrides at both the destination dataset_file level.
    Destination overrides should take priority over dataset_file overrides.
    """
    dest = COLP_MD.get_destination("socrata_prod")
    assert type(dest) == md.SocrataDestination
    soc_md = dest.get_metadata(COLP_MD)

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


def test_unparsed_dataset_name_templating():
    VERSION = "24b"
    dest = COLP_MD.get_destination("socrata_unparsed")
    assert type(dest) == md.SocrataDestination

    assert (
        dest.overrides.get_dataset_destination_name(version=VERSION)
        == f"shapefile_blob_{VERSION}"
    )
