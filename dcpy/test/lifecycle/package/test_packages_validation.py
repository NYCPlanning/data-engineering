from pathlib import Path
import yaml

from dcpy.lifecycle.package import validate
import dcpy.models.product.dataset.metadata_v2 as md

COLP_PACKAGE_PATH = (
    Path(__file__).parent.resolve() / "resources" / "colp_single_feature_package"
)
METADATA_PATH = COLP_PACKAGE_PATH / "metadata.yml"

COLP_VERSION = "24b"
RAW_MD = yaml.safe_load(open(METADATA_PATH, "r"))


def _get_colp_md():
    return md.Metadata.from_path(METADATA_PATH, template_vars={"version": COLP_VERSION})


def test_colp_single_feature_package():
    md = _get_colp_md()
    raw_md_dataset_files = RAW_MD["files"]

    validation = validate.validate_package_from_path(
        COLP_PACKAGE_PATH, metadata_args={"version": COLP_VERSION}
    )
    assert len([f for f in md.files if not f.file.is_metadata]) == len(
        validation
    ), "There should be a validation for each dataset file"
    errors = sum([v.errors for v in validation], [])
    assert 0 == len(errors), "No Errors should have been found"


def test_missing_attachments():
    overridden_md = _get_colp_md()

    fake_attachment_name = "I_dont_exist.pdf"
    fake_attachment_id = "I_dont_exist"
    overridden_md.files.append(
        md.FileAndOverrides(
            file=md.File(
                id=fake_attachment_id, filename=fake_attachment_name, is_metadata=True
            )
        )
    )

    validations = validate.validate_package_files(COLP_PACKAGE_PATH, overridden_md)
    errors = sum([v.errors for v in validations], [])

    assert (
        len(errors) == 1
    ), f"An error should have been found for the missing attachment. Found: {errors}"

    assert (
        fake_attachment_id in errors[0].message
    ), "The error message should mention the missing package file."
