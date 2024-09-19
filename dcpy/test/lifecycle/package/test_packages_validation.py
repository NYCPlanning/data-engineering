from pathlib import Path
import pytest

from dcpy.lifecycle.package import validate
import dcpy.models.product.dataset.metadata_v2 as md


@pytest.fixture
def COLP_PACKAGE_PATH(resources_path: Path):
    return resources_path / "product_metadata" / "colp_single_feature_package"


COLP_VERSION = "24b"


@pytest.fixture
def colp_metadata(COLP_PACKAGE_PATH):
    return md.Metadata.from_path(
        COLP_PACKAGE_PATH / "metadata.yml", template_vars={"version": COLP_VERSION}
    )


def test_colp_single_feature_package(colp_metadata, COLP_PACKAGE_PATH):
    validation = validate.validate_package_from_path(
        COLP_PACKAGE_PATH, metadata_args={"version": COLP_VERSION}
    )
    assert len([f for f in colp_metadata.files if not f.file.is_metadata]) == len(
        validation
    ), "There should be a validation for each dataset file"
    errors = sum([v.errors for v in validation], [])
    assert 0 == len(errors), "No Errors should have been found"


def test_missing_attachments(colp_metadata, COLP_PACKAGE_PATH):
    fake_attachment_name = "I_dont_exist.pdf"
    fake_attachment_id = "I_dont_exist"
    colp_metadata.files.append(
        md.FileAndOverrides(
            file=md.File(
                id=fake_attachment_id, filename=fake_attachment_name, is_metadata=True
            )
        )
    )

    validations = validate.validate_package_files(COLP_PACKAGE_PATH, colp_metadata)
    errors = sum([v.errors for v in validations], [])

    assert (
        len(errors) == 1
    ), f"An error should have been found for the missing attachment. Found: {errors}"

    assert (
        fake_attachment_id in errors[0].message
    ), "The error message should mention the missing package file."
