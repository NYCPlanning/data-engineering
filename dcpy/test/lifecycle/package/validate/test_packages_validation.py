import pytest

from dcpy.lifecycle import package
import dcpy.models.product.dataset.metadata as md


@pytest.fixture
def COLP_PACKAGE_PATH(package_and_dist_test_resources):
    return package_and_dist_test_resources.PACKAGE_PATH_COLP_SINGLE_FEATURE


COLP_VERSION = "24b"


@pytest.fixture
def colp_metadata(COLP_PACKAGE_PATH):
    return md.Metadata.from_path(
        COLP_PACKAGE_PATH / "metadata.yml", template_vars={"version": COLP_VERSION}
    )


def test_colp_single_feature_package(colp_metadata, COLP_PACKAGE_PATH):
    validation = package.validate_package(COLP_PACKAGE_PATH)

    non_metadata_files = [f for f in colp_metadata.files if not f.file.is_metadata]
    assert len(non_metadata_files) == len(validation.file_validations), (
        "There should be a validation for each dataset file"
    )

    assert not validation.has_errors(), "No Errors should have been found"


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

    validation = package.validate_package(COLP_PACKAGE_PATH, colp_metadata)
    assert validation.has_errors(), "the Package validation should have errors"

    files_with_errors = [v for v in validation.file_validations if v.errors]
    assert len(files_with_errors) == 1, (
        f"An single error should have been found for the missing attachment. Validation: {validation}"
    )

    assert fake_attachment_id in files_with_errors[0].errors[0].message, (
        "The error message should mention the missing package file."
    )
