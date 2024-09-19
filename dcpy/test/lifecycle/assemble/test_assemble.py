import pytest
from pathlib import Path
import shutil
from unittest import mock

import dcpy.models.product.dataset.metadata_v2 as md
from dcpy.lifecycle.package import assemble
from dcpy.lifecycle.package.oti_xlsx import OTI_METADATA_FILE_TYPE


@pytest.fixture
def package_path(resources_path: Path):
    return resources_path / "product_metadata" / "assembled_package_and_metadata"


@pytest.fixture
def metadata(package_path: Path):
    return md.Metadata.from_path(package_path / "metadata.yml")


def test_unpack_package(metadata, tmp_path, package_path):
    assemble.unzip_into_package(
        zip_path=package_path / "readme_and_data_file.zip",
        package_path=Path(tmp_path),
        package_id="my_zip",
        product_metadata=metadata,
    )

    # Check that dataset_files and attachments folders exist
    dataset_files_path = tmp_path / "dataset_files"
    attachments_path = tmp_path / "attachments"
    assert dataset_files_path.exists()
    assert attachments_path.exists()

    # Check that files are unpacked correctly
    assert (dataset_files_path / "data_file.xlsx").exists()
    assert (attachments_path / "readme.pdf").exists()


@pytest.fixture
def colp_package_path(resources_path: Path):
    return resources_path / "product_metadata" / "colp_single_feature_package"


@mock.patch("dcpy.lifecycle.distribute.bytes.pull_destination_files")
def test_assemble_from_bytes(pull_destination_files_mock, tmp_path, colp_package_path):
    OTI_DD_FILENAME = "oti_dd.xlsx"
    MOCK_PULLED_PACKAGE_PATH = tmp_path

    pull_destination_files_mock.side_effect = lambda *args, **kwargs: shutil.copytree(
        colp_package_path, MOCK_PULLED_PACKAGE_PATH, dirs_exist_ok=True
    )

    # Add an oti_xlsx file type, which we'll then have to generate
    metadata = md.Metadata.from_path(
        colp_package_path / "metadata.yml", template_vars={"version": "24b"}
    )
    metadata.files.append(
        md.FileAndOverrides(
            file=md.File(
                id="oti_xlsx", filename=OTI_DD_FILENAME, type=OTI_METADATA_FILE_TYPE
            )
        )
    )

    assemble.assemble_dataset_from_bytes(
        metadata, destination_id="socrata", out_path=tmp_path
    )

    attachments_path = MOCK_PULLED_PACKAGE_PATH / "attachments"
    assert attachments_path.exists(), "Sanity check that the mock side_effect works"

    assert (
        attachments_path / OTI_DD_FILENAME
    ).exists(), "The OTI XLSX should have been generated"
