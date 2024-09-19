import pytest
from pathlib import Path

import dcpy.models.product.dataset.metadata_v2 as md
from dcpy.lifecycle.package import assemble


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
