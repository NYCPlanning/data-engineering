import pytest
from pathlib import Path
import dcpy.models.product.dataset.metadata_v2 as md_v2
from dcpy.lifecycle.package import assemble

RESOURCES_PATH = Path(__file__).parent.parent / "shared_resources"
assert RESOURCES_PATH.exists()

TEST_PACKAGE_PATH = (
    RESOURCES_PATH / "product_metadata" / "assembled_package_and_metadata"
)
ZIP_FILE_PATH = TEST_PACKAGE_PATH / "readme_and_data_file.zip"


@pytest.fixture
def metadata():
    return md_v2.Metadata.from_path(TEST_PACKAGE_PATH / "metadata.yml")


def test_unpack_package(metadata, tmp_path):
    assemble.unpackage(
        package_zip=ZIP_FILE_PATH,
        out_path=Path(tmp_path),
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
