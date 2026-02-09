import zipfile
from pathlib import Path

import pytest

import dcpy.models.product.dataset.metadata as md
from dcpy.lifecycle.package import assemble


@pytest.fixture
def package_path(package_and_dist_test_resources):
    return package_and_dist_test_resources.PACKAGE_PATH_ASSEMBLED


@pytest.fixture
def metadata(package_path: Path):
    return md.Metadata.from_path(package_path / "metadata.yml")


def test_unpack_package(metadata, tmp_path, package_path):
    assemble.unzip_into_package(
        zip_path=package_path / "zip_files" / "readme_and_data_file.zip",
        package_path=Path(tmp_path),
        package_id="my_zip",
        dataset_metadata=metadata,
    )

    # Check that dataset_files and attachments folders exist
    dataset_files_path = tmp_path / "dataset_files"
    attachments_path = tmp_path / "attachments"
    assert dataset_files_path.exists()
    assert attachments_path.exists()

    # Check that files are unpacked correctly
    assert (dataset_files_path / "data_file.xlsx").exists()
    assert (attachments_path / "readme.pdf").exists()


def test_unpack_multi_layer_shapefile(
    metadata: md.Metadata, tmp_path: Path, package_path: Path
):
    SHAPEFILE_PACKAGE_ID = "points_and_lines"
    assemble.unpack_multilayer_shapefile(
        file_path=package_path / "zip_files" / "points_and_lines_shp.zip",
        package_path=Path(tmp_path),
        package_id=SHAPEFILE_PACKAGE_ID,
        dataset_metadata=metadata,
    )

    # Check that dataset_files and attachments folders exist
    dataset_files_path = tmp_path / "dataset_files"
    attachments_path = tmp_path / "attachments"
    assert dataset_files_path.exists()
    assert attachments_path.exists()

    expected_shapefile_ids = {
        c.id for c in metadata.get_package(SHAPEFILE_PACKAGE_ID).contents
    }
    expected_shapefile_filenames = [
        f.file.filename for f in metadata.files if f.file.id in expected_shapefile_ids
    ]
    assert {"points.zip", "lines.zip"} == set(expected_shapefile_filenames)

    # Check that files are unpacked correctly
    for n in expected_shapefile_filenames:
        expected_path = dataset_files_path / n
        assert expected_path.exists(), f"Expected {expected_path} to exist"

        filenames = set(zipfile.ZipFile(expected_path).namelist())

        expected_filenames = {
            f"{Path(n).stem}.{ext}" for ext in assemble.SHAPEFILE_SUFFIXES
        }
        assert expected_filenames == filenames, (
            "All expected files should be present in the shapefile"
        )
