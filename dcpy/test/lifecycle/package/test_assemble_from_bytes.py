from pathlib import Path
import pytest
import shutil
from unittest.mock import patch, call

from dcpy.lifecycle.package import assemble
import dcpy.models.product.dataset.metadata_v2 as ds
from dcpy.lifecycle.package.oti_xlsx import OTI_METADATA_FILE_TYPE

SHAPEFILE = ds.File(
    id="shp",
    filename="shp.zip",
    type="shapefile",
)
DATA_DICT = ds.File(
    id="data_dict",
    filename="data_dict.pdf",
    is_metadata=True,
    type="pdf",
)
DATA_DICT_NAME_IN_ZIP_FILE = "zipped_data_dict.pdf"

ZIP_FILE = ds.Package(
    id="my_zip",
    filename="my.zip",
    contents=[
        ds.PackageFile(id=SHAPEFILE.id),
        ds.PackageFile(id=DATA_DICT.id, filename=DATA_DICT_NAME_IN_ZIP_FILE),
    ],
)

BYTES_DEST_WITH_INDIVIDUAL_FILES = "bytes_dest_with_individual_files"
BYTES_DEST_WITH_ZIP = "bytes_dest_with_zip"

SHP_URL = "https://s-media.nyc.gov/agencies/dcp/assets/files/my_shp.zip"
DATA_DICT_URL = "https://s-media.nyc.gov/agencies/dcp/assets/files/my_data_dict.pdf"
ZIP_URL = "https://s-media.nyc.gov/agencies/dcp/assets/files/zip.pdf"

SOCRATA_DEST_ID = "soc_dest"


def make_metadata():
    return ds.Metadata(
        id="test",
        attributes=ds.DatasetAttributes(
            display_name="attrs_display_name",
            description="attrs_description",
            each_row_is_a="attrs_each_row_is_a",
            tags=["attrs_1", "attrs_2"],
            custom={
                "custom_attr_key": "custom_attr_val",
                "custom_attr_key_to_override": "custom_attr_val_to_override",
            },
        ),
        columns=[],
        files=[
            ds.FileAndOverrides(
                file=SHAPEFILE,
            ),
            ds.FileAndOverrides(
                file=DATA_DICT,
            ),
        ],
        destinations=[
            ds.DestinationWithFiles(
                id=BYTES_DEST_WITH_INDIVIDUAL_FILES,
                type="bytes",
                files=[
                    ds.DestinationFile(
                        id=SHAPEFILE.id,
                        custom={"url": SHP_URL},
                    ),
                    ds.DestinationFile(
                        id=DATA_DICT.id,
                        custom={"url": DATA_DICT_URL},
                    ),
                    ds.DestinationFile(
                        id=ZIP_FILE.id,
                        custom={"url": ZIP_URL},
                    ),
                ],
            ),
            ds.DestinationWithFiles(id=SOCRATA_DEST_ID, type="socrata", files=[]),
        ],
        assembly=[ZIP_FILE],
    )


def test_plan():
    assert {
        SHAPEFILE.id: {"path": f"dataset_files/{SHAPEFILE.filename}", "url": SHP_URL},
        DATA_DICT.id: {
            "path": f"attachments/{DATA_DICT.filename}",
            "url": DATA_DICT_URL,
        },
        ZIP_FILE.id: {"path": f"zip_files/{ZIP_FILE.filename}", "url": ZIP_URL},
    } == assemble._get_file_url_mappings_by_id(
        make_metadata(), BYTES_DEST_WITH_INDIVIDUAL_FILES
    )


def test_plan_errors_for_socrata():
    with pytest.raises(Exception, match=assemble.NON_BYTES_DEST_ERROR):
        _plan = assemble.pull_destination_files(
            Path(""), make_metadata(), SOCRATA_DEST_ID
        )


@patch("dcpy.lifecycle.package.assemble.unzip_into_package")
@patch("urllib.request.urlretrieve")
def test_pull_destination_files_mocked(mock_urlretrieve, mock_unpackage, tmp_path):
    assemble.pull_destination_files(
        tmp_path, make_metadata(), BYTES_DEST_WITH_INDIVIDUAL_FILES, unpackage_zips=True
    )

    expected_calls = [
        call(
            "https://s-media.nyc.gov/agencies/dcp/assets/files/my_shp.zip",
            tmp_path / Path("dataset_files/shp.zip"),
        ),
        call(
            "https://s-media.nyc.gov/agencies/dcp/assets/files/my_data_dict.pdf",
            tmp_path / Path("attachments/data_dict.pdf"),
        ),
        call(
            "https://s-media.nyc.gov/agencies/dcp/assets/files/zip.pdf",
            tmp_path / Path("zip_files/my.zip"),
        ),
    ]

    assert len(expected_calls) == mock_urlretrieve.call_count
    mock_urlretrieve.assert_has_calls(expected_calls, any_order=True)

    assert (
        1 == mock_unpackage.call_count
    ), "`unpackage` should have been invoked on the zipfile."


@pytest.fixture
def colp_package_path(resources_path: Path):
    return resources_path / "product_metadata" / "colp_single_feature_package"


@patch("dcpy.lifecycle.package.assemble.pull_destination_files")
def test_assemble_from_bytes(pull_destination_files_mock, tmp_path, colp_package_path):
    OTI_DD_FILENAME = "oti_dd.xlsx"
    MOCK_PULLED_PACKAGE_PATH = tmp_path

    pull_destination_files_mock.side_effect = lambda *args, **kwargs: shutil.copytree(
        colp_package_path, MOCK_PULLED_PACKAGE_PATH, dirs_exist_ok=True
    )

    # Add an oti_xlsx file type, which we'll then have to generate
    metadata = ds.Metadata.from_path(
        colp_package_path / "metadata.yml", template_vars={"version": "24b"}
    )
    metadata.files.append(
        ds.FileAndOverrides(
            file=ds.File(
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
