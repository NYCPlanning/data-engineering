from pathlib import Path
import pytest
from unittest.mock import patch, call

from dcpy.lifecycle.distribute import bytes
import dcpy.models.product.dataset.metadata_v2 as ds

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
    } == bytes._get_file_url_mappings_by_id(
        make_metadata(), BYTES_DEST_WITH_INDIVIDUAL_FILES
    )


def test_plan_errors_for_socrata():
    with pytest.raises(Exception, match=bytes.NON_BYTES_DEST_ERROR):
        plan = bytes.pull_destination_files(Path(""), make_metadata(), SOCRATA_DEST_ID)


@patch("dcpy.lifecycle.package.assemble.unzip_into_package")
@patch("urllib.request.urlretrieve")
def test_pull_destination_files_mocked(mock_urlretrieve, mock_unpackage, tmp_path):
    bytes.pull_destination_files(
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
