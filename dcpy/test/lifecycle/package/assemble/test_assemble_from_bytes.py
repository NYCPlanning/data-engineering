from pathlib import Path
from unittest.mock import patch, call

from dcpy.lifecycle.package import assemble
import dcpy.models.product.dataset.metadata as ds

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


@patch("dcpy.lifecycle.package.assemble.unzip_into_package")
@patch("urllib.request.urlretrieve")
def test_pull_destination_files(mock_urlretrieve, mock_unpackage, tmp_path):
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

    assert 1 == mock_unpackage.call_count, (
        "`unpackage` should have been invoked on the zipfile."
    )


@patch("urllib.request.urlretrieve")
def test_pull_destination_files_md_only(mock_urlretrieve, tmp_path):
    assemble.pull_destination_files(
        tmp_path,
        make_metadata(),
        BYTES_DEST_WITH_INDIVIDUAL_FILES,
        metadata_only=True,
    )

    expected_calls = [
        call(
            "https://s-media.nyc.gov/agencies/dcp/assets/files/my_data_dict.pdf",
            tmp_path / Path("attachments/data_dict.pdf"),
        ),
    ]

    assert len(expected_calls) == mock_urlretrieve.call_count
    mock_urlretrieve.assert_has_calls(expected_calls)


# TODO: This is a useful test, but I want to generally refactor all these product metadata tests to
# use the test_product_metadata repo.

# @pytest.fixture
# def colp_package_path(resources_path: Path):
#     return resources_path / "product_metadata" / "colp_single_feature_package"


# @patch("dcpy.lifecycle.package.assemble.pull_destination_files")
# def test_assemble_from_bytes(pull_destination_files_mock, tmp_path, colp_package_path):
#     MOCK_PULLED_PACKAGE_PATH = tmp_path
#     pull_destination_files_mock.side_effect = lambda *args, **kwargs: shutil.copytree(
#         colp_package_path, MOCK_PULLED_PACKAGE_PATH, dirs_exist_ok=True
#     )
#     metadata = ds.Metadata.from_path(
#         colp_package_path / "metadata.yml", template_vars={"version": "24b"}
#     )

#     TEST_CASE_NAME_TO_OVERRIDES = [
#         ["no_overrides", {}],
#         ["file_overrides", {"file_id": "primary_shapefile"}],
#         [
#             "dest_overrides",
#             {
#                 "file_id": "primary_shapefile",
#                 "destination_id": "socrata_prod",
#             },
#         ],
#     ]

#     for test_case_name, overrides in TEST_CASE_NAME_TO_OVERRIDES:
#         metadata.files.append(
#             ds.FileAndOverrides(
#                 file=ds.File(
#                     id=test_case_name,
#                     filename=test_case_name + ".xlsx",
#                     type=oti_xlsx.OTI_METADATA_FILE_TYPE,
#                     custom={
#                         assemble.ASSEMBLY_INSTRUCTIONS_KEY: {
#                             assemble.METADATA_OVERRIDE_KEY: overrides
#                         }
#                     }
#                     if overrides
#                     else {},
#                 )
#             )
#         )

#     assemble.assemble_dataset_from_bytes(
#         product="colp",
#         dataset_metadata=metadata,
#         source_destination_id="socrata",
#         out_path=tmp_path,
#         version="24c",
#     )

#     attachments_path = MOCK_PULLED_PACKAGE_PATH / "attachments"
#     assert attachments_path.exists(), "Sanity check that the mock side_effect works"

#     for test_case_name, overrides in TEST_CASE_NAME_TO_OVERRIDES:
#         dataset = (
#             metadata.calculate_metadata(**overrides) if overrides else metadata.dataset  # type: ignore
#         )

#         xlsx_path = attachments_path / (test_case_name + ".xlsx")
#         assert xlsx_path.exists(), "The OTI XLSX should have been generated"
#         assert (
#             oti_xlsx._get_dataset_description(xlsx_path)
#             == dataset.attributes.description
#         ), "The XLSX should have the correct description"
