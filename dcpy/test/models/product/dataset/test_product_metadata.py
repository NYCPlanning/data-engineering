import tempfile
from pathlib import Path

import pytest
from pytest import fixture

from dcpy.models.product.dataset import metadata as m

OVERRIDDEN_SHP_NAME_AT_DEST = "overridden_shp_name_at_dest.zip"
DESTINATION_OVERRIDDEN_DISPLAY_NAME = "overridden dest display name"
SHAPEFILE_OVERRIDDEN_DISPLAY_NAME = "overridden dest display name"

BBL_SHAPEFILE_COL_DESC = "bbl description overridden at the column level"
BBL_SOCRATA_COL_DESC = """
bbl description overridden at Socrata

With some bonus new-lines and
• Special Characters
   o to exercise up the (de)serialization tests
"""
OVERRIDDEN_SHAPEFILE_TEMPLATED_FILE_NAME = "my_file_{{ version }}.zip"

# Files
BASIC_SHAPEFILE_ID = "my_basic_shapefile"
OVERRIDDEN_SHAPEFILE_ID = "my_overridden_shapefile"
ZIP_FILE_ID = "my_zip"

# Destinations
SOCRATA_DESTINATION_ID = "socrata_shapefile_dest"
NO_FILES_DEST_ID = "no files dest id"

# COLUMNS
OMITTED_FROM_SHAPEFILE_COL_ID = "omitted_from_shapefile_col_id"
OMITTED_FROM_SOCRATA_SHAPEFILE_COL_ID = "omitted_from_socrata_shapefile_col_id"


@fixture
def md():
    return m.Metadata(
        id="test",
        attributes=m.DatasetAttributes(
            display_name="attrs_display_name",
            description="attrs_description",
            each_row_is_a="attrs_each_row_is_a",
            tags=["attrs_1", "attrs_2"],
            custom={
                "custom_attr_key": "custom_attr_val",
                "custom_attr_key_to_override": "custom_attr_val_to_override",
            },
        ),
        columns=[
            m.DatasetColumn(
                id="id", name="id", data_type="text", description="id description"
            ),
            m.DatasetColumn(
                id="geom",
                data_type="geometry",
                description="geom description",
                name="the_geom",
                custom={"api_name": "geom_api_name"},
            ),
            m.DatasetColumn(
                id="bbl",
                data_type="bbl",
                name="bbl",
                description="bbl description at column level",
                custom={
                    "not_overridden": "should_not_be_overridden",
                    "api_name": "bbl_col_api_name",
                },
            ),
            m.DatasetColumn(
                id="borough",
                data_type="text",
                name="borough",
                description="",
                values=[
                    m.ColumnValue(value="1", description="Manhattan"),
                    m.ColumnValue(value="2", description="Kings"),
                ],
            ),
            m.DatasetColumn(
                id=OMITTED_FROM_SHAPEFILE_COL_ID,
                name=OMITTED_FROM_SHAPEFILE_COL_ID,
                data_type="text",
            ),
            m.DatasetColumn(
                id=OMITTED_FROM_SOCRATA_SHAPEFILE_COL_ID,
                name=OMITTED_FROM_SOCRATA_SHAPEFILE_COL_ID,
                data_type="text",
            ),
        ],
        files=[
            m.FileAndOverrides(
                file=m.File(
                    id=BASIC_SHAPEFILE_ID,
                    filename="shp.zip",
                    type="shapefile",
                    custom={"hi": "there"},
                ),
            ),
            m.FileAndOverrides(
                file=m.File(
                    id=OVERRIDDEN_SHAPEFILE_ID,
                    filename="overridden_shp.zip",
                    type="shapefile",
                ),
                dataset_overrides=m.DatasetOverrides(
                    attributes=m.DatasetAttributesOverride(
                        display_name=SHAPEFILE_OVERRIDDEN_DISPLAY_NAME,
                        custom={
                            "api_name": "geom_api_name_at_shapefiles_level",
                            "custom_attr_key_to_override": "overridden_custom_attr_key_at_file_level",
                        },
                    ),
                    omitted_columns=[OMITTED_FROM_SHAPEFILE_COL_ID],
                    overridden_columns=[
                        m.DatasetColumn(
                            id="bbl",
                            description=BBL_SHAPEFILE_COL_DESC,
                            custom={"api_name": "bbl_shapefile_api_name"},
                        ),
                        m.DatasetColumn(
                            id="borough",
                            description="borough overridden at shapefile",
                            values=[m.ColumnValue(value="3", description="Queens")],
                        ),
                    ],
                ),
            ),
        ],
        destinations=[
            m.DestinationWithFiles(
                id=SOCRATA_DESTINATION_ID,
                type="socrata",
                files=[
                    m.DestinationFile(
                        id=OVERRIDDEN_SHAPEFILE_ID,
                        file_overrides=m.FileOverrides(
                            filename=OVERRIDDEN_SHP_NAME_AT_DEST
                        ),
                        dataset_overrides=m.DatasetOverrides(
                            attributes=m.DatasetAttributesOverride(
                                display_name=DESTINATION_OVERRIDDEN_DISPLAY_NAME,
                                custom={
                                    "custom_attr_key_to_override": "overridden_custom_attr_key_at_dest_level",
                                },
                            ),
                            omitted_columns=[OMITTED_FROM_SOCRATA_SHAPEFILE_COL_ID],
                            overridden_columns=[
                                m.DatasetColumn(
                                    id="bbl",
                                    description=BBL_SOCRATA_COL_DESC,
                                    custom={"api_name": "bbl_dest_api_name"},
                                ),
                            ],
                        ),
                    ),
                ],
            ),
            m.DestinationWithFiles(
                id="bytes_dest",
                type="bytes",
                files=[
                    m.DestinationFile(id=ZIP_FILE_ID),
                ],
            ),
            m.DestinationWithFiles(
                id=NO_FILES_DEST_ID,
                type="bytes",
                files=[
                    m.DestinationFile(id=OVERRIDDEN_SHAPEFILE_ID),
                ],
            ),
        ],
        assembly=[
            m.Package(
                id=ZIP_FILE_ID,
                filename="my.zip",
                contents=[
                    m.PackageFile(id=BASIC_SHAPEFILE_ID),
                    m.PackageFile(
                        id=OVERRIDDEN_SHAPEFILE_ID,
                        filename=OVERRIDDEN_SHAPEFILE_TEMPLATED_FILE_NAME,
                    ),
                ],
            )
        ],
    )


@fixture
def overridden_shapefile_dataset(md):
    return md.calculate_file_dataset_metadata(file_id=OVERRIDDEN_SHAPEFILE_ID)


@fixture
def overridden_socrata_dataset(md):
    return md.calculate_destination_metadata(
        file_id=OVERRIDDEN_SHAPEFILE_ID, destination_id=SOCRATA_DESTINATION_ID
    )


def test_filename_override(overridden_socrata_dataset):
    """
    Tests overriding at the destination.

    Rationale: Certain files have different filenames in our packages vs destinations.
    E.g. we might have metadata.pdf in the package, and metadata_24c.pdf on Socrata.
    """
    assert OVERRIDDEN_SHP_NAME_AT_DEST == overridden_socrata_dataset.file.filename, (
        "The filename should have been overridden by the destination"
    )


def test_attribute_overrides_at_file_level(md, overridden_shapefile_dataset: m.Dataset):
    """
    Often a file will contain data with a slight variation from another file, e.g. Water Included
    vs Water Not Included. This tests that we can override those dataset attributes at the file level.
    """

    # Simple Case: Check Non-overriden files
    non_overridden_file_dataset = md.calculate_file_dataset_metadata(
        file_id=BASIC_SHAPEFILE_ID
    )
    assert (
        md.attributes.description == non_overridden_file_dataset.attributes.description
    ), "The description shouldn't have changed for the non-overridden shapefile."

    # Check that Overrides Work for an overridden file
    assert (
        SHAPEFILE_OVERRIDDEN_DISPLAY_NAME
        == overridden_shapefile_dataset.attributes.display_name
    ), "The display_name field should have been changed by the file override"

    # Check that custom values are merged
    shapefile_overrides = md.get_file_and_overrides(OVERRIDDEN_SHAPEFILE_ID)
    expected_custom_attrs = (
        md.attributes.custom | shapefile_overrides.dataset_overrides.attributes.custom
    )
    assert expected_custom_attrs == overridden_shapefile_dataset.attributes.custom, (
        "The custom attributes should have been merged"
    )


def test_attribute_overrides_at_destination_level(
    md, overridden_socrata_dataset: m.DestinationMetadata
):
    """
    Similar to overrides at a file-level, sometimes it makes sense to override at the destination-level
    in addition to the file-level. This tests that the `name` is overridden, as well as the
    `custom` attributes.
    """
    assert (
        DESTINATION_OVERRIDDEN_DISPLAY_NAME
        == overridden_socrata_dataset.dataset.attributes.display_name
    )

    # Check custom attrs
    shapefile_with_overrides = md.get_file_and_overrides(OVERRIDDEN_SHAPEFILE_ID)
    socrata_dest = md.get_destination(SOCRATA_DESTINATION_ID)
    socrata_dest_file_overrides = [
        f.dataset_overrides
        for f in socrata_dest.files
        if f.id == shapefile_with_overrides.file.id
    ][0]
    expected_custom_attrs = (
        md.attributes.custom
        | shapefile_with_overrides.dataset_overrides.attributes.custom
        | socrata_dest_file_overrides.attributes.custom
    )
    assert expected_custom_attrs == overridden_socrata_dataset.dataset.attributes.custom


def test_omitted_columns(
    md,
    overridden_shapefile_dataset: m.Dataset,
    overridden_socrata_dataset: m.DestinationMetadata,
):
    """
    Test omitting columns when calculating overrides. In particular, the following case.
    Starting from the pure columns in the metadata:
    - a shapefile omits one of the columns
    - then the socrata destination for that shapefile omits another column
    """
    expected_file_col_ids = {
        c.id for c in md.columns if c.id != OMITTED_FROM_SHAPEFILE_COL_ID
    }
    expected_socrata_col_ids = {
        c for c in expected_file_col_ids if c != OMITTED_FROM_SOCRATA_SHAPEFILE_COL_ID
    }

    assert expected_file_col_ids == {
        c.id for c in overridden_shapefile_dataset.columns
    }, "The omitted shapefile column should not be present"
    assert expected_socrata_col_ids == {
        c.id for c in overridden_socrata_dataset.dataset.columns
    }, "The omitted destination column should not be present"


def test_column_overrides_at_file_level(md, overridden_shapefile_dataset: m.Dataset):
    shapefile_with_overrides = md.get_file_and_overrides(OVERRIDDEN_SHAPEFILE_ID)

    # Test that the description column is overridden for the `bbl` column
    actual_overridden_bbl_col = [
        c for c in overridden_shapefile_dataset.columns if c.id == "bbl"
    ][0]

    assert BBL_SHAPEFILE_COL_DESC == actual_overridden_bbl_col.description, (
        "The description field on the bbl column should have been overridden at the shapefile level."
    )

    # Check the `custom` attributes
    base_bbl_col = [c for c in md.columns if c.id == "bbl"][0]
    shapefile_bbl_col = [
        c
        for c in shapefile_with_overrides.dataset_overrides.overridden_columns
        if c.id == "bbl"
    ][0]
    assert (
        actual_overridden_bbl_col.custom
        == base_bbl_col.custom | shapefile_bbl_col.custom
    )
    assert (
        actual_overridden_bbl_col.custom["not_overridden"] == "should_not_be_overridden"
    ), "Sanity check on the `not_overridden` field."

    # Check that Values fields are overwritten, not merged
    # base_borough_col = [c for c in md.columns if c.id == "borough"][0]
    shapefile_borough_col_overrides = [
        c
        for c in shapefile_with_overrides.dataset_overrides.overridden_columns
        if c.id == "borough"
    ][0]
    actual_overridden_borough_col = [
        c for c in overridden_shapefile_dataset.columns if c.id == "borough"
    ][0]
    assert (
        shapefile_borough_col_overrides.values == actual_overridden_borough_col.values
    )


def test_column_overrides_at_destination_level(
    overridden_socrata_dataset: m.DestinationMetadata,
):
    actual_overridden_bbl_col = [
        c for c in overridden_socrata_dataset.dataset.columns if c.id == "bbl"
    ][0]

    assert BBL_SOCRATA_COL_DESC == actual_overridden_bbl_col.description


def test_writing_to_yaml(md):
    with tempfile.TemporaryDirectory() as temp_dir:
        out_file = Path(temp_dir) / "metadata.yml"
        md.write_to_yaml(out_file)

        deserialized = m.Metadata.from_path(out_file)

        assert md.model_dump() == deserialized.model_dump()


def test_templating(md):
    with tempfile.TemporaryDirectory() as temp_dir:
        out_file = Path(temp_dir) / "metadata.yml"
        md.write_to_yaml(out_file)

        version = "24b"
        deserialized = m.Metadata.from_path(
            out_file, template_vars={"version": version}
        )

        assert (
            OVERRIDDEN_SHAPEFILE_TEMPLATED_FILE_NAME.replace("{{ version }}", version)
            == [
                f
                for f in deserialized.get_package(ZIP_FILE_ID).contents
                if f.id == OVERRIDDEN_SHAPEFILE_ID
            ][0].filename
        )


def test_getting_non_existant_packages_dests_files(md: m.Metadata):
    with pytest.raises(Exception):
        md.get_package("blah")

    with pytest.raises(Exception):
        md.get_destination("blah")

    with pytest.raises(Exception):
        md.get_file_and_overrides("blah")

    with pytest.raises(Exception):
        # "This destination has no files, so an exception should be raised when we pass a file."
        md.calculate_destination_metadata(
            file_id=BASIC_SHAPEFILE_ID, destination_id=NO_FILES_DEST_ID
        )


def test_validating_metadata__files__missing_basic_shapefile(md: m.Metadata):
    # remove BASIC_SHAPEFILE_ID
    md.files = [f for f in md.files if f.file.id != BASIC_SHAPEFILE_ID]

    validation = md.validate_consistency()
    assert len(validation) == 1

    assert BASIC_SHAPEFILE_ID in validation[0], (
        "The validation error should reference the correct file"
    )


def test_validating_metadata__files__missing_overridden_shapefile(md: m.Metadata):
    # Three overrides reference the overridden shapefile.
    md.files = [f for f in md.files if f.file.id != OVERRIDDEN_SHAPEFILE_ID]

    validation = md.validate_consistency()
    assert len(validation) == 3, (
        "There should be the correct number of validation errors"
    )


def test_validating_metadata__files__missing_zip_reference(md: m.Metadata):
    nonexistant_file_id = "nonexistant_file_id"

    md.assembly[0].contents.append(m.PackageFile(id=nonexistant_file_id))

    validation = md.validate_consistency()
    assert len(validation) == 1, (
        "There should be the correct number of validation errors"
    )


def test_validating_metadata__file_missing_columns(md: m.Metadata):
    nonexistant_file_col_id = "nonexistant_file_col_id"

    file_with_overrides = [f for f in md.files if f.file.id == OVERRIDDEN_SHAPEFILE_ID][
        0
    ]
    file_with_overrides.dataset_overrides.overridden_columns.append(
        m.DatasetColumn(id=nonexistant_file_col_id)
    )

    validation = md.validate_consistency()
    assert len(validation) == 1, "There should be one column error"

    assert nonexistant_file_col_id in validation[0]


def test_validating_metadata__dest_missing_columns(md: m.Metadata):
    nonexistant_dest_col_id = "nonexistant_dest_col_id"
    nonexistant_dest_omitted_col_id = "nonexistant_dest_omitted_col_id"

    dest_with_overrides = md.get_destination(SOCRATA_DESTINATION_ID)
    file_overrides = [
        f for f in dest_with_overrides.files if f.id == OVERRIDDEN_SHAPEFILE_ID
    ][0]

    # Add a nonexistant omitted Column to the Socrata Dest
    file_overrides.dataset_overrides.omitted_columns.append(
        nonexistant_dest_omitted_col_id
    )
    # Add a nonexistant overridden Column to the Socrata Dest
    file_overrides.dataset_overrides.overridden_columns.append(
        m.DatasetColumn(id=nonexistant_dest_col_id)
    )

    print(md.model_dump())

    validation = md.validate_consistency()
    assert len(validation) == 2, "There should be the correct number of column errors"


def test_missing_column_names(md: m.Metadata):
    md.columns[0] = m.DatasetColumn(
        id="bbl",
        data_type="bbl",
    )
    validation = md.validate_consistency()
    assert len(validation) == 1


def test_missing_column_data_types(md: m.Metadata):
    md.columns[0] = m.DatasetColumn(
        id="bbl",
        name="bbl",
    )
    validation = md.validate_consistency()
    assert len(validation) == 1


def test_column_defaults(md: m.Metadata):
    md.columns = [
        m.DatasetColumn(
            id="bbl",
            data_type="bbl",
        )
    ]
    column_default_definitions = [
        m.DatasetColumn(
            id="bbl",
            name="BBL",
            data_type="bbl",
            description="sample bbl description",
            example="1016370141",
        ),
        m.DatasetColumn(
            id="other",
            name="other",
            data_type="text",
        ),
    ]
    column_defaults = {
        (c.id, c.data_type): c for c in column_default_definitions if c.data_type
    }

    assert md.apply_column_defaults(column_defaults) == [
        m.DatasetColumn(
            id="bbl",
            name="BBL",
            data_type="bbl",
            description="sample bbl description",
            example="1016370141",
        )
    ]
