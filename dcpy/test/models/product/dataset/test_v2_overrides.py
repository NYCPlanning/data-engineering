from dcpy.models.product.dataset import metadata_v2 as m


OVERRIDDEN_SHP_NAME_AT_DEST = "overridden_shp_name_at_dest.zip"

MYSHAPEFILE_DISPLAY_OVERRIDDEN_AT_FILE_LEVEL = (
    "myshapefile display overridden at file-level"
)

BASIC_SHAPEFILE_ID = "my_basic_shapefile"
OVERRIDDEN_SHAPEFILE_ID = "my_overridden_shapefile"

SOCRATA_DESTINATION_ID = "socrata_shapefile_dest"

# COLUMNS
OMITTED_FROM_SHAPEFILE_COL_ID = "omitted_from_shapefile_col_id"
OMITTED_FROM_SOCRATA_SHAPEFILE_COL_ID = "omitted_from_socrata_shapefile_col_id"


def make_metadata():
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
            m.DatasetColumn(id="id", data_type="text", description="id description"),
            m.DatasetColumn(
                id="geom",
                data_type="geometry",
                description="geom description",
                display_name="the_geom",
                custom={"api_name": "geom_api_name"},
            ),
            m.DatasetColumn(
                id="bbl",
                description="bbl description at column level",
                custom={
                    "not_overridden": "should_not_be_overridden",
                    "api_name": "bbl_col_api_name",
                },
            ),
            m.DatasetColumn(
                id="borough",
                description="",
                values=[
                    m.ColumnValue(value="1", description="Manhattan"),
                    m.ColumnValue(value="2", description="Kings"),
                ],
            ),
            m.DatasetColumn(
                id=OMITTED_FROM_SHAPEFILE_COL_ID,
            ),
            m.DatasetColumn(
                id=OMITTED_FROM_SOCRATA_SHAPEFILE_COL_ID,
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
                        display_name="myshapefile display overridden at file-level",
                        custom={
                            "api_name": "geom_api_name_at_shapefiles_level",
                            "custom_attr_key_to_override": "overridden_custom_attr_key_at_file_level",
                        },
                    ),
                    omitted_columns={OMITTED_FROM_SHAPEFILE_COL_ID},
                    overridden_columns=[
                        m.DatasetColumnOverrides(
                            id="bbl",
                            description="bbl overridden at shapefile",
                            custom={"api_name": "bbl_shapefile_api_name"},
                        ),
                        m.DatasetColumnOverrides(
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
                                display_name="display overridden at dest",
                                custom={
                                    "custom_attr_key_to_override": "overridden_custom_attr_key_at_dest_level",
                                },
                            ),
                            omitted_columns={OMITTED_FROM_SOCRATA_SHAPEFILE_COL_ID},
                            overridden_columns=[
                                m.DatasetColumnOverrides(
                                    id="bbl",
                                    description="bbl overridden at destination",
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
                    m.DestinationFile(id="my_basic_shapefile"),
                ],
            ),
        ],
    )


def test_filename_override():
    md = make_metadata()

    dest_overrides = md.calculate_destination_file_metadata(
        file_id=OVERRIDDEN_SHAPEFILE_ID, destination_id=SOCRATA_DESTINATION_ID
    )
    assert (
        OVERRIDDEN_SHP_NAME_AT_DEST == dest_overrides.file.filename
    ), "The filename should have been overridden by the destination"


def test_attribute_overrides_at_file_level():
    md = make_metadata()

    # Simple Case: Check Non-overriden files
    overridden_basic_shapefile = md.calculate_file_dataset_metadata(
        file_id=BASIC_SHAPEFILE_ID
    )
    assert (
        md.attributes.description == overridden_basic_shapefile.attributes.description
    ), "The description shouldn't have changed for the non-overridden shapefile."

    # A little more complex: check that Overrides Work for an overridden file
    overridden_complex_shapefile = md.get_file_and_overrides(OVERRIDDEN_SHAPEFILE_ID)
    calculated_overridden_shapefile_attrs = md.calculate_file_dataset_metadata(
        file_id=OVERRIDDEN_SHAPEFILE_ID
    )
    assert (
        calculated_overridden_shapefile_attrs.attributes.display_name
        == overridden_complex_shapefile.dataset_overrides.attributes.display_name
    ), "The display_name field should have been changed by the file override"

    # Check that custom values are deep merged
    expected_custom_attrs = (
        md.attributes.custom
        | overridden_complex_shapefile.dataset_overrides.attributes.custom
    )
    assert (
        expected_custom_attrs == calculated_overridden_shapefile_attrs.attributes.custom
    ), "The custom attributes should have been merged"


def test_attribute_overrides_at_destination_level():
    md = make_metadata()

    # calc expected
    overridden_shapefile = md.get_file_and_overrides(OVERRIDDEN_SHAPEFILE_ID)
    socrata_dest = md.get_destination(SOCRATA_DESTINATION_ID)
    socrata_dest_file_overrides = [
        f.dataset_overrides
        for f in socrata_dest.files
        if f.id == overridden_shapefile.file.id
    ][0]

    # calc actual
    calculated_attrs = md.calculate_destination_file_metadata(
        file_id=OVERRIDDEN_SHAPEFILE_ID, destination_id=SOCRATA_DESTINATION_ID
    ).dataset

    assert (
        socrata_dest_file_overrides.attributes.display_name
        == calculated_attrs.attributes.display_name
    )

    # Check custom attrs
    expected_custom_attrs = (
        md.attributes.custom
        | overridden_shapefile.dataset_overrides.attributes.custom
        | socrata_dest_file_overrides.attributes.custom
    )
    assert expected_custom_attrs == calculated_attrs.attributes.custom


def test_omitted_columns():
    md = make_metadata()

    file_overrides = md.calculate_file_dataset_metadata(file_id=OVERRIDDEN_SHAPEFILE_ID)
    dest_overrides = md.calculate_destination_file_metadata(
        file_id=OVERRIDDEN_SHAPEFILE_ID, destination_id=SOCRATA_DESTINATION_ID
    ).dataset

    file_expected_col_ids = {
        c.id for c in md.columns if c.id not in {OMITTED_FROM_SHAPEFILE_COL_ID}
    }
    dest_expected_col_ids = {
        c.id
        for c in md.columns
        if c.id
        not in {OMITTED_FROM_SHAPEFILE_COL_ID, OMITTED_FROM_SOCRATA_SHAPEFILE_COL_ID}
    }

    assert file_expected_col_ids == {c.id for c in file_overrides.columns}
    assert dest_expected_col_ids == {c.id for c in dest_overrides.columns}


def test_column_overrides_at_file_level():
    md = make_metadata()

    overridden_shapefile = md.get_file_and_overrides(OVERRIDDEN_SHAPEFILE_ID)
    calced_overridden_cols = md.calculate_file_dataset_metadata(
        file_id=OVERRIDDEN_SHAPEFILE_ID
    ).columns

    # Test that the description colum is overridden for the `bbl` column
    actual_overridden_bbl_col = [c for c in calced_overridden_cols if c.id == "bbl"][0]
    expected_overridden_bbl_col = [
        c
        for c in overridden_shapefile.dataset_overrides.overridden_columns
        if c.id == "bbl"
    ][0]

    assert (
        expected_overridden_bbl_col.description == actual_overridden_bbl_col.description
    ), "The description field on the bbl column should have been overridden at the shapefile level."

    base_bbl_col = [c for c in md.columns if c.id == "bbl"][0]
    shapefile_bbl_col = [
        c
        for c in overridden_shapefile.dataset_overrides.overridden_columns
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
    shapefile_borough_col = [
        c
        for c in overridden_shapefile.dataset_overrides.overridden_columns
        if c.id == "borough"
    ][0]
    actual_overridden_borough_col = [
        c for c in calced_overridden_cols if c.id == "borough"
    ][0]
    assert shapefile_borough_col.values == actual_overridden_borough_col.values


def test_column_overrides_at_destination_level():
    md = make_metadata()

    overridden_shapefile = md.get_file_and_overrides(OVERRIDDEN_SHAPEFILE_ID)
    socrata_dest = md.get_destination(SOCRATA_DESTINATION_ID)

    calced_overridden_cols = md.calculate_destination_file_metadata(
        file_id=overridden_shapefile.file.id, destination_id=socrata_dest.id
    ).dataset.columns
    actual_overridden_bbl_col = [c for c in calced_overridden_cols if c.id == "bbl"][0]

    socrata_dest_file_overrides = [
        f.dataset_overrides
        for f in socrata_dest.files
        if f.id == overridden_shapefile.file.id
    ][0]
    socrata_dest_bbl_col = [
        c for c in socrata_dest_file_overrides.overridden_columns if c.id == "bbl"
    ][0]

    assert socrata_dest_bbl_col.description == actual_overridden_bbl_col.description
