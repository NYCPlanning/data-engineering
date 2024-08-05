from dcpy.models.product.dataset import metadata_v2 as md_v2


def make_metadata():
    return md_v2.Metadata(
        id="test",
        attributes=md_v2.DatasetAttributes(
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
            md_v2.DatasetColumn(
                id="id", data_type="text", description="id description"
            ),
            md_v2.DatasetColumn(
                id="geom",
                data_type="geometry",
                description="geom description",
                display_name="the_geom",
                custom={"api_name": "geom_api_name"},
            ),
            md_v2.DatasetColumn(
                id="bbl",
                description="bbl description at column level",
                custom={
                    "not_overridden": "should_not_be_overridden",
                    "api_name": "bbl_col_api_name",
                },
            ),
            md_v2.DatasetColumn(
                id="borough",
                description="",
                values=[
                    md_v2.ColumnValue(value="1", description="Manhattan"),
                    md_v2.ColumnValue(value="2", description="Kings"),
                ],
            ),
        ],
        files=[
            md_v2.File(
                id="my_basic_shapefile",
                filename="shp.zip",
                type="shapefile",
                custom={"hi": "there"},
            ),
            md_v2.File(
                id="my_overridden_shapefile",
                filename="shp.zip",
                type="shapefile",
                overrides=md_v2.FileOverrides(
                    attributes=md_v2.NullableDatasetAttributes(
                        display_name="myshapefile display overridden at file-level",
                        custom={
                            "api_name": "geom_api_name_at_shapefiles_level",
                            "custom_attr_key_to_override": "overridden_custom_attr_key_at_file_level",
                        },
                    ),
                    omitted_columns=["test"],
                    overridden_columns=[
                        md_v2.OverrideableColumnAttrs(
                            id="bbl",
                            description="bbl overridden at shapefile",
                            custom={"api_name": "bbl_shapefile_api_name"},
                        ),
                        md_v2.OverrideableColumnAttrs(
                            id="borough",
                            description="borough overridden at shapefile",
                            values=[md_v2.ColumnValue(value="3", description="Queens")],
                        ),
                    ],
                ),
            ),
        ],
        destinations=[
            md_v2.Destination(
                id="socrata_dest",
                type="socrata",
                files=[
                    md_v2.DestinationFile(
                        id="my_overridden_shapefile",
                        overrides=md_v2.FileOverrides(
                            attributes=md_v2.NullableDatasetAttributes(
                                display_name="display overridden at dest",
                                custom={
                                    "custom_attr_key_to_override": "overridden_custom_attr_key_at_dest_level",
                                },
                            ),
                            overridden_columns=[
                                md_v2.OverrideableColumnAttrs(
                                    id="bbl",
                                    description="bbl overridden at destination",
                                    custom={"api_name": "bbl_dest_api_name"},
                                ),
                            ],
                        ),
                    ),
                ],
            ),
            md_v2.Destination(
                id="bytes_dest",
                type="bytes",
                files=[
                    md_v2.DestinationFile(id="my_basic_shapefile"),
                ],
            ),
        ],
    )


def test_filename_override():
    assert False, "Implement me!"


def test_omit_colums():
    assert False, "Implement me!"


def test_attribute_overrides_at_file_level():
    md = make_metadata()

    # Simple Case: Check Non-overriden files
    basic_shapefile = [f for f in md.files if f.id == "my_basic_shapefile"][0]
    calculated_basic_shapefile_attrs = md.calculate_overridden_attributes(
        file_id=basic_shapefile.id
    )
    assert (
        md.attributes.description == calculated_basic_shapefile_attrs.description
    ), "The description shouldn't have changed for the non-overridden shapefile."

    # A little more complex: check that Overrides Work for an overridden file
    overridden_shapefile = [f for f in md.files if f.id == "my_overridden_shapefile"][0]
    calculated_overridden_shapefile_attrs = md.calculate_overridden_attributes(
        file_id=overridden_shapefile.id
    )
    assert (
        calculated_overridden_shapefile_attrs.display_name
        == overridden_shapefile.overrides.attributes.display_name
    ), "The display_name field should have been changed by the file override"

    # Check that custom values are deep merged
    expected_custom_attrs = (
        md.attributes.custom | overridden_shapefile.overrides.attributes.custom
    )
    assert (
        expected_custom_attrs == calculated_overridden_shapefile_attrs.custom
    ), "The custom attributes should have been merged"


def test_attribute_overrides_at_destination_level():
    md = make_metadata()

    # calc expected
    overridden_shapefile = [f for f in md.files if f.id == "my_overridden_shapefile"][0]
    socrata_dest = [d for d in md.destinations if d.type == "socrata"][0]
    socrata_dest_file_overrides = [
        f.overrides for f in socrata_dest.files if f.id == overridden_shapefile.id
    ][0]

    # calc actual
    calculated_attrs = md.calculate_overridden_attributes(
        file_id=overridden_shapefile.id, destination_id=socrata_dest.id
    )

    assert (
        socrata_dest_file_overrides.attributes.display_name
        == calculated_attrs.display_name
    )

    # Check custom attrs
    expected_custom_attrs = (
        md.attributes.custom
        | overridden_shapefile.overrides.attributes.custom
        | socrata_dest_file_overrides.attributes.custom
    )
    assert expected_custom_attrs == calculated_attrs.custom


def test_column_overrides_at_file_level():
    md = make_metadata()

    overridden_shapefile = [f for f in md.files if f.id == "my_overridden_shapefile"][0]
    calced_overridden_cols = md.calculate_overridden_columns(
        file_id=overridden_shapefile.id
    )

    # Test that the description colum is overridden for the `bbl` column
    actual_overridden_bbl_col = [c for c in calced_overridden_cols if c.id == "bbl"][0]
    expected_overridden_bbl_col = [
        c for c in overridden_shapefile.overrides.overridden_columns if c.id == "bbl"
    ][0]

    assert (
        expected_overridden_bbl_col.description == actual_overridden_bbl_col.description
    ), "The description field on the bbl column should have been overridden at the shapefile level."

    base_bbl_col = [c for c in md.columns if c.id == "bbl"][0]
    shapefile_bbl_col = [
        c for c in overridden_shapefile.overrides.overridden_columns if c.id == "bbl"
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
        for c in overridden_shapefile.overrides.overridden_columns
        if c.id == "borough"
    ][0]
    actual_overridden_borough_col = [
        c for c in calced_overridden_cols if c.id == "borough"
    ][0]
    assert shapefile_borough_col.values == actual_overridden_borough_col.values


def test_column_overrides_at_destination_level():
    md = make_metadata()

    overridden_shapefile = [f for f in md.files if f.id == "my_overridden_shapefile"][0]
    socrata_dest = [d for d in md.destinations if d.type == "socrata"][0]

    calced_overridden_cols = md.calculate_overridden_columns(
        file_id=overridden_shapefile.id, destination_id=socrata_dest.id
    )
    actual_overridden_bbl_col = [c for c in calced_overridden_cols if c.id == "bbl"][0]

    socrata_dest_file_overrides = [
        f.overrides for f in socrata_dest.files if f.id == overridden_shapefile.id
    ][0]
    socrata_dest_bbl_col = [
        c for c in socrata_dest_file_overrides.overridden_columns if c.id == "bbl"
    ][0]

    assert socrata_dest_bbl_col.description == actual_overridden_bbl_col.description
