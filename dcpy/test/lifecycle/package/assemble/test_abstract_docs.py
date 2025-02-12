import pytest

from dcpy.lifecycle.package import abstract_doc
from dcpy.models.product.metadata import OrgMetadata
from dcpy.models.design import elements as de


@pytest.fixture
def org_metadata(package_and_dist_test_resources):
    return package_and_dist_test_resources.org_md()


def _assert_is_title_subtitle_row(row, comp_def):
    assert row.merge_cells
    title_cell, subtitle_row = row.cells[0].value
    assert title_cell.value.startswith(comp_def.title), (
        "The cell should contain the title"
    )
    assert subtitle_row.value == comp_def.subtitle, (
        "The cell should contain the subtitle"
    )


def _assert_is_image_row(row):
    assert row.merge_cells, "The image row should be merged"
    assert 1 == len(row.cells), "the image row should have one cell"
    assert type(row.cells[0].value) is de.Image, "The the value should be an image"


def test_generating_asset_table(org_metadata: OrgMetadata):
    xlsx_artifact = org_metadata.get_packaging_artifacts()[0]

    # Limit this test to just the first component: the dataset.attributes table
    xlsx_artifact.components = xlsx_artifact.components[0:1]

    dataset_attributes_comp_def = xlsx_artifact.components[0]
    assert dataset_attributes_comp_def.id == "dataset_information"

    transit_zones_ds = (
        org_metadata.product("transit_zones")
        .dataset("transit_zones")
        .dataset.model_dump()
    )
    artifacts = abstract_doc.generate_abstract_artifact(
        artifact=xlsx_artifact,
        org_metadata=org_metadata,
        product="transit_zones",
        dataset="transit_zones",
    )
    assert 1 == len(artifacts)

    dataset_attributes_comp = artifacts[0]

    # Dataset attributes component tests
    assert dataset_attributes_comp_def.rows

    image_row, title_subtitle_row, *attr_rows = dataset_attributes_comp.rows
    assert len(dataset_attributes_comp_def.rows) == len(attr_rows)

    _assert_is_image_row(image_row)
    _assert_is_title_subtitle_row(title_subtitle_row, dataset_attributes_comp_def)

    found_field_with_third_party_note = False
    for field_name, row in zip(dataset_attributes_comp_def.rows, attr_rows):
        data_dict_entry = org_metadata.data_dictionary.dataset["attributes"][field_name]
        assert 2 == len(row.cells)
        title_summary_cell, value_cell = row.cells

        # Check the Value
        assert transit_zones_ds["attributes"][field_name] == value_cell.value, (
            "The attribute value should be correctly pulled from the dataset"
        )

        # A little more complicated. Check the title and summary
        assert 2 == len(title_summary_cell.value), (
            "the summary cell should have the correct number of subcells"
        )
        assert data_dict_entry.summary in title_summary_cell.value[0].value, (
            "The summary should be the top line"
        )

        extra_description_cell = title_summary_cell.value[1].value
        assert data_dict_entry.extra_description in extra_description_cell, (
            "The field's extra description should be included"
        )

        if field_name == "attribution_link":
            found_field_with_third_party_note = True
            extra_note = data_dict_entry.custom["third_party_extra"]
            assert extra_note in extra_description_cell, (
                "The extra description from the third party should be included in the description"
            )

    assert found_field_with_third_party_note, (
        "Sanity check that we're actually testing the extra description"
    )


def test_generating_revisions(org_metadata: OrgMetadata):
    xlsx_artifact = org_metadata.get_packaging_artifacts()[0]
    xlsx_artifact.components = xlsx_artifact.components[1:2]

    # Limit this test to just the second component: the dataset.revisions table
    revisions_comp_def = xlsx_artifact.components[0]
    assert revisions_comp_def.id == "revisions"

    transit_zones_ds = (
        org_metadata.product("transit_zones")
        .dataset("transit_zones")
        .dataset.model_dump()
    )
    artifacts = abstract_doc.generate_abstract_artifact(
        artifact=xlsx_artifact,
        org_metadata=org_metadata,
        product="transit_zones",
        dataset="transit_zones",
    )
    assert 1 == len(artifacts)
    component = artifacts[0]
    assert component

    title_subtitle_row, summary_row, column_name_row, *revision_rows = component.rows

    _assert_is_title_subtitle_row(title_subtitle_row, revisions_comp_def)
    assert revisions_comp_def.description in summary_row.cells[0].value
    assert len(revisions_comp_def.columns) == len(column_name_row.cells)  # type: ignore

    assert len(transit_zones_ds["revisions"]) == len(revision_rows), (
        "Sanity check on destructuring above."
    )


def test_column_docs(org_metadata: OrgMetadata):
    xlsx_artifact = org_metadata.get_packaging_artifacts()[0]

    # Limit this test to just the third component: the dataset.columns table
    xlsx_artifact.components = xlsx_artifact.components[2:3]
    column_comp_def = xlsx_artifact.components[0]
    assert column_comp_def.id == "columns"

    transit_zones_ds = (
        org_metadata.product("transit_zones")
        .dataset("transit_zones")
        .dataset.model_dump()
    )
    artifacts = abstract_doc.generate_abstract_artifact(
        artifact=xlsx_artifact,
        org_metadata=org_metadata,
        product="transit_zones",
        dataset="transit_zones",
    )
    assert 1 == len(artifacts)
    component = artifacts[0]
    assert component

    title_subtitle_row, col_names_row, col_descriptions_row, *col_rows = component.rows

    _assert_is_title_subtitle_row(title_subtitle_row, column_comp_def)

    assert len(transit_zones_ds["columns"]) == len(col_rows), (
        "There should be the correct number of column rows"
    )

    assert column_comp_def.columns
    assert "values" in column_comp_def.columns
    values_col_index = column_comp_def.columns.index("values")

    # The last row is the borough, which has standardized values.
    # There should be a better way to filter for this...
    values_cell_sample = col_rows[-1].cells[values_col_index]
    # sanity check on that though...
    assert "Manhattan" in values_cell_sample.value
    assert abstract_doc.MONOSPACED_FONT == values_cell_sample.style.font.name, (
        "The values table requires a monospaced font, otherwise the columns won't line up!"
    )
