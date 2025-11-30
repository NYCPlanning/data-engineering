from dcpy.connectors.socrata import metadata as md
from dcpy.test.connectors.socrata.resources import sample_metadata


def test_creating_dcp_metadata():
    """Tests generating DCP metadata from a raw response from a Socrata Page."""

    dcp_md = md.make_dcp_metadata(sample_metadata.mih_metadata)

    # Destination Checks
    assert len(dcp_md.destinations) == 1, "There should be one distribution"
    dest = dcp_md.destinations[0]
    assert dest.custom["four_four"] == sample_metadata.MIH_FOUR_FOUR, (
        "The distribution should have the correct id"
    )
    assert dest.type == "socrata", "The distribution should be of type socrata"

    # Column Checks
    assert len(dcp_md.columns) == len(sample_metadata.mih_metadata["columns"]), (
        "The number of columns should match"
    )

    boro_col = [col for col in dcp_md.columns if col.id == "boro"][0]
    assert boro_col.name == "Boro"
    assert boro_col.values

    assert len(boro_col.values) == 5, (
        "The boro column should have registered 5 standardized values"
    )

    # Dataset Checks
    assert len(dcp_md.files) == 2, "Two files should be found"
    attachments = [
        f
        for f in dcp_md.files
        if f.file.filename == sample_metadata.MIH_ATTACHMENT_NAME
    ]
    assert len(attachments) == 1, "There should be the correct number of attachments"

    dataset_files = [
        f
        for f in dcp_md.files
        if f.file.filename != sample_metadata.MIH_ATTACHMENT_NAME
    ]
    assert len(dataset_files) == 1, "There should be one dataset"
