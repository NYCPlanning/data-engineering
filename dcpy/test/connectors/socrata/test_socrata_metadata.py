from dcpy.connectors.socrata import metadata as md

from dcpy.test.connectors.socrata.resources import sample_metadata


def test_creating_dcp_metadata():
    dcp_md = md.make_dcp_metadata(sample_metadata.mih_metadata)

    # Destination Checks
    assert len(dcp_md.destinations) == 1, "There should be one distribution"
    dest = dcp_md.destinations[0]
    assert (
        dest.four_four == sample_metadata.MIH_FOUR_FOUR
    ), "The distribution should have the correct id"
    assert dest.type == "socrata", "The distribution should be of type socrata"

    # Column Checks
    assert len(dcp_md.columns) == len(
        sample_metadata.mih_metadata["columns"]
    ), "The number of columns should match"

    boro_col = next(col for col in dcp_md.columns if col.name == "boro")
    assert boro_col.display_name == "Boro"

    assert (
        len(boro_col.values) == 5
    ), f"The boro column should have registered 5 standardized values"

    # Dataset Checks
    assert (
        len(dcp_md.dataset_package.attachments) == 1
    ), "There should be the correct number of attachments"
    assert dcp_md.dataset_package.attachments[0] == sample_metadata.MIH_ATTACHMENT_NAME
    assert len(dcp_md.dataset_package.datasets) == 1, "There should be one dataset"
