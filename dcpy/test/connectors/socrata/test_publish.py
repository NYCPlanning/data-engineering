import itertools
import pytest
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, call

import dcpy.models.product.dataset.metadata as md
from dcpy.connectors.socrata import publish
from dcpy.connectors.edm.open_data_nyc import OpenDataConnector
from socrata.output_schema import OutputSchema

open_data_conn = OpenDataConnector()
open_data_conn.SOCRATA_DOMAIN = "mock.data.cityofnewyork.us"


@pytest.fixture
def package_path(package_and_dist_test_resources):
    return package_and_dist_test_resources.PACKAGE_PATH_ASSEMBLED


@pytest.fixture
def metadata(package_path: Path):
    return md.Metadata.from_path(package_path / "metadata.yml")


def test_push_dataset_raises_exceptions_no_four_four(
    metadata: md.Metadata, package_path: Path
):
    non_socrata_dest = [
        d
        for d in metadata.destinations
        if d.type == "socrata" and not d.custom.get("four_four")
    ][0]
    # No four-four for socrata
    with pytest.raises(Exception, match=publish.ERROR_MISSING_FOUR_FOUR):
        open_data_conn.distribute_dataset(
            metadata=metadata,
            dataset_destination_id=non_socrata_dest.id,
            dataset_package_path=Path("./"),
        )


def test_socrata_destination_file_uses(metadata: md.Metadata):
    socrata_dest = publish.SocrataDestination(metadata, destination_id="socrata")
    assert socrata_dest.attachment_ids == {"readme", "my_zip"}
    assert socrata_dest.dataset_file_id == "data_file"
    assert socrata_dest.is_unparsed_dataset


def test_socrata_destination_file_uses_multiple_dataset_files(metadata: md.Metadata):
    destination = "socrata"
    for f in metadata.get_destination(destination).files:
        f.custom["destination_use"] = publish.DestinationUses.dataset_file

    with pytest.raises(Exception, match=publish.ERROR_WRONG_DATASET_FILE_COUNT):
        open_data_conn.distribute_dataset(
            metadata=metadata,
            dataset_destination_id=destination,
            dataset_package_path=Path("./"),
        )


@mock.patch("dcpy.connectors.socrata.publish.Dataset")
def test_flow_happy_path(mock_dataset, metadata: md.Metadata):
    mock_dataset_instance = mock_dataset.return_value

    mock_revision = MagicMock(name="mock_revision")
    mock_dataset_instance.create_replace_revision.return_value = mock_revision

    destination = "socrata"
    dataset_file_id = "data_file"

    open_data_conn.distribute_dataset(
        metadata=metadata,
        dataset_destination_id=destination,
        dataset_package_path=Path("./"),
        publish=True,
    )

    mock_dataset_instance.create_replace_revision.assert_called_once()

    assert mock_revision.upload_attachment.call_count == 2, (
        "The correct number of attachments should have uploaded"
    )

    expected_attachment_upload_calls = [
        call(dest_file_name="readme.pdf", path=Path("attachments/readme.pdf")),
        call(dest_file_name="my_overriden_name.zip", path=Path("attachments/my.zip")),
    ]
    mock_revision.upload_attachment.assert_has_calls(
        expected_attachment_upload_calls, any_order=True
    )

    mock_revision.patch_metadata.assert_called_once()
    patch_metadata_args = mock_revision.patch_metadata.call_args.kwargs["metadata"]
    overridden_dataset_name = [
        f
        for f in metadata.get_destination(destination).files
        if f.id == dataset_file_id
    ][0].dataset_overrides.attributes.display_name
    assert patch_metadata_args.name == overridden_dataset_name, (
        "The Metadata's name should be correct in the patch md call"
    )
    assert (
        patch_metadata_args.metadata["rowLabel"] == metadata.attributes.each_row_is_a
    ), "`Each Row Is A: `should be set correctly."

    mock_revision.push_blob.assert_called_once()

    mock_revision.apply.assert_called_once()
    mock_dataset_instance.discard_open_revisions.assert_called_once()


@mock.patch("dcpy.connectors.socrata.publish.Dataset")
def test_flow_shapefile(mock_dataset, metadata: md.Metadata):
    mock_dataset_instance = mock_dataset.return_value

    mock_revision = MagicMock(name="mock_revision")
    mock_dataset_instance.create_replace_revision.return_value = mock_revision

    destination = "socrata"
    dataset_file_id = "data_file"

    metadata.get_file_and_overrides(dataset_file_id).file.type = "shapefile"
    metadata.get_destination(destination).custom["is_unparsed_dataset"] = False
    for f in metadata.get_destination(destination).files:
        f.custom["destination_use"] = (
            publish.DestinationUses.dataset_file
            if f.id == dataset_file_id
            else publish.DestinationUses.attachment
        )

    open_data_conn.distribute_dataset(
        metadata=metadata,
        dataset_destination_id=destination,
        dataset_package_path=Path("./"),
        publish=True,
    )
    mock_revision.push_shp.assert_called_once()


@mock.patch("dcpy.connectors.socrata.publish.Dataset")
def test_flow_csv(mock_dataset, metadata: md.Metadata):
    mock_dataset_instance = mock_dataset.return_value

    mock_revision = MagicMock(name="mock_revision")
    mock_dataset_instance.create_replace_revision.return_value = mock_revision

    destination = "socrata"
    dataset_file_id = "data_file"

    metadata.get_file_and_overrides(dataset_file_id).file.type = "csv"
    metadata.get_destination(destination).custom["is_unparsed_dataset"] = False
    for f in metadata.get_destination(destination).files:
        f.custom["destination_use"] = (
            publish.DestinationUses.dataset_file
            if f.id == dataset_file_id
            else publish.DestinationUses.attachment
        )

    open_data_conn.distribute_dataset(
        metadata=metadata,
        dataset_destination_id=destination,
        dataset_package_path=Path("./"),
        publish=True,
    )

    mock_revision.push_csv.assert_called_once()


@mock.patch("dcpy.connectors.socrata.publish.Dataset")
def test_overriden_dataset_file_name(mock_dataset, metadata: md.Metadata):
    """
    We'll often have files named differently in our package vs in our destination.
    (for example, a version is appended. package: my_shp.zip -> socrata: my_shp_24b.zip

    This tests the case when we have a blob dataset (an xlsx) that needs to be pushed
    with a different filename.
    """
    mock_dataset_instance = mock_dataset.return_value

    mock_revision = MagicMock(name="mock_revision")
    mock_dataset_instance.create_replace_revision.return_value = mock_revision

    destination = "socrata"
    dataset_file_id = "data_file"
    overridden_dataset_file_name = "my_overridden_shp.xlsx"

    # Set a filename override at the socrata destination
    file_overrides = [
        f
        for f in metadata.get_destination(destination).files
        if f.id == dataset_file_id
    ][0].file_overrides
    file_overrides.filename = overridden_dataset_file_name

    open_data_conn.distribute_dataset(
        metadata=metadata,
        dataset_destination_id=destination,
        dataset_package_path=Path("./"),
        publish=True,
    )

    package_dataset_file_path = (
        Path("dataset_files")
        / metadata.get_file_and_overrides(dataset_file_id).file.filename
    )
    mock_revision.push_blob.assert_called_once_with(
        package_dataset_file_path, dest_filename=overridden_dataset_file_name
    )


def test_column_reconcilation_missing_columns_error():
    """Test that our metadata columns reconcile with Socrata columns.
    Specifically, the case where the md pushed to socrata contains an
    unaccounted for columns, and vice versa.
    """

    def _column(field_name) -> dict:
        return {"field_name": field_name, "transform": {"failure_details": None}}

    in_theirs_and_ours = _column("abc_api")
    in_theirs_missing_from_ours = _column("missing_from_ours")
    rds = publish.RevisionDataSource(
        output_schema=OutputSchema(
            None,
            {
                "resource": {
                    "output_columns": [in_theirs_and_ours, in_theirs_missing_from_ours],
                    "sort_bys": {},
                },
                "links": {},
            },
        ),
    )

    in_ours_missing_theirs = md.DatasetColumn(
        id="ours_id", name="ours_name", custom={"api_name": "in_ours_missing_theirs"}
    )
    try:
        rds.calculate_pushed_col_metadata(
            our_columns=[
                in_ours_missing_theirs,
                md.DatasetColumn(  # should be matched
                    id="abc_id",
                    name="abc_name",
                    custom={"api_name": in_theirs_and_ours["field_name"]},
                ),
            ]
        )
    except Exception as e:
        err = e
    assert err, "An Exception should have been thrown"
    err_data = err.args[0]

    assert rds.MISSING_COLUMN_ERROR == err_data["type"], (
        "The Error type should be correct."
    )

    assert [in_ours_missing_theirs.custom.get("api_name")] == err_data[
        "missing_from_theirs"
    ], "Their missing columns should be calculated correctly"

    assert [in_theirs_missing_from_ours["field_name"]] == err_data[
        "missing_from_ours"
    ], "Our missing columns should be calculated correctly"


def test_column_reconcilation_happy_path():
    """Test that our metadata columns reconcile with Socrata columns."""

    our_columns = [
        md.DatasetColumn(
            id="a_id",
        ),
        md.DatasetColumn(
            id="b_id",
            name="b_name",
        ),
        md.DatasetColumn(
            id="c_id",
            name="c_name",
            custom={"api_name": "c_api"},
        ),
    ]

    their_column_names_ordered = [
        our_columns[0].id,
        our_columns[1].id,
        our_columns[2].custom["api_name"],
    ]

    # cycle through every possible disordering of their columns
    for their_column_names_disordered in itertools.permutations(
        their_column_names_ordered
    ):
        cols = [
            {
                "id": f"{n}_soc_id",
                "field_name": n,
                "transform": {"failure_details": None},
                "position": i,
            }
            for i, n in enumerate(their_column_names_disordered)
        ]
        mock = MagicMock()
        mock.transform = lambda a: a
        rds = publish.RevisionDataSource(
            output_schema=OutputSchema(
                None,
                {"resource": {"output_columns": cols, "sort_bys": {}}, "links": {}},
                parent=mock,
            ),
        )

        output_schema = rds.push_socrata_column_metadata(our_columns)
        print(output_schema)
        assert their_column_names_ordered == [
            c["field_name"]
            for c in sorted(
                output_schema["output_columns"],
                key=lambda c: c["position"],
            )
        ], "The socrata columns should have correct positions"


@mock.patch("dcpy.connectors.socrata.publish.Dataset")
def test_metadata_only_deployments(mock_dataset, metadata: md.Metadata):
    mock_dataset_instance = mock_dataset.return_value

    mock_revision = MagicMock(name="mock_revision")
    mock_dataset_instance.create_replace_revision.return_value = mock_revision

    destination = "socrata"

    open_data_conn.distribute_dataset(
        metadata=metadata,
        dataset_destination_id=destination,
        dataset_package_path=Path("./"),
        publish=True,
        metadata_only=True,
    )

    mock_dataset_instance.create_replace_revision.assert_called_once()

    assert not mock_revision.push_csv.called
    assert not mock_revision.push_xlsx.called
    assert not mock_revision.push_blob.called
    assert not mock_revision.push_shp.called
