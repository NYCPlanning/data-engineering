from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pandas as pd

from dcpy.lifecycle import data_loader
from dcpy.lifecycle.builds.models import InputDataset


def _write_parquet(dir: str, n_rows: int) -> Path:
    filepath = Path(dir) / "test.parquet"
    pd.DataFrame({"my col": range(n_rows)}).to_parquet(filepath, index=False)
    return filepath


def test_load_parquet_chunked_replaces_then_appends(monkeypatch):
    # 5 rows at batch size 2 -> 3 batches: replace, append, append
    monkeypatch.setattr(data_loader, "PARQUET_LOAD_BATCH_SIZE", 2)
    pg_client = MagicMock()

    with TemporaryDirectory() as dir:
        filepath = _write_parquet(dir, 5)
        data_loader._load_parquet_chunked(filepath, "my_table", pg_client)

    if_exists_seq = [
        call.kwargs.get("if_exists", call.args[2] if len(call.args) > 2 else None)
        for call in pg_client.insert_dataframe.call_args_list
    ]
    assert if_exists_seq == ["replace", "append", "append"]

    # columns are sanitized before insert, and the pk is added once at the end
    first_df = pg_client.insert_dataframe.call_args_list[0].args[0]
    assert list(first_df.columns) == ["my_col"]
    pg_client.add_pk.assert_called_once_with("my_table", "ogc_fid")


def test_load_parquet_chunked_skips_pk_when_not_requested(monkeypatch):
    monkeypatch.setattr(data_loader, "PARQUET_LOAD_BATCH_SIZE", 2)
    pg_client = MagicMock()

    with TemporaryDirectory() as dir:
        filepath = _write_parquet(dir, 3)
        data_loader._load_parquet_chunked(
            filepath, "my_table", pg_client, include_ogc_fid_col=False
        )

    pg_client.add_pk.assert_not_called()


def test_load_dataset_into_pg_resolves_specific_file_from_directory(tmp_path):
    """A connector's pull() can return a directory rather than a single file (e.g.
    edm.private, which downloads the whole version folder) - custom.filename must
    pick the right one, the same fallback load_dataset_into_duckdb already has.
    Regression test for a real recipe (dcp_cscl_temp_mock_previous_release) that
    crashed with IsADirectoryError before this fallback existed on the pg path."""
    data_dir = tmp_path / "multi_csv_dataset"
    data_dir.mkdir()

    pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]}).to_csv(
        data_dir / "file1.csv", index=False
    )
    pd.DataFrame(
        {"project_id": [101, 102], "title": ["Project A", "Project B"]}
    ).to_csv(data_dir / "projects.csv", index=False)

    dataset = InputDataset(
        id="multi_csv",
        version="v1",
        source="test_source",
        file_type="csv",
        import_as="projects_table",
        custom={"filename": "projects.csv"},
    )
    pg_client = MagicMock()
    pg_client.schema = "test_schema"

    data_loader.load_dataset_into_pg(
        ds=dataset,
        pg_client=pg_client,
        local_dataset_path=data_dir,  # a directory, not a file
    )

    inserted = pg_client.insert_dataframe.call_args.args[0]
    assert list(inserted.columns) == ["project_id", "title"]
    assert inserted["title"].tolist() == ["Project A", "Project B"]


def test_load_df_drops_loader_owned_columns():
    pg_client = MagicMock()
    df = pd.DataFrame(
        {
            "ogc_fid": [1, 2],
            "version": ["25Q4", "26Q2"],
            "data_library_version": ["20240101", "20240101"],
        }
    )

    data_loader._load_df(df, "my_table", pg_client)

    inserted = pg_client.insert_dataframe.call_args.args[0]
    assert list(inserted.columns) == ["version"]
    pg_client.add_pk.assert_called_once_with("my_table", "ogc_fid")


def test_pull_dataset_downloads_a_shared_directory_once(
    create_buckets, tmp_path, monkeypatch
):
    """Two recipe inputs reading different files from one edm.private version share a
    single download, and a later load reuses it."""
    from dcpy.connectors.edm.connectors import PrivateConnector
    from dcpy.connectors.hybrid_pathed_storage import (
        PathedStorageConnector,
        StorageType,
    )
    from dcpy.lifecycle import config, connector_registry
    from dcpy.utils import s3

    from .conftest import TEST_BUCKET

    for name in ["a.csv", "b.csv"]:
        s3.client().put_object(
            Bucket=TEST_BUCKET, Key=f"datasets/shared_dir/26B/{name}", Body=b"x\n1\n"
        )
    private = PrivateConnector.create(
        storage=PathedStorageConnector.from_storage_kwargs(
            conn_type="edm.private",
            storage_backend=StorageType.S3,
            s3_bucket=TEST_BUCKET,
            s3_endpoint_url="https://s3.amazonaws.com",  # where moto intercepts
            root_folder="datasets",
            _validate_root_path=False,
        )
    )
    original = connector_registry.connectors.pull["edm.private"]
    connector_registry.connectors.register(private, conn_type="edm.private")
    monkeypatch.setattr(config, "local_data_path_for_stage", lambda _: tmp_path)

    inputs = [
        InputDataset(
            id="shared_dir",
            version="26B",
            source="edm.private",
            file_type="csv",
            import_as=f"ds_{name}",
            custom={"filename": f"{name}.csv"},
        )
        for name in ["a", "b"]
    ]
    try:
        with patch.object(
            PathedStorageConnector,
            "pull",
            autospec=True,
            side_effect=PathedStorageConnector.pull,
        ) as pull:
            paths = {
                data_loader.pull_dataset(ds, stage="builds.load")
                for _ in range(2)  # two loads
                for ds in inputs
            }
    finally:
        connector_registry.connectors.register(original, conn_type="edm.private")

    assert pull.call_count == 1
    assert paths == {tmp_path / "shared_dir" / "26B"}
