from pathlib import Path

import pytest

from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.connectors.ingest_datastore import Connector as IngestDatastoreConnector
from shared import DOWNSTREAM_DATASET_1, TEST_OUTPUT  # type: ignore[import-not-found]


@pytest.fixture
def connector(tmp_path):
    return IngestDatastoreConnector(
        storage=PathedStorageConnector.from_storage_kwargs(
            conn_type="test_ingest_datastore.local",
            storage_backend=StorageType.LOCAL,
            local_dir=Path(tmp_path),
            _validate_root_path=True,
        )
    )


def test_push_latest_replaces_earlier_files(
    connector: IngestDatastoreConnector, tmp_path: Path
):
    """`latest` mirrors one archive, so a prior archive's files must not survive.

    A library-era `.sql` left beside the new parquet wins file-type resolution, which
    silently points consumers at the older archive.
    """
    ds = DOWNSTREAM_DATASET_1
    latest = tmp_path / ds.id / "latest"
    latest.mkdir(parents=True)
    (latest / f"{ds.id}.sql").write_text("-- archived by library")
    (latest / "config.yml").write_text("dataset:\n  name: leftover\n")

    connector.push_versioned(
        key=ds.id, version=ds.version, config=ds, filepath=TEST_OUTPUT, latest=True
    )

    assert sorted(p.name for p in latest.iterdir()) == [
        "config.json",
        TEST_OUTPUT.name,
    ], "latest should hold only what this archive wrote"


def test_push_latest_is_independent_of_the_versioned_archive(
    connector: IngestDatastoreConnector, tmp_path: Path
):
    """`latest` can be repointed without rewriting the versioned copy.

    Ingest skips archival when a version is already present and unchanged, so
    refreshing `latest` has to work on its own or the pointer goes stale.
    """
    ds = DOWNSTREAM_DATASET_1
    latest = tmp_path / ds.id / "latest"
    latest.mkdir(parents=True)
    (latest / f"{ds.id}.sql").write_text("-- archived by library")

    config_path = tmp_path / "config.json"
    config_path.write_text("{}")
    connector.push_latest(ds.id, filepath=TEST_OUTPUT, config_path=config_path)

    assert sorted(p.name for p in latest.iterdir()) == ["config.json", TEST_OUTPUT.name]
    assert not (tmp_path / ds.id / ds.version).exists(), (
        "refreshing latest should not create the versioned archive"
    )


def test_overwrite_sets_library_archive_aside(
    connector: IngestDatastoreConnector, tmp_path: Path
):
    """Overwriting a library-era version moves library's files to `<version>_library/`.

    Builds resolve `latest` to a version and pick a file type from that version's
    folder, so a library `.sql` left beside the new parquet wins.
    """
    ds = DOWNSTREAM_DATASET_1
    versioned = tmp_path / ds.id / ds.version
    versioned.mkdir(parents=True)
    library_files = {
        "config.json": '{"dataset": {"name": "leftover"}}',
        "config.yml": "dataset:\n  name: leftover\n",
        f"{ds.id}.parquet": "library parquet",
        f"{ds.id}.sql": "-- archived by library",
    }
    for name, text in library_files.items():
        (versioned / name).write_text(text)

    connector.push_versioned(
        key=ds.id, version=ds.version, config=ds, filepath=TEST_OUTPUT, overwrite=True
    )

    assert sorted(p.name for p in versioned.iterdir()) == [
        "config.json",
        TEST_OUTPUT.name,
    ]
    set_aside = tmp_path / ds.id / f"{ds.version}_library"
    assert {p.name: p.read_text() for p in set_aside.iterdir()} == library_files


def test_overwrite_replaces_an_ingest_archive(
    connector: IngestDatastoreConnector, tmp_path: Path
):
    ds = DOWNSTREAM_DATASET_1
    connector.push_versioned(
        key=ds.id, version=ds.version, config=ds, filepath=TEST_OUTPUT
    )
    versioned = tmp_path / ds.id / ds.version
    (versioned / "stray.csv").write_text("a\n1\n")

    connector.push_versioned(
        key=ds.id, version=ds.version, config=ds, filepath=TEST_OUTPUT, overwrite=True
    )

    assert sorted(p.name for p in versioned.iterdir()) == [
        "config.json",
        TEST_OUTPUT.name,
    ]
    assert not (tmp_path / ds.id / f"{ds.version}_library").exists()


def test_pull_versioned_reuses_local_copy(
    connector: IngestDatastoreConnector, tmp_path: Path
):
    src = tmp_path / "ds" / "v1" / "data.csv"
    src.parent.mkdir(parents=True)
    src.write_text("a\n1\n")
    dest = tmp_path / "local"

    first = connector.pull_versioned("ds", "v1", dest, filename="data.csv")
    assert first["path"].read_text() == "a\n1\n"
    assert not (dest / "data.csv.partial").exists()

    src.write_text("changed\n")  # a second pull must not go back to storage
    second = connector.pull_versioned("ds", "v1", dest, filename="data.csv")
    assert second["path"] == first["path"]
    assert second["path"].read_text() == "a\n1\n"


ARCHIVE_KEYS = [
    "ds_a/20260101/ds_a.parquet",
    "ds_a/20260101/config.json",
    "ds_a/20260201/config.json",
    "ds_a/latest/config.json",
    "ds_a/20250101_library/config.json",
    "ds_a/partial/ds_a.parquet",  # no config.json: not archived yet
    "ds_b/26c/config.json",
    "stray.txt",
]


def _assert_all_versions(archived: dict):
    assert {k: sorted(v) for k, v in archived.items()} == {
        "ds_a": ["20260101", "20260201"],
        "ds_b": ["26c"],
    }


def test_list_archived_versions_local(
    connector: IngestDatastoreConnector, tmp_path: Path
):
    for key in ARCHIVE_KEYS:
        (tmp_path / key).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / key).write_text("{}")
    archived = connector.list_archived_versions()
    _assert_all_versions(archived)
    assert archived["ds_b"]["26c"] is not None


@pytest.mark.parametrize("root_folder", ["", "ingest/datasets"])
def test_list_archived_versions_s3(create_buckets, root_folder):
    from dcpy.utils import s3

    from .conftest import TEST_BUCKET

    prefix = f"{root_folder}/" if root_folder else ""
    for key in ARCHIVE_KEYS + ["elsewhere/ds_c/1/config.json"]:
        s3.client().put_object(Bucket=TEST_BUCKET, Key=prefix + key, Body=b"{}")
    if root_folder:
        # a sibling folder sharing the prefix's first characters isn't listed
        s3.client().put_object(
            Bucket=TEST_BUCKET, Key="ingest/datasets_old/ds_x/1/config.json", Body=b"{}"
        )
    connector = IngestDatastoreConnector(
        storage=PathedStorageConnector.from_storage_kwargs(
            conn_type="test_ingest_datastore.s3",
            storage_backend=StorageType.S3,
            s3_bucket=TEST_BUCKET,
            root_folder=root_folder,
            _validate_root_path=False,
        )
    )
    archived = (
        connector.list_archived_versions()
    )  # elsewhere/ds_c is a level too deep to count
    _assert_all_versions(archived)
    assert archived["ds_b"]["26c"] is not None
    assert sorted(connector.list_archived_versions("ds_a")["ds_a"]) == [
        "20260101",
        "20260201",
    ]


def test_list_archived_versions_of_one_key(
    connector: IngestDatastoreConnector, tmp_path: Path
):
    for key in ARCHIVE_KEYS:
        (tmp_path / key).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / key).write_text("{}")
    assert list(connector.list_archived_versions("ds_b")) == ["ds_b"]
    assert sorted(connector.list_archived_versions("ds_a")["ds_a"]) == [
        "20260101",
        "20260201",
    ]
    assert connector.list_archived_versions("missing") == {}
