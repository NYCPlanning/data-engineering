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
