from pathlib import Path

import pytest

from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType
from dcpy.connectors.ingest_datastore import Connector as IngestDatastoreConnector
from dcpy.test.lifecycle.ingest.shared import DOWNSTREAM_DATASET_1, TEST_OUTPUT


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
