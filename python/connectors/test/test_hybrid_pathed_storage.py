from pathlib import Path

import pytest

from dcpy.connectors.hybrid_pathed_storage import (
    HybridPathedStorage,
    LocalPathWrapper,
    PathedStorageConnector,
    StorageType,
)
from dcpy.utils.s3 import S3ConfigError


def test_copytree_default_replaces_existing_contents(tmp_path: Path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_text("a")

    target = tmp_path / "target"
    target.mkdir()
    (target / "stale.txt").write_text("stale")

    LocalPathWrapper(source).copytree(target)

    assert (target / "a.txt").read_text() == "a"
    assert not (target / "stale.txt").exists()


def test_copytree_merge_keeps_existing_unrelated_files(tmp_path: Path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_text("a")

    target = tmp_path / "target"
    target.mkdir()
    (target / "b.txt").write_text("b")

    LocalPathWrapper(source).copytree(target, merge=True)

    assert (target / "a.txt").read_text() == "a"
    assert (target / "b.txt").read_text() == "b"


def test_copytree_merge_overwrites_same_named_files(tmp_path: Path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_text("new")

    target = tmp_path / "target"
    target.mkdir()
    (target / "a.txt").write_text("old")

    LocalPathWrapper(source).copytree(target, merge=True)

    assert (target / "a.txt").read_text() == "new"


def test_two_merged_copytrees_both_survive(tmp_path: Path):
    """Mirrors uploading dataset_files/ then attachments/ into the same build folder."""
    dataset_files = tmp_path / "dataset_files"
    dataset_files.mkdir()
    (dataset_files / "green_fast_track.gdb.zip").write_text("gdb")

    attachments = tmp_path / "attachments"
    attachments.mkdir()
    (attachments / "source_data_versions.csv").write_text("csv")

    target = tmp_path / "build"
    target.mkdir()

    LocalPathWrapper(dataset_files).copytree(target)
    LocalPathWrapper(attachments).copytree(target, merge=True)

    assert (target / "green_fast_track.gdb.zip").read_text() == "gdb"
    assert (target / "source_data_versions.csv").read_text() == "csv"


def _s3_client(storage: HybridPathedStorage):
    return storage.root_path.client.client  # type: ignore[attr-defined]


@pytest.fixture
def s3_env(monkeypatch):
    monkeypatch.setenv("AWS_S3_ENDPOINT", "https://s3.example.test")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
    monkeypatch.delenv("DCPY_S3_ENDPOINT_DENYLIST", raising=False)


def test_s3_storage_defaults_to_configured_endpoint(s3_env):
    storage = HybridPathedStorage.Factory.s3(s3_bucket="bucket")
    assert _s3_client(storage).meta.endpoint_url == "https://s3.example.test"


def test_s3_storage_explicit_endpoint_and_keys_win(s3_env):
    storage = HybridPathedStorage.from_args(
        storage_backend=StorageType.S3,
        s3_bucket="bucket",
        s3_endpoint_url="https://other.example.test",
        s3_access_key_id="explicit-id",
        s3_secret_access_key="explicit-secret",
    )
    client = _s3_client(storage)
    assert client.meta.endpoint_url == "https://other.example.test"
    creds = client._request_signer._credentials
    assert (creds.access_key, creds.secret_key) == ("explicit-id", "explicit-secret")


def test_s3_storage_is_lazy_without_endpoint(monkeypatch):
    """Connectors are built at import, so a missing endpoint only fails on use."""
    monkeypatch.delenv("AWS_S3_ENDPOINT", raising=False)
    conn = PathedStorageConnector.from_storage_kwargs(
        conn_type="test",
        storage_backend=StorageType.S3,
        s3_bucket="bucket",
        root_folder="datasets",
        _validate_root_path=False,
    )
    assert str(conn.storage) == "s3://s3://bucket/datasets"
    with pytest.raises(S3ConfigError, match="AWS_S3_ENDPOINT is not set"):
        conn.exists("anything")


@pytest.mark.parametrize("explicit", [None, "https://nyc3.digitaloceanspaces.com"])
def test_s3_storage_refuses_denylisted_endpoint(s3_env, monkeypatch, explicit):
    monkeypatch.setenv("AWS_S3_ENDPOINT", "https://nyc3.digitaloceanspaces.com")
    monkeypatch.setenv("DCPY_S3_ENDPOINT_DENYLIST", "nyc3.digitaloceanspaces.com")
    storage = HybridPathedStorage.Factory.s3(
        s3_bucket="bucket", s3_endpoint_url=explicit
    )
    with pytest.raises(S3ConfigError, match="DCPY_S3_ENDPOINT_DENYLIST"):
        storage.root_path
