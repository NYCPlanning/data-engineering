from pathlib import Path

from dcpy import configuration
from dcpy.connectors.hybrid_pathed_storage import (
    HybridPathedStorage,
    LocalPathWrapper,
    StorageType,
)


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


def _s3_endpoint(storage: HybridPathedStorage) -> str:
    return storage.root_path.client.client.meta.endpoint_url  # type: ignore[attr-defined]


def test_s3_storage_defaults_to_configured_endpoint(monkeypatch):
    monkeypatch.setattr(configuration, "AWS_S3_ENDPOINT", "https://s3.example.test")
    storage = HybridPathedStorage.Factory.s3(s3_bucket="bucket")
    assert _s3_endpoint(storage) == "https://s3.example.test"


def test_s3_storage_explicit_endpoint_wins(monkeypatch):
    monkeypatch.setattr(configuration, "AWS_S3_ENDPOINT", "https://s3.example.test")
    storage = HybridPathedStorage.from_args(
        storage_backend=StorageType.S3,
        s3_bucket="bucket",
        s3_endpoint_url="https://other.example.test",
    )
    assert _s3_endpoint(storage) == "https://other.example.test"
