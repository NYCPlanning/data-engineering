import re
from pathlib import Path

import pytest

from dcpy.utils import s3, s3_metadata

from .conftest import TEST_BUCKET

AZURE_SAFE = r"^[a-z_][a-z0-9_]*$"


@pytest.mark.parametrize(
    "key, expected",
    [
        ("date-created", "date_created"),
        ("Date-Created", "date_created"),
        ("date_created", "date_created"),
        ("Commit", "commit"),
        ("run-url", "run_url"),
        ("1st", "_1st"),
    ],
)
def test_normalize_key(key, expected):
    assert s3_metadata.normalize_key(key) == expected


def test_for_write_is_azure_safe():
    written = s3_metadata.for_write(
        {"date-created": "2026-01-01", "run-url": "u", "Commit": "abc", "n": 1}
    )
    assert written == {
        "date_created": "2026-01-01",
        "run_url": "u",
        "commit": "abc",
        "n": "1",
    }
    for key in written:
        assert re.match(AZURE_SAFE, key)


def test_for_write_rejects_colliding_keys():
    with pytest.raises(ValueError, match="collide"):
        s3_metadata.for_write({"date-created": "a", "date_created": "b"})


def test_standard_metadata_is_azure_safe(monkeypatch):
    monkeypatch.setenv("CI", "true")
    monkeypatch.setattr(s3_metadata.git, "commit_hash", lambda: "abc")
    monkeypatch.setattr(s3_metadata.git, "action_url", lambda: "https://run")
    md = s3_metadata.standard()
    assert set(md) == {"date_created", "commit", "run_url"}


@pytest.mark.parametrize(
    "stored",
    [
        # what DigitalOcean archives carry
        {"date-created": "2026-01-01", "commit": "abc", "run-url": "u"},
        # what Azure (through versitygw) hands back for the new keys
        {"Date_created": "2026-01-01", "Commit": "abc", "Run_url": "u"},
        # what's written now
        {"date_created": "2026-01-01", "commit": "abc", "run_url": "u"},
    ],
)
def test_for_read_accepts_every_spelling(stored):
    md = s3_metadata.for_read(stored)
    for key in ("date-created", "date_created", "Date-Created", "DATE_CREATED"):
        assert md[key] == "2026-01-01"
        assert md.get(key) == "2026-01-01"
        assert key in md
    assert md.get(s3_metadata.COMMIT) == "abc"
    assert md.get("run-url") == md.get(s3_metadata.RUN_URL) == "u"
    assert md.get("missing", "default") == "default"
    assert md == {"date-created": "2026-01-01", "commit": "abc", "run-url": "u"}


def test_round_trip_through_s3(create_buckets, tmp_path: Path):
    path = tmp_path / "f.txt"
    path.write_text("x")
    s3.upload_file(TEST_BUCKET, path, "f.txt", "private", metadata={"my-key": "v"})

    raw = s3.client().head_object(Bucket=TEST_BUCKET, Key="f.txt")["Metadata"]
    for key in raw:
        assert re.match(AZURE_SAFE, key), raw
    assert {"my_key", "date_created"} <= set(raw)

    custom = s3.get_metadata(TEST_BUCKET, "f.txt").custom
    assert custom["my-key"] == "v"
    assert custom.get("date-created") == raw["date_created"]

    # copy_file merges the caller's keys over the existing ones, whatever their spelling
    s3.copy_file(
        TEST_BUCKET,
        "f.txt",
        "g.txt",
        "private",
        metadata={"date-created": "2020-01-01"},
    )
    copied = s3.get_custom_metadata(TEST_BUCKET, "g.txt")
    assert copied["date_created"] == "2020-01-01"
    assert copied["my_key"] == "v"


def test_reads_old_do_keys_through_s3(create_buckets):
    s3.client().put_object(
        Bucket=TEST_BUCKET,
        Key="old.txt",
        Body=b"x",
        Metadata={"date-created": "2024-05-01T00:00:00", "run-url": "u"},
    )
    custom = s3.get_metadata(TEST_BUCKET, "old.txt").custom
    assert custom.get(s3_metadata.DATE_CREATED) == "2024-05-01T00:00:00"
    assert custom.get("date-created") == "2024-05-01T00:00:00"
    assert custom.get(s3_metadata.RUN_URL) == "u"
