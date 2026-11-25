import pytest

from dcpy import configuration


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, ""),
        ("", ""),
        ("/", ""),
        ("ingest", "ingest"),
        ("/ingest/", "ingest"),
        (" ingest/ ", "ingest"),
        ("a/b/", "a/b"),
    ],
)
def test_normalize_root_folder(value, expected):
    assert configuration.normalize_root_folder(value) == expected


@pytest.mark.parametrize(
    "root, expected",
    [(None, "datasets"), ("", "datasets"), ("/ingest/", "ingest/datasets")],
)
def test_join_root_folder(root, expected):
    assert configuration.join_root_folder(root, "datasets") == expected


def test_recipes_bucket_folder_reads_env_at_call_time(monkeypatch):
    monkeypatch.delenv("RECIPES_BUCKET_ROOT_FOLDER", raising=False)
    assert configuration.recipes_bucket_folder("raw") == "raw"
    monkeypatch.setenv("RECIPES_BUCKET_ROOT_FOLDER", "ingest/")
    assert configuration.recipes_bucket_folder("raw") == "ingest/raw"


def test_inbox_location(monkeypatch):
    monkeypatch.delenv("INBOX_ROOT_FOLDER", raising=False)
    monkeypatch.setenv("RECIPES_BUCKET", "edm-recipes")
    key = "qa_app/inbox/x/v1/x.zip"
    assert configuration.inbox_location("edm-private", key) == ("edm-private", key)
    monkeypatch.setenv("INBOX_ROOT_FOLDER", "/inbox/")
    assert configuration.inbox_location("edm-private", key) == (
        "edm-recipes",
        f"inbox/{key}",
    )
    assert configuration.inbox_location("edm-recipes", key) == ("edm-recipes", key)
