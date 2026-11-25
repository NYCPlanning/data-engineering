from io import BytesIO
from pathlib import Path

import pytest

from dcpy import configuration
from dcpy.connectors.s3 import S3Connector
from dcpy.utils import s3 as s3_utils

from .conftest import RECIPES_BUCKET, TEST_BUCKET

FILE = "test.csv"
SUBFOLDER = "test/"
PREFIX = "datasets/"
KEY = f"{PREFIX}{SUBFOLDER}{FILE}"


@pytest.fixture()
def tmp_file(tmp_path: Path) -> Path:
    file = tmp_path / FILE
    file.touch()
    return file


@pytest.fixture()
def s3_object(create_buckets):
    s3_utils.upload_file_obj(BytesIO(), TEST_BUCKET, KEY, "private")


class TestConnector:
    connector = S3Connector(bucket=TEST_BUCKET)
    connector_prefix = S3Connector(bucket=TEST_BUCKET, prefix=PREFIX)

    def test_bucket(self):
        with pytest.raises(Exception, match="No bucket"):
            S3Connector()._bucket()
        with pytest.raises(Exception, match="Connector 's3' is configured"):
            self.connector._bucket("other bucket")

    def test_subfolder(self, s3_object):
        assert self.connector.get_subfolders("fake_folder") == []
        assert self.connector.get_subfolders("datasets") == ["test"]

    def test_exists(self, s3_object):
        assert not self.connector.exists(KEY + "a")
        assert self.connector.exists(KEY)

    def test_pull(self, s3_object, tmp_path: Path):
        file = tmp_path / FILE
        assert not file.exists()
        self.connector.pull(key=KEY, destination_path=tmp_path)
        assert file.exists()

    def test_pull_prefix(self, s3_object, tmp_path: Path):
        file = tmp_path / FILE
        assert not file.exists()
        self.connector_prefix.pull(key=f"{SUBFOLDER}{FILE}", destination_path=tmp_path)
        assert file.exists()

    def test_push(self, tmp_file: Path, create_buckets):
        self.connector.push(key=KEY, filepath=tmp_file)
        assert s3_utils.object_exists(TEST_BUCKET, KEY)

    def test_push_prefix(self, tmp_file: Path, create_buckets):
        self.connector_prefix.push(key=f"{SUBFOLDER}{FILE}", filepath=tmp_file)
        assert s3_utils.object_exists(TEST_BUCKET, KEY)


class TestInboxRemap:
    """Templates name `bucket: edm-private`; INBOX_ROOT_FOLDER sends those reads elsewhere."""

    connector = S3Connector()
    key = "dob_cofos/v1/dob_cofos.csv"

    @pytest.fixture
    def buckets(self, create_buckets, monkeypatch):
        monkeypatch.delenv("INBOX_ROOT_FOLDER", raising=False)
        s3_utils.client().create_bucket(Bucket="edm-private")
        s3_utils.upload_file_obj(
            BytesIO(b"private"), "edm-private", self.key, "private"
        )
        s3_utils.upload_file_obj(
            BytesIO(b"inbox"), RECIPES_BUCKET, f"inbox/{self.key}", "private"
        )

    def _pull(self, tmp_path: Path, bucket: str) -> bytes:
        path = self.connector.pull(
            key=self.key, bucket=bucket, destination_path=tmp_path
        )["path"]
        return Path(path).read_bytes()

    def test_off_by_default(self, buckets, tmp_path: Path):
        assert self._pull(tmp_path, "edm-private") == b"private"
        assert self.connector.get_subfolders("dob_cofos/", bucket="edm-private") == [
            "v1"
        ]

    @pytest.mark.parametrize("root", ["inbox", "/inbox/"])
    def test_on_reads_recipes_inbox(self, buckets, monkeypatch, tmp_path: Path, root):
        monkeypatch.setenv("INBOX_ROOT_FOLDER", root)
        assert self._pull(tmp_path, "edm-private") == b"inbox"
        assert self.connector.exists(self.key, bucket="edm-private")
        assert self.connector.get_subfolders("dob_cofos/", bucket="edm-private") == [
            "v1"
        ]

    def test_other_buckets_untouched(self, buckets, monkeypatch):
        monkeypatch.setenv("INBOX_ROOT_FOLDER", "inbox")
        s3_utils.upload_file_obj(BytesIO(), RECIPES_BUCKET, self.key, "private")
        assert configuration.inbox_location(RECIPES_BUCKET, self.key) == (
            RECIPES_BUCKET,
            self.key,
        )
        assert self.connector.exists(self.key, bucket=RECIPES_BUCKET)
