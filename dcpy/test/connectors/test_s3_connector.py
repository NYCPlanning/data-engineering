from io import BytesIO
from pathlib import Path

import pytest

from dcpy.connectors.s3 import S3Connector
from dcpy.test.conftest import TEST_BUCKET
from dcpy.utils import s3 as s3_utils

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
