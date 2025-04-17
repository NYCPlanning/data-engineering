import pandas as pd
from pathlib import Path
import pytest
from shutil import SameFileError
from unittest import mock

from dcpy.models.lifecycle.ingest import Source, LocalFileSource, S3Source, GisDataset
from dcpy.utils import s3
from dcpy.connectors import web
from dcpy.lifecycle.ingest import extract

from dcpy.test.conftest import mock_request_get, PUBLISHING_BUCKET
from dcpy.test.connectors.test_esri import mock_query_layer
from .shared import TEST_DATASET_NAME, FAKE_VERSION, SOURCE_FILENAMES

web.get_df = mock.MagicMock(return_value=pd.DataFrame())  # type: ignore


def setup(source: Source, filename: str, file_system: Path) -> Source:
    match source:
        case LocalFileSource():
            tmp_file = file_system / source.path
            tmp_file.parent.mkdir(exist_ok=True)
            tmp_file.touch()
            source.path = tmp_file
        case GisDataset():
            s3.client().put_object(
                Bucket=PUBLISHING_BUCKET,
                Key=f"datasets/{TEST_DATASET_NAME}/{FAKE_VERSION}/{filename}",
            )
        case S3Source():
            s3.client().put_object(
                Bucket=source.bucket,
                Key=source.key,
            )
    return source


@mock.patch("requests.get", side_effect=mock_request_get)
@mock.patch("requests.post", side_effect=mock_query_layer)
@pytest.mark.parametrize(("source", "filename"), SOURCE_FILENAMES)
def test_download_file(get, post, source, filename, create_buckets, tmp_path):
    setup(source, filename, tmp_path)
    path = extract.download_file_from_source(source, FAKE_VERSION, tmp_path)
    print(tmp_path / filename)
    print(path)
    assert (tmp_path / filename).exists()


def test_download_file_invalid_source():
    with pytest.raises(Exception, match="No registered connector"):
        extract.download_file_from_source(
            mock.MagicMock(type="fake"), FAKE_VERSION, Path(".")
        )


def _temp_test_local_already_exists(tmp_path):
    filename = "dummy.txt"
    source = LocalFileSource(type="local_file", path=Path(filename))
    setup(source, filename, tmp_path)
    with pytest.raises(SameFileError):
        extract.download_file_from_source(source, filename, FAKE_VERSION, tmp_path)
