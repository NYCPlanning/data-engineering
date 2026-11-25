from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from dcpy.utils import s3

from .conftest import (
    PUBLISHING_BUCKET,
    RECIPES_BUCKET,
    TEST_BUCKET,
    TEST_BUCKETS,
)

TEST_DIR_NAME_1 = "test-dir-1"
TEST_DIR_NAME_2 = "test-dir-2"
TEST_FILE_NAME = "test-file"

TEST_OBJECTS = [
    f"{TEST_DIR_NAME_1}/{TEST_FILE_NAME}",
    f"{TEST_DIR_NAME_1}/{TEST_DIR_NAME_2}/{TEST_FILE_NAME}",
    f"{TEST_DIR_NAME_2}/{TEST_FILE_NAME}",
]


@pytest.fixture()
def put_test_objects(create_buckets):
    for obj in TEST_OBJECTS:
        s3.client().put_object(Bucket=TEST_BUCKET, Key=obj)


# assert that the default mock credentials work
def test_client_create(aws_credentials):
    s3.client()


def test_client_invalid_endpoint(aws_credentials, monkeypatch):
    monkeypatch.setenv("AWS_S3_ENDPOINT", "testing")
    with pytest.raises(ValueError, match="Invalid endpoint: testing"):
        s3.client()


@pytest.mark.parametrize("value", [None, ""])
def test_missing_endpoint_fails_loudly(aws_credentials, monkeypatch, value):
    if value is None:
        monkeypatch.delenv("AWS_S3_ENDPOINT", raising=False)
    else:
        monkeypatch.setenv("AWS_S3_ENDPOINT", value)
    for make_client in (s3.client, s3.pyarrow_fs, s3.resolve_endpoint):
        with pytest.raises(s3.S3ConfigError, match="AWS_S3_ENDPOINT is not set"):
            make_client()


def test_missing_credentials_fail_loudly(aws_credentials, monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID")
    with pytest.raises(s3.S3ConfigError, match="AWS_ACCESS_KEY_ID"):
        s3.client()


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://nyc3.digitaloceanspaces.com",
        "https://NYC3.digitaloceanspaces.com/",
        "nyc3.digitaloceanspaces.com",
        "https://edm-recipes.nyc3.digitaloceanspaces.com",
    ],
)
def test_denylisted_endpoint_refused(aws_credentials, monkeypatch, endpoint):
    monkeypatch.setenv("AWS_S3_ENDPOINT", endpoint)
    monkeypatch.setenv(
        "DCPY_S3_ENDPOINT_DENYLIST", " example.org , nyc3.digitaloceanspaces.com"
    )
    for make_client in (s3.client, s3.pyarrow_fs):
        with pytest.raises(s3.S3ConfigError, match="DCPY_S3_ENDPOINT_DENYLIST"):
            make_client()


def test_denylist_applies_to_explicit_endpoint(aws_credentials, monkeypatch):
    monkeypatch.setenv("AWS_S3_ENDPOINT", "http://gateway:7070")
    monkeypatch.setenv("DCPY_S3_ENDPOINT_DENYLIST", "nyc3.digitaloceanspaces.com")
    assert s3.client().meta.endpoint_url == "http://gateway:7070"
    with pytest.raises(s3.S3ConfigError, match="DCPY_S3_ENDPOINT_DENYLIST"):
        s3.client(endpoint="https://nyc3.digitaloceanspaces.com")


def test_denylist_matches_hosts_not_substrings(aws_credentials, monkeypatch):
    monkeypatch.setenv("AWS_S3_ENDPOINT", "https://notnyc3.digitaloceanspaces.com")
    monkeypatch.setenv("DCPY_S3_ENDPOINT_DENYLIST", "nyc3.digitaloceanspaces.com")
    assert s3.resolve_endpoint() == "https://notnyc3.digitaloceanspaces.com"


@pytest.mark.parametrize(
    "endpoint",
    ["https://nyc3.digitaloceanspaces.com", "http://gateway.example.test:7070"],
)
def test_client_uses_configured_endpoint(aws_credentials, monkeypatch, endpoint):
    monkeypatch.setenv("AWS_S3_ENDPOINT", endpoint)
    monkeypatch.delenv("DCPY_S3_ENDPOINT_DENYLIST", raising=False)
    s3_client = s3.client()
    assert s3_client.meta.endpoint_url == endpoint
    assert s3_client.meta.config.s3["addressing_style"] == "path"


def test_client_explicit_args_win(aws_credentials, monkeypatch):
    monkeypatch.setenv("AWS_S3_ENDPOINT", "https://nyc3.digitaloceanspaces.com")
    s3_client = s3.client(
        endpoint="http://gateway:7070",
        access_key_id="explicit-id",
        secret_access_key="explicit-secret",
    )
    assert s3_client.meta.endpoint_url == "http://gateway:7070"
    creds = s3_client._request_signer._credentials  # type: ignore[attr-defined]
    assert (creds.access_key, creds.secret_key) == ("explicit-id", "explicit-secret")


def test_client_passes_ca_bundle(aws_credentials, monkeypatch):
    seen = {}
    monkeypatch.setattr(s3.boto3, "client", lambda *args, **kwargs: seen.update(kwargs))
    s3.client(verify="/etc/ssl/city-ca.pem")
    assert seen["verify"] == "/etc/ssl/city-ca.pem"
    s3.client()
    assert seen["verify"] is None


def test_list_buckets(create_buckets):
    """Tests listing s3 buckets."""
    buckets = s3.list_buckets()
    assert buckets == TEST_BUCKETS


@pytest.mark.parametrize(
    "prefix, expected_num_objects",
    [
        ("", 3),
        (TEST_DIR_NAME_1, 2),
        (TEST_DIR_NAME_2, 1),
    ],
)
def test_list_objects(create_buckets, put_test_objects, prefix, expected_num_objects):
    """Tests total number of objects with given prefix."""
    actual_objects = s3.list_objects(bucket=TEST_BUCKET, prefix=prefix)
    assert len(actual_objects) == expected_num_objects


@pytest.mark.parametrize(
    "prefix, expected_files",
    [
        ("", set(TEST_OBJECTS)),
        (TEST_DIR_NAME_1, {TEST_FILE_NAME, f"{TEST_DIR_NAME_2}/{TEST_FILE_NAME}"}),
    ],
)
def test_get_filenames(create_buckets, put_test_objects, prefix, expected_files):
    """Tests total number of objects with given prefix."""
    actual_objects = s3.get_filenames(bucket=TEST_BUCKET, prefix=prefix)
    assert actual_objects == expected_files


@pytest.mark.parametrize(
    "prefix, index, expected_folders",
    [
        ("", 1, [TEST_DIR_NAME_1, TEST_DIR_NAME_2]),
        ("", 2, [f"{TEST_DIR_NAME_1}/{TEST_DIR_NAME_2}"]),
        (TEST_DIR_NAME_1, 1, [TEST_DIR_NAME_2]),
        (TEST_DIR_NAME_1, 2, []),
        (TEST_DIR_NAME_2, 1, []),
    ],
)
def test_get_subfolders(
    create_buckets, put_test_objects, prefix, index, expected_folders
):
    """Tests total number of folders with given prefix and depth (index)."""
    actual_objects = s3.get_subfolders(bucket=TEST_BUCKET, prefix=prefix, index=index)
    assert actual_objects == expected_folders


def test_upload_file(create_buckets):
    RESOURCES_DIR = Path(__file__).parent / "resources"
    test_file_key = TEST_DIR_NAME_1 + "/" + TEST_FILE_NAME
    test_file_path = Path(RESOURCES_DIR / "data_wkb.csv")
    s3.upload_file(
        bucket=TEST_BUCKET, path=test_file_path, key=test_file_key, acl="public-read"
    )
    assert s3.object_exists(TEST_BUCKET, test_file_key)


def test_upload_file_obj(create_buckets):
    test_file_key = TEST_DIR_NAME_1 + "/" + TEST_FILE_NAME
    RESOURCES_DIR = Path(__file__).parent / "resources"
    with open(RESOURCES_DIR / "data_wkb.csv", "rb") as f:
        file_obj = BytesIO(f.read())
    s3.upload_file_obj(
        file_obj=file_obj,
        bucket=TEST_BUCKET,
        key=test_file_key,
        acl="public-read",
    )
    assert s3.object_exists(TEST_BUCKET, test_file_key)


def test_copy_folder_via_download(create_buckets, put_test_objects):
    s3.copy_folder_via_download(
        TEST_BUCKET, RECIPES_BUCKET, TEST_DIR_NAME_1, TEST_DIR_NAME_1, acl="public-read"
    )
    assert s3.get_filenames(RECIPES_BUCKET, "") == set(TEST_OBJECTS[:2])


@patch("dcpy.utils.s3.copy_folder_via_download")
def test_copy_folder_within_bucket(copy: MagicMock, create_buckets, put_test_objects):
    new_folder = "new_folder"
    s3.copy_folder(TEST_BUCKET, TEST_DIR_NAME_1, new_folder, acl="public-read")
    assert not copy.called
    assert s3.get_filenames(TEST_BUCKET, new_folder) == s3.get_filenames(
        TEST_BUCKET, TEST_DIR_NAME_1
    )


mock_regions = {
    TEST_BUCKET: "nyc3b",
    RECIPES_BUCKET: "nyc3b",
    PUBLISHING_BUCKET: "nyc3d",
}


@patch("dcpy.utils.s3.copy_folder_via_download")
@patch("dcpy.utils.s3.get_bucket_region", side_effect=lambda b: mock_regions[b])
def test_copy_folder_within_region(
    bucket_region, copy, create_buckets, put_test_objects
):
    s3.copy_folder(
        TEST_BUCKET,
        TEST_DIR_NAME_1,
        TEST_DIR_NAME_1,
        acl="public-read",
        target_bucket=RECIPES_BUCKET,
    )
    assert not copy.called
    assert s3.get_filenames(RECIPES_BUCKET, "") == set(TEST_OBJECTS[:2])


@patch("dcpy.utils.s3.copy_folder_via_download")
@patch("dcpy.utils.s3.get_bucket_region", side_effect=lambda b: mock_regions[b])
def test_copy_folder_across_regions_calls_manual_copy(
    bucket_region, copy, create_buckets, put_test_objects
):
    s3.copy_folder(
        TEST_BUCKET,
        TEST_DIR_NAME_1,
        TEST_DIR_NAME_1,
        acl="public-read",
        target_bucket=PUBLISHING_BUCKET,
    )
    assert copy.called
    assert len(s3.get_filenames(PUBLISHING_BUCKET, "")) == 0


@patch("dcpy.utils.s3.get_bucket_region", side_effect=lambda b: mock_regions[b])
def test_copy_folder_across_regions(bucket_region, create_buckets, put_test_objects):
    s3.copy_folder(
        TEST_BUCKET,
        TEST_DIR_NAME_1,
        TEST_DIR_NAME_1,
        acl="public-read",
        target_bucket=PUBLISHING_BUCKET,
    )
    assert s3.get_filenames(PUBLISHING_BUCKET, "") == set(TEST_OBJECTS[:2])


@patch("dcpy.utils.s3.get_bucket_region", side_effect=lambda b: mock_regions[b])
def test_copy_folder_across_regions_keep_existing(
    bucket_region, create_buckets, put_test_objects
):
    existing = f"{TEST_DIR_NAME_1}/existing-file"
    s3.client().put_object(Bucket=PUBLISHING_BUCKET, Key=existing)
    s3.copy_folder(
        TEST_BUCKET,
        TEST_DIR_NAME_1,
        TEST_DIR_NAME_1,
        acl="public-read",
        target_bucket=PUBLISHING_BUCKET,
        keep_existing=True,
    )
    assert s3.get_filenames(PUBLISHING_BUCKET, "") == set(TEST_OBJECTS[:2]) | {existing}


MISSING_KEY = "no-such-folder/no-such-file"


@pytest.mark.parametrize(
    "call",
    [
        lambda: s3.download_file(TEST_BUCKET, MISSING_KEY, Path("unused")),
        lambda: s3.get_file(TEST_BUCKET, MISSING_KEY),
        lambda: s3.get_file_as_stream(TEST_BUCKET, MISSING_KEY),
        lambda: s3.get_file_as_text(TEST_BUCKET, MISSING_KEY),
        lambda: s3.get_metadata(TEST_BUCKET, MISSING_KEY),
        lambda: s3.get_custom_metadata(TEST_BUCKET, MISSING_KEY),
    ],
    ids=[
        "download_file",
        "get_file",
        "get_file_as_stream",
        "get_file_as_text",
        "get_metadata",
        "get_custom_metadata",
    ],
)
def test_missing_object_names_itself(create_buckets, call):
    """A missing object reports its bucket and key, not a bare botocore 404."""
    with pytest.raises(FileNotFoundError, match=f"s3://{TEST_BUCKET}/{MISSING_KEY}"):
        call()


def test_object_exists_false_when_missing(create_buckets):
    assert not s3.object_exists(TEST_BUCKET, MISSING_KEY)


@patch("dcpy.utils.s3.client")
def test_object_exists_raises_on_access_denied(mock_client):
    """A permissions failure must not be reported as a missing object."""
    mock_client.return_value.head_object.side_effect = ClientError(
        {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject"
    )
    with pytest.raises(ClientError):
        s3.object_exists(TEST_BUCKET, MISSING_KEY)


def test_client_uses_path_style_for_custom_endpoint(aws_credentials, monkeypatch):
    monkeypatch.setenv("AWS_S3_ENDPOINT", "https://gateway.example.test")
    s3_client = s3.client()
    assert s3_client.meta.endpoint_url == "https://gateway.example.test"
    assert s3_client.meta.config.s3["addressing_style"] == "path"


@patch("dcpy.utils.s3.client")
def test_get_bucket_region_digital_ocean(mock_client, monkeypatch):
    monkeypatch.setenv("AWS_S3_ENDPOINT", "https://nyc3.digitaloceanspaces.com")
    mock_client.return_value.head_bucket.return_value = {
        "ResponseMetadata": {"RequestId": "tx00000-0065-nyc3c"}
    }
    assert s3.get_bucket_region(TEST_BUCKET) == "nyc3c"


@pytest.mark.parametrize(
    "headers, expected",
    [({"x-amz-bucket-region": "eastus2"}, "eastus2"), ({}, "us-east-1")],
)
@patch("dcpy.utils.s3.client")
def test_get_bucket_region_other_endpoint(mock_client, headers, expected, monkeypatch):
    """Off DigitalOcean the request id says nothing about region, so it isn't parsed."""
    monkeypatch.setenv("AWS_S3_ENDPOINT", "https://gateway.example.test")
    mock_client.return_value.head_bucket.return_value = {
        "ResponseMetadata": {"RequestId": "random-request-id", "HTTPHeaders": headers}
    }
    mock_client.return_value.meta.region_name = "us-east-1"
    assert s3.get_bucket_region(TEST_BUCKET) == expected
