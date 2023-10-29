import pytest
from pathlib import Path
import os
import shutil
import uuid  # for generating a unique directory name
import csv
import pandas as pd
from dcpy.connectors.edm import publishing

TEST_ACL = "bucket-owner-read"

TEST_PRODUCT_NAME = "test-product"
TEST_BUILD = "build-branch"
TEST_VERSION = "v001"

TEST_DATA_DIR = Path(__file__).resolve().parent / str(uuid.uuid4())
TEST_FILE = "file.csv"
TEST_DATA = [{"drink": "coffee", "like": "yes"}, {"drink": "tea", "like": "maybe"}]
TEST_DATA_FIELDS = ["drink", "like"]
TEST_VERSION_FILE = "version.txt"

DraftKey = publishing.DraftKey(product=TEST_PRODUCT_NAME, build=TEST_BUILD)
PublishKey = publishing.PublishKey(product=TEST_PRODUCT_NAME, version=TEST_VERSION)


@pytest.fixture(scope="module")
def create_temp_filesystem():
    """Creates a new directory with files and removes it upon test completion.
    The directory is created and removed once per script ('module' scope)."""
    try:
        TEST_DATA_DIR.mkdir(parents=False, exist_ok=False)
    except Exception as err:
        print("❌ Unable to create test dir.")
        raise err

    try:
        txt_file_path = os.path.join(TEST_DATA_DIR, TEST_VERSION_FILE)
        csv_file_path = os.path.join(TEST_DATA_DIR, TEST_FILE)
        with open(txt_file_path, "w") as txt_file:
            txt_file.write(TEST_VERSION)
        with open(csv_file_path, "w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=TEST_DATA_FIELDS)
            writer.writeheader()
            writer.writerows(TEST_DATA)
        print("Created test filesystem ✅")
    except Exception as exc:
        print("❌ Exception occured while creating test files. Deleting test dir...")
        shutil.rmtree(TEST_DATA_DIR)
        raise exc

    yield

    try:
        shutil.rmtree(TEST_DATA_DIR)
        print("Removed test filesystem ✅")
    except Exception as e:
        f"❌ Unable to remove {TEST_DATA_DIR} after running tests"
        raise e


def test_bucket_empty(create_bucket):
    """Sanity check there are no draft or publish versions from previous tests
    or actual data."""
    assert publishing.get_draft_builds(product=TEST_PRODUCT_NAME) == []
    assert publishing.get_published_versions(product=TEST_PRODUCT_NAME) == []


def test_upload(create_bucket, create_temp_filesystem):
    publishing.upload(output_path=TEST_DATA_DIR, draft_key=DraftKey, acl=TEST_ACL)
    assert TEST_BUILD in publishing.get_draft_builds(product=TEST_PRODUCT_NAME)
    assert publishing.get_version(product_key=DraftKey) == TEST_VERSION


def test_publish(create_bucket, create_temp_filesystem):
    publishing.publish(
        draft_key=DraftKey, acl=TEST_ACL, publishing_version=None, keep_draft=False
    )
    assert publishing.get_draft_builds(product=DraftKey.product) == []
    assert [TEST_VERSION] == publishing.get_published_versions(
        product=TEST_PRODUCT_NAME
    )

    test_data = pd.DataFrame(data=TEST_DATA, columns=TEST_DATA_FIELDS)
    published_data = publishing.read_csv(product_key=PublishKey, filepath=TEST_FILE)
    assert published_data.equals(test_data)
