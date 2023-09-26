from datetime import date
from pathlib import Path

from library import (
    aws_access_key_id,
    aws_s3_bucket,
    aws_s3_endpoint,
    aws_secret_access_key,
    pp,
)
from library.s3 import S3

from . import console

s3 = S3(aws_access_key_id, aws_secret_access_key, aws_s3_endpoint, aws_s3_bucket)
version = "2021-01-22"


def test_s3_upload_file():
    # Make sure file doesn't already exist
    if s3.exists(f"test/{version}/test.yml"):
        s3.rm(f"test/{version}/test.yml")

    assert not s3.exists(f"test/{version}/test.yml")

    # Attempt to upload to {version}/test.yml
    s3.put(
        key=f"test/{version}/test.yml", path=f"{Path(__file__).parent}/data/socrata.yml"
    )

    # Make sure file now exists
    assert s3.exists(f"test/{version}/test.yml")
    s3.rm(f"test/{version}/test.yml")


def test_s3_ls():
    console.print(s3.ls("test", detail=True))
    assert True


def test_s3_info():
    print("\nTest info...")
    if not s3.exists(f"test/{version}/test.yml"):
        s3.put(
            key=f"test/{version}/test.yml",
            path=f"{Path(__file__).parent}/data/socrata.yml",
            metadata={"Version": version},
        )

    # Make sure file exists before trying to pull info
    assert s3.exists(f"test/{version}/test.yml")

    # Pull file info
    info = s3.info(key=f"test/{version}/test.yml")
    if info:
        console.print(info)
    else:
        console.print("File exists, but no info retrieved.")
    s3.rm(f"test/{version}/test.yml")


def test_s3_cp():
    print("\nTest cp...")
    if not s3.exists(f"test/{version}/test.yml"):
        s3.upload_file(
            name="test",
            version=version,
            path=f"{Path(__file__).parent}/data/socrata.yml",
        )
    # Make sure the {version} version exists, but the latest version doesn't
    assert s3.exists(f"test/{version}/test.yml")

    # Copy {version} to latest
    s3.cp(source_key=f"test/{version}/test.yml", dest_key="test/latest/test.yml")

    # Make sure the {version} version exists, and the latest version doesn't
    assert s3.exists(f"test/{version}/test.yml")
    assert s3.exists("test/latest/test.yml")
    s3.rm(f"test/{version}/test.yml", f"test/latest/test.yml")


def test_s3_mv():
    print("\nTest mv...")
    if not s3.exists(f"test/{version}/test.yml"):
        s3.upload_file(
            name="test",
            version=version,
            path=f"{Path(__file__).parent}/data/socrata.yml",
        )
    if s3.exists("test/moved/test.yml"):
        s3.rm("test/moved/test.yml")
    # Make sure that the {version} version exists, but the moved version doesn't
    assert s3.exists(f"test/{version}/test.yml")
    assert not s3.exists("test/moved/test.yml")

    # Move the {version} version to moved
    s3.mv(source_key=f"test/{version}/test.yml", dest_key="test/moved/test.yml")

    # Make sure that the moved version exists, but the {version} version doesn't
    assert s3.exists("test/moved/test.yml")
    assert not s3.exists(f"test/{version}/test.yml")
    s3.rm("test/moved/test.yml")


def test_s3_rm():
    print("\nTest rm and clean up test directory...")
    # Make sure that the moved version and latest version exist prior to rm
    if not s3.exists("test/moved/test.yml"):
        s3.put(
            key="test/moved/test.yml", path=f"{Path(__file__).parent}/data/socrata.yml"
        )
    if not s3.exists("test/latest/test.yml"):
        s3.put(
            key="test/latest/test.yml", path=f"{Path(__file__).parent}/data/socrata.yml"
        )

    assert s3.exists("test/moved/test.yml")
    assert s3.exists("test/latest/test.yml")
    # Remove files
    s3.rm(*["test/moved/test.yml", "test/latest/test.yml"])
    # Make sure the file no longer exists
    assert not s3.exists("test/moved/test.yml")
    assert not s3.exists("test/latest/test.yml")
