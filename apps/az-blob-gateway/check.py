"""Smoke check for the az-blob-gateway S3 API. One PASS/FAIL line per step; exits 1 on failure.

python apps/az-blob-gateway/check.py --endpoint http://localhost:7070 --bucket local
"""

import argparse
import hashlib
import os
import sys
import tempfile
import uuid

import boto3
from boto3.s3.transfer import TransferConfig

MB = 1024 * 1024


class Sha256Writer:
    def __init__(self):
        self.h = hashlib.sha256()

    def write(self, b):
        self.h.update(b)
        return len(b)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", default=os.environ.get("AWS_S3_ENDPOINT"))
    p.add_argument("--access-key", default=os.environ.get("AWS_ACCESS_KEY_ID"))
    p.add_argument("--secret-key", default=os.environ.get("AWS_SECRET_ACCESS_KEY"))
    p.add_argument("--bucket", default=os.environ.get("GATEWAY_BUCKET"))
    p.add_argument("--size", type=int, default=100, help="multipart upload size, MB")
    a = p.parse_args()
    if not (a.endpoint and a.bucket):
        p.error(
            "--endpoint and --bucket (or AWS_S3_ENDPOINT, GATEWAY_BUCKET) are required"
        )

    s3 = boto3.client(
        "s3",
        endpoint_url=a.endpoint,
        aws_access_key_id=a.access_key,
        aws_secret_access_key=a.secret_key,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )
    prefix = f"_check/{uuid.uuid4().hex}"
    big, small, copy = f"{prefix}/big", f"{prefix}/small", f"{prefix}/copy"
    failed = False

    def step(name, fn):
        nonlocal failed
        try:
            print(f"PASS {name}: {fn()}", flush=True)
        except Exception as e:
            failed = True
            print(f"FAIL {name}: {type(e).__name__}: {e}", flush=True)

    def put_get_delete():
        body = os.urandom(1024)
        s3.put_object(Bucket=a.bucket, Key=small, Body=body)
        assert s3.get_object(Bucket=a.bucket, Key=small)["Body"].read() == body, (
            "body differs"
        )
        s3.delete_object(Bucket=a.bucket, Key=small)
        return "1 KB"

    def multipart():
        h = hashlib.sha256()
        with tempfile.TemporaryFile() as f:
            for _ in range(a.size):
                chunk = os.urandom(MB)
                h.update(chunk)
                f.write(chunk)
            f.seek(0)
            s3.upload_fileobj(f, a.bucket, big, Config=TransferConfig())
        out = Sha256Writer()
        s3.download_fileobj(a.bucket, big, out)
        assert out.h.hexdigest() == h.hexdigest(), "sha256 differs"
        return f"{a.size} MB, sha256 {h.hexdigest()[:12]}"

    def copy_object():
        s3.copy_object(
            Bucket=a.bucket, Key=copy, CopySource={"Bucket": a.bucket, "Key": big}
        )
        size = s3.head_object(Bucket=a.bucket, Key=copy)["ContentLength"]
        assert size == a.size * MB, f"copy is {size} bytes"
        return f"{size} bytes"

    def cleanup():
        keys = s3.list_objects_v2(Bucket=a.bucket, Prefix=prefix).get("Contents", [])
        for k in keys:
            s3.delete_object(Bucket=a.bucket, Key=k["Key"])
        return f"{len(keys)} deleted"

    step("put/get/delete", put_get_delete)
    step("multipart upload + sha256", multipart)
    step("copy_object", copy_object)
    step("cleanup", cleanup)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
