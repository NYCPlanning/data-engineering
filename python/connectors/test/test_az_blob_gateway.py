import boto3
from moto import mock_aws

from dcpy.connectors.az_blob_gateway import AzBlobGateway


def test_copy_and_delete_prefix(aws_credentials, monkeypatch):
    monkeypatch.setenv("AWS_S3_ENDPOINT", "https://s3.amazonaws.com")
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="gateway-test")
        gw = AzBlobGateway("gateway-test")
        big = b"x" * (9 * 2**20)  # above boto3's default 8 MB multipart threshold
        (gw.root / "staging/a/1/big.bin").write_bytes(big)
        (gw.root / "staging/a/1/sub/small.txt").write_text("hi")
        (gw.root / "staging/b/other.txt").write_text("other")

        calls = []
        events = gw._copy_root.client.s3.meta.client.meta.events  # type: ignore[attr-defined]
        events.register("before-call.s3.*", lambda model, **_: calls.append(model.name))

        assert gw.copy_prefix("staging/a/1/", "final/") == 2
        assert calls.count("CopyObject") == 2 and "UploadPartCopy" not in calls
        assert (gw.root / "final/big.bin").read_bytes() == big
        assert (gw.root / "final/sub/small.txt").read_text() == "hi"
        assert gw.copy_prefix("staging/missing/", "final/") == 0

        gw.delete_prefix("staging/")
        assert not (gw.root / "staging").exists()
        gw.delete_prefix("staging/")
