from moto import mock_aws

from dcpy.connectors.az_blob_gateway import AzBlobGateway


def test_copy_and_delete_prefix(monkeypatch):
    monkeypatch.setenv("AZ_BLOB_GATEWAY_ENDPOINT", "https://s3.amazonaws.com")
    monkeypatch.setenv("AZ_BLOB_GATEWAY_ACCESS_KEY_ID", "gateway-key")
    monkeypatch.setenv("AZ_BLOB_GATEWAY_SECRET_ACCESS_KEY", "gateway-secret")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "do-key")
    with mock_aws():
        gw = AzBlobGateway("gateway-test")
        client = gw.root.client
        assert client.sess.get_credentials().access_key == "gateway-key"  # type: ignore[attr-defined]
        client.client.create_bucket(Bucket="gateway-test")  # type: ignore[attr-defined]
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
