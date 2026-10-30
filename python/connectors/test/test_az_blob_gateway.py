from moto import mock_aws

from dcpy.connectors.az_blob_gateway import AzBlobGateway


def test_uses_gateway_credentials(monkeypatch):
    monkeypatch.setenv("AZ_BLOB_GATEWAY_ENDPOINT", "https://s3.amazonaws.com")
    monkeypatch.setenv("AZ_BLOB_GATEWAY_ACCESS_KEY_ID", "gateway-key")
    monkeypatch.setenv("AZ_BLOB_GATEWAY_SECRET_ACCESS_KEY", "gateway-secret")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "do-key")
    with mock_aws():
        gw = AzBlobGateway("gateway-test")
        client = gw.root.client
        assert client.sess.get_credentials().access_key == "gateway-key"  # type: ignore[attr-defined]
        client.client.create_bucket(Bucket="gateway-test")  # type: ignore[attr-defined]
        (gw.root / "a/b.txt").write_text("hi")
        assert (gw.root / "a/b.txt").read_text() == "hi"
