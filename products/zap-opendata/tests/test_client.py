def test_access_token(test_client):
    assert isinstance(test_client.access_token, str)


def test_request_header(test_client):
    assert isinstance(test_client.request_header, dict)
