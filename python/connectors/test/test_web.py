from unittest import mock

from dcpy.connectors import web


@mock.patch("dcpy.connectors.web.requests.get")
def test_download_file_streams_to_disk(get, tmp_path):
    get.return_value.iter_content.return_value = [b"a,b\n", b"1,2\n"]

    path = tmp_path / "file.csv"
    web.download_file("https://example.com/file.csv", path)

    assert path.read_bytes() == b"a,b\n1,2\n"
    assert get.call_args.kwargs["stream"] is True
