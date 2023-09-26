from library.utils import format_url


def test_format_url():
    test_cases = [
        dict(
            path="https://website.com/path/file.csv",
            subpath="",
            expected="https://website.com/path/file.csv",
        ),
        dict(
            path="https://website.com/path/file.zip",
            subpath="",
            expected="/vsizip//vsicurl/https://website.com/path/file.zip",
        ),
        dict(
            path="https://website.com/path/file.zip",
            subpath="file.shp",
            expected="/vsizip//vsicurl/https://website.com/path/file.zip/file.shp",
        ),
        dict(
            path="https://website.com/path/file.zip",
            subpath="/file.shp",
            expected="/vsizip//vsicurl/https://website.com/path/file.zip/file.shp",
        ),
        dict(
            path="https://website.com/path/file.zip/",
            subpath="file.shp",
            expected="/vsizip//vsicurl/https://website.com/path/file.zip/file.shp",
        ),
        dict(
            path="s3://bucket/path/file.csv",
            subpath="",
            expected="/vsis3/bucket/path/file.csv",
        ),
        dict(
            path="s3://bucket/path/file.zip",
            subpath="",
            expected="/vsizip//vsis3/bucket/path/file.zip",
        ),
        dict(
            path="local/directory/file.zip",
            subpath="",
            expected="/vsizip/local/directory/file.zip",
        ),
    ]
    for case in test_cases:
        output = format_url(case["path"], case["subpath"])
        assert output == case["expected"]
