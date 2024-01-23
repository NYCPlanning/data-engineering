from urllib.parse import urlparse


def parse_engine(url: str) -> str:
    """
    url: postgres connection string
    e.g. postgresql://username:password@host:port/database
    """
    result = urlparse(url)
    username = result.username
    password = result.password
    database = result.path[1:]
    hostname = result.hostname
    portnum = result.port
    return f"PG:host={hostname} port={portnum} user={username} dbname={database} password={password}"


def format_url(path: str, subpath: str = "") -> str:
    """
    Adds "vsis3" if [url] is from s3
    - s3://edm-recipes/recipes.csv

    Adds "vsizip" to [url] if [url] is zipped.
    - abcd.zip/abcd.shp
    - abcd.zip/abcd.csv
    - etc

    Adds "vsicurl" if [url] contains http
    - https://rawgithubcontent.come/somerepo/somefile.csv
    """
    subpath = subpath if subpath is not None else ""
    if len(subpath) > 0:
        subpath = subpath[1:] if subpath[0] == "/" else subpath
    path = path[:-1] if path[-1] == "/" else path
    url = path if len(subpath) == 0 else path + "/" + subpath

    if "s3://" in url:
        url = url.replace("s3://", "/vsis3/")

    if ".zip" in url:
        if "http" in url:
            url = "/vsicurl/" + url
        url = "/vsizip/" + url

    return url
