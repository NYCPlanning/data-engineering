from datetime import datetime
from pathlib import Path
from typing import Literal

import requests

from dcpy.connectors.socrata.configuration import Org, ValidFormat, server_url
from dcpy.utils.logging import logger

from .utils import _socrata_request


def _get_base_url(org: Org) -> str:
    return f"https://{server_url(org)}/api"


def get_metadata_url(org: Org, uid: str) -> str:
    base_url = _get_base_url(org)
    return f"{base_url}/views/{uid}.json"


def get_download_url(org: Org, uid: str, format: ValidFormat) -> str:
    """For a given dataset (org and four-four), get the url to download
    it in a specific format."""
    base_url = _get_base_url(org)
    match format:
        case "csv":
            url = f"{base_url}/views/{uid}/rows.csv"
        case "geojson":
            url = f"{base_url}/geospatial/{uid}?method=export&format=GeoJSON"
        case "shapefile":
            url = f"{base_url}/geospatial/{uid}?method=export&format=Shapefile"
        case format:
            raise Exception(f"Unsupported socrata format: '{format}'")
    return url


def get_query_url(org: Org, uid: str, format: ValidFormat) -> str:
    """For a given dataset, get the url to query it in a specific format. Query results name
    columns by their API field names (route_id rather than the export's "Route ID")."""
    match format:
        case "csv" | "geojson":
            return f"https://{server_url(org)}/resource/{uid}.{format}"
        case format:
            raise Exception(f"Unsupported socrata query format: '{format}'")


DownloadFrom = Literal["export", "query"]

# Rows per query request. Each page is its own response, so a dropped connection only
# costs that page.
QUERY_PAGE_SIZE = 500_000
QUERY_PAGE_ATTEMPTS = 3


def count_rows(org: Org, uid: str, where: str | None = None) -> int:
    """Number of rows, or with `where`, the number matching that SoQL where clause."""
    url = f"https://{server_url(org)}/resource/{uid}.json"
    params = {"$select": "count(*)"}
    if where:
        params["$where"] = where
    resp = _socrata_request(url, "GET", params=params)
    return int(resp.json()[0]["count"])


def get_version(org: Org, uid: str) -> str:
    """For given socrata dataset, get date last updated formatted as a string.
    This is used as a proxy for a 'version' of the dataset."""
    url = get_metadata_url(org, uid)
    resp = _socrata_request(url, "GET")
    return datetime.fromtimestamp(resp.json()["rowsUpdatedAt"]).strftime("%Y%m%d")


def _write_stream(
    response: requests.Response, f, skip_first_line: bool = False
) -> None:
    for chunk in response.iter_content(chunk_size=1024 * 1024):
        if skip_first_line:
            newline = chunk.find(b"\n")
            if newline == -1:
                continue
            chunk = chunk[newline + 1 :]
            skip_first_line = False
        f.write(chunk)


def _download_query_csv(org: Org, uid: str, path: Path, where: str | None) -> None:
    """Page through the query endpoint into one csv, retrying a page whose connection drops.
    Ordering by :id keeps pages from overlapping or skipping rows."""
    total = count_rows(org, uid, where)
    url = get_query_url(org, uid, "csv")
    with open(path, "wb") as f:
        for offset in range(0, max(total, 1), QUERY_PAGE_SIZE):
            params: dict[str, str | int] = {
                "$order": ":id",
                "$limit": QUERY_PAGE_SIZE,
                "$offset": offset,
            }
            if where:
                params["$where"] = where
            page_start = f.tell()
            for attempt in range(1, QUERY_PAGE_ATTEMPTS + 1):
                try:
                    response = _socrata_request(url, "GET", params=params, stream=True)
                    # every page repeats the header row
                    _write_stream(response, f, skip_first_line=offset > 0)
                    break
                except requests.exceptions.RequestException as e:
                    if attempt == QUERY_PAGE_ATTEMPTS:
                        raise
                    logger.warning(f"Retrying rows {offset:,}+ of {uid} after: {e!r}")
                    f.seek(page_start)
                    f.truncate()
            logger.info(
                f"Downloaded {min(offset + QUERY_PAGE_SIZE, total):,} of {total:,} rows of {uid}"
            )


def download(
    org: Org,
    uid: str,
    format: ValidFormat,
    path: Path,
    where: str | None = None,
    download_from: DownloadFrom = "export",
) -> None:
    """Download a dataset from the export, or from the query endpoint, which names columns by
    their API field names and formats dates as ISO. `where` (a SoQL where clause) needs the
    query endpoint."""
    path.parent.mkdir(exist_ok=True, parents=True)
    if where or download_from == "query":
        if format == "csv":
            _download_query_csv(org, uid, path, where)
            return
        # geojson pages can't be appended to each other, so it's one request
        params: dict[str, str | int] = {"$limit": max(count_rows(org, uid, where), 1)}
        if where:
            params["$where"] = where
        response = _socrata_request(
            get_query_url(org, uid, format), "GET", params=params, stream=True
        )
    else:
        response = _socrata_request(
            get_download_url(org, uid, format), "GET", stream=True
        )
    # streamed to disk, so a large download never has to fit in memory
    with open(path, "wb") as f:
        _write_stream(response, f)
