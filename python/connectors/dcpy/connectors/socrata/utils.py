import os
from typing import Literal

import requests

SOCRATA_USER = os.getenv("SOCRATA_USER")
SOCRATA_PASSWORD = os.getenv("SOCRATA_PASSWORD")


def _simple_auth() -> tuple[str, str] | None:
    if SOCRATA_USER and SOCRATA_PASSWORD:
        return (SOCRATA_USER, SOCRATA_PASSWORD)
    else:
        return None


def _socrata_request(
    url,
    method: Literal["GET", "PUT", "POST", "PATCH", "DELETE"],
    **kwargs,
) -> requests.Response:
    """Request wrapper to add auth, and raise exceptions on error."""
    request_fn = getattr(requests, method.lower())
    resp: requests.Response = request_fn(url, auth=_simple_auth(), **kwargs)
    resp.raise_for_status()
    return resp
