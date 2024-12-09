import os
import requests
from typing import Literal

SOCRATA_USER = os.getenv("SOCRATA_USER")
SOCRATA_PASSWORD = os.getenv("SOCRATA_PASSWORD")


def _simple_auth():
    return (SOCRATA_USER, SOCRATA_PASSWORD)


def _socrata_request(
    url,
    method: Literal["GET", "PUT", "POST", "PATCH", "DELETE"],
    **kwargs,
) -> dict:
    """Request wrapper to add auth, and raise exceptions on error."""
    request_fn = getattr(requests, method.lower())
    resp = request_fn(url, auth=_simple_auth(), **kwargs)
    if not resp.ok:
        raise Exception(resp.text)
    return resp.json()
