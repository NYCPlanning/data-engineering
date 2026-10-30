"""Standard-library HTTP client for the dagster-broker API.

404 -> :class:`NotFound` (the queue forgot the item), 409 -> :class:`Stale` (the lease is
lost), 503 or connection refused/reset -> :class:`Unavailable`, retried with backoff
unless ``retry=False``.
"""

import json
import os
import random
import time
import urllib.error
import urllib.request


class BrokerError(Exception):
    def __init__(self, status, detail):
        self.status = status
        super().__init__(f"{status}: {detail}")


class NotFound(BrokerError):
    pass


class Stale(BrokerError):
    pass


class Unavailable(BrokerError):
    pass


ERRORS = {404: NotFound, 409: Stale, 503: Unavailable}


class BrokerClient:
    """Defaults to ``DAGSTER_BROKER_URL`` and ``DAGSTER_BROKER_SHARED_KEY``."""

    def __init__(self, url=None, key=None, retries=5):
        self.url = (url or os.environ["DAGSTER_BROKER_URL"]).rstrip("/")
        self.key = key or os.environ.get("DAGSTER_BROKER_SHARED_KEY")
        self.retries = retries

    def get(self, path: str):
        return self.call("GET", path)[1]

    def post(self, path: str, body: dict | None = None, retry=True):
        return self.call("POST", path, body or {}, retry)[1]

    def call(self, method, path, body=None, retry=True) -> tuple[int, object]:
        for n in range(self.retries + 1):
            try:
                return self._once(method, path, body)
            except Unavailable:
                if not retry or n == self.retries:
                    raise
                time.sleep(min(10, 0.5 * 2**n) * (0.5 + random.random()))
        raise AssertionError("unreachable")

    def _once(self, method, path, body):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.url + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.key:
            req.add_header("X-Dagster-Broker-Key", self.key)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raise ERRORS.get(e.code, BrokerError)(
                e.code, e.read().decode(errors="replace")
            )
        except (urllib.error.URLError, ConnectionError) as e:
            # A timeout is ambiguous (the request may have landed), so it isn't retried.
            if isinstance(getattr(e, "reason", e), TimeoutError):
                raise
            raise Unavailable(None, e) from e
