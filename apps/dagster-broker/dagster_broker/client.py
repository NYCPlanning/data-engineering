"""HTTP client for the dagster-broker API. Standard library only, so it runs anywhere a worker
does (Linux containers, Windows desktops) without extra installs.

Errors map onto the API's contract:

- :class:`NotFound` (404): the queue forgot the item (expired, or Valkey restarted). A
  producer re-enqueues with the same idempotency key; a worker abandons the attempt.
- :class:`Stale` (409): this (item, attempt, worker) no longer holds the lease.
- :class:`Unavailable` (503, 507, connection refused/reset): dagster-broker or its Valkey is
  down or full. Calls in :data:`RETRY_SAFE` are retried with backoff first.
"""

from __future__ import annotations

import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Iterable, Sequence

KEY_HEADER = "X-Dagster-Broker-Key"


class BrokerError(Exception):
    def __init__(self, status: int | None, detail: Any, method: str, path: str):
        self.status = status
        self.detail = detail
        super().__init__(f"{method} {path} -> {status}: {detail}")


class NotFound(BrokerError):
    """404: the queue doesn't know this item (never existed, expired, or restarted)."""


class Stale(BrokerError):
    """409: the fencing token (item, attempt, worker) no longer holds the item."""


class Unavailable(BrokerError):
    """503 / 507 / connection error: try again later."""


class TooManyEvents(BrokerError):
    """429: the item hit its event cap."""


_BY_STATUS = {
    404: NotFound,
    409: Stale,
    429: TooManyEvents,
    503: Unavailable,
    507: Unavailable,
}


class BrokerClient:
    """One method per endpoint. Thread-safe (no shared connection state).

    ``retries``: how many times calls that are safe to repeat are retried on
    :class:`Unavailable`, with jittered exponential backoff capped at ``max_backoff``.
    """

    def __init__(
        self,
        url: str,
        key: str | None = None,
        *,
        timeout: float = 30,
        retries: int = 5,
        max_backoff: float = 10,
    ):
        self.url = url.rstrip("/")
        self.key = key
        self.timeout = timeout
        self.retries = retries
        self.max_backoff = max_backoff

    @classmethod
    def from_env(cls, **kwargs: Any) -> BrokerClient:
        """``DAGSTER_BROKER_URL`` (required) and ``DAGSTER_BROKER_SHARED_KEY`` (optional)."""
        return cls(
            os.environ["DAGSTER_BROKER_URL"], os.environ.get("DAGSTER_BROKER_SHARED_KEY"), **kwargs
        )

    # ---- transport ------------------------------------------------------------------

    def _once(
        self, method: str, path: str, body: Any = None, query: dict | None = None
    ) -> tuple[int, Any]:
        url = self.url + path
        if query:
            url += "?" + urllib.parse.urlencode(
                {k: v for k, v in query.items() if v is not None}, doseq=True
            )
        data = None if body is None else json.dumps(body, default=str).encode()
        req = urllib.request.Request(url, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if self.key:
            req.add_header(KEY_HEADER, self.key)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                detail = json.loads(raw).get("detail", raw.decode(errors="replace"))
            except Exception:
                detail = raw.decode(errors="replace")
            raise _BY_STATUS.get(e.code, BrokerError)(e.code, detail, method, path)
        except (urllib.error.URLError, ConnectionError) as e:
            reason = getattr(e, "reason", e)
            # Refused, reset, DNS failure while dagster-broker restarts: unavailable. A timeout
            # is ambiguous (the request may have been processed), so it's raised as is.
            if isinstance(reason, TimeoutError):
                raise
            raise Unavailable(None, str(reason), method, path) from e

    def _call(
        self,
        method: str,
        path: str,
        body: Any = None,
        query: dict | None = None,
        *,
        retry: bool = True,
    ) -> tuple[int, Any]:
        attempt = 0
        while True:
            try:
                return self._once(method, path, body, query)
            except Unavailable:
                if not retry or attempt >= self.retries:
                    raise
                time.sleep(
                    min(self.max_backoff, 0.5 * 2**attempt) * (0.5 + random.random())
                )
                attempt += 1

    # ---- producer side --------------------------------------------------------------

    def enqueue(
        self,
        *,
        idempotency_key: str | None = None,
        run_id: str | None = None,
        asset_key: str | None = None,
        partition_key: str | None = None,
        required_caps: Sequence[str] = (),
        priority: int = 0,
        max_attempts: int = 3,
        payload: dict | None = None,
    ) -> tuple[dict, bool]:
        """Returns (item, created). Retried only with an idempotency key, which makes a
        repeat return the existing item instead of adding a second one."""
        status, item = self._call(
            "POST",
            "/items",
            {
                "idempotency_key": idempotency_key,
                "run_id": run_id,
                "asset_key": asset_key,
                "partition_key": partition_key,
                "required_caps": list(required_caps),
                "priority": priority,
                "max_attempts": max_attempts,
                "payload": payload or {},
            },
            retry=idempotency_key is not None,
        )
        return item, status == 201

    def get_item(self, item_id: int) -> dict:
        return self._call("GET", f"/items/{item_id}")[1]

    def events(
        self,
        item_id: int,
        *,
        after: str | None = None,
        attempt: int | None = None,
        limit: int = 1000,
    ) -> tuple[list[dict], str | None]:
        """(events oldest first, cursor). Pass the cursor back as ``after``."""
        body = self._call(
            "GET",
            f"/items/{item_id}/events",
            query={"after": after, "attempt": attempt, "limit": limit},
        )[1]
        return body["events"], body["cursor"]

    def cancel(self, item_id: int) -> dict:
        return self._call("POST", f"/items/{item_id}/cancel")[1]

    def demand(self, caps: Iterable[str] = (), min_age_seconds: float = 0) -> dict:
        return self._call(
            "GET",
            "/metrics/demand",
            query={"caps": ",".join(caps), "min_age_seconds": min_age_seconds},
        )[1]

    # ---- worker side ----------------------------------------------------------------

    def claim(
        self,
        *,
        worker_id: str,
        caps: Sequence[str],
        affinity_run_ids: Sequence[str] = (),
    ) -> dict | None:
        """An item, or None if nothing matches. Not retried: a lost response would leave a
        claimed item nobody works on (the reaper requeues it after the lease)."""
        status, item = self._call(
            "POST",
            "/work/claim",
            {
                "worker_id": worker_id,
                "caps": list(caps),
                "affinity_run_ids": list(affinity_run_ids),
            },
            retry=False,
        )
        return None if status == 204 else item

    def heartbeat(self, item_id: int, *, worker_id: str, attempt: int) -> dict:
        """``{lease_until, cancel}``."""
        return self._call(
            "POST",
            f"/work/{item_id}/heartbeat",
            {"worker_id": worker_id, "attempt": attempt},
        )[1]

    def add_events(
        self, item_id: int, *, worker_id: str, attempt: int, events: Sequence[dict]
    ) -> list[str]:
        """Append ``[{kind, data}, ...]`` all or nothing."""
        return self._call(
            "POST",
            f"/work/{item_id}/events",
            {"worker_id": worker_id, "attempt": attempt, "events": list(events)},
        )[1]["ids"]

    def complete(
        self, item_id: int, *, worker_id: str, attempt: int, result: dict | None = None
    ) -> dict:
        return self._call(
            "POST",
            f"/work/{item_id}/complete",
            {"worker_id": worker_id, "attempt": attempt, "result": result or {}},
        )[1]

    def fail(
        self,
        item_id: int,
        *,
        worker_id: str,
        attempt: int,
        error: str | None = None,
        retryable: bool = True,
    ) -> dict:
        return self._call(
            "POST",
            f"/work/{item_id}/fail",
            {
                "worker_id": worker_id,
                "attempt": attempt,
                "error": error,
                "retryable": retryable,
            },
        )[1]
