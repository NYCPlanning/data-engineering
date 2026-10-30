"""Worker side of Dagster Pipes over dispatch: used INSIDE the step subprocess.

Needs only ``dagster-pipes`` (zero-dependency) plus :mod:`dispatch.client` (stdlib).

The context comes inline: :mod:`dispatch.pipes_dagster` puts the Pipes bootstrap env vars
in the item payload and :mod:`dispatch.worker` sets them on the step, so the default
``PipesEnvVarParamsLoader`` + ``PipesDefaultContextLoader`` load it with no blob round trip.

Messages go to dispatch, not blob storage: :class:`DispatchMessageWriter` batches them
(dagster-pipes' ``PipesBlobStoreMessageWriterChannel`` upload loop) and POSTs each batch to
``/work/{item}/events`` as ``pipes`` events with this attempt's fencing token, so a stale
attempt's messages are refused (409) rather than mixed into the live one.

    from dispatch.pipes_worker import open_dispatch_pipes

    with open_dispatch_pipes() as pipes:
        pipes.log.info("hello")
        pipes.report_asset_materialization(metadata={"rows": 3})
"""

from __future__ import annotations

import json
import os
import sys
from typing import IO, Any

from dagster_pipes import (
    PipesBlobStoreMessageWriter,
    PipesBlobStoreMessageWriterChannel,
    PipesContext,
    PipesParams,
    open_dagster_pipes,
)

from dispatch.client import DispatchClient, DispatchError, NotFound, Stale

PIPES_EVENT_KIND = "pipes"
#: the messages params the Dagster-side reader hands out (the token comes from the worker)
TRANSPORT = "dispatch"


class DispatchMessageWriterChannel(PipesBlobStoreMessageWriterChannel):
    def __init__(
        self,
        client: DispatchClient,
        *,
        item_id: int,
        attempt: int,
        worker_id: str,
        interval: float,
    ):
        super().__init__(interval=interval)
        self.client = client
        self.item_id = item_id
        self.attempt = attempt
        self.worker_id = worker_id
        self.abandoned = False

    def upload_messages_chunk(self, payload: IO, index: int) -> None:
        if self.abandoned:
            return
        events = [
            {"kind": PIPES_EVENT_KIND, "data": json.loads(line)}
            for line in payload.read().splitlines()
            if line.strip()
        ]
        if not events:
            return
        try:
            self.client.add_events(
                self.item_id,
                worker_id=self.worker_id,
                attempt=self.attempt,
                events=events,
            )
        except (NotFound, Stale) as e:
            # The attempt no longer holds the item; the worker is about to kill us.
            self.abandoned = True
            print(
                f"[dispatch pipes] attempt abandoned, dropping messages: {e}",
                file=sys.stderr,
            )
        except (DispatchError, OSError) as e:
            # Don't let the upload thread die: later chunks may still get through.
            print(
                f"[dispatch pipes] dropped {len(events)} messages: {e}", file=sys.stderr
            )


class DispatchMessageWriter(PipesBlobStoreMessageWriter[DispatchMessageWriterChannel]):
    """Reads the fencing token and dispatch location from the environment the worker set
    (``DISPATCH_URL``, ``DISPATCH_SHARED_KEY``, ``DISPATCH_ITEM_ID``, ``DISPATCH_ATTEMPT``,
    ``DISPATCH_WORKER_ID``)."""

    def __init__(self, *, interval: float = 1):
        super().__init__(interval=interval)

    def make_channel(self, params: PipesParams) -> DispatchMessageWriterChannel:
        if params.get("transport") != TRANSPORT:
            raise ValueError(f"expected dispatch message params, got {params!r}")
        env = os.environ
        return DispatchMessageWriterChannel(
            DispatchClient(env["DISPATCH_URL"], env.get("DISPATCH_SHARED_KEY")),
            item_id=int(env["DISPATCH_ITEM_ID"]),
            attempt=int(env["DISPATCH_ATTEMPT"]),
            worker_id=env["DISPATCH_WORKER_ID"],
            interval=self.interval,
        )

    def get_opened_extras(self) -> dict[str, Any]:
        return {
            "dispatch_item": os.environ.get("DISPATCH_ITEM_ID"),
            "dispatch_attempt": os.environ.get("DISPATCH_ATTEMPT"),
            "worker_id": os.environ.get("DISPATCH_WORKER_ID"),
        }


def open_dispatch_pipes(*, interval: float = 1) -> PipesContext:
    """``open_dagster_pipes`` with the dispatch message writer. Use it as a context
    manager so the final batch (with the ``closed`` message) is flushed before exit."""
    return open_dagster_pipes(message_writer=DispatchMessageWriter(interval=interval))


def staging_prefix() -> str:
    """Where this attempt writes outputs (``_staging/.../<attempt>/``); the Dagster side
    promotes them to the asset's final prefix after dispatch reports the item done."""
    return os.environ["DISPATCH_STAGING_PREFIX"]
