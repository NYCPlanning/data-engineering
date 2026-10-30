"""Worker side of Dagster Pipes over dagster-broker: used INSIDE the step subprocess.

Needs only ``dagster-pipes`` (zero-dependency) plus :mod:`dagster_broker.client` (stdlib).

The context comes inline: :mod:`dagster_broker.pipes_dagster` puts the Pipes bootstrap env vars
in the item payload and :mod:`dagster_broker.worker` sets them on the step, so the default
``PipesEnvVarParamsLoader`` + ``PipesDefaultContextLoader`` load it with no blob round trip.

Messages go to dagster-broker, not blob storage: :class:`BrokerMessageWriter` batches them
(dagster-pipes' ``PipesBlobStoreMessageWriterChannel`` upload loop) and POSTs each batch to
``/work/{item}/events`` as ``pipes`` events with this attempt's fencing token, so a stale
attempt's messages are refused (409) rather than mixed into the live one.

    from dagster_broker.pipes_worker import open_broker_pipes

    with open_broker_pipes() as pipes:
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

from dagster_broker.client import BrokerClient, BrokerError, NotFound, Stale

PIPES_EVENT_KIND = "pipes"
#: the messages params the Dagster-side reader hands out (the token comes from the worker)
TRANSPORT = "dagster_broker"


class BrokerMessageWriterChannel(PipesBlobStoreMessageWriterChannel):
    def __init__(
        self,
        client: BrokerClient,
        *,
        item_id: str,
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
                f"[dagster-broker pipes] attempt abandoned, dropping messages: {e}",
                file=sys.stderr,
            )
        except (BrokerError, OSError) as e:
            # Don't let the upload thread die: later chunks may still get through.
            print(
                f"[dagster-broker pipes] dropped {len(events)} messages: {e}",
                file=sys.stderr,
            )


class BrokerMessageWriter(PipesBlobStoreMessageWriter[BrokerMessageWriterChannel]):
    """Reads the fencing token and dagster-broker location from the environment the worker set
    (``DAGSTER_BROKER_URL``, ``DAGSTER_BROKER_SHARED_KEY``, ``DAGSTER_BROKER_ITEM_ID``, ``DAGSTER_BROKER_ATTEMPT``,
    ``DAGSTER_BROKER_WORKER_ID``)."""

    def __init__(self, *, interval: float = 1):
        super().__init__(interval=interval)

    def make_channel(self, params: PipesParams) -> BrokerMessageWriterChannel:
        if params.get("transport") != TRANSPORT:
            raise ValueError(f"expected dagster-broker message params, got {params!r}")
        env = os.environ
        return BrokerMessageWriterChannel(
            BrokerClient(
                env["DAGSTER_BROKER_URL"], env.get("DAGSTER_BROKER_SHARED_KEY")
            ),
            item_id=env["DAGSTER_BROKER_ITEM_ID"],
            attempt=int(env["DAGSTER_BROKER_ATTEMPT"]),
            worker_id=env["DAGSTER_BROKER_WORKER_ID"],
            interval=self.interval,
        )

    def get_opened_extras(self) -> dict[str, Any]:
        return {
            "broker_item": os.environ.get("DAGSTER_BROKER_ITEM_ID"),
            "broker_attempt": os.environ.get("DAGSTER_BROKER_ATTEMPT"),
            "worker_id": os.environ.get("DAGSTER_BROKER_WORKER_ID"),
        }


def open_broker_pipes(*, interval: float = 1) -> PipesContext:
    """``open_dagster_pipes`` with the dagster-broker message writer. Use it as a context
    manager so the final batch (with the ``closed`` message) is flushed before exit."""
    return open_dagster_pipes(message_writer=BrokerMessageWriter(interval=interval))


def staging_prefix() -> str:
    """Where this attempt writes outputs (``_staging/.../<attempt>/``); the Dagster side
    promotes them to the asset's final prefix after dagster-broker reports the item done."""
    return os.environ["DAGSTER_BROKER_STAGING_PREFIX"]
