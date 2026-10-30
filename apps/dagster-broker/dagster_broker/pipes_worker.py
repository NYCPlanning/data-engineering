"""Runs a plain step function under Dagster Pipes, on a dagster-broker worker:
``python -m dagster_broker.pipes_worker <module.function>``.

The function gets the Pipes extra ``args`` as keyword arguments. Its std logging goes to Dagster as Pipes logs, and the dict it returns becomes
the materialization's metadata.

The context arrives as env vars (the worker sets the bootstrap vars from the item payload).
Messages are POSTed in batches to /work/{item}/events with this attempt's fencing token,
so a stale attempt's messages are refused rather than mixed into the live one.
"""

import importlib
import json
import logging
import os
import sys

from dagster_pipes import (
    PipesBlobStoreMessageWriter,
    PipesBlobStoreMessageWriterChannel,
    open_dagster_pipes,
)

from dcpy.infra.dagster_broker_client import BrokerClient, BrokerError, NotFound, Stale


class BrokerChannel(PipesBlobStoreMessageWriterChannel):
    def __init__(self, interval: float):
        super().__init__(interval=interval)
        self.client = BrokerClient()
        self.path = f"/work/{os.environ['DAGSTER_BROKER_ITEM_ID']}/events"
        self.token = {
            "attempt": int(os.environ["DAGSTER_BROKER_ATTEMPT"]),
            "worker_id": os.environ["DAGSTER_BROKER_WORKER_ID"],
        }
        self.abandoned = False

    def upload_messages_chunk(self, payload, index: int) -> None:
        lines = [line for line in payload.read().splitlines() if line.strip()]
        if self.abandoned or not lines:
            return
        events = [{"kind": "pipes", "data": json.loads(line)} for line in lines]
        try:
            self.client.post(self.path, {**self.token, "events": events})
        except (NotFound, Stale) as e:
            self.abandoned = True  # the attempt is over; the worker is killing us
            print(f"[dagster-broker pipes] attempt abandoned: {e}", file=sys.stderr)
        except (BrokerError, OSError) as e:
            print(f"[dagster-broker pipes] dropped messages: {e}", file=sys.stderr)


class BrokerMessageWriter(PipesBlobStoreMessageWriter):
    def make_channel(self, params) -> BrokerChannel:
        return BrokerChannel(self.interval)


class ToPipes(logging.Handler):
    def __init__(self, pipes):
        super().__init__()
        self.pipes = pipes

    def emit(self, record) -> None:
        self.pipes.log.handle(record)


def main() -> None:
    module, _, name = sys.argv[1].rpartition(".")
    with open_dagster_pipes(message_writer=BrokerMessageWriter(interval=1)) as pipes:
        logging.getLogger().addHandler(ToPipes(pipes))
        logging.getLogger().setLevel(logging.INFO)
        kwargs = pipes.extras.get("args") or {}
        metadata = getattr(importlib.import_module(module), name)(**kwargs)
        pipes.report_asset_materialization(metadata=metadata)


if __name__ == "__main__":
    main()
