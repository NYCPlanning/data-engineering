"""Dagster side of Pipes over dagster-broker: a PipesClient whose launch is "enqueue an
item and wait for a worker to finish it". It holds no storage credentials.

The Pipes context travels inline in the item payload (bootstrap env vars). The client
polls the item and its events: ``pipes`` events are Pipes messages, ``stdio`` events are
the step's output. 404 means the queue forgot the item (Valkey restarted), so it
re-enqueues with the same idempotency key. If Dagster terminates the run, it cancels the
item and the worker kills the step on its next heartbeat.
"""

import time
from contextlib import contextmanager
from typing import Any

from dagster import (
    DagsterExecutionInterruptedError,
    PipesClient,
    PipesClientCompletedInvocation,
    PipesEnvContextInjector,
    PipesMessageReader,
    open_pipes_session,
)
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.errors import DagsterPipesExecutionError
from dcpy.infra.dagster_broker_client import BrokerClient, NotFound

LIVE = {"opened", "log", "log_external_stream"}
TERMINAL = {"done", "failed", "dead", "cancelled"}


class BrokerMessageReader(PipesMessageReader):
    """Fed by the client's wait loop. Logs go to Dagster live; results (materializations,
    checks, ``closed``) are held per attempt and only the attempt that ends ``done`` is
    reported, so a crashed attempt can't report twice."""

    def __init__(self) -> None:
        self.handler: Any = None
        self.attempt: int | None = None
        self.held: list[dict] = []
        self.opened = False

    @contextmanager
    def read_messages(self, handler):
        self.handler = handler
        yield {}

    def no_messages_debug_text(self) -> str:
        return "Pipes messages come from the dagster-broker item's 'pipes' events."

    def feed(self, attempt: int, message: dict) -> None:
        if attempt != self.attempt:
            self.attempt, self.held = attempt, []
        method = message["method"]
        if method not in LIVE:
            self.held.append(message)
        elif not (method == "opened" and self.opened):  # a retry re-opens: report once
            self.opened |= method == "opened"
            self.handler.handle_message(message)

    def flush(self, attempt: int) -> None:
        if attempt == self.attempt:
            for message in self.held:
                self.handler.handle_message(message)


class PipesBrokerClient(PipesClient, TreatAsResourceParam):
    def __init__(self, *, broker: BrokerClient | None = None, poll=1.0):
        self._broker = broker
        self.poll = poll

    @property
    def broker(self) -> BrokerClient:
        # made on first use, so loading the definitions needs no broker env vars
        self._broker = self._broker or BrokerClient()
        return self._broker

    def run(  # type: ignore[override]
        self,
        *,
        context,
        function: str,
        args: dict | None = None,
        required_caps=(),
        env=None,
        retry_on_failure=True,
    ) -> PipesClientCompletedInvocation:
        """Enqueue a call of ``function`` (a dotted path to a plain function) with
        ``args`` and block until a worker finishes it."""
        run_id, step = context.run.run_id, context.get_step_execution_context().step.key
        partition = context.partition_key if context.has_partition_key else ""
        reader = BrokerMessageReader()
        extras = {"args": args or {}}
        with open_pipes_session(
            context=context,
            context_injector=PipesEnvContextInjector(),
            message_reader=reader,
            extras=extras,
        ) as session:
            body = {
                "idempotency_key": f"{run_id}:{step}:{partition}:{context.retry_number}",
                "run_id": run_id,
                "partition_key": partition or None,
                "required_caps": list(required_caps),
                "payload": {
                    "function": function,
                    "env": dict(env or {}),
                    "pipes": session.get_bootstrap_env_vars(),
                    "retry_on_failure": retry_on_failure,
                },
            }
            item = self.broker.post("/items", body)
            context.log.info(f"[dagster-broker] enqueued item {item['id']}")
            try:
                item = self.wait(context, reader, item, body)
            except (DagsterExecutionInterruptedError, KeyboardInterrupt):
                self.cancel(context, item["id"])
                raise
        metadata = {
            "broker_item": item["id"],
            "broker_attempt": item["attempt"],
            **item.get("result", {}),
        }
        return PipesClientCompletedInvocation(session, metadata=metadata)

    def wait(self, context, reader, item: dict, body: dict) -> dict:
        cursor, seen = None, set()
        while True:
            try:
                item = self.broker.get(f"/items/{item['id']}")
                cursor = self.drain(context, reader, item["id"], cursor)
            except NotFound:
                context.log.warning(
                    f"[dagster-broker] item {item['id']} is gone; re-enqueueing"
                )
                item, cursor = self.broker.post("/items", body), None
                continue
            if item["status"] == "claimed" and item["attempt"] not in seen:
                seen.add(item["attempt"])
                context.log.info(
                    f"[dagster-broker] item {item['id']} attempt {item['attempt']} "
                    f"on worker {item['worker_id']}"
                )
            if item["status"] == "done":
                reader.flush(item["attempt"])
                return item
            if item["status"] in TERMINAL:
                raise DagsterPipesExecutionError(
                    f"dagster-broker item {item['id']} ended {item['status']} after "
                    f"attempt {item['attempt']}: {item.get('error')}"
                )
            time.sleep(self.poll)

    def drain(self, context, reader, item_id: str, cursor):
        while True:
            query = f"?after={cursor}" if cursor else ""
            page = self.broker.get(f"/items/{item_id}/events{query}")
            for ev in page["events"]:
                if ev["kind"] == "pipes":
                    reader.feed(ev["attempt"], ev["data"])
                elif ev["kind"] == "stdio":
                    for line in ev["data"]["text"].splitlines():
                        context.log.info(f"[{ev['worker_id']} #{ev['attempt']}] {line}")
            if not page["events"]:
                return cursor
            cursor = page["cursor"]

    def cancel(self, context, item_id: str) -> None:
        context.log.warning(
            f"[dagster-broker] run terminated: cancelling item {item_id}"
        )
        try:
            self.broker.post(f"/items/{item_id}/cancel")
            for _ in range(40):
                if self.broker.get(f"/items/{item_id}")["status"] in TERMINAL:
                    return
                time.sleep(0.5)
        except Exception as e:  # never mask the interrupt
            context.log.warning(
                f"[dagster-broker] cancel of item {item_id} failed: {e}"
            )
