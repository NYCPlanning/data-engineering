"""Dagster side of Pipes over dispatch: a ``PipesClient`` whose launch is "enqueue a
dispatch item and wait for a worker to run it".

Flow (tracker inv-k0d.4):

1. Open a Pipes session with ``PipesEnvContextInjector``: the context is small, so it
   travels inline in the item payload as the bootstrap env vars (no blob round trip).
2. Enqueue with idempotency key ``run_id:step_key:partition:retry`` and a fresh staging
   prefix ``_staging/<run_id>/<step_key>/<nonce>``; the worker appends ``/<attempt>/``.
3. Poll the item and its events by cursor. ``pipes`` events are Pipes messages:
   logs are forwarded live from every attempt, while results (materializations, checks,
   custom messages, ``closed``) are buffered per attempt and only the attempt that
   finishes ``done`` is handed to Dagster, so a crashed attempt can't double-report.
   ``stdio`` events (the step's stdout/stderr, posted by the worker) go to the run log.
4. 404 means the queue forgot the item (Valkey restarted): re-enqueue with the same
   idempotency key and a new staging nonce. A zombie attempt can't write into the new
   item's staging prefix, and its messages are refused by dispatch's fencing.
5. On ``done``, promote ``<staging>/<attempt>/*`` to ``outputs_prefix`` through the S3
   gateway (one server-side CopyObject per object), delete the staging copies, then
   return. ``failed``/``dead``/``cancelled`` raise.
6. If Dagster terminates the run, cancel the item (the worker kills the step on its next
   heartbeat) and wait briefly for dispatch to confirm.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from typing import Any, Iterator

from dagster import (
    AssetExecutionContext,
    DagsterExecutionInterruptedError,
    OpExecutionContext,
    PipesClient,
    PipesClientCompletedInvocation,
    PipesEnvContextInjector,
    PipesMessageReader,
    open_pipes_session,
)
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.errors import DagsterPipesExecutionError
from dagster._core.pipes.context import PipesMessageHandler
from dagster_pipes import PipesExtras, PipesParams

from dispatch.client import DispatchClient, NotFound
from dispatch.pipes_worker import PIPES_EVENT_KIND, TRANSPORT

LIVE_METHODS = frozenset({"opened", "log", "log_external_stream"})
TERMINAL = frozenset({"done", "failed", "dead", "cancelled"})


class DispatchMessageReader(PipesMessageReader):
    """Routes dispatch events to the Pipes handler. It doesn't poll on its own: the client
    feeds it from its wait loop, which also tracks the item's status and attempt."""

    def __init__(self) -> None:
        self.handler: PipesMessageHandler | None = None
        self.attempt: int | None = None
        self.buffered: list[dict] = []
        self.opened = False

    @contextmanager
    def read_messages(self, handler: PipesMessageHandler) -> Iterator[PipesParams]:
        self.handler = handler
        yield {"transport": TRANSPORT}

    def no_messages_debug_text(self) -> str:
        return (
            "Messages are read from the dispatch item's events (kind 'pipes'); check the "
            "worker's logs and that the step uses dispatch.pipes_worker.open_dispatch_pipes."
        )

    def start_attempt(self, attempt: int, context: Any) -> None:
        if self.attempt is not None and self.buffered:
            context.log.warning(
                f"[dispatch] discarding {len(self.buffered)} unreported result message(s) "
                f"from attempt {self.attempt}"
            )
        self.attempt = attempt
        self.buffered = []

    def feed(self, message: dict) -> None:
        assert self.handler is not None
        method = message.get("method")
        if method in LIVE_METHODS:
            if method == "opened":
                if self.opened:  # a retry re-opens; Dagster expects one writer
                    return
                self.opened = True
            self.handler.handle_message(message)  # type: ignore[arg-type]
        else:
            self.buffered.append(message)

    def flush(self) -> None:
        assert self.handler is not None
        for message in self.buffered:
            self.handler.handle_message(message)  # type: ignore[arg-type]
        self.buffered = []


class PipesDispatchClient(PipesClient, TreatAsResourceParam):
    """Run a Pipes step on a dispatch worker. Usable directly as a resource (like
    ``PipesSubprocessClient``).

    Args:
        dispatch: the dispatch API client (default: from ``DISPATCH_URL`` /
            ``DISPATCH_SHARED_KEY``).
        s3: a boto3 S3 client for the artifact gateway, used to promote outputs.
        bucket: the gateway bucket for staging and final outputs.
        poll_interval: seconds between status/event polls.
        cancel_wait: after a Dagster termination, how long to wait for dispatch to
            confirm the cancel before giving up.
    """

    def __init__(
        self,
        *,
        bucket: str,
        s3: Any = None,
        dispatch: DispatchClient | None = None,
        poll_interval: float = 1.0,
        cancel_wait: float = 20.0,
    ):
        self.bucket = bucket
        self._s3 = s3
        self.dispatch = dispatch or DispatchClient.from_env()
        self.poll_interval = poll_interval
        self.cancel_wait = cancel_wait

    @property
    def s3(self) -> Any:
        if self._s3 is None:
            import os

            import boto3

            self._s3 = boto3.client(
                "s3", endpoint_url=os.environ.get("AWS_S3_ENDPOINT")
            )
        return self._s3

    def run(  # type: ignore[override]
        self,
        *,
        context: AssetExecutionContext | OpExecutionContext,
        command: Sequence[str],
        extras: PipesExtras | None = None,
        required_caps: Sequence[str] = (),
        outputs_prefix: str | None = None,
        env: Mapping[str, str] | None = None,
        priority: int = 0,
        max_attempts: int = 3,
        retry_on_failure: bool = True,
    ) -> PipesClientCompletedInvocation:
        """Enqueue ``command`` and block until a worker finishes it.

        ``outputs_prefix``: where the step's staged outputs are promoted to (a key prefix
        in ``bucket``); None means the step writes nothing to promote.
        """
        step = context.get_step_execution_context()
        run_id = context.run.run_id
        partition = context.partition_key if context.has_partition_key else None
        retry = context.retry_number
        step_key = step.step.key
        asset_key = (
            context.asset_key.to_user_string()
            if isinstance(context, AssetExecutionContext)
            and len(context.selected_asset_keys) == 1
            else None
        )
        idem = f"{run_id}:{step_key}:{partition or ''}:{retry}"

        reader = DispatchMessageReader()
        with open_pipes_session(
            context=context,
            context_injector=PipesEnvContextInjector(),
            message_reader=reader,
            extras={
                **(extras or {}),
                "bucket": self.bucket,
                "outputs_prefix": outputs_prefix,
            },
        ) as session:
            bootstrap = session.get_bootstrap_env_vars()

            def enqueue() -> tuple[dict, str]:
                staging = f"_staging/{run_id}/{step_key}/{uuid.uuid4().hex[:12]}"
                item, created = self.dispatch.enqueue(
                    idempotency_key=idem,
                    run_id=run_id,
                    asset_key=asset_key,
                    partition_key=partition,
                    required_caps=required_caps,
                    priority=priority,
                    max_attempts=max_attempts,
                    payload={
                        "command": list(command),
                        "env": dict(env or {}),
                        "pipes": dict(bootstrap),
                        "staging_prefix": staging,
                        "bucket": self.bucket,
                        "retry_on_failure": retry_on_failure,
                    },
                )
                # an existing item (Dagster retried this call) keeps its own staging
                staging = item["payload"].get("staging_prefix", staging)
                context.log.info(
                    f"[dispatch] {'enqueued' if created else 'reattached to'} item "
                    f"{item['id']} caps={list(required_caps)} key={idem}"
                )
                return item, staging

            item, staging = enqueue()
            try:
                item = self._wait(context, reader, item, enqueue_again=enqueue)
            except (DagsterExecutionInterruptedError, KeyboardInterrupt):
                self._cancel(context, item["id"])
                raise
            staging = item["payload"].get("staging_prefix", staging)

            promoted = 0
            if outputs_prefix is not None:
                promoted = self._promote(
                    context, f"{staging}/{item['attempt']}/", outputs_prefix
                )
            metadata = {
                "dispatch_item": item["id"],
                "dispatch_attempt": item["attempt"],
                "worker_id": item.get("worker_id")
                or (item.get("result") or {}).get("worker_id"),
                "promoted_objects": promoted,
            }
            if outputs_prefix is not None:
                metadata["outputs_uri"] = f"s3://{self.bucket}/{outputs_prefix}"
        return PipesClientCompletedInvocation(session, metadata=metadata)

    # ---- waiting --------------------------------------------------------------------

    def _wait(
        self, context, reader: DispatchMessageReader, item: dict, enqueue_again
    ) -> dict:
        cursor: str | None = None
        claimed_seen: set[int] = set()
        while True:
            try:
                current = self.dispatch.get_item(item["id"])
                cursor = self._drain(context, reader, item["id"], cursor)
            except NotFound:
                context.log.warning(
                    f"[dispatch] item {item['id']} is gone (dispatch restarted?); "
                    "re-enqueueing with the same idempotency key"
                )
                reader.start_attempt(0, context)
                item, _ = enqueue_again()
                cursor = None
                continue
            item = current
            if (
                item["attempt"]
                and item["attempt"] not in claimed_seen
                and item["status"] != "queued"
            ):
                claimed_seen.add(item["attempt"])
                context.log.info(
                    f"[dispatch] item {item['id']} attempt {item['attempt']} on worker "
                    f"{item.get('worker_id')}"
                )
            status = item["status"]
            if status == "done":
                if reader.attempt != item["attempt"]:
                    reader.start_attempt(item["attempt"], context)
                reader.flush()
                return item
            if status in TERMINAL:
                raise DagsterPipesExecutionError(
                    f"dispatch item {item['id']} ended {status} after attempt "
                    f"{item['attempt']}/{item['max_attempts']}: {item.get('error')}"
                )
            time.sleep(self.poll_interval)

    def _drain(
        self, context, reader: DispatchMessageReader, item_id: int, cursor: str | None
    ) -> str | None:
        while True:
            events, new_cursor = self.dispatch.events(item_id, after=cursor, limit=1000)
            for ev in events:
                if ev["attempt"] != reader.attempt:
                    reader.start_attempt(ev["attempt"], context)
                if ev["kind"] == PIPES_EVENT_KIND:
                    reader.feed(ev["data"])
                elif ev["kind"] == "stdio":
                    for line in ev["data"].get("text", "").splitlines():
                        context.log.info(f"[{ev['worker_id']} #{ev['attempt']}] {line}")
            if not events or new_cursor == cursor:
                return new_cursor
            cursor = new_cursor

    def _cancel(self, context, item_id: int) -> None:
        context.log.warning(f"[dispatch] run terminated: cancelling item {item_id}")
        try:
            self.dispatch.cancel(item_id)
            deadline = time.monotonic() + self.cancel_wait
            while time.monotonic() < deadline:
                if self.dispatch.get_item(item_id)["status"] in TERMINAL:
                    context.log.info(f"[dispatch] item {item_id} cancelled")
                    return
                time.sleep(0.5)
            context.log.warning(
                f"[dispatch] item {item_id} not confirmed cancelled yet"
            )
        except Exception as e:  # never mask the interrupt
            context.log.warning(f"[dispatch] cancel of item {item_id} failed: {e}")

    # ---- promotion ------------------------------------------------------------------

    def _promote(self, context, staging: str, final: str) -> int:
        """Copy every object under ``staging`` to ``final`` (server-side), then delete the
        staged copies. Returns the number of objects promoted."""
        from boto3.s3.transfer import TransferConfig

        # one CopyObject per object up to S3's 5 GiB single-copy limit
        single = TransferConfig(
            multipart_threshold=5 * 1024**3, multipart_chunksize=512 * 1024**2
        )
        keys: list[str] = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=staging
        ):
            keys += [o["Key"] for o in page.get("Contents", [])]
        final = final.rstrip("/") + "/"
        for key in keys:
            self.s3.copy(
                {"Bucket": self.bucket, "Key": key},
                self.bucket,
                final + key[len(staging) :],
                Config=single,
            )
        for i in range(0, len(keys), 1000):
            self.s3.delete_objects(
                Bucket=self.bucket,
                Delete={
                    "Objects": [{"Key": k} for k in keys[i : i + 1000]],
                    "Quiet": True,
                },
            )
        context.log.info(
            f"[dispatch] promoted {len(keys)} object(s) {staging} -> {final}"
        )
        return len(keys)
