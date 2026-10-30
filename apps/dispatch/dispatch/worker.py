"""The worker loop: claim an item, run its step as a subprocess, heartbeat, report back.

Standard library only (the step subprocess brings its own dagster-pipes). Run with
``python -m dispatch.worker``; configured from the environment (:meth:`WorkerConfig.from_env`).

One item at a time:

1. ``claim`` with this worker's caps, preferring run_ids it ran recently (affinity).
2. Run ``payload.command`` in a fresh working directory as a subprocess in its own
   process group, so an OOM or crash in the step can't take down the loop or heartbeats.
   The step's environment gets the Pipes bootstrap variables (``payload.pipes``), the
   fencing token (``DISPATCH_ITEM_ID`` / ``DISPATCH_ATTEMPT`` / ``DISPATCH_WORKER_ID``,
   used by :mod:`dispatch.pipes_worker` to post Pipes messages) and
   ``DISPATCH_STAGING_PREFIX`` (``payload.staging_prefix/<attempt>/``), where the step
   writes outputs for the Dagster side to promote.
3. A heartbeat thread extends the lease. It kills the step when dispatch says ``cancel``,
   answers 404 (the queue restarted or forgot the item) or 409 (the lease was lost).
4. Step stdout/stderr are forwarded as ``stdio`` events in batches.
5. Exit 0 -> ``complete``. Non-zero -> ``fail`` (retryable unless
   ``payload.retry_on_failure`` is false). Cancelled -> ``fail`` (dispatch records it as
   cancelled). Lost lease -> nothing: the attempt is abandoned.

On SIGTERM the worker kills its step, fails the attempt as retryable (so it's requeued
now rather than after the lease) and exits. With ``WORKER_IDLE_EXIT_SECONDS`` > 0 it
exits after that long without work (scale-to-zero jobs); 0 means never (desktops).
"""

from __future__ import annotations

import collections
import logging
import os
import queue
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field

from dispatch.client import (
    DispatchClient,
    DispatchError,
    NotFound,
    Stale,
    Unavailable,
)

logger = logging.getLogger("dispatch.worker")

MAX_LINE = 4000
STDIO_FLUSH_SECONDS = 1.0
STDIO_BATCH = 200


@dataclass
class WorkerConfig:
    dispatch_url: str
    shared_key: str | None = None
    worker_id: str = field(
        default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}"
    )
    caps: list[str] = field(default_factory=list)
    idle_exit_seconds: float = 0  # 0: never exit when idle
    poll_seconds: float = 2
    heartbeat_seconds: float = 5  # keep well under a third of the dispatch lease
    kill_grace_seconds: float = 10
    workdir: str | None = None  # parent dir for per-item working dirs (default: tmp)
    affinity_size: int = 5

    @classmethod
    def from_env(cls) -> WorkerConfig:
        env = os.environ.get
        cfg = cls(
            dispatch_url=os.environ["DISPATCH_URL"],
            shared_key=env("DISPATCH_SHARED_KEY") or None,
            caps=[c.strip() for c in env("WORKER_CAPS", "").split(",") if c.strip()],
            idle_exit_seconds=float(env("WORKER_IDLE_EXIT_SECONDS", 0)),
            poll_seconds=float(env("WORKER_POLL_SECONDS", 2)),
            heartbeat_seconds=float(env("WORKER_HEARTBEAT_SECONDS", 5)),
            kill_grace_seconds=float(env("WORKER_KILL_GRACE_SECONDS", 10)),
            workdir=env("WORKER_WORKDIR") or None,
        )
        if env("WORKER_ID"):
            cfg.worker_id = os.environ["WORKER_ID"]
        return cfg


class _Attempt:
    """One claimed (item, attempt): the step process and why it was stopped, if it was."""

    def __init__(self, item: dict, proc: subprocess.Popen, grace: float):
        self.item_id: int = item["id"]
        self.attempt: int = item["attempt"]
        self.proc = proc
        self.grace = grace
        self.stop_reason: str | None = None
        self.finished = threading.Event()
        self._lock = threading.Lock()

    def stop(self, reason: str) -> None:
        """Kill the step's process group (TERM, then KILL after the grace period)."""
        with self._lock:
            if self.stop_reason is not None:
                return
            self.stop_reason = reason
        logger.warning(
            "item %s attempt %s: stopping step (%s)", self.item_id, self.attempt, reason
        )
        threading.Thread(target=self._kill, daemon=True).start()

    def _kill(self) -> None:
        _signal_group(self.proc, signal.SIGTERM)
        try:
            self.proc.wait(self.grace)
        except subprocess.TimeoutExpired:
            _signal_group(
                self.proc, signal.SIGKILL if os.name != "nt" else signal.SIGTERM
            )


def _signal_group(proc: subprocess.Popen, sig: int) -> None:
    if proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            proc.kill() if sig != signal.SIGTERM else proc.terminate()
        else:
            os.killpg(proc.pid, sig)
    except (ProcessLookupError, PermissionError):
        pass


class Worker:
    def __init__(self, config: WorkerConfig, client: DispatchClient | None = None):
        self.cfg = config
        self.client = client or DispatchClient(config.dispatch_url, config.shared_key)
        self.recent_runs: collections.deque[str] = collections.deque(
            maxlen=config.affinity_size
        )
        self.shutdown = threading.Event()
        self.current: _Attempt | None = None

    # ---- main loop ------------------------------------------------------------------

    def run_forever(self) -> None:
        logger.info(
            "worker %s caps=%s dispatch=%s idle_exit=%ss",
            self.cfg.worker_id,
            self.cfg.caps,
            self.cfg.dispatch_url,
            self.cfg.idle_exit_seconds or "never",
        )
        last_work = time.monotonic()
        while not self.shutdown.is_set():
            try:
                item = self.client.claim(
                    worker_id=self.cfg.worker_id,
                    caps=self.cfg.caps,
                    affinity_run_ids=list(self.recent_runs),
                )
            except (DispatchError, OSError) as e:
                logger.warning("claim failed (%s); retrying", e)
                item = None
            if item is None:
                if (
                    self.cfg.idle_exit_seconds
                    and time.monotonic() - last_work > self.cfg.idle_exit_seconds
                ):
                    logger.info("idle for %ss, exiting", self.cfg.idle_exit_seconds)
                    return
                self.shutdown.wait(self.cfg.poll_seconds)
                continue
            if item.get("run_id"):
                self.recent_runs.append(item["run_id"])
            try:
                self.run_item(item)
            except Exception:
                logger.exception("item %s: worker error", item["id"])
            last_work = time.monotonic()

    def request_shutdown(self, *_: object) -> None:
        logger.warning("shutdown requested")
        self.shutdown.set()
        if self.current:
            self.current.stop("worker shutdown")

    # ---- one item -------------------------------------------------------------------

    def step_env(self, item: dict) -> dict[str, str]:
        payload = item.get("payload") or {}
        env = {
            **os.environ,
            **{k: str(v) for k, v in (payload.get("env") or {}).items()},
        }
        env.update(payload.get("pipes") or {})
        env.update(
            DISPATCH_URL=self.cfg.dispatch_url,
            DISPATCH_ITEM_ID=str(item["id"]),
            DISPATCH_ATTEMPT=str(item["attempt"]),
            DISPATCH_WORKER_ID=self.cfg.worker_id,
        )
        if self.cfg.shared_key:
            env["DISPATCH_SHARED_KEY"] = self.cfg.shared_key
        if payload.get("staging_prefix"):
            env["DISPATCH_STAGING_PREFIX"] = (
                f"{payload['staging_prefix'].rstrip('/')}/{item['attempt']}/"
            )
        return env

    def run_item(self, item: dict) -> None:
        item_id, attempt = item["id"], item["attempt"]
        payload = item.get("payload") or {}
        command = payload.get("command")
        token = {"worker_id": self.cfg.worker_id, "attempt": attempt}
        logger.info(
            "claimed item %s attempt %s (%s %s): %s",
            item_id,
            attempt,
            item.get("asset_key"),
            item.get("partition_key"),
            command,
        )
        if not command:
            self._report(
                self.client.fail,
                item_id,
                error="payload has no command",
                retryable=False,
                **token,
            )
            return

        workdir = tempfile.mkdtemp(
            prefix=f"item-{item_id}-{attempt}-", dir=self.cfg.workdir
        )
        started = time.monotonic()
        try:
            proc = subprocess.Popen(
                command,
                cwd=workdir,
                env=self.step_env(item),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                text=True,
                errors="replace",
                bufsize=1,
                **(
                    {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}  # type: ignore[attr-defined]
                    if os.name == "nt"
                    else {"start_new_session": True}
                ),
            )
        except OSError as e:
            shutil.rmtree(workdir, ignore_errors=True)
            self._report(
                self.client.fail,
                item_id,
                error=f"could not start step: {e}",
                retryable=False,
                **token,
            )
            return

        att = _Attempt(item, proc, self.cfg.kill_grace_seconds)
        self.current = att
        tail: collections.deque[str] = collections.deque(maxlen=20)
        hb = threading.Thread(target=self._heartbeat_loop, args=(att,), daemon=True)
        fwd = threading.Thread(
            target=self._forward_stdio, args=(att, tail), daemon=True
        )
        hb.start()
        fwd.start()
        if self.shutdown.is_set():
            att.stop("worker shutdown")
        rc = proc.wait()
        fwd.join()  # stdout closed: every line has been posted (or dropped on 404/409)
        att.finished.set()
        hb.join(timeout=5)
        self.current = None
        shutil.rmtree(workdir, ignore_errors=True)
        took = round(time.monotonic() - started, 1)

        reason = att.stop_reason
        if reason in ("lost", "stale"):
            logger.warning(
                "item %s attempt %s abandoned (%s)", item_id, attempt, reason
            )
        elif reason == "cancelled":
            self._report(
                self.client.fail, item_id, error="cancelled", retryable=False, **token
            )
        elif reason == "worker shutdown":
            self._report(
                self.client.fail,
                item_id,
                error="worker shut down mid-step",
                retryable=True,
                **token,
            )
        elif rc == 0:
            logger.info("item %s attempt %s done in %ss", item_id, attempt, took)
            self._report(
                self.client.complete,
                item_id,
                result={
                    "exit_code": 0,
                    "seconds": took,
                    "worker_id": self.cfg.worker_id,
                },
                **token,
            )
        else:
            logger.warning("item %s attempt %s exited %s", item_id, attempt, rc)
            self._report(
                self.client.fail,
                item_id,
                error=f"step exited with code {rc}; last output:\n" + "\n".join(tail),
                retryable=bool(payload.get("retry_on_failure", True)),
                **token,
            )

    def _report(self, fn, item_id: int, **kwargs) -> None:
        """complete/fail; 404/409 just mean someone else owns the outcome now."""
        try:
            fn(item_id, **kwargs)
        except (NotFound, Stale) as e:
            logger.warning("item %s: %s ignored (%s)", item_id, fn.__name__, e)

    def _heartbeat_loop(self, att: _Attempt) -> None:
        while not att.finished.wait(self.cfg.heartbeat_seconds):
            try:
                hb = self.client.heartbeat(
                    att.item_id, worker_id=self.cfg.worker_id, attempt=att.attempt
                )
            except NotFound:
                att.stop("lost")  # the queue forgot the item: the producer re-enqueues
                return
            except Stale:
                att.stop("stale")  # reaped and maybe claimed by someone else
                return
            except (Unavailable, DispatchError, OSError) as e:
                logger.warning(
                    "heartbeat for item %s failed (%s); still running", att.item_id, e
                )
                continue
            if hb.get("cancel"):
                att.stop("cancelled")
                return

    def _forward_stdio(self, att: _Attempt, tail: collections.deque) -> None:
        """Pump the step's merged stdout/stderr: echo it, keep a tail for errors, and post
        it as ``stdio`` events in batches."""
        lines: queue.Queue[str | None] = queue.Queue()

        def pump() -> None:
            assert att.proc.stdout is not None
            for line in att.proc.stdout:
                lines.put(line.rstrip("\n")[:MAX_LINE])
            lines.put(None)

        threading.Thread(target=pump, daemon=True).start()
        batch: list[str] = []
        posting = True
        last = time.monotonic()
        while True:
            try:
                line = lines.get(timeout=STDIO_FLUSH_SECONDS)
            except queue.Empty:
                line = ""  # timer tick
            eof = line is None
            if line:
                print(f"[item {att.item_id}#{att.attempt}] {line}", flush=True)
                tail.append(line)
                batch.append(line)
            due = (
                time.monotonic() - last >= STDIO_FLUSH_SECONDS
                or len(batch) >= STDIO_BATCH
            )
            if batch and (due or eof):
                if posting:
                    try:
                        self.client.add_events(
                            att.item_id,
                            worker_id=self.cfg.worker_id,
                            attempt=att.attempt,
                            events=[
                                {"kind": "stdio", "data": {"text": "\n".join(batch)}}
                            ],
                        )
                    except (NotFound, Stale):
                        posting = (
                            False  # the attempt is over; the heartbeat stops the step
                        )
                    except (DispatchError, OSError) as e:
                        logger.warning("dropping %d stdio lines (%s)", len(batch), e)
                batch = []
                last = time.monotonic()
            if eof:
                return


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("WORKER_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    worker = Worker(WorkerConfig.from_env())
    signal.signal(signal.SIGTERM, worker.request_shutdown)
    signal.signal(signal.SIGINT, worker.request_shutdown)
    worker.run_forever()


if __name__ == "__main__":
    main()
