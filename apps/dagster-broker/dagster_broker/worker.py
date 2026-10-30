"""The worker: claim an item, run its step function in a ``dagster_broker.pipes_worker``
subprocess while heartbeating, then complete or fail it.
``python -m dagster_broker.worker``, configured by env:

    DAGSTER_BROKER_URL, DAGSTER_BROKER_SHARED_KEY
    DAGSTER_BROKER_WORKER_CAPS               comma-separated, e.g. small,standard
    DAGSTER_BROKER_WORKER_SLOTS              items run at once, one thread each (default 1)
    DAGSTER_BROKER_WORKER_IDLE_EXIT_SECONDS  exit a slot after this long idle (0 = never)
    DAGSTER_BROKER_WORKER_STEP_MEMORY_MB     soft address-space cap per step (0 = off)
    DAGSTER_BROKER_WORKER_MIN_FREE_DISK_GB   don't claim with less free disk than this
"""

import collections
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

from dcpy.infra.dagster_broker_client import BrokerClient, BrokerError, NotFound, Stale

logger = logging.getLogger(__name__)

HEARTBEAT_SECONDS = 5
POLL_SECONDS = 2
KILL_GRACE_SECONDS = 10


def _kill(proc: subprocess.Popen) -> None:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(proc.pid, sig)
            proc.wait(KILL_GRACE_SECONDS)
            return
        except ProcessLookupError:
            return
        except subprocess.TimeoutExpired:
            pass


class Attempt:
    """One claimed (item, attempt): its step process, and why it was stopped, if it was."""

    def __init__(self, client: BrokerClient, item: dict, worker_id: str):
        self.client = client
        self.path = f"/work/{item['id']}"
        self.token = {"worker_id": worker_id, "attempt": item["attempt"]}
        self.label = f"item {item['id']} attempt {item['attempt']}"
        self.stopped: str | None = None
        self.done = threading.Event()
        self.proc: subprocess.Popen | None = None
        self.tail: collections.deque[str] = collections.deque(maxlen=20)

    def post(self, action: str, **body):
        return self.client.post(f"{self.path}/{action}", {**self.token, **body})

    def report(self, action: str, **body) -> None:
        try:
            self.post(action, **body)
        except (NotFound, Stale) as e:
            logger.warning("%s: %s ignored (%s)", self.label, action, e)

    def stop(self, reason: str) -> None:
        if self.stopped:
            return
        self.stopped = reason
        logger.warning("%s: stopping step (%s)", self.label, reason)
        if self.proc:
            threading.Thread(target=_kill, args=(self.proc,), daemon=True).start()

    def start(self, command, env, cwd, memory_mb: int) -> None:
        self.proc = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            text=True,
            errors="replace",
            start_new_session=True,
        )
        if memory_mb:
            import resource  # Linux; set as the step starts, before it does real work

            limit = memory_mb * 2**20
            resource.prlimit(self.proc.pid, resource.RLIMIT_AS, (limit, limit))
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()

    def heartbeat_loop(self) -> None:
        while not self.stopped and not self.done.wait(HEARTBEAT_SECONDS):
            try:
                if self.post("heartbeat")["cancel"]:
                    self.stop("cancelled")
            except NotFound:
                self.stop("lost")  # the queue forgot the item; the producer re-enqueues
            except Stale:
                self.stop("stale")  # reaped, maybe claimed by someone else
            except (BrokerError, OSError) as e:
                logger.warning("%s: heartbeat failed (%s)", self.label, e)

    def forward_output(self) -> None:
        """Echo the step's output, keep a tail, and post it as ``stdio`` events."""
        assert self.proc and self.proc.stdout
        batch, last = [], time.monotonic()
        for line in self.proc.stdout:
            line = line.rstrip("\n")[:4000]
            print(f"[{self.label}] {line}", flush=True)
            self.tail.append(line)
            batch.append(line)
            if time.monotonic() - last >= 1 or len(batch) >= 200:
                self.post_output(batch)
                batch, last = [], time.monotonic()
        self.post_output(batch)

    def post_output(self, lines: list[str]) -> None:
        if not lines or self.stopped:
            return
        try:
            self.post(
                "events", events=[{"kind": "stdio", "data": {"text": "\n".join(lines)}}]
            )
        except (BrokerError, OSError) as e:
            logger.warning(
                "%s: dropped %d output lines (%s)", self.label, len(lines), e
            )


class Worker:
    def __init__(
        self,
        caps=(),
        slots=1,
        idle_exit_seconds=0.0,
        step_memory_mb=0,
        min_free_disk_gb=0.0,
        client: BrokerClient | None = None,
    ):
        self.caps = list(caps)
        self.slots = slots
        self.idle_exit_seconds = idle_exit_seconds
        self.step_memory_mb = step_memory_mb
        self.min_free_disk_gb = min_free_disk_gb
        self.client = client or BrokerClient()
        self.recent_runs: collections.deque[str] = collections.deque(maxlen=5)
        self.running: set[Attempt] = set()
        self.shutdown = threading.Event()

    @classmethod
    def from_env(cls) -> "Worker":
        def env(name, default="0"):
            return os.environ.get(f"DAGSTER_BROKER_WORKER_{name}") or default

        return cls(
            caps=[c.strip() for c in env("CAPS", "").split(",") if c.strip()],
            slots=int(env("SLOTS", "1")),
            idle_exit_seconds=float(env("IDLE_EXIT_SECONDS")),
            step_memory_mb=int(env("STEP_MEMORY_MB")),
            min_free_disk_gb=float(env("MIN_FREE_DISK_GB")),
        )

    def run(self) -> None:
        host = f"{socket.gethostname()}-{os.getpid()}"
        logger.info("worker %s caps=%s slots=%s", host, self.caps, self.slots)
        ids = [f"{host}-{i}" for i in range(self.slots)]
        threads = [threading.Thread(target=self.slot, args=(i,)) for i in ids]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def stop(self, *_) -> None:
        logger.warning("shutdown requested")
        self.shutdown.set()
        for att in list(self.running):
            att.stop("worker shutdown")

    def slot(self, worker_id: str) -> None:
        last_work = time.monotonic()
        while not self.shutdown.is_set():
            item = self.claim(worker_id)
            if item:
                try:
                    self.run_item(item, worker_id)
                except Exception:
                    logger.exception("item %s: worker error", item["id"])
                last_work = time.monotonic()
            elif self.idle_exit_seconds and (
                time.monotonic() - last_work > self.idle_exit_seconds
            ):
                logger.info(
                    "%s idle for %ss, exiting", worker_id, self.idle_exit_seconds
                )
                return
            else:
                self.shutdown.wait(POLL_SECONDS)

    def claim(self, worker_id: str) -> dict | None:
        free_gb = shutil.disk_usage(tempfile.gettempdir()).free / 1e9
        if free_gb < self.min_free_disk_gb:
            logger.warning("only %.1f GB free disk, not claiming", free_gb)
            return None
        body = {"worker_id": worker_id, "caps": self.caps}
        body["affinity_run_ids"] = list(self.recent_runs)
        try:
            # never retried: a lost response would hold an item nobody runs until reaped
            return self.client.post("/work/claim", body, retry=False)
        except (BrokerError, OSError) as e:
            logger.warning("claim failed (%s)", e)
            return None

    def run_item(self, item: dict, worker_id: str) -> None:
        p = item["payload"]
        if item.get("run_id"):
            self.recent_runs.append(item["run_id"])
        env = {
            **os.environ,
            **p.get("env", {}),
            **p.get("pipes", {}),
            "DAGSTER_BROKER_URL": self.client.url,
            "DAGSTER_BROKER_SHARED_KEY": self.client.key or "",
            "DAGSTER_BROKER_ITEM_ID": item["id"],
            "DAGSTER_BROKER_ATTEMPT": str(item["attempt"]),
            "DAGSTER_BROKER_WORKER_ID": worker_id,
        }
        att = Attempt(self.client, item, worker_id)
        logger.info("%s claimed: %s", att.label, p.get("function"))
        workdir = tempfile.mkdtemp(prefix=f"item-{item['id']}-")
        self.running.add(att)
        started = time.monotonic()
        try:
            try:
                command = [
                    sys.executable,
                    "-m",
                    "dagster_broker.pipes_worker",
                    p["function"],
                ]
                att.start(command, env, workdir, self.step_memory_mb)
            except (KeyError, OSError) as e:
                att.report(
                    "fail", error=f"could not start step: {e!r}", retryable=False
                )
                return
            if self.shutdown.is_set():
                att.stop("worker shutdown")
            att.forward_output()
            rc = att.proc.wait()  # type: ignore[union-attr]
            self.finish(att, rc, p, time.monotonic() - started)
        finally:
            att.done.set()
            self.running.discard(att)
            shutil.rmtree(workdir, ignore_errors=True)

    def finish(self, att: Attempt, rc: int, p: dict, took: float) -> None:
        if att.stopped in ("lost", "stale"):
            logger.warning("%s abandoned (%s)", att.label, att.stopped)
        elif att.stopped == "cancelled":
            att.report("fail", error="cancelled", retryable=False)
        elif att.stopped:
            att.report("fail", error="worker shut down mid-step", retryable=True)
        elif rc != 0:
            error = f"step exited with code {rc}; last output:\n" + "\n".join(att.tail)
            retry = p.get("retry_on_failure", True)
            att.report("fail", error=error, retryable=retry)
        else:
            logger.info("%s done in %.1fs", att.label, took)
            result = {"seconds": round(took, 1), "worker_id": att.token["worker_id"]}
            att.report("complete", result=result)


def main() -> None:
    logging.basicConfig(
        level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    worker = Worker.from_env()
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    worker.run()


if __name__ == "__main__":
    main()
