"""The HTTP client, the worker loop and the Pipes pieces against a live dispatch server
(uvicorn in a thread) on the test Valkey. Steps are small python subprocesses."""

import sys
import threading
import time

import pytest
import redis
import uvicorn

from dispatch.app import create_app
from dispatch.client import DispatchClient, DispatchError, NotFound
from dispatch.config import Settings
from dispatch.worker import Worker, WorkerConfig

KEY = "sekrit"


@pytest.fixture
def server(valkey_url):
    settings = Settings(
        valkey_url=valkey_url,
        shared_key=KEY,
        lease_seconds=3,
        reap_interval_seconds=0.2,
    )
    srv = uvicorn.Server(
        uvicorn.Config(
            create_app(settings), host="127.0.0.1", port=0, log_level="warning"
        )
    )
    t = threading.Thread(target=srv.run, daemon=True)
    t.start()
    for _ in range(100):
        if srv.started:
            break
        time.sleep(0.05)
    port = srv.servers[0].sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}"
    srv.should_exit = True
    t.join(timeout=10)


@pytest.fixture
def api(server):
    return DispatchClient(server, KEY, retries=0)


def make_worker(server, **kw) -> Worker:
    cfg = WorkerConfig(
        dispatch_url=server,
        shared_key=KEY,
        worker_id=kw.pop("worker_id", "w1"),
        heartbeat_seconds=0.3,
        kill_grace_seconds=2,
        poll_seconds=0.1,
        **kw,
    )
    return Worker(cfg)


def py(code: str) -> list[str]:
    return [sys.executable, "-c", code]


def run_one(worker: Worker, api: DispatchClient) -> dict:
    item = api.claim(worker_id=worker.cfg.worker_id, caps=worker.cfg.caps)
    assert item is not None
    worker.run_item(item)
    return api.get_item(item["id"])


def stdio(api, item_id) -> str:
    events, _ = api.events(item_id)
    return "\n".join(e["data"]["text"] for e in events if e["kind"] == "stdio")


def test_client_errors_and_idempotency(server, api):
    with pytest.raises(DispatchError) as e:
        DispatchClient(server, "wrong", retries=0).get_item(1)
    assert e.value.status == 401
    with pytest.raises(NotFound):
        api.get_item(12345)
    a, created = api.enqueue(idempotency_key="k1", payload={"x": 1})
    b, again = api.enqueue(idempotency_key="k1", payload={"x": 2})
    assert created and not again and a["id"] == b["id"] and b["payload"] == {"x": 1}
    assert api.claim(worker_id="w", caps=[]) is not None
    assert api.claim(worker_id="w", caps=[]) is None


def test_worker_runs_step_and_forwards_output(server, api):
    api.enqueue(
        run_id="r1",
        payload={
            "command": py(
                "import os; print('hello from', os.environ['DISPATCH_WORKER_ID']);"
                "print('staging', os.environ['DISPATCH_STAGING_PREFIX']);"
                "print('custom', os.environ['FOO'])"
            ),
            "env": {"FOO": "bar"},
            "staging_prefix": "_staging/r1/step/abc",
        },
    )
    w = make_worker(server)
    item = run_one(w, api)
    assert item["status"] == "done", item
    assert item["result"]["exit_code"] == 0
    out = stdio(api, item["id"])
    assert "hello from w1" in out
    assert "staging _staging/r1/step/abc/1/" in out
    assert "custom bar" in out
    assert list(w.recent_runs) == []  # run_item alone doesn't track affinity


def test_failing_step_retries_then_dies(server, api):
    api.enqueue(
        max_attempts=2,
        payload={"command": py("import sys; print('boom'); sys.exit(3)")},
    )
    w = make_worker(server)
    first = run_one(w, api)
    assert first["status"] == "queued" and first["attempt"] == 1
    second = run_one(w, api)
    assert second["status"] == "dead" and second["attempt"] == 2
    assert "exited with code 3" in second["error"] and "boom" in second["error"]


def test_non_retryable_failure(server, api):
    api.enqueue(
        payload={"command": py("raise SystemExit(1)"), "retry_on_failure": False}
    )
    assert run_one(make_worker(server), api)["status"] == "failed"


def test_cancel_kills_the_step(server, api):
    item, _ = api.enqueue(
        payload={"command": py("import time; print('up', flush=True); time.sleep(60)")}
    )
    w = make_worker(server)
    claimed = api.claim(worker_id="w1", caps=[])
    t = threading.Thread(target=w.run_item, args=(claimed,))
    started = time.monotonic()
    t.start()
    time.sleep(1)
    api.cancel(item["id"])
    t.join(timeout=15)
    assert not t.is_alive()
    assert time.monotonic() - started < 10
    assert api.get_item(item["id"])["status"] == "cancelled"


def test_lost_item_aborts_the_step(server, api, valkey_url):
    api.enqueue(payload={"command": py("import time; time.sleep(60)")})
    w = make_worker(server)
    claimed = api.claim(worker_id="w1", caps=[])
    t = threading.Thread(target=w.run_item, args=(claimed,))
    t.start()
    time.sleep(1)
    redis.Redis.from_url(valkey_url).flushdb()  # as if Valkey restarted
    t.join(timeout=15)
    assert not t.is_alive()


def test_worker_loop_caps_affinity_and_idle_exit(server, api):
    api.enqueue(
        run_id="gpu-run", required_caps=["gpu"], payload={"command": py("pass")}
    )
    plain, _ = api.enqueue(run_id="r2", payload={"command": py("pass")})
    w = make_worker(server, caps=["esri"], idle_exit_seconds=1)
    w.run_forever()  # runs the one item it can, then exits after 1s idle
    assert api.get_item(plain["id"])["status"] == "done"
    assert list(w.recent_runs) == ["r2"]
    assert api.demand(caps=["gpu"])["demand"] == 1


# ---- Pipes ------------------------------------------------------------------------

STEP = """
from dispatch.pipes_worker import open_dispatch_pipes, staging_prefix
with open_dispatch_pipes() as pipes:
    pipes.log.info(f"partition {pipes.partition_key} staging {staging_prefix()}")
    pipes.report_asset_materialization(metadata={"rows": 3})
"""


def test_pipes_end_to_end_through_dagster(server, api, monkeypatch):
    dagster = pytest.importorskip("dagster")
    from dispatch.pipes_dagster import PipesDispatchClient

    parts = dagster.StaticPartitionsDefinition(["a"])

    @dagster.asset(partitions_def=parts)
    def thing(context, pipes: PipesDispatchClient):
        return pipes.run(
            context=context, command=py(STEP), extras={"hello": "world"}
        ).get_materialize_result()

    w = make_worker(server, caps=[])
    t = threading.Thread(target=w.run_forever, daemon=True)
    t.start()
    try:
        res = dagster.materialize(
            [thing],
            partition_key="a",
            resources={
                "pipes": PipesDispatchClient(
                    bucket="unused", dispatch=api, poll_interval=0.2, s3=object()
                )
            },
        )
    finally:
        w.shutdown.set()
        t.join(timeout=10)
    assert res.success
    mats = res.asset_materializations_for_node("thing")
    assert len(mats) == 1
    md = mats[0].metadata
    assert md["rows"].value == 3
    assert md["dispatch_attempt"].value == 1
    assert md["worker_id"].value == "w1"
    item = api.get_item(md["dispatch_item"].value)
    events, _ = api.events(item["id"])
    pipes_msgs = [e["data"] for e in events if e["kind"] == "pipes"]
    methods = [m["method"] for m in pipes_msgs]
    assert methods[0] == "opened" and methods[-1] == "closed"
    assert "report_asset_materialization" in methods
    log = next(m["params"]["message"] for m in pipes_msgs if m["method"] == "log")
    assert log.startswith("partition a staging _staging/")


def test_reader_only_reports_results_of_the_finishing_attempt():
    pytest.importorskip("dagster")
    from dispatch.pipes_dagster import DispatchMessageReader

    handled = []

    class Handler:
        def handle_message(self, m):
            handled.append(m["method"])

    class Log:
        def warning(self, msg):
            handled.append("warning")

    class Ctx:
        log = Log()

    reader = DispatchMessageReader()
    reader.handler = Handler()
    reader.start_attempt(1, Ctx())
    for m in ("opened", "log", "report_asset_materialization"):
        reader.feed({"method": m})
    reader.start_attempt(2, Ctx())  # attempt 1 died: its materialization is dropped
    for m in ("opened", "log", "report_asset_materialization", "closed"):
        reader.feed({"method": m})
    reader.flush()
    assert handled == [
        "opened",
        "log",
        "warning",
        "log",
        "report_asset_materialization",
        "closed",
    ]
