"""A Dagster asset run through Pipes on a worker thread, via a live broker server. Needs
DAGSTER_BROKER_TEST_VALKEY_URL (a throwaway Valkey; it gets flushed), as CI sets."""

import os
import socket
import subprocess
import sys
import threading
import time

import pytest
import redis

from dcpy.infra.dagster_runners import worker
from dcpy.infra.dagster_runners.client import BrokerClient, Unavailable

STEP = """
import os
from dcpy.infra.dagster_runners.pipes_worker import open_broker_pipes
with open_broker_pipes() as pipes:
    print("stdout line", flush=True)
    pipes.log.info(f"partition {pipes.partition_key} on {os.environ['FOO']}")
    pipes.report_asset_materialization(metadata={"rows": 3})
"""


@pytest.fixture
def broker(monkeypatch):
    url = os.environ.get("DAGSTER_BROKER_TEST_VALKEY_URL")
    if not url:
        pytest.skip("DAGSTER_BROKER_TEST_VALKEY_URL not set")
    redis.Redis.from_url(url).flushdb()
    monkeypatch.setattr(worker, "HEARTBEAT_SECONDS", 0.3)
    monkeypatch.setattr(worker, "POLL_SECONDS", 0.1)
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    env = {
        **os.environ,
        "DAGSTER_BROKER_VALKEY_URL": url,
        "DAGSTER_BROKER_SHARED_KEY": "k",
    }
    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "dagster_broker.app:create_app", "--factory"]
        + ["--port", str(port), "--log-level", "warning"],
        env=env,
    )
    client = BrokerClient(f"http://127.0.0.1:{port}", "k", retries=0)
    for _ in range(100):
        try:
            client.get("/readyz")
            break
        except Unavailable:
            time.sleep(0.1)
    yield client
    server.terminate()
    server.wait(10)


def test_pipes_round_trip(broker):
    dg = pytest.importorskip("dagster")
    from dcpy.infra.dagster_runners.pipes_dagster import PipesBrokerClient

    @dg.asset(partitions_def=dg.StaticPartitionsDefinition(["a"]))
    def thing(context, pipes: PipesBrokerClient):
        cmd = [sys.executable, "-c", STEP]
        res = pipes.run(context=context, command=cmd, env={"FOO": "bar"})
        return res.get_materialize_result()

    w = worker.Worker(caps=["small"], client=broker)
    t = threading.Thread(target=w.run)
    t.start()
    try:
        pipes = PipesBrokerClient(bucket="b", broker=broker, poll=0.2)
        res = dg.materialize([thing], partition_key="a", resources={"pipes": pipes})
    finally:
        w.stop()
        t.join(timeout=10)
    assert res.success
    md = res.asset_materializations_for_node("thing")[0].metadata
    assert md["rows"].value == 3 and md["broker_attempt"].value == 1
    assert md["worker_id"].value.endswith("-0")
    events = broker.get(f"/items/{md['broker_item'].value}/events")["events"]
    pipes_msgs = [e["data"] for e in events if e["kind"] == "pipes"]
    assert pipes_msgs[0]["method"] == "opened"
    assert pipes_msgs[-1]["method"] == "closed"
    assert any("partition a on bar" in str(m) for m in pipes_msgs)
    assert any("stdout line" in e["data"].get("text", "") for e in events)


def test_reader_reports_only_the_finishing_attempt():
    pytest.importorskip("dagster")
    from dcpy.infra.dagster_runners.pipes_dagster import BrokerMessageReader

    handled = []
    reader = BrokerMessageReader()
    reader.handler = type("H", (), {"handle_message": lambda _, m: handled.append(m)})()
    for attempt, method in [
        (1, "opened"), (1, "log"), (1, "report_asset_materialization"),
        (2, "opened"), (2, "log"), (2, "report_asset_materialization"), (2, "closed"),
    ]:  # fmt: skip
        reader.feed(attempt, {"method": method, "attempt": attempt})
    reader.flush(2)
    assert [(m["attempt"], m["method"]) for m in handled] == [
        (1, "opened"), (1, "log"), (2, "log"),
        (2, "report_asset_materialization"), (2, "closed"),
    ]  # fmt: skip
