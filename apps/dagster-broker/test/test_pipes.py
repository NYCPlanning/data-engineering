"""A Dagster asset run through Pipes on a worker thread, via a live broker (uvicorn):
apps/dagster's PipesBrokerClient -> broker -> worker -> dagster_broker.pipes_worker ->
dagster_broker.test_steps.work."""

import sys
import threading
import time
from pathlib import Path

import dagster as dg
import pytest
import uvicorn

from dagster_broker import worker
from dagster_broker.app import create_app
from dcpy.infra.dagster_broker_client import BrokerClient

from .test_broker import valkey_url  # noqa: F401 (fixture)

sys.path.append(
    str(Path(__file__).parents[2] / "dagster")
)  # apps/dagster's dagster_utils
from dagster_utils.pipes_broker import BrokerMessageReader, PipesBrokerClient  # noqa: E402


@pytest.fixture
def broker(valkey_url, monkeypatch):  # noqa: F811
    monkeypatch.setenv("DAGSTER_BROKER_VALKEY_URL", valkey_url)
    monkeypatch.setenv("DAGSTER_BROKER_SHARED_KEY", "k")
    monkeypatch.setattr(worker, "HEARTBEAT_SECONDS", 0.3)
    monkeypatch.setattr(worker, "POLL_SECONDS", 0.1)
    config = uvicorn.Config(create_app(), port=0, log_level="warning")
    server = uvicorn.Server(config)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    while not server.started:
        time.sleep(0.05)
    port = server.servers[0].sockets[0].getsockname()[1]
    yield BrokerClient(f"http://127.0.0.1:{port}", "k", retries=0)
    server.should_exit = True
    t.join(timeout=10)


def test_pipes_round_trip(broker):
    @dg.asset(partitions_def=dg.StaticPartitionsDefinition(["a"]))
    def thing(context, pipes: PipesBrokerClient):
        args = {"seconds": 0, "parts": 2}
        res = pipes.run(
            context=context, function="dagster_broker.test_steps.work", args=args
        )
        return res.get_materialize_result()

    w = worker.Worker(caps=["small"], client=broker)
    t = threading.Thread(target=w.run)
    t.start()
    try:
        pipes = PipesBrokerClient(broker=broker, poll=0.2)
        res = dg.materialize([thing], partition_key="a", resources={"pipes": pipes})
    finally:
        w.stop()
        t.join(timeout=10)
    assert res.success
    md = res.asset_materializations_for_node("thing")[0].metadata
    assert md["bytes"].value == 0 and md["broker_attempt"].value == 1
    assert md["worker_id"].value.endswith("-0")
    events = broker.get(f"/items/{md['broker_item'].value}/events")["events"]
    pipes_msgs = [e["data"] for e in events if e["kind"] == "pipes"]
    assert pipes_msgs[0]["method"] == "opened"
    assert pipes_msgs[-1]["method"] == "closed"
    assert any("wrote part 2/2" in str(m) for m in pipes_msgs)


def test_reader_reports_only_the_finishing_attempt():

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
