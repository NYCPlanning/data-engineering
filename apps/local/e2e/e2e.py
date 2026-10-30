"""Dagster-side half of the local e2e (apps/local/e2e/e2e.sh drives it).

Runs inside the dagster container (DAGSTER_HOME, the GraphQL API on :3000 and the gateway
are all there): ``python /app/repos/data-engineering/apps/local/e2e/e2e.py <command> ...``.
Launches runner_tests jobs over GraphQL, reads results from the Dagster instance and checks
promoted outputs through the S3 gateway. Each command prints one line, or fails with exit 1.
"""

import argparse
import json
import os
import re
import sys
import time

import boto3
from dagster import DagsterEventType, DagsterInstance, DagsterRunStatus
from dagster_graphql import DagsterGraphQLClient

TERMINAL = {
    DagsterRunStatus.SUCCESS,
    DagsterRunStatus.FAILURE,
    DagsterRunStatus.CANCELED,
}
CLAIMED = re.compile(
    r"\[dagster-broker\] item ([0-9a-f]+) attempt (\d+) on worker (\S+)"
)
LOCATIONS = """{ workspaceOrError { ... on Workspace { locationEntries {
  name locationOrLoadError { __typename ... on PythonError { message } } } } } }"""


def gql() -> DagsterGraphQLClient:
    return DagsterGraphQLClient("localhost", port_number=3000)


def fail(msg: str) -> None:
    print(f"FAIL {msg}")
    sys.exit(1)


def poll(timeout: float, fn, what: str):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if found := fn():
            return found
        time.sleep(0.5)
    fail(f"timed out after {timeout}s waiting for {what}")


def locations(a) -> None:
    entries = gql()._execute(LOCATIONS)["workspaceOrError"]["locationEntries"]
    for e in entries:
        if e["locationOrLoadError"]["__typename"] != "RepositoryLocation":
            fail(f"location {e['name']} didn't load: {e['locationOrLoadError']}")
    print(" ".join(sorted(e["name"] for e in entries)))


def launch(a) -> None:
    run_config = {}
    if a.seconds is not None:
        config = {"seconds": a.seconds, "parts": 3}
        run_config = {"ops": {a.job: {"config": config}}}
    print(gql().submit_job_execution(a.job, run_config=run_config))


def wait(a) -> None:
    inst = DagsterInstance.get()

    def status():
        run = inst.get_run_by_id(a.run_id)
        return run.status.value if run and run.status in TERMINAL else None

    print(poll(a.timeout, status, f"run {a.run_id} to finish"))


def wait_claimed(a) -> None:
    """Print 'item attempt worker' once the step's item is claimed (attempt > a.after)."""
    inst = DagsterInstance.get()

    def claimed():
        found = [
            (m[1], int(m[2]), m[3])
            for e in inst.all_logs(a.run_id)
            if e.step_key == a.step and (m := CLAIMED.search(e.user_message or ""))
        ]
        found = [c for c in found if c[0] != a.item or c[1] > a.after]
        return found[-1] if found else None

    print(*poll(a.timeout, claimed, f"a claim of {a.step} in run {a.run_id}"))


def wait_log(a) -> None:
    inst = DagsterInstance.get()

    def seen():
        return any(
            (a.step is None or e.step_key == a.step)
            and a.text in (e.user_message or "")
            for e in inst.all_logs(a.run_id)
        )

    poll(a.timeout, seen, f"log line {a.text!r} in run {a.run_id}")
    print("found")


def terminate(a) -> None:
    gql().terminate_run(a.run_id)
    print("terminated")


def assert_run(a) -> None:
    """The run succeeded, each materialization says where it ran, its outputs were
    promoted, and no staging objects are left."""
    inst = DagsterInstance.get()
    run = inst.get_run_by_id(a.run_id)
    if run.status != DagsterRunStatus.SUCCESS:
        fail(f"run {a.run_id} is {run.status.value}")
    s3 = boto3.client("s3", endpoint_url=os.environ["AWS_S3_ENDPOINT"])
    bucket = os.environ["DAGSTER_BROKER_BUCKET"]
    report = {}
    events = inst.all_logs(a.run_id, of_type=DagsterEventType.ASSET_MATERIALIZATION)
    for e in events:
        mat = e.asset_materialization
        md = {k: v.value for k, v in mat.metadata.items()}
        name = mat.asset_key.to_user_string()
        for k in (
            "broker_item",
            "worker_id",
            "promoted_objects",
            "outputs_uri",
            "bytes",
        ):
            if md.get(k) in (None, ""):
                fail(f"{name} metadata lacks {k}: {md}")
        prefix = md["outputs_uri"].split("/", 3)[3]
        listed = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
        if not listed or len(listed) != md["promoted_objects"]:
            fail(
                f"{name}: promoted {md['promoted_objects']}, gateway has {len(listed)}"
            )
        if sum(o["Size"] for o in listed) != md["bytes"]:
            fail(f"{name}: wrote {md['bytes']} bytes, gateway has {listed}")
        report[name] = {
            "item": md["broker_item"],
            "attempt": md["broker_attempt"],
            "worker": md["worker_id"],
            "objects": len(listed),
            "bytes": md["bytes"],
        }
    if not report:
        fail(f"run {a.run_id} materialized nothing")
    staged = s3.list_objects_v2(Bucket=bucket, Prefix=f"_staging/{a.run_id}/")
    if staged.get("KeyCount"):
        fail(f"{staged['KeyCount']} staging objects left for run {a.run_id}")
    print(json.dumps(report))


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("locations").set_defaults(fn=locations)
    s = sub.add_parser("launch")
    s.add_argument("job")
    s.add_argument("--seconds", type=float)
    s.set_defaults(fn=launch)
    s = sub.add_parser("wait")
    s.add_argument("run_id")
    s.add_argument("--timeout", type=float, default=300)
    s.set_defaults(fn=wait)
    s = sub.add_parser("wait-claimed")
    s.add_argument("run_id")
    s.add_argument("step")
    s.add_argument("--item", help="with --after: wait for a claim other than this one")
    s.add_argument("--after", type=int, default=0)
    s.add_argument("--timeout", type=float, default=120)
    s.set_defaults(fn=wait_claimed)
    s = sub.add_parser("wait-log")
    s.add_argument("run_id")
    s.add_argument("text")
    s.add_argument("--step")
    s.add_argument("--timeout", type=float, default=120)
    s.set_defaults(fn=wait_log)
    s = sub.add_parser("terminate")
    s.add_argument("run_id")
    s.set_defaults(fn=terminate)
    s = sub.add_parser("assert-run")
    s.add_argument("run_id")
    s.set_defaults(fn=assert_run)
    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
