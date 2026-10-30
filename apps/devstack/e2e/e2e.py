"""Dagster-side half of the devstack e2e (apps/devstack/e2e/e2e.sh drives it).

Runs inside the dagster-webserver container (DAGSTER_HOME, the GraphQL API on :3000 and
the gateway are all there): ``python /repo/apps/devstack/e2e/e2e.py <command> ...``.
Launches runs over GraphQL, reads results from the Dagster instance and checks promoted
outputs through the S3 gateway. Each command prints one line and exits non-zero on failure.
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request

import boto3
from dagster import AssetKey, AssetRecordsFilter, DagsterInstance, DagsterRunStatus
from dagster_graphql import DagsterGraphQLClient

TERMINAL = {
    DagsterRunStatus.SUCCESS,
    DagsterRunStatus.FAILURE,
    DagsterRunStatus.CANCELED,
}
CLAIMED = re.compile(r"\[dagster-broker\] item (\d+) attempt (\d+) on worker (\S+)")
BUCKET = os.environ.get("DEVSTACK_BUCKET", "devstack")


def gql() -> DagsterGraphQLClient:
    return DagsterGraphQLClient("localhost", port_number=3000)


def fail(msg: str) -> None:
    print(f"FAIL {msg}")
    sys.exit(1)


def launch(a) -> None:
    run_config = {}
    if a.build_sleep is not None:
        run_config = {
            "ops": {
                "build": {
                    "config": {"sleep_seconds": a.build_sleep, "files": a.build_files}
                }
            }
        }
    run_id = gql().submit_job_execution(
        a.job,
        repository_location_name="devstack_demo",
        run_config=run_config,
        tags={"dagster/partition": a.partition, "e2e": "1"},
    )
    print(run_id)


def wait(a) -> None:
    inst = DagsterInstance.get()
    deadline = time.monotonic() + a.timeout
    while time.monotonic() < deadline:
        run = inst.get_run_by_id(a.run_id)
        if run and run.status in TERMINAL:
            print(run.status.value)
            return
        time.sleep(1)
    fail(
        f"run {a.run_id} not finished after {a.timeout}s ({run.status.value if run else 'missing'})"
    )


def claims(inst: DagsterInstance, run_id: str, step: str) -> list[tuple[int, int, str]]:
    out = []
    for e in inst.all_logs(run_id):
        if e.step_key == step and (m := CLAIMED.search(e.user_message or "")):
            out.append((int(m[1]), int(m[2]), m[3]))
    return out


def wait_claimed(a) -> None:
    """Print 'item attempt worker' once the step's item is claimed (attempt > a.after)."""
    inst = DagsterInstance.get()
    deadline = time.monotonic() + a.timeout
    while time.monotonic() < deadline:
        found = claims(inst, a.run_id, a.step)
        if a.item is not None:
            found = [c for c in found if c[0] != a.item or c[1] > a.after]
        if found:
            print(*found[-1])
            return
        time.sleep(0.5)
    fail(f"step {a.step} of run {a.run_id} never claimed")


def wait_log(a) -> None:
    inst = DagsterInstance.get()
    deadline = time.monotonic() + a.timeout
    while time.monotonic() < deadline:
        for e in inst.all_logs(a.run_id):
            if (a.step is None or e.step_key == a.step) and a.text in (
                e.user_message or ""
            ):
                print("found")
                return
        time.sleep(0.5)
    fail(f"log line {a.text!r} not seen in run {a.run_id}")


def logs(a) -> None:
    for e in DagsterInstance.get().all_logs(a.run_id):
        if a.grep is None or a.grep in (e.user_message or ""):
            print(
                e.step_key,
                "|",
                (e.user_message or "").splitlines()[0][:200] if e.user_message else "",
            )


def terminate(a) -> None:
    gql().terminate_run(a.run_id)
    print("terminated")


def latest_metadata(inst, asset: str, partition: str) -> tuple[dict, str]:
    res = inst.fetch_materializations(
        AssetRecordsFilter(asset_key=AssetKey(asset), asset_partitions=[partition]),
        limit=1,
    )
    if not res.records:
        fail(f"no materialization of {asset}[{partition}]")
    mat = res.records[0].asset_materialization
    return {k: v.value for k, v in mat.metadata.items()}, res.records[0].run_id


def assert_chain(a) -> None:
    inst = DagsterInstance.get()
    run = inst.get_run_by_id(a.run_id)
    if run.status != DagsterRunStatus.SUCCESS:
        fail(f"run {a.run_id} is {run.status.value}")
    s3 = boto3.client("s3", endpoint_url=os.environ["AWS_S3_ENDPOINT"])
    report = {}
    for asset, needed in (
        ("plan", {"tasks": 3}),
        ("build", {"files": a.build_files, "plan_tasks": 3}),
        ("publish", {"objects": a.build_files}),
    ):
        md, mat_run = latest_metadata(inst, asset, a.partition)
        if mat_run != a.run_id:
            fail(
                f"latest {asset}[{a.partition}] came from run {mat_run}, not {a.run_id}"
            )
        for k, v in needed.items():
            if md.get(k) != v:
                fail(
                    f"{asset}[{a.partition}] metadata {k}={md.get(k)!r}, want {v!r}: {md}"
                )
        for k in (
            "broker_item",
            "broker_attempt",
            "worker_id",
            "promoted_objects",
            "outputs_uri",
            "step_host",
        ):
            if md.get(k) in (None, ""):
                fail(f"{asset}[{a.partition}] metadata lacks {k}: {md}")
        prefix = f"demo/{asset}/{a.partition}/"
        keys = [
            o["Key"]
            for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get(
                "Contents", []
            )
        ]
        if md["promoted_objects"] < 1 or len(keys) < md["promoted_objects"]:
            fail(f"{asset}: promoted {md['promoted_objects']} but gateway has {keys}")
        report[asset] = {
            "item": md["broker_item"],
            "attempt": md["broker_attempt"],
            "worker": md["worker_id"],
            "objects": len(keys),
        }
    staged = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"_staging/{a.run_id}/").get(
        "KeyCount", 0
    )
    if a.expect_clean_staging and staged:
        fail(f"{staged} staging objects left for run {a.run_id}")
    # publish ran on the esri worker (caps routing)
    if not report["publish"]["worker"]:
        fail("publish has no worker id")
    report["staging_left"] = staged
    print(json.dumps(report))


def broker_get(path: str) -> dict:
    req = urllib.request.Request(
        os.environ["DAGSTER_BROKER_URL"] + path,
        headers={"X-Dagster-Broker-Key": os.environ.get("DAGSTER_BROKER_SHARED_KEY", "")},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def item(a) -> None:
    print(json.dumps(broker_get(f"/items/{a.item_id}")))


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("launch")
    s.add_argument("job")
    s.add_argument("partition")
    s.add_argument("--build-sleep", type=float)
    s.add_argument("--build-files", type=int, default=3)
    s.set_defaults(fn=launch)
    s = sub.add_parser("wait")
    s.add_argument("run_id")
    s.add_argument("--timeout", type=float, default=300)
    s.set_defaults(fn=wait)
    s = sub.add_parser("wait-claimed")
    s.add_argument("run_id")
    s.add_argument("step")
    s.add_argument(
        "--item", type=int, help="with --after: wait for a claim other than this one"
    )
    s.add_argument("--after", type=int, default=0)
    s.add_argument("--timeout", type=float, default=120)
    s.set_defaults(fn=wait_claimed)
    s = sub.add_parser("wait-log")
    s.add_argument("run_id")
    s.add_argument("text")
    s.add_argument("--step")
    s.add_argument("--timeout", type=float, default=120)
    s.set_defaults(fn=wait_log)
    s = sub.add_parser("logs")
    s.add_argument("run_id")
    s.add_argument("--grep")
    s.set_defaults(fn=logs)
    s = sub.add_parser("terminate")
    s.add_argument("run_id")
    s.set_defaults(fn=terminate)
    s = sub.add_parser("assert-chain")
    s.add_argument("run_id")
    s.add_argument("partition")
    s.add_argument("--build-files", type=int, default=3)
    s.add_argument("--expect-clean-staging", action="store_true")
    s.set_defaults(fn=assert_chain)
    s = sub.add_parser("item")
    s.add_argument("item_id", type=int)
    s.set_defaults(fn=item)
    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
