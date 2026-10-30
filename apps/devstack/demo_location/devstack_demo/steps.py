"""Demo step entrypoints, run on a dagster-broker worker: ``python -m devstack_demo.steps <name>``.

Each opens Dagster Pipes over dagster-broker (context from the env the worker set, messages
posted to dagster-broker), reads upstream outputs from the S3 gateway, writes its own outputs to
this attempt's staging prefix, and reports materialization metadata. Needs dagster-pipes,
boto3 and dagster-broker only.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import sys
import time

import boto3

from dagster_broker.pipes_worker import open_broker_pipes, staging_prefix


def s3():
    return boto3.client("s3", endpoint_url=os.environ.get("AWS_S3_ENDPOINT"))


def plan(pipes, client, bucket: str) -> dict:
    p = pipes.partition_key
    doc = {
        "partition": p,
        "tasks": ["extract", "transform", "load"],
        "host": socket.gethostname(),
    }
    client.put_object(
        Bucket=bucket, Key=staging_prefix() + "plan.json", Body=json.dumps(doc).encode()
    )
    pipes.log.info(f"planned {len(doc['tasks'])} tasks for {p}")
    return {"tasks": len(doc["tasks"])}


def build(pipes, client, bucket: str) -> dict:
    extras = pipes.extras
    plan_doc = json.loads(
        client.get_object(Bucket=bucket, Key=extras["inputs"]["plan"] + "plan.json")[
            "Body"
        ].read()
    )
    files, size = int(extras["files"]), int(extras["file_kb"]) * 1024
    sleep = float(extras["sleep_seconds"])
    pipes.log.info(
        f"building {files} files from plan {plan_doc['tasks']}, {sleep}s of work"
    )
    print(f"build: {files} files x {size} bytes", flush=True)  # stdout -> stdio events
    total = 0
    for i in range(files):
        # The 'work': spread the sleep over the files so progress shows up live.
        time.sleep(sleep / files)
        body = hashlib.sha256(f"{plan_doc['partition']}-{i}".encode()).digest() * (
            size // 32
        )
        client.put_object(
            Bucket=bucket, Key=f"{staging_prefix()}part-{i:03d}.bin", Body=body
        )
        total += len(body)
        pipes.log.info(f"wrote part {i + 1}/{files}")
    return {"files": files, "bytes": total, "plan_tasks": len(plan_doc["tasks"])}


def publish(pipes, client, bucket: str) -> dict:
    src = pipes.extras["inputs"]["build"]
    objects = client.list_objects_v2(Bucket=bucket, Prefix=src).get("Contents", [])
    digest = hashlib.sha256()
    for o in sorted(objects, key=lambda o: o["Key"]):
        digest.update(client.get_object(Bucket=bucket, Key=o["Key"])["Body"].read())
    manifest = {
        "partition": pipes.partition_key,
        "objects": [o["Key"] for o in objects],
        "sha256": digest.hexdigest(),
    }
    client.put_object(
        Bucket=bucket,
        Key=staging_prefix() + "manifest.json",
        Body=json.dumps(manifest).encode(),
    )
    pipes.log.info(f"published manifest over {len(objects)} objects")
    return {"objects": len(objects), "sha256": manifest["sha256"]}


STEPS = {"plan": plan, "build": build, "publish": publish}


def main() -> None:
    name = sys.argv[1]
    with open_broker_pipes() as pipes:
        bucket = pipes.extras["bucket"]
        metadata = STEPS[name](pipes, s3(), bucket)
        metadata["step_host"] = socket.gethostname()
        pipes.report_asset_materialization(metadata=metadata)


if __name__ == "__main__":
    main()
