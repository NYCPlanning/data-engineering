"""Launch an ingest asset on the local stack through the webserver's GraphQL API and wait for it.

    python apps/azure-local/launch_ingest.py bpl_libraries [--version 20261124] [--no-latest]

Registers the version as a key in the ingest_version dynamic partitions def, then launches
ingest_<template> for that partition with IngestConfig. Standard library only.
"""

import argparse
import json
import sys
import time
import urllib.request
from datetime import date

LOCATION = "data-engineering"
REPOSITORY = "__repository__"
PARTITIONS_DEF = "ingest_version"
TERMINAL = {"SUCCESS", "FAILURE", "CANCELED"}


def gql(url: str, query: str, variables: dict) -> dict:
    req = urllib.request.Request(
        f"{url}/graphql",
        data=json.dumps({"query": query, "variables": variables}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        body = json.load(resp)
    if body.get("errors"):
        sys.exit(f"GraphQL errors: {body['errors']}")
    return body["data"]


ADD_PARTITION = """
mutation($selector: RepositorySelector!, $name: String!, $key: String!) {
  addDynamicPartition(repositorySelector: $selector, partitionsDefName: $name, partitionKey: $key) {
    __typename
    ... on PythonError { message }
    ... on DuplicateDynamicPartitionError { partitionName }
  }
}
"""

LAUNCH = """
mutation($params: ExecutionParams!) {
  launchPipelineExecution(executionParams: $params) {
    __typename
    ... on LaunchRunSuccess { run { runId } }
    ... on PythonError { message stack }
    ... on RunConfigValidationInvalid { errors { message } }
    ... on InvalidSubsetError { message }
  }
}
"""

STATUS = "query($runId: ID!) { runOrError(runId: $runId) { ... on Run { status } } }"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("template")
    parser.add_argument("--version", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--no-latest", action="store_true")
    parser.add_argument("--url", default="http://127.0.0.1:3000")
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()

    selector = {"repositoryLocationName": LOCATION, "repositoryName": REPOSITORY}
    added = gql(
        args.url,
        ADD_PARTITION,
        {"selector": selector, "name": PARTITIONS_DEF, "key": args.version},
    )["addDynamicPartition"]
    if added["__typename"] not in (
        "AddDynamicPartitionSuccess",
        "DuplicateDynamicPartitionError",
    ):
        sys.exit(f"addDynamicPartition: {added}")

    asset = f"ingest_{args.template}"
    params = {
        "selector": {
            **selector,
            "pipelineName": "__ASSET_JOB",
            "assetSelection": [{"path": [asset]}],
        },
        "runConfigData": {
            "ops": {asset: {"config": {"push": True, "latest": not args.no_latest}}}
        },
        "executionMetadata": {
            "tags": [{"key": "dagster/partition", "value": args.version}]
        },
        "mode": "default",
    }
    launched = gql(args.url, LAUNCH, {"params": params})["launchPipelineExecution"]
    if launched["__typename"] != "LaunchRunSuccess":
        sys.exit(f"launch failed: {launched}")
    run_id = launched["run"]["runId"]
    print(f"run {run_id}: {args.url}/runs/{run_id}", flush=True)

    deadline = time.time() + args.timeout
    status = None
    while time.time() < deadline:
        status = gql(args.url, STATUS, {"runId": run_id})["runOrError"]["status"]
        if status in TERMINAL:
            break
        time.sleep(5)
    print(f"run {run_id}: {status}")
    sys.exit(0 if status == "SUCCESS" else 1)


if __name__ == "__main__":
    main()
