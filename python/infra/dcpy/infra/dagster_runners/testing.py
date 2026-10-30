"""The step the runner_tests code location runs on workers:
``python -m dcpy.infra.dagster_runners.testing``.

Driven by the Pipes extras: works for ``seconds``, writing ``mb`` MB to this attempt's
staging prefix in ``parts`` objects (the worker promotes them), then fails if ``fail``.
"""

import os
import socket
import sys
import time

import boto3

from dcpy.infra.dagster_runners.pipes_worker import open_broker_pipes, staging_prefix


def main() -> None:
    with open_broker_pipes() as pipes:
        x = pipes.extras
        parts, size = int(x["parts"]), int(x["mb"]) * 2**20 // int(x["parts"])
        s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_S3_ENDPOINT"))
        print(f"writing {parts} x {size} bytes over {x['seconds']}s", flush=True)
        for i in range(parts):
            time.sleep(float(x["seconds"]) / parts)
            key = f"{staging_prefix()}part-{i:03d}.bin"
            s3.put_object(Bucket=x["bucket"], Key=key, Body=os.urandom(size))
            pipes.log.info(f"wrote part {i + 1}/{parts}")
        if x["fail"]:
            sys.exit("failing, as asked")
        pipes.report_asset_materialization(
            metadata={"bytes": parts * size, "step_host": socket.gethostname()}
        )


if __name__ == "__main__":
    main()
