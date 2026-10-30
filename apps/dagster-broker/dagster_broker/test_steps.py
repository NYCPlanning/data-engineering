"""Fake steps for apps/dagster's runner_tests code location. A worker calls them by dotted
path and reports the returned dict as the materialization's metadata."""

import logging
import os
import socket
import time

import boto3

logger = logging.getLogger(__name__)


def work(
    seconds: float = 1.0,
    mb: int = 0,
    parts: int = 1,
    fail: bool = False,
    output_bucket: str = "",
    output_prefix: str = "",
) -> dict:
    """Works for ``seconds`` in ``parts``, writing ``mb`` MB in total under
    ``output_prefix``, then raises if ``fail``."""
    size = mb * 2**20 // parts
    s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_S3_ENDPOINT"))
    for i in range(parts):
        time.sleep(seconds / parts)
        if size:
            key = f"{output_prefix}part-{i:03d}.bin"
            s3.put_object(Bucket=output_bucket, Key=key, Body=os.urandom(size))
        logger.info(f"wrote part {i + 1}/{parts}")
    if fail:
        raise RuntimeError("failing, as asked")
    return {"bytes": parts * size, "step_host": socket.gethostname()}
