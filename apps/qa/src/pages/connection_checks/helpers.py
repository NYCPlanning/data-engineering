"""Live connectivity checks for the QAQC app's external dependencies.

Each check returns a CheckResult and never raises: a failure (including blocked egress or
missing credentials) becomes ok=False with a short detail. Secret values are never included
in results; env vars are reported as set/unset only.
"""

import os
import time
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlparse

import requests

from app_globals import QAQC_DB
from dcpy import configuration
from dcpy.utils import postgres
from dcpy.utils.git import github

TIMEOUT_SECONDS = 8

REQUIRED_ENV_VARS = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_S3_ENDPOINT",
    "BUILD_ENGINE_SERVER",
    "GHP_TOKEN",
    "PUBLISHING_BUCKET",
]
OPTIONAL_ENV_VARS = ["RECIPES_BUCKET"]


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    latency_ms: float | None
    detail: str


def _timed(name: str, fn: Callable[[], str]) -> CheckResult:
    """Run fn (returns a detail string, raises on failure) and wrap the outcome."""
    start = time.perf_counter()
    try:
        detail = fn()
        ok = True
    except Exception as e:
        # type name only for unexpected errors; messages from clients can echo URLs
        detail = f"{type(e).__name__}: {_scrub(str(e))}"
        ok = False
    return CheckResult(name, ok, (time.perf_counter() - start) * 1000, detail)


def _scrub(message: str) -> str:
    """Remove secret values from an error message before it's shown."""
    for var in ("AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "GHP_TOKEN"):
        value = os.environ.get(var)
        if value:
            message = message.replace(value, "***")
    server = os.environ.get("BUILD_ENGINE_SERVER")
    if server:
        password = urlparse(server).password
        if password:
            message = message.replace(password, "***")
    return message[:300]


def check_env_vars() -> list[CheckResult]:
    results = []
    for var in REQUIRED_ENV_VARS:
        is_set = bool(os.environ.get(var))
        results.append(
            CheckResult(
                f"env: {var}", is_set, None, "set" if is_set else "unset (required)"
            )
        )
    for var in OPTIONAL_ENV_VARS:
        is_set = bool(os.environ.get(var))
        detail = (
            "set"
            if is_set
            else f"unset (optional; using {configuration.RECIPES_BUCKET})"
        )
        results.append(CheckResult(f"env: {var}", True, None, detail))
    return results


def check_postgres() -> CheckResult:
    def run() -> str:
        import psycopg2

        server = os.environ.get("BUILD_ENGINE_SERVER")
        if not server:
            raise RuntimeError("BUILD_ENGINE_SERVER is not set")
        uri = postgres.generate_engine_uri(server, QAQC_DB)
        conn = psycopg2.connect(uri, connect_timeout=TIMEOUT_SECONDS)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        finally:
            conn.close()
        return f"SELECT 1 ok on database {QAQC_DB} at {urlparse(server).hostname}"

    return _timed("postgres", run)


def _short_timeout_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ.get("AWS_S3_ENDPOINT"),
        config=Config(
            connect_timeout=TIMEOUT_SECONDS,
            read_timeout=TIMEOUT_SECONDS,
            retries={"max_attempts": 1},
        ),
    )


def check_s3_bucket(label: str, bucket: str | None, client=None) -> CheckResult:
    def run() -> str:
        if not bucket:
            raise RuntimeError("bucket name is not configured")
        c = client if client is not None else _short_timeout_client()
        c.list_objects_v2(Bucket=bucket, MaxKeys=1)
        endpoint = urlparse(os.environ.get("AWS_S3_ENDPOINT") or "").hostname
        return f"listed bucket {bucket} at {endpoint or 'default AWS endpoint'}"

    return _timed(f"s3: {label}", run)


def check_s3(client=None) -> list[CheckResult]:
    return [
        check_s3_bucket("publishing", configuration.PUBLISHING_BUCKET, client),
        check_s3_bucket("recipes", configuration.RECIPES_BUCKET, client),
    ]


def check_github() -> CheckResult:
    def run() -> str:
        r = requests.get(
            "https://api.github.com/rate_limit",
            headers=github.get_headers(),
            timeout=TIMEOUT_SECONDS,
        )
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized: token rejected")
        r.raise_for_status()
        remaining = r.json()["resources"]["core"]["remaining"]
        return f"authenticated; {remaining} core requests remaining"

    return _timed("github", run)


def check_repo_paths() -> list[CheckResult]:
    products_dir = configuration.PROJECT_ROOT_PATH / "products"
    paths = {
        "product-metadata": configuration.PRODUCT_METADATA_REPO_PATH,
        "products dir": products_dir,
    }
    return [
        CheckResult(
            f"path: {name}",
            path.is_dir(),
            None,
            f"{path} " + ("exists" if path.is_dir() else "missing"),
        )
        for name, path in paths.items()
    ]


def run_all_checks() -> list[CheckResult]:
    return [
        *check_env_vars(),
        check_postgres(),
        *check_s3(),
        check_github(),
        *check_repo_paths(),
    ]
