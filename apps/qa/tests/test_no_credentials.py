"""Every page module must import with no credentials in the environment, so the app can
boot (and the Connection Checks page can report what's missing) before secrets are wired up."""

import subprocess
import sys
from pathlib import Path

import pytest

from shared.constants import PAGES

_SRC = Path(__file__).resolve().parent.parent / "src"
_CRED_VARS = [
    "GHP_TOKEN",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_S3_ENDPOINT",
    "BUILD_ENGINE_SERVER",
]


def test_conftest_creds_are_removable(monkeypatch):
    for var in _CRED_VARS:
        monkeypatch.delenv(var, raising=False)
    import os

    assert not any(v in os.environ for v in _CRED_VARS)


@pytest.mark.parametrize("page", sorted(PAGES.values()))
def test_page_imports_without_credentials(page: str) -> None:
    # fresh interpreter: modules already imported by other tests (with the conftest's
    # placeholder credentials) would otherwise mask an import-time env read
    import os

    env = {
        k: v
        for k, v in os.environ.items()
        if k not in _CRED_VARS and not k.startswith("AWS_")
    }
    # not a secret; the edde page asserts it at import, and deploy config always sets it
    env["PUBLISHING_BUCKET"] = "edm-publishing"
    env["PYTHONPATH"] = str(_SRC)
    result = subprocess.run(
        [sys.executable, "-c", f"import pages.{page}.{page} as m; m.{page}"],
        env=env,
        capture_output=True,
        text=True,
        cwd=_SRC,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr[-2000:]
