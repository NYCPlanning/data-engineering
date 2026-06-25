"""Import every app module so dead/stale imports fail in CI, not on a user's page click.

The qa app loads page modules lazily (per page), so a moved/removed dependency — e.g. an
import of a dcpy module that has since been relocated — only surfaces when someone navigates
to that page. Importing every module here turns that into a deterministic, docker-free
collection-time failure. Complements the container boot smoke test (which checks "does it
serve", not "does every page import").
"""

import importlib
import pathlib

import pytest

_SRC = pathlib.Path(__file__).resolve().parent.parent / "src"
_MODULES = sorted(
    ".".join(p.relative_to(_SRC).with_suffix("").parts)
    for p in _SRC.rglob("*.py")
    if "__pycache__" not in p.parts
)


@pytest.mark.parametrize("module", _MODULES)
def test_module_imports(module: str) -> None:
    importlib.import_module(module)
