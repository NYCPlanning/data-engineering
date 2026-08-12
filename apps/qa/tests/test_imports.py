"""Import every app module so dead/stale imports fail in CI, not on a user's page click.

The qa app loads page modules lazily (per page), so a moved/removed dependency — e.g. an
import of a dcpy module that has since been relocated — only surfaces when someone navigates
to that page. Importing every module here turns that into a deterministic, docker-free
collection-time failure. Complements the container boot smoke test (which checks "does it
serve", not "does every page import").
"""

import ast
import importlib
import pathlib

import pytest

_SRC = pathlib.Path(__file__).resolve().parent.parent / "src"
_FILES = sorted(p for p in _SRC.rglob("*.py") if "__pycache__" not in p.parts)
_MODULES = sorted(".".join(p.relative_to(_SRC).with_suffix("").parts) for p in _FILES)


@pytest.mark.parametrize("module", _MODULES)
def test_module_imports(module: str) -> None:
    importlib.import_module(module)


def _absolute_imports(path: pathlib.Path) -> list[tuple[int, str]]:
    """Every module a file imports, as an absolute dotted name, relative imports resolved.

    Walks the syntax tree rather than importing, so imports nested inside a function body count
    too. Page modules put nearly every import inside their page function, so those are invisible
    to `test_module_imports` above — which is how a bad import reaches production: it only fires
    when a user opens that page.
    """
    package = path.relative_to(_SRC).parts[:-1]
    found = []
    for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
        if isinstance(node, ast.Import):
            found.extend((node.lineno, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                if node.module:
                    found.append((node.lineno, node.module))
            else:
                base = package[: len(package) - node.level + 1]
                found.append(
                    (
                        node.lineno,
                        ".".join([*base, *([node.module] if node.module else [])]),
                    )
                )
    return found


def _resolves_under_src(module: str) -> bool:
    path = _SRC.joinpath(*module.split("."))
    return path.is_dir() or path.with_suffix(".py").is_file()


@pytest.mark.parametrize("path", _FILES, ids=lambda p: str(p.relative_to(_SRC)))
def test_no_src_prefixed_imports(path: pathlib.Path) -> None:
    """`src` is not importable in production, so `from src.shared...` is always wrong.

    Deployment runs `streamlit run .../apps/qa/src/index.py`, which puts `src/` itself on
    sys.path — never its parent — and there is no `__init__.py` to make `src` a package. Such an
    import only resolves when `apps/qa` happens to be on sys.path, which is true when a developer
    runs from that directory and false in the container (`WORKDIR /app`).
    """
    offenders = [
        f"{path.relative_to(_SRC)}:{line} imports {module}"
        for line, module in _absolute_imports(path)
        if module == "src" or module.startswith("src.")
    ]
    assert not offenders, "import relative to src/ instead: " + "; ".join(offenders)


@pytest.mark.parametrize("path", _FILES, ids=lambda p: str(p.relative_to(_SRC)))
def test_first_party_imports_resolve(path: pathlib.Path) -> None:
    """A first-party import must point at something that exists under src/.

    Catches a moved or renamed module even where the import sits inside a page function, which
    `test_module_imports` cannot reach.
    """
    broken = [
        f"{path.relative_to(_SRC)}:{line} imports {module}"
        for line, module in _absolute_imports(path)
        if _resolves_under_src(module.split(".")[0]) and not _resolves_under_src(module)
    ]
    assert not broken, "no such module under src/: " + "; ".join(broken)
