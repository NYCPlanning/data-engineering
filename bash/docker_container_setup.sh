#!/bin/bash
set -e

PARENT_DIR=$(dirname "$(readlink -f "$0")")
mkdir -p $HOME

if [[ -z "$CI" ]]; then
    # local dev
    git config --global --add safe.directory /workspace
else
    # running in github CI
    git config --global --add safe.directory /__w/data-engineering/data-engineering
    # in case the devcontainer is being used
    git config --global --add safe.directory /home/vscode/workspace
fi

# dcpy is a uv workspace (see python/*/pyproject.toml) rather than a single
# top-level package installable via `pip install .` - setuptools can't auto-discover
# a package root here (there are several: apps/, dcpy/, products/, etc). `uv sync`
# installs every dcpy-* workspace package editable in one shot instead.
#
# The venv is created with access to system site-packages (where dbt, mypy, sqlfluff,
# and the rest of admin/run_environment/requirements.in already live in this image) -
# product build scripts commonly need both those AND dcpy in the same process, e.g.
# products/template/build_scripts/transform.py imports both `dbt` and `dcpy.utils`.
#
# Some jobs run a bare `uv sync --all-packages` earlier in the same job (before this
# script), which creates a plain venv without that flag. Reusing it here would silently
# leave dbt/mypy/etc unreachable, so check for the flag rather than just venv existence,
# and recreate if it's missing.
if [[ ! -d .venv ]] || ! grep -qx "include-system-site-packages = true" .venv/pyvenv.cfg 2>/dev/null; then
    rm -rf .venv
    uv venv --system-site-packages
fi
uv sync --all-packages

# Put the synced venv on PATH for the rest of this job, so every later step's plain
# `python3`, `dcpy`, `pytest`, etc. resolves against it - without that, only commands
# explicitly prefixed with `uv run` would see the workspace, and product build/pipeline
# scripts across the repo call python and the `dcpy` console script directly, unprefixed.
#
# Only works for native GH Actions `container:` jobs and normal (non-container) runner
# jobs, where the runner bind-mounts the real $GITHUB_PATH file into the execution
# context. Jobs that instead run this script inside a docker-compose service via
# `docker exec` (e.g. pytest_dcpy) inherit $GITHUB_PATH as a stale env var (dumped into
# the container via `env_file`) pointing at a path that only exists on the runner's own
# filesystem, not inside the container - so guard on the file actually being reachable
# here rather than just the var being set. Those jobs invoke `uv run` explicitly instead.
if [[ -n "$GITHUB_PATH" && -f "$GITHUB_PATH" ]]; then
    echo "$(pwd)/.venv/bin" >> "$GITHUB_PATH"
fi
