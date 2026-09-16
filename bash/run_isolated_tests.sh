#!/bin/bash

PARENT_DIR=$(dirname "$(readlink -f "$0")")
cd "$PARENT_DIR/.."

# Mirrors the "dcpy main pytests" / "dcpy library pytests" steps in
# .github/workflows/test_helper.yml - keep both in sync.
#
# `uv sync --all-packages` (the default local dev setup) installs every dcpy-* package
# into one shared venv, so a package's tests can pass by silently importing a sibling
# package that was never declared as a real dependency (e.g. dcpy.utils importing
# dcpy.lifecycle would work by accident, even though nothing should depend on
# dcpy-lifecycle - it's the top of the hierarchy). Re-syncing the same venv scoped to
# just one package at a time (uv sync --package dcpy-X) actually prunes it, so tests
# like each package's test_package_boundary.py only pass when the boundary is real.
#
# Runs every package regardless of earlier failures, then exits non-zero at the end if
# anything failed, same as bash/run_mypy.sh.

# Computed from python/*/pyproject.toml rather than hardcoded, so a newly added
# workspace package is picked up without editing this file.
PACKAGES=()
for pyproject in python/*/pyproject.toml; do
    PACKAGES+=("$(basename "$(dirname "$pyproject")")")
done

# dcpy-connectors and dcpy-product-metadata have a real, currently-undeclared runtime
# dependency on dcpy-lifecycle (dcpy/connectors/ingest_datastore.py,
# dcpy/connectors/edm/open_data_nyc.py, dcpy/connectors/edm/publishing.py, and
# dcpy/product_metadata/writers/oti_xlsx/xlsx_writer.py all import from dcpy.lifecycle)
# - sync lifecycle alongside them too, or their tests won't even collect. Their own
# test_package_boundary.py is marked xfail to track this.
declare -A EXTRA_SYNC=(
    [connectors]="dcpy-lifecycle"
    [product-metadata]="dcpy-lifecycle"
)

# dcpy-utils/test/test_postgres.py needs geopandas, which dcpy-utils imports in
# production code (dcpy.utils.postgres) but doesn't declare - see the comment in
# .github/workflows/test_helper.yml. Excluded here too until that's resolved.
declare -A EXTRA_PYTEST_ARGS=(
    [utils]="--ignore=python/utils/test/test_postgres.py"
)

FAILED=()

for pkg in "${PACKAGES[@]}"; do
    echo "=== uv sync --package dcpy-$pkg ${EXTRA_SYNC[$pkg]:+--package ${EXTRA_SYNC[$pkg]}} ==="
    uv sync --package "dcpy-$pkg" ${EXTRA_SYNC[$pkg]:+--package "${EXTRA_SYNC[$pkg]}"} || {
        FAILED+=("$pkg (sync)")
        continue
    }

    if [ ! -d "python/$pkg/test" ]; then
        continue
    fi

    echo "=== pytest python/$pkg/test ==="
    # shellcheck disable=SC2086
    uv run --no-sync python3 -m pytest "python/$pkg/test" ${EXTRA_PYTEST_ARGS[$pkg]:-} -v || FAILED+=("$pkg")
done

if [ ${#FAILED[@]} -ne 0 ]; then
    echo
    echo "isolated pytest failed on: ${FAILED[*]}"
    exit 1
fi
