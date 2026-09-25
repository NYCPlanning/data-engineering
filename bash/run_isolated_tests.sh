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

# GDAL/geosupport-backed functionality lives behind each package's own "geo" extra (see
# python/{lifecycle,data,utils}/pyproject.toml) rather than being a hard dependency, so a
# plain `uv sync --package dcpy-X` for these won't install it - but their test suites
# exercise it for real: dcpy.lifecycle.ingest.validate's gpd.GeoDataFrame usage,
# dcpy.data.compare's spatial comparisons, dcpy.utils.test_datastores's geo fixtures, and
# connectors/test/edm/test_recipes.py's direct `from dcpy.library import models` (only
# reachable via dcpy-lifecycle's own "geo" extra, since it's synced in alongside above).
declare -A EXTRA_ARGS=(
    [lifecycle]="--extra geo"
    [data]="--extra geo"
    [utils]="--extra geo"
    [connectors]="--extra geo"
)

FAILED=()

for pkg in "${PACKAGES[@]}"; do
    echo "=== uv sync --package dcpy-$pkg ${EXTRA_SYNC[$pkg]:+--package ${EXTRA_SYNC[$pkg]}} ${EXTRA_ARGS[$pkg]:-} ==="
    # shellcheck disable=SC2086
    uv sync --package "dcpy-$pkg" ${EXTRA_SYNC[$pkg]:+--package "${EXTRA_SYNC[$pkg]}"} ${EXTRA_ARGS[$pkg]:-} || {
        FAILED+=("$pkg (sync)")
        continue
    }

    if [ ! -d "python/$pkg/test" ]; then
        continue
    fi

    echo "=== pytest python/$pkg/test ==="
    uv run --no-sync python3 -m pytest "python/$pkg/test" -v || FAILED+=("$pkg")
done

if [ ${#FAILED[@]} -ne 0 ]; then
    echo
    echo "isolated pytest failed on: ${FAILED[*]}"
    exit 1
fi
