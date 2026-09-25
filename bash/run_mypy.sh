#!/bin/bash

PARENT_DIR=$(dirname "$(readlink -f "$0")")
cd "$PARENT_DIR/.."

# Mirrors the two mypy steps in .github/workflows/test_helper.yml - keep both in sync.
#
# Runs every target regardless of earlier failures (unlike CI, which stops a step at its
# first failing line) so a local run always shows the full picture in one pass, then exits
# non-zero at the end if anything failed.

STANDARD_TARGETS=(
    apps/dcpy_integration_tests
    products/facilities
    products/template
    products/ceqr/build_scripts
    admin/ops
)

# The real dcpy workspace source (python/*/dcpy). Each package needs
# --namespace-packages --explicit-package-bases here: without them mypy computes
# "utils.foo" instead of "dcpy.utils.foo" for these files and then errors with "Source
# file found twice", since [tool.mypy] mypy_path in pyproject.toml already resolves them
# the other way. Not applied to STANDARD_TARGETS above - doing so globally makes mypy
# treat the repo root as an implicit package base too, which collides with
# products/template/python/ (a same-named but unrelated local dir). Each package is a
# separate invocation because checking them together hits "Duplicate module" on their
# same-named test/conftest.py files.
WORKSPACE_PACKAGES=(
    python/utils
    python/connectors
    python/data
    python/geospatial
    python/geosupport
    python/library
    python/lifecycle
    python/product-metadata
)

FAILED=()

for target in "${STANDARD_TARGETS[@]}"; do
    echo "=== mypy $target ==="
    mypy "$target" || FAILED+=("$target")
done

for target in "${WORKSPACE_PACKAGES[@]}"; do
    echo "=== mypy --namespace-packages --explicit-package-bases $target ==="
    mypy --namespace-packages --explicit-package-bases "$target" || FAILED+=("$target")
done

if [ ${#FAILED[@]} -ne 0 ]; then
    echo
    echo "mypy failed on: ${FAILED[*]}"
    exit 1
fi
