#!/bin/bash
# End-to-end Melissa pipeline, driven by recipe.yml.
#
# Two environments are involved, because neither has everything this needs:
#   - dcp_load_recipe (planning + loading melissa_input/melissa_corrections/
#     melissa_outsideofnyc from edm-private) needs this repo's dcpy package
#     and AWS credentials -- only available in the host dev environment.
#   - Geocoding needs python-geosupport, which only exists inside the
#     nycplanning/build-geosupport Docker image (not installed in this repo's
#     own .venv).
#
# The DuckDB file dcp_load_recipe produces lives wherever dcpy's own lifecycle
# config puts it (DCPY_LIFECYCLE_DATA_DIR/builds/build/melissa/<version>/...),
# which is why the path is resolved via `dcpy lifecycle builds build path`
# rather than reconstructed here -- there should be exactly one place that
# knows that pathing convention.
#
# Output lands in the same build directory as the DuckDB file itself
# (DCPY_LIFECYCLE_DATA_DIR/builds/build/melissa/<version>/), not in this repo --
# it's a build artifact, keyed to that DuckDB file's tables, same as
# build_metadata.json/source_data_versions.csv already are.
#
# Usage: ./run_e2e.sh [output.csv] [limit]
#   output.csv -- filename only (no directory); written next to the .duckdb file.
#   limit -- only process the first N rows of melissa_input, for sanity-checking
#            throughput/output on a slice before committing to a full run (the
#            real input file is several million rows; a full local run under
#            Docker's amd64 emulation can take many hours).

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
OUTPUT_FILE="${1:-melissa_geocoded.csv}"
LIMIT="${2:-}"
IMAGE="nycplanning/build-geosupport:latest"

cd "$HERE"

echo "Loading direnv environment..."
eval "$(direnv export bash)"

if [ -z "${DCPY_LIFECYCLE_DATA_DIR:-}" ]; then
    echo "Error: DCPY_LIFECYCLE_DATA_DIR is not set (needed to mount the build" >&2
    echo "  directory into Docker). Check your direnv/dotenv setup." >&2
    exit 1
fi
if [ -z "${BUILD_ENGINE_SCHEMA:-}" ]; then
    echo "Error: BUILD_ENGINE_SCHEMA is not set. dcp_load_recipe loads into this" >&2
    echo "  schema; pipeline.py needs the same value to find the loaded tables." >&2
    exit 1
fi

echo "Loading recipe (dataloading)..."
dcp_load_recipe

echo "Resolving DuckDB build path..."
HOST_DB_PATH="$(python3 -m dcpy lifecycle builds build path --duckdb)"
echo "  ${HOST_DB_PATH}"
CONTAINER_DB_PATH="/lifecycle${HOST_DB_PATH#"$DCPY_LIFECYCLE_DATA_DIR"}"
CONTAINER_OUTPUT_PATH="$(dirname "$CONTAINER_DB_PATH")/${OUTPUT_FILE}"
HOST_OUTPUT_PATH="$(dirname "$HOST_DB_PATH")/${OUTPUT_FILE}"

# Built as a single array (never empty -- always at least the base args) so
# the docker invocation below is safe under /bin/bash on macOS, which is
# still bash 3.2 and treats "${arr[@]}" on a zero-length array as unbound
# under `set -u`.
PIPELINE_ARGS=(
  "${CONTAINER_OUTPUT_PATH}"
  --skip-load
  --db-path "${CONTAINER_DB_PATH}"
  --schema "${BUILD_ENGINE_SCHEMA}"
)
if [ -n "$LIMIT" ]; then
    echo "Limiting to first ${LIMIT} rows of melissa_input"
    PIPELINE_ARGS+=(--limit "$LIMIT")
fi

echo "Geocoding + building final output inside ${IMAGE}..."
docker run --rm \
  -v "${REPO_ROOT}:/app" \
  -v "${DCPY_LIFECYCLE_DATA_DIR}:/lifecycle" \
  -w /app/products/melissa \
  "${IMAGE}" \
  python3 pipeline.py "${PIPELINE_ARGS[@]}"

echo ""
echo "Done. Output: ${HOST_OUTPUT_PATH}"
