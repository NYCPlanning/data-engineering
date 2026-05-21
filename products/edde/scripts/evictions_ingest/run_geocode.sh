#!/bin/bash
# Run DOI Evictions geocoding using Geosupport

set -e

IMAGE="nycplanning/build-geosupport:latest"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EDDE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROJECT_ROOT="$(cd "${EDDE_ROOT}/../.." && pwd)"

# Use first argument as input file, or default to doi_evictions.parquet
INPUT_FILE="${1:-${SCRIPT_DIR}/doi_evictions.parquet}"
OUTPUT_FILE="${2:-${SCRIPT_DIR}/doi_evictions_geocoded.parquet}"

# Get relative paths for docker (relative to /app/products/edde working directory)
# Convert absolute paths to relative from EDDE_ROOT
INPUT_REL="${INPUT_FILE#${EDDE_ROOT}/}"
OUTPUT_REL="${OUTPUT_FILE#${EDDE_ROOT}/}"

echo "================================"
echo "DOI Evictions Geocoding"
echo "================================"
echo "Input:  ${INPUT_FILE}"
echo "Output: ${OUTPUT_FILE}"
echo ""
echo "Mounting ${PROJECT_ROOT} into docker container..."
echo "Working directory: /app/products/edde"
echo ""

docker run --rm \
  -v "${PROJECT_ROOT}:/app" \
  -w /app/products/edde \
  -e PYTHONPATH=/app \
  ${IMAGE} \
  python3 scripts/evictions_ingest/process_eviction_address_for_ingest.py \
    "${INPUT_REL}" \
    "${OUTPUT_REL}"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Geocoding complete! Output: ${OUTPUT_FILE}"
else
    echo ""
    echo "✗ Geocoding failed."
    exit 1
fi
