#!/bin/bash
# Run the Melissa geocoding pipeline (DuckDB-based, no Postgres required)

IMAGE="nycplanning/build-geosupport:latest"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

INPUT_FILE="melissa.txt"
OUTPUT_FILE="melissa_geocoded.csv"

echo "Starting Melissa geocoding pipeline..."
echo ""

docker run --rm \
  -v "${REPO_ROOT}:/app" \
  -w /app \
  ${IMAGE} \
  python3 products/melissa/pipeline.py \
    ${OUTPUT_FILE} \
    --input-file ${INPUT_FILE}

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Geocoding complete! Output: ${OUTPUT_FILE}"
else
    echo ""
    echo "✗ Geocoding failed."
fi
