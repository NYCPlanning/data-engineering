#!/bin/bash
# Run Melissa geocoding in streaming mode (safer for large files)

IMAGE="nycplanning/build-geosupport:latest"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

INPUT_FILE="melissa.txt"
OUTPUT_FILE="melissa_geocoded.txt"

echo "Starting streaming geocoder..."
echo "This will process one row at a time and save progress automatically."
echo "Safe to Ctrl+C and restart - it will resume from where it left off."
echo ""

docker run --rm \
  -v "${REPO_ROOT}:/app" \
  -w /app \
  ${IMAGE} \
  python3 experimental/melissa/geocode.py \
    ${INPUT_FILE} \
    ${OUTPUT_FILE}

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Geocoding complete! Output: ${OUTPUT_FILE}"
else
    echo ""
    echo "✗ Geocoding interrupted or failed."
    echo "  Just run this script again - it will resume automatically!"
fi
